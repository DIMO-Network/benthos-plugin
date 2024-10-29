package nameindexer

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/DIMO-Network/nameindexer"
	chindexer "github.com/DIMO-Network/nameindexer/pkg/clickhouse"
	"github.com/DIMO-Network/nameindexer/pkg/clickhouse/migrations"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pressly/goose"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const pluginName = "name_indexer"
const subjectLenth = 40

// Configuration specification for the processor.
var configSpec = service.NewConfigSpec().
	Summary("Create an indexable string from provided Bloblang parameters.").
	Field(service.NewInterpolatedStringField("timestamp").Description("Timestamp for the index")).
	Field(service.NewInterpolatedStringField("primary_filler").Description("Primary filler for the index").Default("MM")).
	Field(service.NewInterpolatedStringField("secondary_filler").Description("Secondary filler for the index").Default("00")).
	Field(service.NewInterpolatedStringField("data_type").Description("Data type for the index").Default("FP/v0.0.1")).
	Field(service.NewObjectField("subject",
		service.NewInterpolatedStringField("address").Description("Ethereum address for the index").Optional(),
		service.NewInterpolatedStringField("token_id").Description("Token Id for the index").Optional(),
		service.NewInterpolatedStringField("imei").Description("IMEI subject for the index").Optional(),
	)).
	Field(service.NewStringField("migration").Default("").Description("DSN connection string for database where migration should be run. If set, the plugin will run a database migration on startup using the provided DNS string."))

func init() {
	if err := service.RegisterProcessor(pluginName, configSpec, ctor); err != nil {
		panic(err)
	}
}

// Processor is a processor that creates an indexable string from the provided parameters.
type Processor struct {
	timestamp       *service.InterpolatedString
	primaryFiller   *service.InterpolatedString
	secondaryFiller *service.InterpolatedString
	dataType        *service.InterpolatedString
	subject         *subjectInterpolatedString
}
type subjectInfo uint8

const (
	typeAddress subjectInfo = iota
	typeTokenID
	typeIMEI
)

type subjectInterpolatedString struct {
	interpolatedString *service.InterpolatedString
	subjectType        subjectInfo
}

// TryIndexSubject evaluates the subject field and returns a nameindexer.Subject.
// The subject field can be either an address or a token_id.
func (s *subjectInterpolatedString) TryIndexSubject(msg *service.Message) (string, error) {
	subjectStr, err := s.interpolatedString.TryString(msg)
	if err != nil {
		return "", fmt.Errorf("failed to evaluate subject: %w", err)
	}
	switch s.subjectType {
	case typeIMEI:
		return EncodeIMEI(subjectStr), nil
	case typeAddress:
		if !common.IsHexAddress(subjectStr) {
			return "", fmt.Errorf("address is not a valid hexadecimal address: %s", subjectStr)
		}
		return nameindexer.EncodeAddress(common.HexToAddress(subjectStr)), nil
	case typeTokenID:
		tokenID, err := strconv.ParseUint(subjectStr, 10, 32)
		if err != nil {
			return "", fmt.Errorf("failed to parse token_id: %w", err)
		}
		tokenID32 := uint32(tokenID)
		return EncodeTokenID(tokenID32), nil
	default:
		return "", fmt.Errorf("unknown subject type")
	}
}

// Constructor for the Processor.
func ctor(conf *service.ParsedConfig, _ *service.Resources) (service.Processor, error) {
	timestamp, err := conf.FieldInterpolatedString("timestamp")
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp field: %w", err)
	}
	primaryFiller, err := conf.FieldInterpolatedString("primary_filler")
	if err != nil {
		return nil, fmt.Errorf("failed to parse primary filler field: %w", err)
	}
	secondaryFiller, err := conf.FieldInterpolatedString("secondary_filler")
	if err != nil {
		return nil, fmt.Errorf("failed to parse secondary filler field: %w", err)
	}
	dataType, err := conf.FieldInterpolatedString("data_type")
	if err != nil {
		return nil, fmt.Errorf("failed to parse data type field: %w", err)
	}

	subject, err := getSubject(conf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse subject field: %w", err)
	}

	migration, err := conf.FieldString("migration")
	if err != nil {
		return nil, fmt.Errorf("failed to parse migration field: %w", err)
	}
	if migration != "" {
		if err := runMigration(migration); err != nil {
			return nil, fmt.Errorf("failed to run migration: %w", err)
		}
	}

	return &Processor{
		timestamp:       timestamp,
		primaryFiller:   primaryFiller,
		secondaryFiller: secondaryFiller,
		dataType:        dataType,
		subject:         subject,
	}, nil
}

// Process creates an indexable string from the provided parameters and adds it to the message metadata.
func (p *Processor) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Evaluate Bloblang expressions using TryString to handle errors
	timestampStr, err := p.timestamp.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate timestamp: %w", err)
	}
	primaryFiller, err := p.primaryFiller.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate primary filler: %w", err)
	}
	secondaryFiller, err := p.secondaryFiller.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate secondary filler: %w", err)
	}
	dataType, err := p.dataType.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate data type: %w", err)
	}
	timestamp, err := time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp format: %w", err)
	}

	idxSubject, err := p.subject.TryIndexSubject(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate subject: %w", err)
	}

	// Create the index
	index := nameindexer.Index{
		Timestamp:       timestamp,
		PrimaryFiller:   primaryFiller,
		SecondaryFiller: secondaryFiller,
		DataType:        dataType,
		Subject:         idxSubject,
	}

	// Encode the index
	encodedIndex, err := nameindexer.EncodeIndex(&index)
	if err != nil {
		return nil, fmt.Errorf("failed to encode index: %w", err)
	}

	// Set the encoded index in the message metadata
	msg.MetaSetMut("index", encodedIndex)
	indexValues, err := chindexer.IndexToSlice(&index)
	if err != nil {
		return nil, fmt.Errorf("failed to convert index to slice: %w", err)
	}
	msg.MetaSetMut("index_values", indexValues)

	return service.MessageBatch{msg}, nil
}

// Close does nothing because our processor doesn't need to clean up resources.
func (*Processor) Close(context.Context) error {
	return nil
}

// getSubject parses the subject field from the configuration.
func getSubject(config *service.ParsedConfig) (*subjectInterpolatedString, error) {
	subConfig := config.Namespace("subject")
	addrSet := subConfig.Contains("address")
	tokenIDSet := subConfig.Contains("token_id")
	imeiSet := subConfig.Contains("imei")

	// check only one is set
	if addrSet && tokenIDSet || addrSet && imeiSet || tokenIDSet && imeiSet {
		return nil, fmt.Errorf("only one of address, token_id or imei must be set as the subject")
	}
	if !addrSet && !tokenIDSet && !imeiSet {
		return nil, fmt.Errorf("either address, token_id or imei must be set as the subject")
	}
	if addrSet {
		interpolatedString, err := subConfig.FieldInterpolatedString("address")
		if err != nil {
			return nil, fmt.Errorf("failed to parse address field: %w", err)
		}
		return &subjectInterpolatedString{
			interpolatedString: interpolatedString,
			subjectType:        typeAddress,
		}, nil
	}
	if tokenIDSet {
		interpolatedString, err := subConfig.FieldInterpolatedString("token_id")
		if err != nil {
			return nil, fmt.Errorf("failed to parse token_id field: %w", err)
		}
		return &subjectInterpolatedString{
			interpolatedString: interpolatedString,
			subjectType:        typeTokenID,
		}, nil
	}
	interpolatedString, err := subConfig.FieldInterpolatedString("imei")
	if err != nil {
		return nil, fmt.Errorf("failed to parse imei field: %w", err)
	}
	return &subjectInterpolatedString{
		interpolatedString: interpolatedString,
		subjectType:        typeIMEI,
	}, nil
}

func runMigration(dsn string) error {
	db, err := goose.OpenDBWithDriver("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("failed to open db: %w", err)
	}
	err = migrations.RunGoose(context.Background(), []string{"up", "-v"}, db)
	if err != nil {
		return fmt.Errorf("failed to run migration: %w", err)
	}
	return nil
}

// EncodeTokenID converts a token ID to a string for legacy subject encoding.
func EncodeTokenID(tokenID uint32) string {
	return fmt.Sprintf("T%0*d", subjectLenth-1, tokenID)
}

// EncodeIMEI converts an IMEI string to a string for legacy subject encoding.
func EncodeIMEI(imei string) string {
	fullIMEI := imei
	if len(fullIMEI) == 14 {
		fullIMEI += calculateCheckDigit(imei)
	}
	return fmt.Sprintf("IMEI%0*s", subjectLenth-4, fullIMEI)
}

// calculateCheckDigit calculates the check digit for an IMEI string. using the Luhn algorithm.
func calculateCheckDigit(imei string) string {
	// convert to a slice of digits
	digits := make([]int, len(imei))
	for i, r := range imei {
		digits[i] = int(r - '0')
	}
	// calculate the check digit
	sum := 0
	for i := 0; i < len(digits); i++ {
		if i%2 == 1 {
			digits[i] *= 2
			if digits[i] > 9 {
				digits[i] -= 9
			}
		}
		sum += digits[i]
	}
	checkDigit := (10 - (sum % 10))
	return strconv.Itoa(checkDigit)
}
