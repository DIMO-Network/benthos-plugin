package ruptela_parser

import (
	"context"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const pluginName = "ruptela_parser"

func init() {
	configSpec := service.NewConfigSpec().
		Summary("Parses Ruptela hex packet data and converts it to JSON format.").
		Description("This processor takes hex string input containing Ruptela packet data and parses it into structured JSON format for further processing in the pipeline.").
		Field(service.NewBoolField("validate_crc").
			Description("Whether to validate CRC checksums.").
			Default(true)).
		Field(service.NewBoolField("validate_length").
			Description("Whether to validate packet length.").
			Default(true)).
		Field(service.NewBoolField("skip_validation").
			Description("Skip all validation checks.").
			Default(false)).
		Field(service.NewIntField("max_packet_size").
			Description("Maximum allowed packet size in bytes.").
			Default(2048)).
		Field(service.NewIntField("max_records").
			Description("Maximum number of records per packet.").
			Default(100)).
		Field(service.NewIntField("max_io_elements").
			Description("Maximum number of IO elements per record.").
			Default(1000)).
		Field(service.NewBoolField("enable_debug").
			Description("Enable debug logging.").
			Default(false))

	err := service.RegisterProcessor(pluginName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

type ruptelaProcessor struct {
	opts   *ParserOptions
	logger *service.Logger
}

func ctor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	validateCRC, err := conf.FieldBool("validate_crc")
	if err != nil {
		return nil, fmt.Errorf("failed to parse validate_crc: %w", err)
	}

	validateLength, err := conf.FieldBool("validate_length")
	if err != nil {
		return nil, fmt.Errorf("failed to parse validate_length: %w", err)
	}

	skipValidation, err := conf.FieldBool("skip_validation")
	if err != nil {
		return nil, fmt.Errorf("failed to parse skip_validation: %w", err)
	}

	maxPacketSize, err := conf.FieldInt("max_packet_size")
	if err != nil {
		return nil, fmt.Errorf("failed to parse max_packet_size: %w", err)
	}

	maxRecords, err := conf.FieldInt("max_records")
	if err != nil {
		return nil, fmt.Errorf("failed to parse max_records: %w", err)
	}

	maxIOElements, err := conf.FieldInt("max_io_elements")
	if err != nil {
		return nil, fmt.Errorf("failed to parse max_io_elements: %w", err)
	}

	enableDebug, err := conf.FieldBool("enable_debug")
	if err != nil {
		return nil, fmt.Errorf("failed to parse enable_debug: %w", err)
	}

	opts := &ParserOptions{
		ValidateCRC:    validateCRC,
		ValidateLength: validateLength,
		MaxPacketSize:  int(maxPacketSize),
		MaxRecords:     int(maxRecords),
		MaxIOElements:  int(maxIOElements),
		EnableDebug:    enableDebug,
		SkipValidation: skipValidation,
	}

	return &ruptelaProcessor{
		opts:   opts,
		logger: mgr.Logger(),
	}, nil
}

func (r *ruptelaProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Sprintf("panic in ruptela processor: %v", r))
		}
	}()

	// Get the input data from message body
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get message bytes: %w", err)
	}
	inputData := strings.TrimSpace(string(msgBytes))
	if inputData == "" {
		return nil, fmt.Errorf("empty input data")
	}

	// Parse the hex string using the configured options
	packet, err := ParseRuptelaPacketWithOptions(inputData, r.opts)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ruptela packet: %w", err)
	}

	// Convert packet to JSON
	jsonData, err := packet.ToJSONCompact()
	if err != nil {
		return nil, fmt.Errorf("failed to convert packet to JSON: %w", err)
	}

	// Create new message with parsed data
	newMsg := msg.Copy()
	newMsg.SetBytes(jsonData)

	return service.MessageBatch{newMsg}, nil
}

func (r *ruptelaProcessor) Close(ctx context.Context) error {
	return nil
}
