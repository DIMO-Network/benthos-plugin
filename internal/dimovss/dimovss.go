package dimovss

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/DIMO-Network/benthos-plugin/internal/service/deviceapi"
	"github.com/DIMO-Network/model-garage/pkg/convert"
	"github.com/DIMO-Network/model-garage/pkg/migrations"
	"github.com/DIMO-Network/model-garage/pkg/nativestatus"
	"github.com/DIMO-Network/model-garage/pkg/vss"
	"github.com/pressly/goose"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/mod/semver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	pluginName         = "vss_vehicle"
	pluginSummary      = "Converts a Status message from a DIMO device into a list of values for insertion into clickhouse."
	grpcFieldName      = "devices_api_grpc_addr"
	grpcFieldDesc      = "The address of the devices API gRPC server."
	migrationFieldName = "init_migration"
)

func init() {
	// Config spec is empty for now as we don't have any dynamic fields.
	grpcField := service.NewStringField(grpcFieldName)
	grpcField.Description(grpcFieldDesc)
	chConfig := service.NewStringField(migrationFieldName)
	chConfig.Default("")
	chConfig.Description("If set, the plugin will run a database migration on startup. using the provided DNS string.")
	configSpec := service.NewConfigSpec()
	configSpec.Summary(pluginSummary)
	configSpec.Field(grpcField)
	configSpec.Field(chConfig)

	err := service.RegisterProcessor(pluginName, configSpec, ctor)
	if err != nil {
		panic(err)
	}
}

func ctor(cfg *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	grpcAddr, err := cfg.FieldString(grpcFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get grpc address: %w", err)
	}

	dsn, err := cfg.FieldString(migrationFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to get dsn: %w", err)
	}
	if dsn != "" {
		err = runMigration(dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to run migration: %w", err)
		}
	}

	return newVSSProcessor(mgr.Logger(), grpcAddr)
}

type vssProcessor struct {
	logger      *service.Logger
	tokenGetter nativestatus.TokenIDGetter
}

func newVSSProcessor(lgr *service.Logger, devicesAPIGRPCAddr string) (*vssProcessor, error) {
	devicesConn, err := grpc.NewClient(devicesAPIGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial devices api: %w", err)
	}
	deviceAPI := deviceapi.NewService(devicesConn)
	limitedDeviceAPI := NewLimitedTokenGetter(deviceAPI, lgr)
	return &vssProcessor{
		logger:      lgr,
		tokenGetter: limitedDeviceAPI,
	}, nil
}

func (v *vssProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	// Get the JSON message and convert it to a DIMO status.
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("failed to extract message bytes: %w", err)
	}
	schemaVersion := nativestatus.GetSchemaVersion(msgBytes)
	if semver.Compare(nativestatus.StatusV1Converted, schemaVersion) == 0 {
		// ignore v1.1 messages
		return nil, nil
	}
	var partialErr *service.Message
	var retMsgs service.MessageBatch
	signals, err := nativestatus.SignalsFromPayload(ctx, v.tokenGetter, msgBytes)
	if err != nil {
		if errors.As(err, &deviceapi.NotFoundError{}) {
			// If we do not have an Token for this device we want to drop the message. But we don't want to log an error.
			v.logger.Trace(fmt.Sprintf("dropping message: %v", err))
			return nil, nil
		}

		convertErr := convert.ConversionError{}
		if !errors.As(err, &convertErr) {
			return nil, fmt.Errorf("failed to convert signals: %w", err)
		}
		// if we have a conversion error we will add a error message with metadata to the batch.
		// but still return the signals that we could decode.
		partialErr = msg.Copy()
		partialErr.SetError(err)
		data, err := json.Marshal(convertErr)
		if err == nil {
			partialErr.SetBytes(data)
		} else {
			partialErr.SetBytes(nil)
		}
		retMsgs = append(retMsgs, partialErr)
		signals = convertErr.DecodedSignals
	}

	for i := range signals {
		sigVals := vss.SignalToSlice(signals[i])
		msgCpy := msg.Copy()
		msgCpy.SetStructured(sigVals)
		retMsgs = append(retMsgs, msgCpy)
	}

	return retMsgs, nil
}

// Close does nothing because our processor doesn't need to clean up resources.
func (*vssProcessor) Close(context.Context) error {
	return nil
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
