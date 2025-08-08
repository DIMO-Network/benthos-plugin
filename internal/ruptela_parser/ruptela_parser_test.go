package ruptela_parser

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestRuptelaParserProcessor(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectErr      bool
		errorMsg       string
		expectFields   []string
		validateCRC    bool
		maxPacketSize  int
		maxIOElements  int
		skipValidation bool
	}{
		{
			name:           "Empty input",
			input:          "",
			expectErr:      true,
			errorMsg:       "empty input data",
			validateCRC:    false,
			maxPacketSize:  2048,
			maxIOElements:  1000,
			skipValidation: false,
		},
		{
			name:           "Invalid hex string",
			input:          "invalid_hex",
			expectErr:      true,
			errorMsg:       "input hex string must have even length",
			validateCRC:    false,
			maxPacketSize:  2048,
			maxIOElements:  1000,
			skipValidation: false,
		},
		{
			name:           "Odd length hex string",
			input:          "12345",
			expectErr:      true,
			errorMsg:       "input hex string must have even length",
			validateCRC:    false,
			maxPacketSize:  2048,
			maxIOElements:  1000,
			skipValidation: false,
		},
		{
			name:           "Invalid hex characters",
			input:          "GGHHIIJJ",
			expectErr:      true,
			errorMsg:       "invalid hex string",
			validateCRC:    false,
			maxPacketSize:  2048,
			maxIOElements:  1000,
			skipValidation: false,
		},
		{
			name:           "Too short packet",
			input:          "0102030405",
			expectErr:      true,
			errorMsg:       "packet too short",
			validateCRC:    false,
			maxPacketSize:  2048,
			maxIOElements:  1000,
			skipValidation: false,
		},
		{
			name:           "Invalid CRC with validation enabled",
			input:          "000D00030EA2BC939936440001FFFF",
			expectErr:      true,
			errorMsg:       "CRC check failed",
			validateCRC:    true,
			maxPacketSize:  2048,
			maxIOElements:  1000,
			skipValidation: false,
		},
		{
			name:           "Packet too large",
			input:          "000D00030EA2BC9399364400010000",
			expectErr:      true,
			errorMsg:       "packet too large",
			validateCRC:    false,
			maxPacketSize:  10, // Set very small to trigger the error
			maxIOElements:  1000,
			skipValidation: false,
		},
		{
			name:           "Invalid packet with validation disabled - should return error not panic",
			input:          "000D00030EA2BC939936440001", // Incomplete packet that would cause panic
			expectErr:      true,
			errorMsg:       "insufficient data",
			validateCRC:    false,
			maxPacketSize:  2048,
			maxIOElements:  1000,
			skipValidation: true,
		},
		{
			name:           "Skip validation bypasses size limit",
			input:          "000D00030EA2BC9399364400010000", // Valid packet structure
			expectErr:      true,
			errorMsg:       "insufficient data",
			validateCRC:    false,
			maxPacketSize:  10, // Set very small but should be ignored with SkipValidation
			maxIOElements:  1000,
			skipValidation: true,
		},
		{
			name:           "Custom error types - validation error",
			input:          "000D00030EA2BC939936440001FFFF", // Valid structure but with invalid CRC
			expectErr:      true,
			errorMsg:       "validation error for crc",
			validateCRC:    true,
			maxPacketSize:  2048,
			maxIOElements:  1000,
			skipValidation: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create processor with configuration based on test case
			processor := &ruptelaProcessor{
				opts: &ParserOptions{
					ValidateCRC:    tt.validateCRC,
					ValidateLength: false,
					SkipValidation: tt.skipValidation,
					MaxPacketSize:  tt.maxPacketSize,
					MaxRecords:     100,
					MaxIOElements:  tt.maxIOElements,
					EnableDebug:    false,
				},
				logger: nil, // Safe to pass nil for testing
			}

			msg := service.NewMessage([]byte(tt.input))
			batch, err := processor.Process(context.Background(), msg)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errorMsg != "" {
					require.Contains(t, err.Error(), tt.errorMsg)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, batch)
			require.Len(t, batch, 1)

			// Get the output message
			outputBytes, err := batch[0].AsBytes()
			require.NoError(t, err)

			// Verify that the output is valid JSON
			var parsedOutput interface{}
			err = json.Unmarshal(outputBytes, &parsedOutput)
			require.NoError(t, err, "Output should be valid JSON")

			// Verify that the output has the expected structure
			outputMap, ok := parsedOutput.(map[string]interface{})
			require.True(t, ok, "Output should be a JSON object")

			// Check for expected fields
			for _, field := range tt.expectFields {
				_, hasField := outputMap[field]
				require.True(t, hasField, "Expected field '%s' in output", field)
			}
		})
	}
}
