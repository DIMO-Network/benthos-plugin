# Benthos Plugin

This repository contains custom processors for Benthos, a stream processing platform.

## Processors

### check_signature
Validates the signature of a message using Ethereum's ecrecover function.

### dimovss
Converts a Status message from a DIMO device into a list of values for insertion into clickhouse.

### name_indexer
Creates an indexable string from provided Bloblang parameters.

### ruptela_parser
Parses Ruptela hex packet data and converts it to JSON format for further processing in the pipeline.

#### Configuration
```yaml
- type: ruptela_parser
  ruptela_parser:
    validate_crc: true           # Whether to validate CRC checksums
    validate_length: true        # Whether to validate packet length
    skip_validation: false       # Skip all validation checks
    max_packet_size: 2048        # Maximum allowed packet size in bytes
    max_records: 100            # Maximum number of records per packet
    max_io_elements: 1000       # Maximum number of IO elements per record
    enable_debug: false         # Enable debug logging
```

#### Usage Examples

**Basic usage with default settings:**
```yaml
- type: ruptela_parser
  ruptela_parser: {}
```

**Relaxed validation for testing:**
```yaml
- type: ruptela_parser
  ruptela_parser:
    validate_crc: false
    validate_length: false
    skip_validation: true
    max_packet_size: 4096
    max_records: 200
    max_io_elements: 2000
    enable_debug: true
```

#### Input Format
- Single hex string: `"01EB00030EA2BC939936440006632AE87E..."`

#### Output Format
The processor outputs structured JSON containing:
- `length`: Packet length
- `crc`: CRC value
- `imei`: Device IMEI
- `command_id`: Command identifier
- `records_flag`: Records flag
- `num_records`: Number of records
- `records`: Array of parsed records with GPS data, IO elements, etc.

## Building

```bash
go build -o benthos-plugin .
```

## Testing

```bash
go test ./internal/...
```
