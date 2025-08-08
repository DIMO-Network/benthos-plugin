package ruptela_parser

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ParseError represents an error encountered during parsing.
type ParseError struct {
	Message string // Description of the error
	Offset  int    // Position in the input data where the error occurred
	Data    []byte // The data being parsed when the error occurred
}

// Error implements the error interface for ParseError.
func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at offset %d: %s", e.Offset, e.Message)
}

// ValidationError represents an error encountered during validation.
type ValidationError struct {
	Field   string      // The name of the field that failed validation
	Value   interface{} // The invalid value
	Message string      // Description of the validation error
}

// Error implements the error interface for ValidationError.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for %s (value: %v): %s", e.Field, e.Value, e.Message)
}

// RuptelaPacket represents the top-level packet structure
type RuptelaPacket struct {
	Length      uint16
	CRC         uint16
	IMEI        uint64
	CommandID   uint8
	RecordsFlag uint8
	NumRecords  uint8
	Records     []RuptelaRecord
}

type RuptelaRecord struct {
	Timestamp          time.Time
	TimestampExtension uint8
	RecordExtension    *uint8 // only for cmd 68
	Priority           uint8
	Longitude          float64
	Latitude           float64
	Altitude           float64
	Angle              float64
	Satellites         uint8
	Speed              uint16
	HDOP               float64
	EventIO            uint16
	IOElements         []IOElement
}

type IOElement struct {
	Size  int // 1, 2, 4, 8
	ID    uint16
	Value string // Hex string representation of the value
}

// CRC16-CCITT lookup table for faster calculation
var crc16Table = [256]uint16{
	0x0000, 0x1189, 0x2312, 0x329B, 0x4624, 0x57AD, 0x6536, 0x74BF,
	0x8C48, 0x9DC1, 0xAF5A, 0xBED3, 0xCA6C, 0xDBE5, 0xE97E, 0xF8F7,
	0x1081, 0x0108, 0x3393, 0x221A, 0x56A5, 0x472C, 0x75B7, 0x643E,
	0x9CC9, 0x8D40, 0xBFDB, 0xAE52, 0xDAED, 0xCB64, 0xF9FF, 0xE876,
	0x2102, 0x308B, 0x0210, 0x1399, 0x6726, 0x76AF, 0x4434, 0x55BD,
	0xAD4A, 0xBCC3, 0x8E58, 0x9FD1, 0xEB6E, 0xFAE7, 0xC87C, 0xD9F5,
	0x3183, 0x200A, 0x1291, 0x0318, 0x77A7, 0x662E, 0x54B5, 0x453C,
	0xBDCB, 0xAC42, 0x9ED9, 0x8F50, 0xFBEF, 0xEA66, 0xD8FD, 0xC974,
	0x4204, 0x538D, 0x6116, 0x709F, 0x0420, 0x15A9, 0x2732, 0x36BB,
	0xCE4C, 0xDFC5, 0xED5E, 0xFCD7, 0x8868, 0x99E1, 0xAB7A, 0xBAF3,
	0x5285, 0x430C, 0x7197, 0x601E, 0x14A1, 0x0528, 0x37B3, 0x263A,
	0xDECD, 0xCF44, 0xFDDF, 0xEC56, 0x98E9, 0x8960, 0xBBFB, 0xAA72,
	0x6306, 0x728F, 0x4014, 0x519D, 0x2522, 0x34AB, 0x0630, 0x17B9,
	0xEF4E, 0xFEC7, 0xCC5C, 0xDDD5, 0xA96A, 0xB8E3, 0x8A78, 0x9BF1,
	0x7387, 0x620E, 0x5095, 0x411C, 0x35A3, 0x242A, 0x16B1, 0x0738,
	0xFFCF, 0xEE46, 0xDCDD, 0xCD54, 0xB9EB, 0xA862, 0x9AF9, 0x8B70,
	0x8408, 0x9581, 0xA71A, 0xB693, 0xC22C, 0xD3A5, 0xE13E, 0xF0B7,
	0x0840, 0x19C9, 0x2B52, 0x3ADB, 0x4E64, 0x5FED, 0x6D76, 0x7CFF,
	0x9489, 0x8500, 0xB79B, 0xA612, 0xD2AD, 0xC324, 0xF1BF, 0xE036,
	0x18C1, 0x0948, 0x3BD3, 0x2A5A, 0x5EE5, 0x4F6C, 0x7DF7, 0x6C7E,
	0xA50A, 0xB483, 0x8618, 0x9791, 0xE32E, 0xF2A7, 0xC03C, 0xD1B5,
	0x2942, 0x38CB, 0x0A50, 0x1BD9, 0x6F66, 0x7EEF, 0x4C74, 0x5DFD,
	0xB58B, 0xA402, 0x9699, 0x8710, 0xF3AF, 0xE226, 0xD0BD, 0xC134,
	0x39C3, 0x284A, 0x1AD1, 0x0B58, 0x7FE7, 0x6E6E, 0x5CF5, 0x4D7C,
	0xC60C, 0xD785, 0xE51E, 0xF497, 0x8028, 0x91A1, 0xA33A, 0xB2B3,
	0x4A44, 0x5BCD, 0x6956, 0x78DF, 0x0C60, 0x1DE9, 0x2F72, 0x3EFB,
	0xD68D, 0xC704, 0xF59F, 0xE416, 0x90A9, 0x8120, 0xB3BB, 0xA232,
	0x5AC5, 0x4B4C, 0x79D7, 0x685E, 0x1CE1, 0x0D68, 0x3FF3, 0x2E7A,
	0xE70E, 0xF687, 0xC41C, 0xD595, 0xA12A, 0xB0A3, 0x8238, 0x93B1,
	0x6B46, 0x7ACF, 0x4854, 0x59DD, 0x2D62, 0x3CEB, 0x0E70, 0x1FF9,
	0xF78F, 0xE606, 0xD49D, 0xC514, 0xB1AB, 0xA022, 0x92B9, 0x8330,
	0x7BC7, 0x6A4E, 0x58D5, 0x495C, 0x3DE3, 0x2C6A, 0x1EF1, 0x0F78,
}

// CRC16CCITT calculates the CRC16-CCITT (0x8408) for the given data using lookup table
func CRC16CCITT(data []byte) uint16 {
	var crc uint16 = 0
	for _, b := range data {
		crc = (crc >> 8) ^ crc16Table[(crc^uint16(b))&0xFF]
	}
	return crc
}

// Fix IMEI decoding: BCD to uint64
func decodeIMEI(bcd []byte) uint64 {
	var imei uint64
	for i := 0; i < 8; i++ {
		high := (bcd[i] >> 4) & 0xF
		low := bcd[i] & 0xF
		if i == 0 && high == 0 {
			imei = imei*10 + uint64(low)
		} else {
			imei = imei*10 + uint64(high)
			imei = imei*10 + uint64(low)
		}
	}
	return imei
}

// ParserOptions configures parsing behavior
type ParserOptions struct {
	ValidateCRC    bool
	ValidateLength bool
	SkipValidation bool
	MaxPacketSize  int
	MaxRecords     int
	MaxIOElements  int
	EnableDebug    bool
}

// DefaultParserOptions returns sensible defaults
func DefaultParserOptions() *ParserOptions {
	return &ParserOptions{
		ValidateCRC:    true,
		ValidateLength: true,
		MaxPacketSize:  2048,
		MaxRecords:     100,
		MaxIOElements:  1000,
		EnableDebug:    false,
		SkipValidation: false,
	}
}

// ParseRuptelaPacket parses a hex string into a RuptelaPacket struct
// Input must be sanitized: no spaces, uppercase or lowercase hex is fine (hex.DecodeString is case-insensitive)
func ParseRuptelaPacket(hexStr string) (*RuptelaPacket, error) {
	return ParseRuptelaPacketWithOptions(hexStr, nil)
}

// ParseRuptelaPacketWithOptions parses with custom options
func ParseRuptelaPacketWithOptions(hexStr string, opts *ParserOptions) (*RuptelaPacket, error) {
	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Sprintf("panic in ParseRuptelaPacketWithOptions: %v", r))
		}
	}()

	if opts == nil {
		opts = DefaultParserOptions()
	}

	hexStr = strings.ReplaceAll(hexStr, " ", "")
	// Removed ToUpper for performance; hex.DecodeString is case-insensitive
	if len(hexStr)%2 != 0 {
		return nil, &ParseError{
			Message: "input hex string must have even length",
			Offset:  0,
			Data:    []byte(hexStr),
		}
	}
	data, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, &ParseError{
			Message: fmt.Sprintf("invalid hex string: %v", err),
			Offset:  0,
			Data:    []byte(hexStr),
		}
	}

	// Always check minimum packet size to prevent panics
	if len(data) < 13 { // 2 bytes len, 8 bytes IMEI, 1 byte cmd, 2 bytes CRC min
		return nil, &ParseError{
			Message: "packet too short",
			Offset:  0,
			Data:    data,
		}
	}

	if !opts.SkipValidation {
		if opts.MaxPacketSize > 0 && len(data) > opts.MaxPacketSize {
			return nil, &ValidationError{
				Field:   "packet_size",
				Value:   len(data),
				Message: fmt.Sprintf("packet too large: %d bytes (max: %d)", len(data), opts.MaxPacketSize),
			}
		}
	}

	idx := 0
	pkt := &RuptelaPacket{}

	// Helper function to safely read bytes
	readBytes := func(n int) ([]byte, error) {
		if idx+n > len(data) {
			return nil, &ParseError{
				Message: fmt.Sprintf("insufficient data: need %d bytes, have %d", n, len(data)-idx),
				Offset:  idx,
				Data:    data,
			}
		}
		result := data[idx : idx+n]
		idx += n
		return result, nil
	}

	// Length (2 bytes)
	lengthBytes, err := readBytes(2)
	if err != nil {
		return nil, &ParseError{
			Message: fmt.Sprintf("failed to read length: %v", err),
			Offset:  idx,
			Data:    data,
		}
	}
	pkt.Length = binary.BigEndian.Uint16(lengthBytes)

	if !opts.SkipValidation && opts.ValidateLength && int(pkt.Length) != len(data)-4 { // minus 2 bytes len, 2 bytes CRC
		return nil, &ValidationError{
			Field:   "packet_length",
			Value:   pkt.Length,
			Message: fmt.Sprintf("Invalid packet. Actual packet data length (%d B) is different from the one specified in the packet (%d B)", len(data)-4, pkt.Length),
		}
	}

	// CRC (last 2 bytes)
	if len(data) < 2 {
		return nil, &ParseError{
			Message: "insufficient data for CRC",
			Offset:  0,
			Data:    data,
		}
	}
	pkt.CRC = binary.BigEndian.Uint16(data[len(data)-2:])

	if !opts.SkipValidation && opts.ValidateCRC {
		if int(pkt.Length)+2 > len(data) {
			return nil, &ParseError{
				Message: fmt.Sprintf("invalid packet length: %d", pkt.Length),
				Offset:  0,
				Data:    data,
			}
		}
		crcData := data[2 : 2+pkt.Length]
		calcCRC := CRC16CCITT(crcData)
		if pkt.CRC != calcCRC {
			return nil, &ValidationError{
				Field:   "crc",
				Value:   fmt.Sprintf("%04X", pkt.CRC),
				Message: fmt.Sprintf("CRC check failed. Packet CRC: %04X, Calculated CRC: %04X", pkt.CRC, calcCRC),
			}
		}
	}

	// IMEI (8 bytes BCD, but JS treats as uint64)
	imeiBytes, err := readBytes(8)
	if err != nil {
		return nil, &ParseError{
			Message: fmt.Sprintf("failed to read IMEI: %v", err),
			Offset:  idx,
			Data:    data,
		}
	}
	pkt.IMEI = decodeIMEI(imeiBytes)

	// Command ID (1 byte)
	cmdBytes, err := readBytes(1)
	if err != nil {
		return nil, &ParseError{
			Message: fmt.Sprintf("failed to read command ID: %v", err),
			Offset:  idx,
			Data:    data,
		}
	}
	pkt.CommandID = cmdBytes[0]

	if pkt.CommandID == 68 || pkt.CommandID == 1 {
		// Records flag (1 byte)
		flagBytes, err := readBytes(1)
		if err != nil {
			return nil, &ParseError{
				Message: fmt.Sprintf("failed to read records flag: %v", err),
				Offset:  idx,
				Data:    data,
			}
		}
		pkt.RecordsFlag = flagBytes[0]

		// Number of records (1 byte)
		numRecBytes, err := readBytes(1)
		if err != nil {
			return nil, &ParseError{
				Message: fmt.Sprintf("failed to read number of records: %v", err),
				Offset:  idx,
				Data:    data,
			}
		}
		pkt.NumRecords = numRecBytes[0]

		if !opts.SkipValidation && opts.MaxRecords > 0 && int(pkt.NumRecords) > opts.MaxRecords {
			return nil, &ValidationError{
				Field:   "num_records",
				Value:   pkt.NumRecords,
				Message: fmt.Sprintf("too many records: %d (max: %d)", pkt.NumRecords, opts.MaxRecords),
			}
		}

		pkt.Records = make([]RuptelaRecord, 0, pkt.NumRecords)
		for rec := 0; rec < int(pkt.NumRecords); rec++ {
			r := RuptelaRecord{}

			// Timestamp (4 bytes, big endian)
			tsBytes, err := readBytes(4)
			if err != nil {
				return nil, &ParseError{
					Message: fmt.Sprintf("failed to read timestamp for record %d: %v", rec, err),
					Offset:  idx,
					Data:    data,
				}
			}
			ts := binary.BigEndian.Uint32(tsBytes)
			r.Timestamp = time.Unix(int64(ts), 0).UTC()

			// Timestamp extension (1 byte)
			tsExtBytes, err := readBytes(1)
			if err != nil {
				return nil, &ParseError{
					Message: fmt.Sprintf("failed to read timestamp extension for record %d: %v", rec, err),
					Offset:  idx,
					Data:    data,
				}
			}
			r.TimestampExtension = tsExtBytes[0]

			// Record extension (1 byte, only for cmd 68)
			if pkt.CommandID == 68 {
				rextBytes, err := readBytes(1)
				if err != nil {
					return nil, &ParseError{
						Message: fmt.Sprintf("failed to read record extension for record %d: %v", rec, err),
						Offset:  idx,
						Data:    data,
					}
				}
				rext := rextBytes[0]
				r.RecordExtension = &rext
			}

			// Priority (1 byte)
			priorityBytes, err := readBytes(1)
			if err != nil {
				return nil, &ParseError{
					Message: fmt.Sprintf("failed to read priority for record %d: %v", rec, err),
					Offset:  idx,
					Data:    data,
				}
			}
			r.Priority = priorityBytes[0]

			// Longitude (4 bytes, signed int32, 1e-7 deg)
			lonBytes, err := readBytes(4)
			if err != nil {
				return nil, &ParseError{
					Message: fmt.Sprintf("failed to read longitude for record %d: %v", rec, err),
					Offset:  idx,
					Data:    data,
				}
			}
			lon := int32(binary.BigEndian.Uint32(lonBytes))
			r.Longitude = float64(lon) / 1e7

			// Latitude (4 bytes, signed int32, 1e-7 deg)
			latBytes, err := readBytes(4)
			if err != nil {
				return nil, &ParseError{
					Message: fmt.Sprintf("failed to read latitude for record %d: %v", rec, err),
					Offset:  idx,
					Data:    data,
				}
			}
			lat := int32(binary.BigEndian.Uint32(latBytes))
			r.Latitude = float64(lat) / 1e7

			// Altitude (2 bytes, signed int16, /10)
			altBytes, err := readBytes(2)
			if err != nil {
				return nil, &ParseError{
					Message: fmt.Sprintf("failed to read altitude for record %d: %v", rec, err),
					Offset:  idx,
					Data:    data,
				}
			}
			alt := int16(binary.BigEndian.Uint16(altBytes))
			r.Altitude = float64(alt) / 10.0

			// Angle (2 bytes, /100)
			angleBytes, err := readBytes(2)
			if err != nil {
				return nil, &ParseError{
					Message: fmt.Sprintf("failed to read angle for record %d: %v", rec, err),
					Offset:  idx,
					Data:    data,
				}
			}
			angle := binary.BigEndian.Uint16(angleBytes)
			r.Angle = float64(angle) / 100.0

			// Satellites (1 byte)
			satBytes, err := readBytes(1)
			if err != nil {
				return nil, &ParseError{
					Message: fmt.Sprintf("failed to read satellites for record %d: %v", rec, err),
					Offset:  idx,
					Data:    data,
				}
			}
			r.Satellites = satBytes[0]

			// Speed (2 bytes)
			speedBytes, err := readBytes(2)
			if err != nil {
				return nil, &ParseError{
					Message: fmt.Sprintf("failed to read speed for record %d: %v", rec, err),
					Offset:  idx,
					Data:    data,
				}
			}
			r.Speed = binary.BigEndian.Uint16(speedBytes)

			// HDOP (1 byte, /10)
			hdopBytes, err := readBytes(1)
			if err != nil {
				return nil, &ParseError{
					Message: fmt.Sprintf("failed to read HDOP for record %d: %v", rec, err),
					Offset:  idx,
					Data:    data,
				}
			}
			r.HDOP = float64(hdopBytes[0]) / 10.0

			// Event IO (2 bytes for cmd 68, 1 byte for cmd 1)
			if pkt.CommandID == 68 {
				eventIOBytes, err := readBytes(2)
				if err != nil {
					return nil, &ParseError{
						Message: fmt.Sprintf("failed to read event IO for record %d: %v", rec, err),
						Offset:  idx,
						Data:    data,
					}
				}
				r.EventIO = binary.BigEndian.Uint16(eventIOBytes)
			} else {
				eventIOBytes, err := readBytes(1)
				if err != nil {
					return nil, &ParseError{
						Message: fmt.Sprintf("failed to read event IO for record %d: %v", rec, err),
						Offset:  idx,
						Data:    data,
					}
				}
				r.EventIO = uint16(eventIOBytes[0])
			}

			// Preallocate IO elements slice with estimated capacity
			r.IOElements = make([]IOElement, 0, 50) // Estimate 100 IO elements per record

			// IO Elements (1, 2, 4, 8 bytes)
			for _, size := range []int{1, 2, 4, 8} {
				ioCountBytes, err := readBytes(1)
				if err != nil {
					return nil, &ParseError{
						Message: fmt.Sprintf("failed to read IO count for record %d, size %d: %v", rec, size, err),
						Offset:  idx,
						Data:    data,
					}
				}
				ioCount := int(ioCountBytes[0])

				if !opts.SkipValidation && opts.MaxIOElements > 0 && len(r.IOElements)+ioCount > opts.MaxIOElements {
					return nil, &ValidationError{
						Field:   "io_elements",
						Value:   len(r.IOElements) + ioCount,
						Message: fmt.Sprintf("too many IO elements: %d (max: %d)", len(r.IOElements)+ioCount, opts.MaxIOElements),
					}
				}

				for j := 0; j < ioCount; j++ {
					var ioID uint16
					if pkt.CommandID == 68 {
						ioIDBytes, err := readBytes(2)
						if err != nil {
							return nil, &ParseError{
								Message: fmt.Sprintf("failed to read IO ID for record %d, size %d, element %d: %v", rec, size, j, err),
								Offset:  idx,
								Data:    data,
							}
						}
						ioID = binary.BigEndian.Uint16(ioIDBytes)
					} else {
						ioIDBytes, err := readBytes(1)
						if err != nil {
							return nil, &ParseError{
								Message: fmt.Sprintf("failed to read IO ID for record %d, size %d, element %d: %v", rec, size, j, err),
								Offset:  idx,
								Data:    data,
							}
						}
						ioID = uint16(ioIDBytes[0])
					}

					var ioValBytes []byte
					for b := 0; b < size; b++ {
						valBytes, err := readBytes(1)
						if err != nil {
							return nil, &ParseError{
								Message: fmt.Sprintf("failed to read IO value byte %d for record %d, size %d, element %d: %v", b, rec, size, j, err),
								Offset:  idx,
								Data:    data,
							}
						}
						ioValBytes = append(ioValBytes, valBytes[0])
					}
					// Convert to hex string, ensuring proper byte order (big endian)
					hexValue := strings.ToUpper(hex.EncodeToString(ioValBytes))
					el := IOElement{Size: size, ID: ioID, Value: hexValue}
					r.IOElements = append(r.IOElements, el)
				}
			}
			pkt.Records = append(pkt.Records, r)
		}
	}
	return pkt, nil
}

// ToJSON converts the RuptelaPacket to JSON format
func (pkt *RuptelaPacket) ToJSON() ([]byte, error) {
	return json.MarshalIndent(pkt, "", "  ")
}

// ToJSONCompact converts the RuptelaPacket to compact JSON format
func (pkt *RuptelaPacket) ToJSONCompact() ([]byte, error) {
	return json.Marshal(pkt)
}
