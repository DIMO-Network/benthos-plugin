package h3

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestGetH3HexSuccess(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	exec, err := bloblang.Parse("root = h3_lat_lng_to_hex(lat: this.latitude, lng: this.longitude, resolution: 5)")
	require.NoError(t, err)

	hexId, err := exec.Query(map[string]any{
		"latitude": 40.776676, "longitude": -73.971321,
	})
	require.NoError(t, err)
	require.Equal(t, "852a100bfffffff", hexId)
}

func TestGetH3LatLon(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	exec, err := bloblang.Parse("root = h3_hex_to_geo(hex_id: this.hex)")
	require.NoError(t, err)

	latLng, err := exec.Query(map[string]any{
		"hex": "852a100bfffffff",
	})
	require.NoError(t, err)
	require.Equal(t, map[string]float64{"latitude": 40.85293293570688, "longitude": -73.99191613398101}, latLng)
}

func TestValidH3Hex(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	exec, err := bloblang.Parse("root = h3_valid_hex(hex_id: this.hex)")
	require.NoError(t, err)

	isValid, err := exec.Query(map[string]any{
		"hex": "852a100bfffffff",
	})
	require.NoError(t, err)
	require.Equal(t, true, isValid)
}

func TestInValidH3Hex(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	exec, err := bloblang.Parse("root = h3_valid_hex(hex_id: this.hex)")
	require.NoError(t, err)

	isValid, err := exec.Query(map[string]any{
		"hex": "0",
	})
	require.NoError(t, err)
	require.Equal(t, false, isValid)
}

func TestGetResolutionSuccess(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	exec, err := bloblang.Parse("root = h3_get_resolution(hex_id: this.hex)")
	require.NoError(t, err)

	resolution, err := exec.Query(map[string]any{
		"hex": "852a100bfffffff",
	})
	require.NoError(t, err)
	require.Equal(t, 5, resolution)
}

func TestGetResolutionFail(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	exec, err := bloblang.Parse("root = h3_get_resolution(hex_id: this.hex)")
	require.NoError(t, err)

	resolution, err := exec.Query(map[string]any{
		"hex": "852a100b",
	})
	require.NoError(t, err)
	require.Equal(t, -1, resolution)
}

func TestGetParentAtResolutionSuccess(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	exec, err := bloblang.Parse("root = h3_hex_parent_id(hex_id: this.hex, resolution: 4)")
	require.NoError(t, err)

	resolution, err := exec.Query(map[string]any{
		"hex": "852a100bfffffff",
	})
	require.NoError(t, err)
	require.Equal(t, "842a101ffffffff", resolution)
}

func TestGetParentAtInvalidHexResolutionFail(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	exec, err := bloblang.Parse("root = h3_hex_parent_id(hex_id: this.hex, resolution: 4)")
	require.NoError(t, err)

	_, err = exec.Query(map[string]any{
		"hex": "852a100",
	})
	require.Error(t, err)
}

func TestGetParentAtInvalidResolutionFail(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	exec, err := bloblang.Parse("root = h3_hex_parent_id(hex_id: this.hex, resolution: 200)")
	require.NoError(t, err)

	_, err = exec.Query(map[string]any{
		"hex": "852a100",
	})
	require.Error(t, err)
}

func TestGetParentGeoAtResolutionSuccess(t *testing.T) {
	// It's safe to pass nil in place of a logger for testing purposes
	exec, err := bloblang.Parse("root = h3_hex_parent_to_geo(hex_id: this.hex, resolution: 3)")
	require.NoError(t, err)

	resolution, err := exec.Query(map[string]any{
		"hex": "852a100bfffffff",
	})
	require.NoError(t, err)
	require.Equal(t, map[string]float64{"latitude": 40.85841614742274, "longitude": -73.7819279919521}, resolution)
}
