package h3

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/uber/h3-go/v4"
)

func init() {
	latLngToHexSpec := bloblang.NewPluginSpec().
		Description("Returns hex id for given latitude and longitude at desired resolution.").
		Param(bloblang.NewFloat64Param("lat")).
		Param(bloblang.NewFloat64Param("lng")).
		Param(bloblang.NewInt64Param("resolution"))

	err := bloblang.RegisterFunctionV2("h3_lat_lng_to_hex", latLngToHexSpec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
		lat, err := args.GetFloat64("lat")
		if err != nil {
			return nil, err
		}

		lng, err := args.GetFloat64("lng")
		if err != nil {
			return nil, err
		}

		resolution, err := args.GetInt64("resolution")
		if err != nil {
			return nil, err
		}

		return func() (interface{}, error) {
			if resolution < 0 && resolution < 15 {
				return nil, fmt.Errorf("resolution should be between 0 and 15")
			}
			latLng := h3.NewLatLng(lat, lng)
			cell := h3.LatLngToCell(latLng, int(resolution))
			return cell.String(), err
		}, nil
	})
	if err != nil {
		panic(err)
	}

	hexLatLonSpec := bloblang.NewPluginSpec().
		Description("Returns lat,lon for given hex.").
		Param(bloblang.NewStringParam("hex_id"))

	err = bloblang.RegisterFunctionV2("h3_hex_to_geo", hexLatLonSpec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
		hex_id, err := args.GetString("hex_id")
		if err != nil {
			return nil, err
		}
		return func() (interface{}, error) {
			index := h3.IndexFromString(hex_id)
			cell := h3.Cell(index)
			if !cell.IsValid() {
				return nil, fmt.Errorf("failed to parse hex id")
			}
			return map[string]float64{"latitude": cell.LatLng().Lat, "longitude": cell.LatLng().Lng}, nil
		}, nil
	})

	if err != nil {
		panic(err)
	}

	validHexSpec := bloblang.NewPluginSpec().
		Description("Returns true if the given hex id is valid.").
		Param(bloblang.NewStringParam("hex_id"))

	err = bloblang.RegisterFunctionV2("h3_valid_hex", validHexSpec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
		hex_id, err := args.GetString("hex_id")
		if err != nil {
			return nil, err
		}
		return func() (interface{}, error) {
			index := h3.IndexFromString(hex_id)
			cell := h3.Cell(index)
			return cell.IsValid(), nil
		}, nil
	})

	if err != nil {
		panic(err)
	}

	getH3ResolutionSpec := bloblang.NewPluginSpec().
		Description("Returns the resolution of the given hex id.").
		Param(bloblang.NewStringParam("hex_id"))

	err = bloblang.RegisterFunctionV2("h3_get_resolution", getH3ResolutionSpec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
		hex_id, err := args.GetString("hex_id")
		if err != nil {
			return nil, err
		}
		return func() (interface{}, error) {
			index := h3.IndexFromString(hex_id)
			cell := h3.Cell(index)
			if !cell.IsValid() {
				return nil, fmt.Errorf("failed to parse hex id")
			}
			return cell.Resolution(), nil
		}, nil
	})

	if err != nil {
		panic(err)
	}

	getH3ParentIdSpec := bloblang.NewPluginSpec().
		Description("Returns the parent hex id of the given hex id at the given resolution.").
		Param(bloblang.NewStringParam("hex_id")).
		Param(bloblang.NewInt64Param("resolution"))

	err = bloblang.RegisterFunctionV2("h3_hex_parent_id", getH3ParentIdSpec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
		hex_id, err := args.GetString("hex_id")
		if err != nil {
			return nil, err
		}

		resolution, err := args.GetInt64("resolution")
		if err != nil {
			return nil, err
		}

		return func() (interface{}, error) {
			if resolution < 0 && resolution < 15 {
				return nil, fmt.Errorf("resolution should be between 0 and 15")
			}
			index := h3.IndexFromString(hex_id)
			cell := h3.Cell(index)
			if !cell.IsValid() {
				return nil, fmt.Errorf("failed to parse hex id")
			}
			return cell.Parent(int(resolution)).String(), nil
		}, nil
	})

	if err != nil {
		panic(err)
	}

	getH3ParentLatLonSpec := bloblang.NewPluginSpec().
		Description("Returns the parent hex lat,lon of the given hex id at the given resolution.").
		Param(bloblang.NewStringParam("hex_id")).
		Param(bloblang.NewInt64Param("resolution"))

	err = bloblang.RegisterFunctionV2("h3_hex_parent_to_geo", getH3ParentLatLonSpec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
		hex_id, err := args.GetString("hex_id")
		if err != nil {
			return nil, err
		}

		resolution, err := args.GetInt64("resolution")
		if err != nil {
			return nil, err
		}

		return func() (interface{}, error) {
			if resolution < 0 && resolution < 15 {
				return nil, fmt.Errorf("resolution should be between 0 and 15")
			}
			index := h3.IndexFromString(hex_id)
			cell := h3.Cell(index)

			if !cell.IsValid() {
				return nil, fmt.Errorf("failed to parse hex id")
			}
			parent := cell.Parent(int(resolution))
			return map[string]float64{"latitude": parent.LatLng().Lat, "longitude": parent.LatLng().Lng}, nil
		}, nil
	})

	if err != nil {
		panic(err)
	}
}
