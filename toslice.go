package mframe

import (
	"net/http"

	"github.com/quantfall/rerror"
	"google.golang.org/grpc/codes"
)

func (d *DataFrame) ToSlice() []map[string]interface{} {
	var result = make([]map[string]interface{}, 0, 1)

	for _, row := range d.Data {
		result = append(result, row)
	}

	return result
}

func (d *DataFrame) SliceOf(field string) ([]interface{}, *rerror.Error) {
	var list []interface{}
	for _, v := range d.Data {
		value, ok := v[field]
		if !ok {
			return []interface{}{}, rerror.ErrorF(http.StatusBadRequest, codes.InvalidArgument, "field '%s' not found in log '%v'", field, v)
		}
		list = append(list, value)
	}

	return list, nil
}

func (d *DataFrame) SliceOfFloat64(field string) ([]float64, *rerror.Error) {
	list, e := d.SliceOf(field)
	if e != nil {
		return []float64{}, e
	}

	var fList = []float64{}
	for _, value := range list {
		v, ok := value.(float64)
		if !ok {
			return []float64{}, rerror.ErrorF(http.StatusBadRequest, codes.InvalidArgument, "'%v' is not type of float64", value)
		}
		fList = append(fList, v)
	}
	return fList, nil
}
