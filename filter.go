package mframe

import (
	"github.com/google/uuid"
	"log"
	"net"
	"regexp"
	"strings"
)

// Operator defines a set of comparison or matching operations that can be applied in conditional logic.
type Operator int

const (
	Equals        Operator = 1
	NotEquals     Operator = 2
	Major         Operator = 3
	Minor         Operator = 4
	MajorEquals   Operator = 5
	MinorEquals   Operator = 6
	InList        Operator = 7
	NotInList     Operator = 8
	RegExp        Operator = 9
	NotRegExp     Operator = 10
	InCIDR        Operator = 11
	NotInCIDR     Operator = 12
	Contains      Operator = 13
	NotContains   Operator = 14
	StartsWith    Operator = 15
	NotStartsWith Operator = 16
	EndsWith      Operator = 17
	NotEndsWith   Operator = 18
)

// Filter applies a filtering operation to the DataFrame based on the operator, key, value, and optional parameters.
// operator specifies the condition (e.g., Equals, NotEquals) to filter data.
// key indicates the column to filter on.
// value represents the target value(s) used for filtering.
// options is an optional map to specify additional filter settings (e.g., case sensitivity).
// Returns a new DataFrame containing the filtered rows.
//
// Available Operators:
// - Equals: Available for numeric, string and bool types.
// - NotEquals: Available for numeric, string and bool types.
// - Major: Available for numeric types.
// - Minor: Available for numeric types.
// - MajorEquals: Available for numeric types.
// - MinorEquals: Available for numeric types.
// - InList: Available for numeric and string types.
// - NotInList: Available for numeric and string types.
// - RegExp: Available for string types.
// - NotRegExp Available for string types.
// - InCIDR Available for string types.
// - NotInCIDR Available for string types.
// - Contains Available for string types.
// - NotContains Available for string types.
// - StartsWith Available for string types.
// - NotStartsWith Available for string types.
// - EndsWith Available for string types.
// - NotEndsWith Available for string types.
func (d *DataFrame) Filter(operator Operator, key KeyName, value any, options map[FilterOption]bool) *DataFrame {
	d.Locker.RLock()
	defer d.Locker.RUnlock()

	var keys = make(map[KeyName]KeyType)

	if ContainsF(string(key), "^") || ContainsF(string(key), "[") || ContainsF(string(key), "(") {
		for dataFrameKey, keyType := range d.Keys {
			if m, e := MatchesRegExpF(string(dataFrameKey), string(key)); e == nil && m {
				keys[dataFrameKey] = keyType
			}
		}
	} else {
		keys[key] = d.Keys[key]
	}

	var results = new(DataFrame)
	results.Init(d.TTL)

	for dataFrameKey, keyType := range keys {
		switch keyType {
		case Numeric:
			switch operator {
			case Equals:
				floatValue, ok := value.(float64)
				if !ok {
					return results
				}
				if ids, ok := d.Numerics[dataFrameKey][floatValue]; ok {
					for id := range ids {
						results.Insert(d.Data[id])
					}
				}
			case NotEquals:
				floatValue, ok := value.(float64)
				if !ok {
					return results
				}
				if keyValues, ok := d.Numerics[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if EqualsF(keyValue, floatValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case Major:
				floatValue, ok := value.(float64)
				if !ok {
					return results
				}
				if keyValues, ok := d.Numerics[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if !MajorThanF(keyValue, floatValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case Minor:
				floatValue, ok := value.(float64)
				if !ok {
					return results
				}
				if keyValues, ok := d.Numerics[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if MajorThanF(keyValue, floatValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case MajorEquals:
				floatValue, ok := value.(float64)
				if !ok {
					return results
				}
				if keyValues, ok := d.Numerics[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if !EqualsF(keyValue, floatValue) && !MajorThanF(keyValue, floatValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case MinorEquals:
				floatValue, ok := value.(float64)
				if !ok {
					return results
				}
				if keyValues, ok := d.Numerics[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if !EqualsF(keyValue, floatValue) && MajorThanF(keyValue, floatValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case InList:
				floatValues, ok := value.([]float64)
				if !ok {
					return results
				}
				if keyValues, ok := d.Numerics[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if !InListF(keyValue, floatValues) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case NotInList:
				floatValues, ok := value.([]float64)
				if !ok {
					return results
				}
				if keyValues, ok := d.Numerics[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if InListF(keyValue, floatValues) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			default:
				log.Printf("incorrect operator '%v' for key '%s' of type '%v'", operator, key, keyType)
			}
		case String:
			switch operator {
			case Equals:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if sensitive, ok := options[CaseSensitive]; ok && !sensitive {
							keyValue = strings.ToLower(keyValue)
							stringValue = strings.ToLower(stringValue)
						}

						if !EqualsF(keyValue, stringValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case NotEquals:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if sensitive, ok := options[CaseSensitive]; ok && !sensitive {
							keyValue = strings.ToLower(keyValue)
							stringValue = strings.ToLower(stringValue)
						}

						if EqualsF(keyValue, stringValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case RegExp:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if m, e := MatchesRegExpF(keyValue, stringValue); e != nil && !m {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case NotRegExp:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if m, e := MatchesRegExpF(keyValue, stringValue); e != nil || m {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case InList:
				stringValues, ok := value.([]string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if sensitive, ok := options[CaseSensitive]; ok && !sensitive {
							keyValue = strings.ToLower(keyValue)

							tmpStringValues := make([]string, 0, len(stringValues))
							for _, v := range stringValues {
								tmpStringValues = append(tmpStringValues, strings.ToLower(v))
							}
							stringValues = tmpStringValues
						}

						if !InListF(keyValue, stringValues) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case NotInList:
				stringValues, ok := value.([]string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if sensitive, ok := options[CaseSensitive]; ok && !sensitive {
							keyValue = strings.ToLower(keyValue)

							tmpStringValues := make([]string, 0, len(stringValues))
							for _, v := range stringValues {
								tmpStringValues = append(tmpStringValues, strings.ToLower(v))
							}
							stringValues = tmpStringValues
						}

						if InListF(keyValue, stringValues) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case InCIDR:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if m, e := InCIDRF(keyValue, stringValue); e != nil || !m {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case NotInCIDR:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if m, e := InCIDRF(keyValue, stringValue); e != nil || m {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case Contains:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if sensitive, ok := options[CaseSensitive]; ok && !sensitive {
							keyValue = strings.ToLower(keyValue)
							stringValue = strings.ToLower(stringValue)
						}

						if !ContainsF(keyValue, stringValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case NotContains:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if sensitive, ok := options[CaseSensitive]; ok && !sensitive {
							keyValue = strings.ToLower(keyValue)
							stringValue = strings.ToLower(stringValue)
						}

						if ContainsF(keyValue, stringValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case StartsWith:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if sensitive, ok := options[CaseSensitive]; ok && !sensitive {
							keyValue = strings.ToLower(keyValue)
							stringValue = strings.ToLower(stringValue)
						}

						if !StartsWithF(keyValue, stringValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case NotStartsWith:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if sensitive, ok := options[CaseSensitive]; ok && !sensitive {
							keyValue = strings.ToLower(keyValue)
							stringValue = strings.ToLower(stringValue)
						}

						if StartsWithF(keyValue, stringValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case EndsWith:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if sensitive, ok := options[CaseSensitive]; ok && !sensitive {
							keyValue = strings.ToLower(keyValue)
							stringValue = strings.ToLower(stringValue)
						}

						if !EndsWithF(keyValue, stringValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			case NotEndsWith:
				stringValue, ok := value.(string)
				if !ok {
					return results
				}
				if keyValues, ok := d.Strings[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if sensitive, ok := options[CaseSensitive]; ok && !sensitive {
							keyValue = strings.ToLower(keyValue)
							stringValue = strings.ToLower(stringValue)
						}

						if EndsWithF(keyValue, stringValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			default:
				log.Printf("incorrect operator '%v' for key '%s' of type '%v'", operator, key, keyType)
			}
		case Boolean:
			boolValue, ok := value.(bool)
			if !ok {
				return results
			}
			switch operator {
			case Equals:
				if ids, ok := d.Booleans[dataFrameKey][boolValue]; ok {
					for id := range ids {
						results.Insert(d.Data[id])
					}
				}
			case NotEquals:
				if keyValues, ok := d.Booleans[dataFrameKey]; ok {
					for keyValue, ids := range keyValues {
						if EqualsF(boolValue, keyValue) {
							continue
						}

						for id := range ids {
							results.Insert(d.Data[id])
						}
					}
				}
			default:
				log.Printf("incorrect operator '%v' for key '%s' of type '%v'", operator, key, keyType)
			}
		}
	}

	return results
}

// FindFirstByKey retrieves the first occurrence of a key within a DataFrame and returns its UUID, key name, and value.
func (d *DataFrame) FindFirstByKey(key KeyName) (uuid.UUID, KeyName, interface{}) {
	d.Locker.RLock()
	defer d.Locker.RUnlock()

	var keys = make(map[KeyName]KeyType)

	if ContainsF(string(key), "^") || ContainsF(string(key), "[") || ContainsF(string(key), "(") {
		for dataFrameKey, keyType := range d.Keys {
			if m, e := MatchesRegExpF(string(dataFrameKey), string(key)); e == nil && m {
				keys[dataFrameKey] = keyType
			}
		}
	} else {
		keys[key] = d.Keys[key]
	}

	for dataFrameKey, keyType := range keys {
		switch keyType {
		case Numeric:
			if keyValues, ok := d.Numerics[dataFrameKey]; ok {
				for _, keyValue := range keyValues {
					for row := range keyValue {
						return row, dataFrameKey, d.Data[row][dataFrameKey]
					}
				}
			}
		case String:
			if keyValues, ok := d.Strings[dataFrameKey]; ok {
				for _, keyValue := range keyValues {
					for row := range keyValue {
						return row, dataFrameKey, d.Data[row][dataFrameKey]
					}
				}
			}
		case Boolean:
			if keyValues, ok := d.Booleans[dataFrameKey]; ok {
				for _, keyValue := range keyValues {
					for row := range keyValue {
						return row, dataFrameKey, d.Data[row][dataFrameKey]
					}
				}
			}
		}
	}

	return uuid.Nil, "", nil
}

// EqualsF compares two values of type float64, string, or bool and returns true if they are equal, otherwise false.
func EqualsF[v float64 | string | bool](left, right v) bool {
	return left == right
}

// MatchesRegExpF checks if a given string matches a specified regular expression and returns a boolean or an error.
func MatchesRegExpF(value, regExp string) (bool, error) {
	re, err := regexp.Compile(regExp)
	if err != nil {
		return false, err
	}

	if re.MatchString(value) {
		return true, nil
	}

	return false, nil
}

// MajorThanF compares two float64 numbers and returns true if the first number is greater than the second.
func MajorThanF(left, right float64) bool {
	return left > right
}

// InListF checks if a given value of type float64 or string is present in the provided list and returns true if found.
func InListF[v float64 | string](value v, list []v) bool {
	for _, element := range list {
		if element == value {
			return true
		}
	}
	return false
}

// InCIDRF checks if an IP address (value) belongs to a given CIDR range and returns a boolean. Errors on invalid CIDR.
func InCIDRF(value, cidr string) (bool, error) {
	_, subnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return false, err
	}

	ip := net.ParseIP(value)
	if ip != nil {
		if subnet.Contains(ip) {
			return true, nil
		}
	}

	return false, nil
}

// ContainsF checks if the `substring` is present within the `value` and returns true if found, otherwise false.
func ContainsF(value, substring string) bool {
	return strings.Contains(value, substring)
}

// StartsWithF checks if the given string 'value' starts with the specified 'prefix' and returns true if it does.
func StartsWithF(value, prefix string) bool {
	return strings.HasPrefix(value, prefix)
}

// EndsWithF checks if the given string 'value' ends with the specified 'suffix'. Returns true if it does, otherwise false.
func EndsWithF(value, suffix string) bool {
	return strings.HasSuffix(value, suffix)
}
