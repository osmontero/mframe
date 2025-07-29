package mframe

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ExplainResult contains information about how a filter would be executed
type ExplainResult struct {
	Operator      string
	Key           string
	KeyType       string
	IndexUsed     bool
	EstimatedRows int
	TotalRows     int
	Details       []string
}

// Explain analyzes how a filter operation would be executed without actually running it
func (d *DataFrame) Explain(operator Operator, key KeyName, value any) ExplainResult {
	d.Locker.RLock()
	defer d.Locker.RUnlock()

	result := ExplainResult{
		Operator:  operatorToString(operator),
		Key:       string(key),
		TotalRows: len(d.Data),
		Details:   make([]string, 0),
	}

	// Check if key uses regex pattern
	if ContainsF(string(key), "^") || ContainsF(string(key), "[") || ContainsF(string(key), "(") {
		result.Details = append(result.Details, "Key uses regex pattern matching")
		matchCount := 0
		if re, err := d.getCompiledRegex(string(key)); err == nil {
			for dataFrameKey := range d.Keys {
				if re.MatchString(string(dataFrameKey)) {
					matchCount++
				}
			}
		}
		result.Details = append(result.Details, fmt.Sprintf("Pattern matches %d keys", matchCount))
	}

	// Get key type
	keyType, exists := d.Keys[key]
	if !exists {
		result.KeyType = "Unknown"
		result.IndexUsed = false
		result.Details = append(result.Details, "Key not found in indexes")
		return result
	}

	result.KeyType = keyTypeToString(keyType)
	result.IndexUsed = true

	// Estimate row count based on index
	switch keyType {
	case Numeric:
		if index, ok := d.Numerics[key]; ok {
			result.Details = append(result.Details, fmt.Sprintf("Numeric index contains %d unique values", len(index)))
			result.EstimatedRows = estimateNumericRows(operator, value, index)
		}
	case String:
		if index, ok := d.Strings[key]; ok {
			result.Details = append(result.Details, fmt.Sprintf("String index contains %d unique values", len(index)))
			result.EstimatedRows = estimateStringRows(operator, value, index)
		}
	case Boolean:
		if index, ok := d.Booleans[key]; ok {
			result.Details = append(result.Details, fmt.Sprintf("Boolean index contains %d unique values", len(index)))
			result.EstimatedRows = estimateBooleanRows(operator, value, index)
		}
	case Time:
		if index, ok := d.Times[key]; ok {
			result.Details = append(result.Details, fmt.Sprintf("Time index contains %d unique values", len(index)))
			result.EstimatedRows = estimateTimeRows(operator, value, index)
		}
	}

	// Add selectivity information
	if result.TotalRows > 0 {
		selectivity := float64(result.EstimatedRows) / float64(result.TotalRows) * 100
		result.Details = append(result.Details, fmt.Sprintf("Estimated selectivity: %.2f%%", selectivity))
	}

	return result
}

// String returns a formatted string representation of the explain result
func (e ExplainResult) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("EXPLAIN: Filter(%s, %s, <value>)\n", e.Operator, e.Key))
	sb.WriteString(fmt.Sprintf("  Key Type: %s\n", e.KeyType))
	sb.WriteString(fmt.Sprintf("  Index Used: %v\n", e.IndexUsed))
	sb.WriteString(fmt.Sprintf("  Total Rows: %d\n", e.TotalRows))
	sb.WriteString(fmt.Sprintf("  Estimated Rows: %d\n", e.EstimatedRows))

	if len(e.Details) > 0 {
		sb.WriteString("  Details:\n")
		for _, detail := range e.Details {
			sb.WriteString(fmt.Sprintf("    - %s\n", detail))
		}
	}

	return sb.String()
}

func operatorToString(op Operator) string {
	switch op {
	case Equals:
		return "Equals"
	case NotEquals:
		return "NotEquals"
	case Greater:
		return "Greater"
	case Less:
		return "Less"
	case GreaterOrEqual:
		return "GreaterOrEqual"
	case LessOrEqual:
		return "LessOrEqual"
	case InList:
		return "InList"
	case NotInList:
		return "NotInList"
	case RegExp:
		return "RegExp"
	case NotRegExp:
		return "NotRegExp"
	case InCIDR:
		return "InCIDR"
	case NotInCIDR:
		return "NotInCIDR"
	case Contains:
		return "Contains"
	case NotContains:
		return "NotContains"
	case StartsWith:
		return "StartsWith"
	case NotStartsWith:
		return "NotStartsWith"
	case EndsWith:
		return "EndsWith"
	case NotEndsWith:
		return "NotEndsWith"
	case Between:
		return "Between"
	case NotBetween:
		return "NotBetween"
	default:
		return "Unknown"
	}
}

func keyTypeToString(kt KeyType) string {
	switch kt {
	case String:
		return "String"
	case Numeric:
		return "Numeric"
	case Boolean:
		return "Boolean"
	case Time:
		return "Time"
	default:
		return "Unknown"
	}
}

func estimateNumericRows(op Operator, value any, index map[float64]map[uuid.UUID]bool) int {
	count := 0
	switch op {
	case Equals:
		if v, ok := value.(float64); ok {
			if ids, exists := index[v]; exists {
				count = len(ids)
			}
		}
	case NotEquals:
		if v, ok := value.(float64); ok {
			for key, ids := range index {
				if key != v {
					count += len(ids)
				}
			}
		}
	case Greater, Less, GreaterOrEqual, LessOrEqual:
		if v, ok := value.(float64); ok {
			for key, ids := range index {
				switch op {
				case Greater:
					if key > v {
						count += len(ids)
					}
				case Less:
					if key < v {
						count += len(ids)
					}
				case GreaterOrEqual:
					if key >= v {
						count += len(ids)
					}
				case LessOrEqual:
					if key <= v {
						count += len(ids)
					}
				}
			}
		}
	case Between, NotBetween:
		if vals, ok := value.([]float64); ok && len(vals) == 2 {
			min, max := vals[0], vals[1]
			if min > max {
				min, max = max, min
			}
			for key, ids := range index {
				if op == Between && key >= min && key <= max {
					count += len(ids)
				} else if op == NotBetween && (key < min || key > max) {
					count += len(ids)
				}
			}
		}
	default:
		// For other operators, return total rows in index
		for _, ids := range index {
			count += len(ids)
		}
	}
	return count
}

func estimateStringRows(op Operator, value any, index map[string]map[uuid.UUID]bool) int {
	count := 0
	switch op {
	case Equals:
		if v, ok := value.(string); ok {
			if ids, exists := index[v]; exists {
				count = len(ids)
			}
		}
	case NotEquals:
		if v, ok := value.(string); ok {
			for key, ids := range index {
				if key != v {
					count += len(ids)
				}
			}
		}
	case InList, NotInList:
		if vals, ok := value.([]string); ok {
			valMap := make(map[string]bool)
			for _, v := range vals {
				valMap[v] = true
			}
			for key, ids := range index {
				if op == InList && valMap[key] {
					count += len(ids)
				} else if op == NotInList && !valMap[key] {
					count += len(ids)
				}
			}
		}
	default:
		// For pattern-based operators, we can't easily estimate
		// Return total rows as upper bound
		for _, ids := range index {
			count += len(ids)
		}
	}
	return count
}

func estimateBooleanRows(op Operator, value any, index map[bool]map[uuid.UUID]bool) int {
	count := 0
	switch op {
	case Equals:
		if v, ok := value.(bool); ok {
			if ids, exists := index[v]; exists {
				count = len(ids)
			}
		}
	case NotEquals:
		if v, ok := value.(bool); ok {
			if ids, exists := index[!v]; exists {
				count = len(ids)
			}
		}
	default:
		for _, ids := range index {
			count += len(ids)
		}
	}
	return count
}

func estimateTimeRows(op Operator, value any, index map[time.Time]map[uuid.UUID]bool) int {
	count := 0
	switch op {
	case Between, NotBetween:
		if vals, ok := value.([]time.Time); ok && len(vals) == 2 {
			start, end := vals[0], vals[1]
			if start.After(end) {
				start, end = end, start
			}
			for key, ids := range index {
				if op == Between && !key.Before(start) && !key.After(end) {
					count += len(ids)
				} else if op == NotBetween && (key.Before(start) || key.After(end)) {
					count += len(ids)
				}
			}
		}
	default:
		for _, ids := range index {
			count += len(ids)
		}
	}
	return count
}
