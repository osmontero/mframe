# mframe

A high-performance, in-memory DataFrame library for Go with TTL support, advanced filtering, and statistical operations.

## Features

- 🚀 **High-performance** in-memory data storage with multiple indexes
- ⏰ **TTL support** with automatic expiration and cleanup
- 🔍 **Advanced filtering** with 20 different operators including time-based filtering
- 📊 **Extended statistical operations** (sum, average, median, min, max, variance, standard deviation, percentile, mode, range, geometric mean, harmonic mean)
- 🔒 **Thread-safe** operations with RWMutex
- 🎯 **Type-safe** indexing for strings, numerics, booleans, and time values
- ⚡ **Batch operations** for efficient bulk inserts
- 🔄 **Regex caching** for improved pattern matching performance

## Installation

```bash
go get github.com/threatwinds/mframe
```

## Quick Start

### Basic Usage

```go
package main

import (
    "fmt"
    "time"
    "github.com/threatwinds/mframe"
)

func main() {
    // Initialize a DataFrame with 24-hour TTL
    var df mframe.DataFrame
    df.Init(24 * time.Hour)
    
    // Insert data
    df.Insert(map[mframe.KeyName]interface{}{
        "name":    "John Doe",
        "age":     30,
        "active":  true,
        "score":   85.5,
        "ip":      "192.168.1.1",
    })
    
    df.Insert(map[mframe.KeyName]interface{}{
        "name":    "Jane Smith",
        "age":     25,
        "active":  true,
        "score":   92.0,
        "ip":      "192.168.1.2",
    })
    
    // Get total count
    fmt.Printf("Total records: %d\n", df.Count())
}
```

## Core Operations

### Data Insertion

```go
// Insert single row
df.Insert(map[mframe.KeyName]interface{}{
    "product_id": "P123",
    "price":      29.99,
    "in_stock":   true,
    "tags":       []string{"electronics", "mobile"},
    "created_at": time.Now(),
})

// Batch insert for better performance
batch := []map[mframe.KeyName]interface{}{
    {"product_id": "P124", "price": 39.99, "created_at": time.Now()},
    {"product_id": "P125", "price": 49.99, "created_at": time.Now()},
    {"product_id": "P126", "price": 59.99, "created_at": time.Now()},
}
err := df.InsertBatch(batch)

// Batch insert with specific IDs
entries := map[uuid.UUID]map[mframe.KeyName]interface{}{
    uuid.New(): {"product_id": "P127", "price": 69.99},
    uuid.New(): {"product_id": "P128", "price": 79.99},
}
err = df.InsertBatchWithIDs(entries)

// Append data from another DataFrame
var df2 mframe.DataFrame
df2.Init(1 * time.Hour)
df2.Insert(map[mframe.KeyName]interface{}{"product_id": "P456", "price": 19.99})

// Append all rows from df2 to df
df.Append(&df2, "merged")
```

### Filtering Operations

mframe supports 20 different operators for filtering:

```go
// String operations
activeUsers := df.Filter(mframe.Equals, "status", "active", nil)
johnUsers := df.Filter(mframe.StartsWith, "name", "John", nil)
emailUsers := df.Filter(mframe.Contains, "email", "@gmail.com", nil)
patternMatch := df.Filter(mframe.RegExp, "phone", `^\+1-\d{3}-\d{3}-\d{4}$`, nil)

// Numeric operations
adults := df.Filter(mframe.MajorEquals, "age", 18, nil)
highScores := df.Filter(mframe.Major, "score", 90.0, nil)
midRange := df.Filter(mframe.Major, "price", 10.0, nil).Filter(mframe.Minor, "price", 50.0, nil)

// Boolean operations
activeOnly := df.Filter(mframe.Equals, "active", true, nil)

// List operations
selectedProducts := df.Filter(mframe.InList, "product_id", []string{"P123", "P456", "P789"}, nil)
excludedCategories := df.Filter(mframe.NotInList, "category", []string{"deprecated", "test"}, nil)

// Time-based filtering
startTime := time.Now().Add(-24 * time.Hour)
endTime := time.Now()
recentRecords := df.Filter(mframe.Between, "created_at", []time.Time{startTime, endTime}, nil)
oldRecords := df.Filter(mframe.NotBetween, "created_at", []time.Time{startTime, endTime}, nil)

// Numeric range filtering
priceRange := df.Filter(mframe.Between, "price", []float64{10.0, 50.0}, nil)
outOfRange := df.Filter(mframe.NotBetween, "price", []float64{10.0, 50.0}, nil)

// Network operations (CIDR)
localNetwork := df.Filter(mframe.InCIDR, "ip", "192.168.0.0/16", nil)
publicIPs := df.Filter(mframe.NotInCIDR, "ip", "10.0.0.0/8", nil)

// Case-insensitive filtering
options := map[mframe.FilterOption]bool{mframe.CaseSensitive: false}
names := df.Filter(mframe.Equals, "name", "john doe", options)
```

### Complete List of Operators

| Operator        | Description            | Example Value Types      |
|-----------------|------------------------|--------------------------|
| `Equals`        | Exact match            | string, numeric, boolean |
| `NotEquals`     | Not equal to           | string, numeric, boolean |
| `Major`         | Greater than           | numeric                  |
| `Minor`         | Less than              | numeric                  |
| `MajorEquals`   | Greater or equal       | numeric                  |
| `MinorEquals`   | Less or equal          | numeric                  |
| `InList`        | Value in list          | string, numeric          |
| `NotInList`     | Value not in list      | string, numeric          |
| `RegExp`        | Regex match            | string                   |
| `NotRegExp`     | Regex not match        | string                   |
| `InCIDR`        | IP in CIDR range       | string (IP)              |
| `NotInCIDR`     | IP not in CIDR         | string (IP)              |
| `Contains`      | String contains        | string                   |
| `NotContains`   | String not contains    | string                   |
| `StartsWith`    | String starts with     | string                   |
| `NotStartsWith` | String not starts with | string                   |
| `EndsWith`      | String ends with       | string                   |
| `NotEndsWith`   | String not ends with   | string                   |
| `Between`       | Value in range         | numeric, time.Time       |
| `NotBetween`    | Value not in range     | numeric, time.Time       |

### Statistical Operations

```go
// Count operations
total := df.Count()
uniqueCounts := df.CountUnique("category") // map[interface{}]int

// Mathematical operations (works on numeric fields)
sum, err := df.Sum("price")
avg, err := df.Average("score")
median, err := df.Median("age")
max, err := df.Max("price")
min, err := df.Min("price")
variance, err := df.Variance("score")
stdDev, err := df.StandardDeviation("score")

// Advanced statistical operations
percentile95, err := df.Percentile("response_time", 95.0)
modeValues, err := df.Mode("category_id")  // Returns []float64 for ties
valueRange, err := df.Range("temperature")
geomMean, err := df.GeometricMean("growth_rate")
harmMean, err := df.HarmonicMean("speed")

// Handle errors for non-numeric fields
if err != nil {
    fmt.Printf("Error: %v\n", err)
}
```

### Data Export and Conversion

```go
// Convert to slice of rows
rows := df.ToSlice()
for _, row := range rows {
    fmt.Printf("Row: %+v\n", row)
}

// Get slice of values for a specific field
names := df.SliceOf("name")           // []interface{}
prices := df.SliceOfFloat64("price")  // []float64

// Find first occurrence of a key
uuid, keyName, value := df.FindFirstByKey("email")
if uuid != (uuid.UUID{}) {
    fmt.Printf("Found %s = %v (ID: %s)\n", keyName, value, uuid)
}
```

### Manual Data Management

```go
// Remove specific element by UUID
var id uuid.UUID // obtained from insert or filter operations
df.RemoveElement(id)

// Manual cleanup of expired data (usually runs automatically)
// df.CleanExpired() // This runs in a goroutine automatically
```

## Advanced Usage

### Background Operations

```go
// Initialize with automatic cleanup
df.Init(1 * time.Hour)
// CleanExpired() runs automatically in background

// Enable statistics logging (prints to stdout every minute)
go df.Stats("MyDataFrame")
```

### Chaining Operations

```go
// Complex filtering with chaining
results := df.
    Filter(mframe.Major, "age", 18, nil).
    Filter(mframe.InList, "status", []string{"active", "premium"}, nil).
    Filter(mframe.NotInCIDR, "ip", "192.168.0.0/16", nil)

// Statistical analysis on filtered data
avgScore, _ := results.Average("score")
fmt.Printf("Average score of adult active users: %.2f\n", avgScore)
```

### Working with TTL

```go
// Short-lived cache (5 minutes)
var cache mframe.DataFrame
cache.Init(5 * time.Minute)

// Insert temporary data
cache.Insert(map[mframe.KeyName]interface{}{
    "session_id": "sess_123",
    "user_id":    "user_456",
    "last_seen":  time.Now(),
})

// Data automatically expires after 5 minutes
```

## Performance Tips

### 1. **Choose the Right TTL**

- Shorter TTL = less memory usage but more frequent cleanups
- Longer TTL = more memory usage but less CPU overhead
- Default cleanup runs every minute

### 2. **Use Type-Specific Indexes**

- String fields are indexed for exact and pattern matching
- Numeric fields are indexed for range queries
- Boolean fields are indexed for true/false filtering
- Time fields are indexed for temporal queries

### 3. **Batch Operations**

```go
// Use batch insert for multiple rows
batch := make([]map[mframe.KeyName]interface{}, 1000)
for i := 0; i < 1000; i++ {
    batch[i] = map[mframe.KeyName]interface{}{
        "id": i,
        "value": rand.Float64(),
    }
}
df.InsertBatch(batch) // Much faster than 1000 individual inserts

// Chain filters for complex queries
filtered := df.
    Filter(mframe.Greater, "age", 18, nil).
    Filter(mframe.Less, "age", 65, nil)
```

### 4. **Memory Considerations**

- Each index maintains its own data structure
- Consider memory usage when storing large datasets
- Use `Stats()` to monitor memory usage
- Regex patterns are cached to improve performance (configurable cache size)

### 5. **Concurrent Access**

- All operations are thread-safe
- Reads can happen concurrently
- Writes are serialized for consistency

## Common Use Cases

### 1. **Time-Series Data Analysis**

```go
var metrics mframe.DataFrame
metrics.Init(24 * time.Hour) // Keep last 24 hours

// Collect metrics
metrics.Insert(map[mframe.KeyName]interface{}{
    "metric_name": "cpu_usage",
    "value":       75.5,
    "timestamp":   time.Now(),
    "host":        "server01",
})

// Analyze recent data
lastHour := time.Now().Add(-1 * time.Hour)
recentMetrics := metrics.Filter(mframe.Between, "timestamp", 
    []time.Time{lastHour, time.Now()}, nil)

// Calculate statistics
avgCPU, _ := recentMetrics.Average("value")
p95CPU, _ := recentMetrics.Percentile("value", 95.0)
```

### 2. **Session Store"

```go
var sessions mframe.DataFrame
sessions.Init(30 * time.Minute) // 30-minute sessions

sessions.Insert(map[mframe.KeyName]interface{}{
    "session_id": sessionID,
    "user_id":    userID,
    "ip":         clientIP,
    "created_at": time.Now(),
})
```

### 3. **Rate Limiting**

```go
var requests mframe.DataFrame
requests.Init(1 * time.Minute) // 1-minute window

// Check request count
userRequests := requests.Filter(mframe.Equals, "user_id", userID, nil)
if userRequests.Count() >= 100 {
    // Rate limit exceeded
}
```

### 4. **Real-time Analytics**

```go
var events mframe.DataFrame
events.Init(1 * time.Hour)

// Track events
events.Insert(map[mframe.KeyName]interface{}{
    "event_type": "page_view",
    "url":        "/products",
    "user_id":    userID,
    "timestamp":  time.Now().Unix(),
})

// Analyze
pageViews := events.Filter(mframe.Equals, "event_type", "page_view", nil)
uniqueUsers := pageViews.CountUnique("user_id")
```

### 5. **Caching**

```go
var cache mframe.DataFrame
cache.Init(5 * time.Minute)

// Cache API responses
cache.Insert(map[mframe.KeyName]interface{}{
    "endpoint":    "/api/users",
    "params_hash": paramsHash,
    "response":    responseData,
    "cached_at":   time.Now(),
})

// Retrieve from cache
cached := cache.Filter(mframe.Equals, "params_hash", paramsHash, nil)
if cached.Count() > 0 {
    // Use cached response
}
```

## Performance Optimizations

### Regex Caching

The DataFrame automatically caches compiled regex patterns for improved performance:

```go
// First call compiles and caches the regex
df.Filter(mframe.RegExp, "email", `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`, nil)

// Subsequent calls use the cached compiled regex
df.Filter(mframe.RegExp, "email", `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`, nil)
```

### Batch Processing

For bulk operations, use batch methods:

```go
// Efficient batch insert
batch := make([]map[mframe.KeyName]interface{}, 0, 10000)
// ... populate batch ...
df.InsertBatch(batch)

// Process results in batches
results := df.Filter(mframe.Greater, "score", 80.0, nil)
for _, row := range results.ToSlice() {
    // Process each row
}
```

## Error Handling

Most operations that can fail return an error:

```go
// Mathematical operations return errors for non-numeric fields
sum, err := df.Sum("name") // Error: field contains non-numeric values
if err != nil {
    log.Printf("Cannot sum non-numeric field: %v", err)
}

// Batch operations validate input
err := df.InsertBatch(nil)
if err != nil {
    log.Printf("Batch insert failed: %v", err)
}

// Filter operations with regex can fail
filtered := df.Filter(mframe.RegExp, "email", "[invalid regex", nil)
// Invalid regex patterns are handled gracefully (no matches)
```

## API Improvements

### Clearer Operator Names

The library now provides more intuitive operator names while maintaining backward compatibility:

```go
// New clearer names
df.Filter(mframe.Greater, "age", 18, nil)        // Instead of Major
df.Filter(mframe.Less, "age", 65, nil)           // Instead of Minor
df.Filter(mframe.GreaterOrEqual, "score", 90, nil) // Instead of MajorEquals
df.Filter(mframe.LessOrEqual, "price", 100, nil)   // Instead of MinorEquals

// Old names still work for backward compatibility
df.Filter(mframe.Major, "age", 18, nil)  // Deprecated but functional
```

## License

See [LICENSE](LICENSE) file for details.