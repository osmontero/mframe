# mframe

A high-performance, in-memory DataFrame library for Go with TTL support, advanced filtering, and statistical operations.

## Features

- 🚀 **High-performance** in-memory data storage with multiple indexes
- ⏰ **TTL support** with automatic expiration and cleanup
- 🔍 **Advanced filtering** with 18 different operators
- 📊 **Statistical operations** (sum, average, median, min, max, variance)
- 🔒 **Thread-safe** operations with RWMutex
- 🎯 **Type-safe** indexing for strings, numerics, and booleans

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
})

// Append data from another DataFrame
var df2 mframe.DataFrame
df2.Init(1 * time.Hour)
df2.Insert(map[mframe.KeyName]interface{}{"product_id": "P456", "price": 19.99})

// Append all rows from df2 to df
df.Append(&df2, "merged")
```

### Filtering Operations

mframe supports 18 different operators for filtering:

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

### 3. **Batch Operations**

```go
// Instead of multiple filters, chain them
filtered := df.
    Filter(mframe.Major, "age", 18, nil).
    Filter(mframe.Minor, "age", 65, nil)
```

### 4. **Memory Considerations**

- Each index maintains its own data structure
- Consider memory usage when storing large datasets
- Use `Stats()` to monitor memory usage

### 5. **Concurrent Access**

- All operations are thread-safe
- Reads can happen concurrently
- Writes are serialized for consistency

## Common Use Cases

### 1. **Session Store**

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

### 2. **Rate Limiting**

```go
var requests mframe.DataFrame
requests.Init(1 * time.Minute) // 1-minute window

// Check request count
userRequests := requests.Filter(mframe.Equals, "user_id", userID, nil)
if userRequests.Count() >= 100 {
    // Rate limit exceeded
}
```

### 3. **Real-time Analytics**

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

### 4. **Caching**

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

## Error Handling

Most operations that can fail return an error:

```go
// Mathematical operations return errors for non-numeric fields
sum, err := df.Sum("name") // Error: field contains non-numeric values
if err != nil {
    log.Printf("Cannot sum non-numeric field: %v", err)
}

// Filter operations with regex can fail
filtered := df.Filter(mframe.RegExp, "email", "[invalid regex", nil)
// Invalid regex patterns are handled gracefully (no matches)
```

## License

See [LICENSE](LICENSE) file for details.