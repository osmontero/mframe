# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an in-memory DataFrame library written in Go that provides TTL-based data storage with advanced filtering and
indexing capabilities. The library is designed for high-performance temporary data storage scenarios.

## Commands

### Development Commands

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run specific test
go test -run TestDataFrame_Init

# Update dependencies
go mod tidy

# Download dependencies
go mod download

# Run tests with verbose output
go test -v ./...
```

## Architecture

### Core Components

1. **DataFrame Structure** (`dataframe.go`):
    - Main data structure with concurrent access control (RWMutex)
    - Multiple indexes for fast lookups:
        - `Data`: Primary storage mapping UUID to data rows
        - `Keys`: Maps field names to their typed keys
        - `Strings`, `Numerics`, `Booleans`: Type-specific indexes for efficient filtering
        - `ExpireAt`: TTL tracking for automatic data expiration

2. **Key Operations**:
    - **Insert** (`insert.go`): Adds data with automatic type detection and indexing
    - **Filter** (`filter.go`): Advanced filtering with operators:
        - String: equals, regex, contains, hasPrefix, hasSuffix
        - Numeric: equals, greater, greaterOrEqual, lower, lowerOrEqual, between
        - Time: between, notBetween (for time.Time values)
        - Network: CIDR matching
        - List operations: in, notIn
    - **Cleaner** (`cleaner.go`): Background goroutine for TTL-based cleanup
    - **Math** (`math.go`): Statistical operations (sum, average, median, min, max)

3. **Data Model**:
    - Each row is identified by a UUID
    - Data is stored as `map[string]interface{}`
    - Automatic type detection and indexing on insert
    - TTL is applied per row, not per field

### Testing Patterns

- Black-box testing using `mframe_test` package
- Table-driven tests for multiple scenarios
- Integration tests covering full workflows (insert → filter → delete)
- Standard Go testing package, no external testing frameworks

### Key Design Decisions

1. **Thread Safety**: All operations use RWMutex for concurrent access
2. **Type Safety**: Separate indexes for different data types with typed keys
3. **Performance**: Multiple indexes trade memory for query speed
4. **Flexibility**: Support for various filter operators and data types
5. **Automatic Cleanup**: TTL-based expiration runs in background