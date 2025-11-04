package driver

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	pq "github.com/parquet-go/parquet-go"
)

// inferCSVSchema reads the first few rows of a CSV file to infer the schema
func (s *S3) inferCSVSchema(ctx context.Context, stream *types.Stream, key string) (*types.Stream, error) {
	logger.Infof("Inferring CSV schema for file: %s", key)

	// Get the object from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Wrap with decompression reader if needed
	reader, err := s.getReader(result.Body, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	// Create CSV reader
	csvReader := csv.NewReader(reader)
	csvReader.Comma = rune(s.config.Delimiter[0])
	if s.config.QuoteCharacter != "" {
		csvReader.LazyQuotes = true
	}

	// Skip rows if configured
	for i := 0; i < s.config.SkipRows; i++ {
		_, err := csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to skip row %d: %w", i, err)
		}
	}

	var headers []string
	if s.config.HasHeader {
		// Read header row
		headers, err = csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV headers: %w", err)
		}
	} else {
		// Read first data row to determine column count
		firstRow, err := csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read first CSV row: %w", err)
		}
		// Generate column names as column_0, column_1, etc.
		for i := range firstRow {
			headers = append(headers, fmt.Sprintf("column_%d", i))
		}
	}

	// Read a few sample rows to infer data types
	sampleRows := [][]string{}
	maxSamples := 100
	for i := 0; i < maxSamples; i++ {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Warnf("Error reading sample row %d: %v", i, err)
			break
		}
		sampleRows = append(sampleRows, row)
	}

	// Infer types for each column
	for i, header := range headers {
		dataType := s.inferColumnType(sampleRows, i)
		stream.UpsertField(header, dataType, true) // Allow nulls by default
		stream.WithCursorField(header)
	}

	logger.Infof("Inferred schema with %d columns from CSV", len(headers))
	return stream, nil
}

// inferJSONSchema reads the first few records of a JSON file to infer the schema
func (s *S3) inferJSONSchema(ctx context.Context, stream *types.Stream, key string) (*types.Stream, error) {
	logger.Infof("Inferring JSON schema for file: %s", key)

	// Get the object from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Wrap with decompression reader if needed
	reader, err := s.getReader(result.Body, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader: %w", err)
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	// Collect sample records
	var sampleRecords []map[string]interface{}
	maxSamples := 100

	if s.config.JSONLineDelimited {
		// Line-delimited JSON (JSONL)
		decoder := json.NewDecoder(reader)
		for i := 0; i < maxSamples; i++ {
			var record map[string]interface{}
			if err := decoder.Decode(&record); err == io.EOF {
				break
			} else if err != nil {
				logger.Warnf("Error reading JSON record %d: %v", i, err)
				break
			}
			sampleRecords = append(sampleRecords, record)
		}
	} else {
		// JSON array
		var records []map[string]interface{}
		decoder := json.NewDecoder(reader)
		if err := decoder.Decode(&records); err != nil {
			return nil, fmt.Errorf("failed to decode JSON array: %w", err)
		}
		if len(records) > maxSamples {
			sampleRecords = records[:maxSamples]
		} else {
			sampleRecords = records
		}
	}

	if len(sampleRecords) == 0 {
		return nil, fmt.Errorf("no records found in JSON file")
	}

	// Collect all unique field names across samples
	fieldTypes := make(map[string][]interface{})
	for _, record := range sampleRecords {
		for fieldName, value := range record {
			fieldTypes[fieldName] = append(fieldTypes[fieldName], value)
		}
	}

	// Infer type for each field
	for fieldName, values := range fieldTypes {
		dataType := s.inferJSONFieldType(values)
		stream.UpsertField(fieldName, dataType, true) // Allow nulls by default
		stream.WithCursorField(fieldName)
	}

	logger.Infof("Inferred schema with %d fields from JSON", len(fieldTypes))
	return stream, nil
}

// inferParquetSchema reads Parquet file schema
func (s *S3) inferParquetSchema(ctx context.Context, stream *types.Stream, key string) (*types.Stream, error) {
	logger.Infof("Inferring Parquet schema for file: %s", key)

	// Get the object from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Read the parquet file content into memory
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet file: %w", err)
	}

	// Open parquet file to read schema
	pqFile, err := pq.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	// Get the schema from parquet file
	schema := pqFile.Schema()

	// Convert parquet schema to Olake schema
	for _, field := range schema.Fields() {
		// Use string type for all fields for simplicity
		// The parquet reader will handle the actual type conversions
		stream.UpsertField(field.Name(), types.String, true)
		stream.WithCursorField(field.Name())
	}

	logger.Infof("Inferred %d columns from Parquet file", len(schema.Fields()))

	return stream, nil
}

// inferColumnType infers the data type of a CSV column from sample values
func (s *S3) inferColumnType(sampleRows [][]string, columnIndex int) types.DataType {
	if len(sampleRows) == 0 {
		return types.String
	}

	hasInt := false
	hasFloat := false
	hasBool := false

	for _, row := range sampleRows {
		if columnIndex >= len(row) {
			continue
		}

		value := strings.TrimSpace(row[columnIndex])
		if value == "" || strings.ToLower(value) == "null" {
			continue
		}

		// Try to parse as different types
		if _, err := strconv.ParseInt(value, 10, 64); err == nil {
			hasInt = true
		} else if _, err := strconv.ParseFloat(value, 64); err == nil {
			hasFloat = true
		} else if strings.ToLower(value) == "true" || strings.ToLower(value) == "false" {
			hasBool = true
		}
	}

	// Determine type based on what we found
	if hasBool && !hasInt && !hasFloat {
		return types.Bool
	}
	if hasFloat {
		return types.Float64
	}
	if hasInt {
		return types.Int64
	}

	// Default to string
	return types.String
}

// inferJSONFieldType infers the data type of a JSON field from sample values
func (s *S3) inferJSONFieldType(values []interface{}) types.DataType {
	if len(values) == 0 {
		return types.String
	}

	hasInt := false
	hasFloat := false
	hasBool := false
	hasString := false
	hasObject := false
	hasArray := false

	for _, value := range values {
		if value == nil {
			continue
		}

		switch v := value.(type) {
		case bool:
			hasBool = true
		case float64:
			// JSON numbers are always float64
			if v == float64(int64(v)) {
				hasInt = true
			} else {
				hasFloat = true
			}
		case string:
			hasString = true
		case map[string]interface{}:
			hasObject = true
		case []interface{}:
			hasArray = true
		}
	}

	// Determine type based on what we found
	if hasBool && !hasInt && !hasFloat && !hasString && !hasObject && !hasArray {
		return types.Bool
	}
	if hasFloat {
		return types.Float64
	}
	if hasInt && !hasFloat {
		return types.Int64
	}
	if hasObject || hasArray {
		return types.String // Complex types should be stored as JSON string
	}

	// Default to string
	return types.String
}

// getReader returns an appropriate reader based on compression settings
func (s *S3) getReader(body io.Reader, key string) (io.Reader, error) {
	lowerKey := strings.ToLower(key)

	if s.config.Compression == CompressionGzip || strings.HasSuffix(lowerKey, ".gz") {
		gzipReader, err := gzip.NewReader(body)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return gzipReader, nil
	}

	// TODO: Add support for zip compression if needed
	return body, nil
}

// SchemaCache represents cached schema metadata
type SchemaCache struct {
	Fields          map[string]types.DataType `json:"fields"`
	CursorFields    []string                  `json:"cursor_fields"`
	CachedAt        string                    `json:"cached_at"`         // Timestamp when schema was cached
	MaxLastModified string                    `json:"max_last_modified"` // Max LastModified of files when schema was cached
}

// shouldReinferSchema checks if schema needs to be re-inferred
// Returns true if no cached schema exists or if files have been modified since caching
func (s *S3) shouldReinferSchema(streamName string, files []FileObject) bool {
	// No state available
	if s.state == nil {
		return true
	}

	// Get cached schema from state
	cacheKey := fmt.Sprintf("schema_cache_%s", streamName)
	tempStream := types.NewStream(streamName, "s3", &s.config.BucketName)
	cachedData := s.state.GetCursor(&types.ConfiguredStream{
		Stream: tempStream,
	}, cacheKey)

	if cachedData == nil {
		logger.Debugf("No cached schema found for stream %s, will infer", streamName)
		return true
	}

	// Parse cached schema
	cachedJSON, ok := cachedData.(string)
	if !ok {
		logger.Warnf("Invalid cached schema format for stream %s, will re-infer", streamName)
		return true
	}

	var cache SchemaCache
	if err := json.Unmarshal([]byte(cachedJSON), &cache); err != nil {
		logger.Warnf("Failed to unmarshal cached schema for stream %s: %v, will re-infer", streamName, err)
		return true
	}

	// Find max LastModified from current files
	var maxLastModified string
	for _, file := range files {
		if file.LastModified > maxLastModified {
			maxLastModified = file.LastModified
		}
	}

	// Compare with cached max LastModified
	if maxLastModified > cache.MaxLastModified {
		logger.Infof("Files modified since schema cache for stream %s (cached: %s, current: %s), will re-infer",
			streamName, cache.MaxLastModified, maxLastModified)
		return true
	}

	logger.Debugf("Using cached schema for stream %s (cached at: %s)", streamName, cache.CachedAt)
	return false
}

// loadSchemaFromState loads cached schema from state and applies it to the stream
func (s *S3) loadSchemaFromState(stream *types.Stream, streamName string) (*types.Stream, error) {
	cacheKey := fmt.Sprintf("schema_cache_%s", streamName)
	tempStream := types.NewStream(streamName, "s3", &s.config.BucketName)
	cachedData := s.state.GetCursor(&types.ConfiguredStream{
		Stream: tempStream,
	}, cacheKey)

	if cachedData == nil {
		return nil, fmt.Errorf("no cached schema found for stream %s", streamName)
	}

	cachedJSON, ok := cachedData.(string)
	if !ok {
		return nil, fmt.Errorf("invalid cached schema format for stream %s", streamName)
	}

	var cache SchemaCache
	if err := json.Unmarshal([]byte(cachedJSON), &cache); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cached schema: %w", err)
	}

	// Apply cached fields to stream
	for fieldName, dataType := range cache.Fields {
		stream.UpsertField(fieldName, dataType, true) // Allow nulls by default
	}

	// Apply cursor fields
	for _, cursorField := range cache.CursorFields {
		stream.WithCursorField(cursorField)
	}

	logger.Infof("Loaded cached schema for stream %s with %d fields", streamName, len(cache.Fields))
	return stream, nil
}

// cacheSchema stores the stream schema in state for future use
func (s *S3) cacheSchema(stream *types.Stream, streamName string, files []FileObject) error {
	if s.state == nil {
		return nil // No state to cache to
	}

	// Find max LastModified from files
	var maxLastModified string
	for _, file := range files {
		if file.LastModified > maxLastModified {
			maxLastModified = file.LastModified
		}
	}

	// Build schema cache
	cache := SchemaCache{
		Fields:          make(map[string]types.DataType),
		CursorFields:    []string{},
		CachedAt:        maxLastModified, // Use max LastModified as cache timestamp
		MaxLastModified: maxLastModified,
	}

	// Extract fields from stream schema
	if stream.Schema != nil {
		stream.Schema.Properties.Range(func(key, value interface{}) bool {
			fieldName, ok := key.(string)
			if !ok {
				return true
			}
			prop, ok := value.(*types.Property)
			if !ok {
				return true
			}
			cache.Fields[fieldName] = prop.DataType()
			return true
		})
	}

	// Extract cursor fields
	if stream.AvailableCursorFields != nil {
		for _, cursorField := range stream.AvailableCursorFields.Array() {
			cache.CursorFields = append(cache.CursorFields, cursorField)
		}
	}

	// Serialize to JSON
	cacheJSON, err := json.Marshal(cache)
	if err != nil {
		return fmt.Errorf("failed to marshal schema cache: %w", err)
	}

	// Store in state
	cacheKey := fmt.Sprintf("schema_cache_%s", streamName)
	tempStream := types.NewStream(streamName, "s3", &s.config.BucketName)
	s.state.SetCursor(&types.ConfiguredStream{
		Stream: tempStream,
	}, cacheKey, string(cacheJSON))

	logger.Infof("Cached schema for stream %s with %d fields", streamName, len(cache.Fields))
	return nil
}
