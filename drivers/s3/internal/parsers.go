package driver

import (
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
	// TODO: Implement Parquet schema inference
	// This would require a Parquet library like github.com/xitongsys/parquet-go
	return nil, fmt.Errorf("Parquet format support is not yet implemented")
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
