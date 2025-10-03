package driver

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// GetOrSplitChunks returns chunks for parallel processing of files
// For S3, we treat each file as a single chunk (no splitting within files for now)
func (s *S3) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	// Each file is one chunk
	chunks := types.NewSet[types.Chunk]()

	// The stream name is the S3 key
	streamName := stream.Name()

	// Find the file object
	var fileObj *FileObject
	for i := range s.discoveredFiles {
		if s.discoveredFiles[i].FileKey == streamName {
			fileObj = &s.discoveredFiles[i]
			break
		}
	}

	if fileObj == nil {
		return nil, fmt.Errorf("file not found: %s", streamName)
	}

	// Add estimated row count to stats (rough estimate based on file size)
	// Assuming average row size of 100 bytes for estimation
	estimatedRows := fileObj.Size / 100
	if estimatedRows < 1 {
		estimatedRows = 1
	}
	pool.AddRecordsToSyncStats(estimatedRows)

	// Create a single chunk for the entire file
	chunks.Insert(types.Chunk{
		Min: streamName, // Start of file
		Max: nil,        // End of file
	})

	return chunks, nil
}

// ChunkIterator reads and processes records from an S3 file
func (s *S3) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn abstract.BackfillMsgFn) error {
	key := chunk.Min.(string)
	logger.Infof("Processing file: %s", key)

	// Get the object from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	// Wrap with decompression reader if needed
	reader, err := s.getReader(result.Body, key)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	// Process based on file format
	switch s.config.FileFormat {
	case FormatCSV:
		return s.processCSVFile(ctx, reader, stream, processFn)
	case FormatJSON:
		return s.processJSONFile(ctx, reader, processFn)
	case FormatParquet:
		return s.processParquetFile(ctx, reader, processFn)
	default:
		return fmt.Errorf("unsupported file format: %s", s.config.FileFormat)
	}
}

// processCSVFile reads and processes a CSV file
func (s *S3) processCSVFile(ctx context.Context, reader io.Reader, stream types.StreamInterface, processFn abstract.BackfillMsgFn) error {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = rune(s.config.Delimiter[0])
	if s.config.QuoteCharacter != "" {
		csvReader.LazyQuotes = true
	}

	// Skip rows if configured
	for i := 0; i < s.config.SkipRows; i++ {
		_, err := csvReader.Read()
		if err != nil {
			return fmt.Errorf("failed to skip row %d: %w", i, err)
		}
	}

	var headers []string
	if s.config.HasHeader {
		// Read header row
		var err error
		headers, err = csvReader.Read()
		if err != nil {
			return fmt.Errorf("failed to read CSV headers: %w", err)
		}
	} else {
		// Generate default column names based on first row
		firstRow, err := csvReader.Read()
		if err != nil {
			return fmt.Errorf("failed to read first row: %w", err)
		}
		for i := range firstRow {
			headers = append(headers, fmt.Sprintf("column_%d", i))
		}
		// Process the first row as data
		record := make(map[string]any)
		for i, value := range firstRow {
			if i < len(headers) {
				fieldType, _ := stream.Schema().GetType(headers[i])
				convertedValue := s.convertValue(value, fieldType)
				record[headers[i]] = convertedValue
			}
		}
		if err := processFn(ctx, record); err != nil {
			return fmt.Errorf("failed to process first record: %w", err)
		}
	}

	// Process records in batches
	batch := make([]map[string]any, 0, s.config.BatchSize)
	recordCount := 0

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Warnf("Error reading CSV row %d: %v", recordCount, err)
			continue
		}

		// Convert row to map
		record := make(map[string]any)
		for i, value := range row {
			if i < len(headers) {
				// Convert value based on schema type
				fieldType, _ := stream.Schema().GetType(headers[i])
				convertedValue := s.convertValue(value, fieldType)
				record[headers[i]] = convertedValue
			}
		}

		batch = append(batch, record)
		recordCount++

		// Process batch when it reaches batch size
		if len(batch) >= s.config.BatchSize {
			for _, rec := range batch {
				if err := processFn(ctx, rec); err != nil {
					return fmt.Errorf("failed to process record: %w", err)
				}
			}
			batch = batch[:0] // Reset batch
		}
	}

	// Process remaining records in batch
	for _, rec := range batch {
		if err := processFn(ctx, rec); err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
	}

	logger.Infof("Processed %d records from CSV file", recordCount)
	return nil
}

// processJSONFile reads and processes a JSON file
func (s *S3) processJSONFile(ctx context.Context, reader io.Reader, processFn abstract.BackfillMsgFn) error {
	recordCount := 0

	if s.config.JSONLineDelimited {
		// Line-delimited JSON (JSONL)
		decoder := json.NewDecoder(reader)

		for {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			var record map[string]interface{}
			if err := decoder.Decode(&record); err == io.EOF {
				break
			} else if err != nil {
				logger.Warnf("Error reading JSON record %d: %v", recordCount, err)
				continue
			}

			if err := processFn(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %w", err)
			}
			recordCount++
		}
	} else {
		// JSON array
		var records []map[string]interface{}
		decoder := json.NewDecoder(reader)
		if err := decoder.Decode(&records); err != nil {
			return fmt.Errorf("failed to decode JSON array: %w", err)
		}

		for _, record := range records {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if err := processFn(ctx, record); err != nil {
				return fmt.Errorf("failed to process record: %w", err)
			}
			recordCount++
		}
	}

	logger.Infof("Processed %d records from JSON file", recordCount)
	return nil
}

// processParquetFile reads and processes a Parquet file
func (s *S3) processParquetFile(ctx context.Context, reader io.Reader, processFn abstract.BackfillMsgFn) error {
	// TODO: Implement Parquet file processing
	return fmt.Errorf("Parquet format support is not yet implemented")
}

// convertValue converts a string value to the appropriate type
func (s *S3) convertValue(value string, fieldType types.DataType) interface{} {
	if value == "" || value == "null" {
		return nil
	}

	switch fieldType {
	case types.Int64:
		if intVal, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
			return intVal
		}
	case types.Float64:
		if floatVal, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
			return floatVal
		}
	case types.Bool:
		lowerVal := strings.ToLower(strings.TrimSpace(value))
		if lowerVal == "true" || lowerVal == "1" {
			return true
		}
		if lowerVal == "false" || lowerVal == "0" {
			return false
		}
	}

	// Default to string
	return value
}
