package driver

import (
	"bytes"
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
	pq "github.com/parquet-go/parquet-go"
)

// GetOrSplitChunks returns chunks for parallel processing of files
// For S3, we treat each file as a single chunk (no splitting within files for now)
func (s *S3) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	streamName := stream.Name()
	files, exists := s.discoveredFiles[streamName]

	if !exists || len(files) == 0 {
		logger.Warnf("No files found for stream %s, returning empty chunk set", streamName)
		return types.NewSet[types.Chunk](), nil
	}

	chunks := types.NewSet[types.Chunk]()

	// Estimate total row count across all files in the stream
	var totalSize int64
	for _, file := range files {
		totalSize += file.Size
	}

	// Assuming average row size of 100 bytes for estimation
	estimatedRows := totalSize / 100
	if estimatedRows < 1 {
		estimatedRows = 1
	}
	pool.AddRecordsToSyncStats(estimatedRows)

	logger.Infof("Creating %d chunks for stream %s (total size: %d bytes, estimated rows: %d)",
		len(files), streamName, totalSize, estimatedRows)

	// Create one chunk per file (Phase 1 - no file splitting)
	for _, file := range files {
		chunks.Insert(types.Chunk{
			Min: file.FileKey, // File key as chunk identifier
			Max: nil,          // Single chunk per file
		})
	}

	return chunks, nil
}

// ChunkIterator reads and processes records from an S3 file
func (s *S3) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn abstract.BackfillMsgFn) error {
	key, ok := chunk.Min.(string)
	if !ok {
		return fmt.Errorf("invalid chunk Min type: expected string, got %T", chunk.Min)
	}

	logger.Infof("Processing file: %s", key)

	// Get the object from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check if file was deleted between discovery and processing
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "NotFound") {
			logger.Warnf("File %s was deleted or not found, skipping", key)
			return nil // Don't fail the entire sync for a missing file
		}
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
	// Read all data into memory (required for parquet-go)
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read parquet data: %w", err)
	}

	// Open parquet file
	pqFile, err := pq.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	// Create a generic reader for map[string]interface{}
	pqReader := pq.NewGenericReader[map[string]interface{}](pqFile)
	defer pqReader.Close()

	recordCount := 0
	// Read records one by one
	records := make([]map[string]interface{}, 1)
	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read next record
		n, err := pqReader.Read(records)
		if err == io.EOF || n == 0 {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read parquet record: %w", err)
		}

		// Process the record
		if err := processFn(ctx, records[0]); err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
		recordCount++
	}

	logger.Infof("Processed %d records from Parquet file", recordCount)
	return nil
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
