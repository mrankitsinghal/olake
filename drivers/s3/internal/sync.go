package driver

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/drivers/parser"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// ============================================
// SECTION 1: Cursor & Filtering
// ============================================
// This section handles the unified cursor-based filtering logic
// that enables both backfill (cursor="") and incremental (cursor=timestamp) modes

// getCursorFromState retrieves the cursor timestamp from state
// Returns empty string for backfill (no state), or the last synced timestamp for incremental
//
// Backfill mode: No cursor configured or no previous state → returns ""
// Incremental mode: Cursor exists in state → returns timestamp (e.g., "2024-01-02T10:00:00Z")
func (s *S3) getCursorFromState(stream types.StreamInterface) string {
	// Try to cast to ConfiguredStream to access state
	configuredStream, ok := stream.(*types.ConfiguredStream)
	if !ok {
		// Not a configured stream, this is backfill - return empty string (process all files)
		logger.Debugf("Stream %s is not configured, using backfill mode (cursor: epoch)", stream.Name())
		return ""
	}

	// Get primary cursor field from stream configuration
	primaryCursor, _ := configuredStream.Cursor()
	if primaryCursor == "" {
		// No cursor configured, this is backfill
		logger.Debugf("Stream %s has no cursor configured, using backfill mode (cursor: epoch)", stream.Name())
		return ""
	}

	// Get cursor value from state
	cursorValue := s.state.GetCursor(configuredStream, primaryCursor)
	if cursorValue == nil {
		// First sync - return empty string (process all files)
		logger.Infof("Stream %s has no previous state, using backfill mode (cursor: epoch)", stream.Name())
		return ""
	}

	// Parse cursor as string (timestamp format: 2006-01-02T15:04:05Z)
	lastSynced, ok := cursorValue.(string)
	if !ok {
		logger.Warnf("Invalid cursor format in state for stream %s, using backfill mode", stream.Name())
		return ""
	}

	logger.Infof("Stream %s using incremental mode (cursor: %s)", stream.Name(), lastSynced)
	return lastSynced
}

// filterFilesByCursor filters files based on LastModified timestamp cursor
// This is the core logic that unifies backfill and incremental sync:
//
// For backfill (empty cursor):     Returns all files
// For incremental (cursor != ""):  Returns only files where LastModified > cursor
//
// The cursor timestamp is in ISO 8601 format (2006-01-02T15:04:05Z) and uses
// string comparison which works correctly for this format.
func (s *S3) filterFilesByCursor(files []FileObject, cursorTimestamp string) []FileObject {
	// If no cursor (backfill), return all files
	if cursorTimestamp == "" {
		logger.Debugf("No cursor timestamp, processing all %d files (backfill mode)", len(files))
		return files
	}

	// Filter files modified after cursor
	var filteredFiles []FileObject
	for _, file := range files {
		if file.LastModified > cursorTimestamp {
			filteredFiles = append(filteredFiles, file)
			logger.Debugf("File %s will be processed (modified: %s > cursor: %s)", 
				file.FileKey, file.LastModified, cursorTimestamp)
		} else {
			logger.Debugf("File %s skipped (modified: %s <= cursor: %s)", 
				file.FileKey, file.LastModified, cursorTimestamp)
		}
	}

	logger.Infof("Filtered %d files to process out of %d total (incremental mode, cursor: %s)", 
		len(filteredFiles), len(files), cursorTimestamp)
	return filteredFiles
}

// ============================================
// SECTION 2: Chunk Management
// ============================================
// This section handles grouping files into optimized chunks for parallel processing

// GetOrSplitChunks returns chunks for parallel processing of files
// Unified implementation: filters files by cursor (epoch for backfill, state value for incremental)
// Groups multiple small files into ~2GB chunks to reduce state size
// Large files (>2GB) are kept as individual chunks
func (s *S3) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	streamName := stream.Name()
	files, exists := s.discoveredFiles[streamName]

	if !exists || len(files) == 0 {
		logger.Warnf("No files found for stream %s, returning empty chunk set", streamName)
		return types.NewSet[types.Chunk](), nil
	}

	// Get cursor from state for incremental filtering
	// For backfill (no state), this returns empty string which means all files pass the filter
	cursorTimestamp := s.getCursorFromState(stream)
	
	// Filter files based on LastModified cursor
	filesToProcess := s.filterFilesByCursor(files, cursorTimestamp)
	
	if len(filesToProcess) == 0 {
		logger.Infof("No files to process for stream %s after cursor filtering (cursor: %s)", streamName, cursorTimestamp)
		return types.NewSet[types.Chunk](), nil
	}

	logger.Infof("Processing %d files for stream %s (filtered from %d total, cursor: %s)", 
		len(filesToProcess), streamName, len(files), cursorTimestamp)

	// Calculate total file size across files to process
	var totalSize int64
	for _, file := range filesToProcess {
		totalSize += file.Size
	}

	// Convert to human-readable format
	sizeInMB := float64(totalSize) / (1024 * 1024)
	var sizeDisplay string
	if sizeInMB < 1 {
		sizeDisplay = fmt.Sprintf("%.2f KB", float64(totalSize)/1024)
	} else if sizeInMB < 1024 {
		sizeDisplay = fmt.Sprintf("%.2f MB", sizeInMB)
	} else {
		sizeDisplay = fmt.Sprintf("%.2f GB", sizeInMB/1024)
	}

	// Use file count as a proxy for sync stats instead of estimated rows
	pool.AddRecordsToSyncStats(int64(len(filesToProcess)))

	// Group files into chunks based on size to optimize state storage and parallel processing
	chunks := s.groupFilesIntoChunks(filesToProcess)

	logger.Infof("Creating %d chunks for stream %s (total files: %d, total size: %s)",
		chunks.Len(), streamName, len(filesToProcess), sizeDisplay)

	return chunks, nil
}

// groupFilesIntoChunks groups multiple small files into ~2GB chunks while keeping large files separate
// This reduces state size (fewer chunks to track) while maintaining parallel processing efficiency
func (s *S3) groupFilesIntoChunks(files []FileObject) *types.Set[types.Chunk] {
	const targetChunkSize int64 = 2 * 1024 * 1024 * 1024 // 2GB target chunk size
	const largeFileThreshold int64 = targetChunkSize      // Files > 2GB get their own chunk

	chunks := types.NewSet[types.Chunk]()
	var currentChunkFiles []string
	var currentChunkSize int64

	for _, file := range files {
		// Large files get their own chunk (single file per chunk)
		if file.Size >= largeFileThreshold {
			// Flush any accumulated small files first
			if len(currentChunkFiles) > 0 {
				chunks.Insert(types.Chunk{
					Min: currentChunkFiles,  // Array of file keys
					Max: currentChunkSize,   // Total size of files in this chunk
				})
				currentChunkFiles = nil
				currentChunkSize = 0
			}

			// Add large file as separate chunk (array with single element for consistency)
			chunks.Insert(types.Chunk{
				Min: []string{file.FileKey},  // Array of one file key (consistent with small files)
				Max: file.Size,               // File size
			})
			logger.Debugf("Large file (%d MB) in separate chunk: %s", 
				file.Size/(1024*1024), file.FileKey)
			continue
		}

		// Group small files into chunks
		if currentChunkSize+file.Size > targetChunkSize && len(currentChunkFiles) > 0 {
			// Current chunk would exceed target size, flush it
			chunks.Insert(types.Chunk{
				Min: currentChunkFiles,  // Array of file keys
				Max: currentChunkSize,   // Total size of files in this chunk
			})
			logger.Debugf("Created chunk with %d files (%d MB)", 
				len(currentChunkFiles), currentChunkSize/(1024*1024))
			currentChunkFiles = nil
			currentChunkSize = 0
		}

		// Add file to current chunk
		currentChunkFiles = append(currentChunkFiles, file.FileKey)
		currentChunkSize += file.Size
	}

	// Flush remaining files
	if len(currentChunkFiles) > 0 {
		chunks.Insert(types.Chunk{
			Min: currentChunkFiles,  // Array of file keys
			Max: currentChunkSize,   // Total size of files in this chunk
		})
		logger.Debugf("Created final chunk with %d files (%d MB)", 
			len(currentChunkFiles), currentChunkSize/(1024*1024))
	}

	return chunks
}

// ============================================
// SECTION 3: Chunk Processing
// ============================================
// This section handles reading and processing records from S3 files

// ChunkIterator reads and processes records from an S3 file or multiple files
// Handles chunks as arrays (Min = []string): single file = array size 1, grouped files = array size > 1
func (s *S3) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn abstract.BackfillMsgFn) error {
	// Check if this is a typed array or interface array (from JSON state deserialization)
	switch v := chunk.Min.(type) {
	case []string:
		// Chunk with file(s) - process each file sequentially
		// Single large file: array size = 1
		// Grouped small files: array size > 1
		// TODO: Consider parallelizing file processing within chunks for better performance
		// OR simplify by making each file its own chunk (remove grouping logic entirely)
		logger.Infof("Processing chunk with %d file(s)", len(v))
		for i, key := range v {
			logger.Debugf("Processing file %d/%d in chunk: %s", i+1, len(v), key)
			fileSize := s.getFileSize(stream.Name(), key)
			if err := s.processFile(ctx, stream, key, fileSize, processFn); err != nil {
				return fmt.Errorf("failed to process file %s in chunk: %w", key, err)
			}
		}
		return nil

	case []interface{}:
		// Handle []interface{} from state deserialization after restart
		// JSON unmarshaling converts []string to []interface{} when Min is type `any`
		logger.Infof("Processing chunk with %d file(s) (from state)", len(v))
		for i, item := range v {
			key, ok := item.(string)
			if !ok {
				return fmt.Errorf("invalid file key type in chunk: expected string, got %T", item)
			}
			logger.Debugf("Processing file %d/%d in chunk: %s", i+1, len(v), key)
			fileSize := s.getFileSize(stream.Name(), key)
			if err := s.processFile(ctx, stream, key, fileSize, processFn); err != nil {
				return fmt.Errorf("failed to process file %s in chunk: %w", key, err)
			}
		}
		return nil

	default:
		return fmt.Errorf("invalid chunk Min type: expected []string, got %T", chunk.Min)
	}
}

// processFile handles the processing of a single S3 file using the parser package
func (s *S3) processFile(ctx context.Context, stream types.StreamInterface, key string, fileSize int64, processFn abstract.BackfillMsgFn) error {
	// Get reader for the file (S3-specific: handles S3 API, decompression, range requests)
	reader, err := s.getStreamReader(ctx, key, fileSize)
	if err != nil {
		// Check if file was deleted between discovery and processing
		if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "NotFound") {
			logger.Warnf("File %s was deleted or not found, skipping", key)
			return nil // Don't fail the entire sync for a missing file
		}
		return fmt.Errorf("failed to get reader: %w", err)
	}
	defer func() {
		if closer, ok := reader.(io.Closer); ok {
			closer.Close()
		}
	}()

	// Create callback adapter from abstract.BackfillMsgFn to parser.RecordCallback
	callback := func(ctx context.Context, record map[string]any) error {
		return processFn(ctx, record)
	}

	// Use appropriate parser based on file format
	// Note: stream is StreamInterface, we need to get the underlying Stream for the parser
	underlyingStream := &types.Stream{
		Name:      stream.Name(),
		Namespace: stream.Namespace(),
		Schema:    stream.Schema(),
	}
	
	var parseErr error
	switch s.config.FileFormat {
	case FormatCSV:
		csvParser := parser.NewCSVParser(*s.config.GetCSVConfig(), underlyingStream)
		parseErr = csvParser.StreamRecords(ctx, reader, callback)
	case FormatJSON:
		jsonParser := parser.NewJSONParser(*s.config.GetJSONConfig(), underlyingStream)
		parseErr = jsonParser.StreamRecords(ctx, reader, callback)
	case FormatParquet:
		// For Parquet, use the streaming implementation with range requests if enabled
		parquetConfig := s.config.GetParquetConfig()
		if parquetConfig.StreamingEnabled && fileSize > 0 {
			// Use S3 range reader for streaming
			return s.processParquetFileStreamingWithParser(ctx, stream, key, fileSize, processFn)
		}
		// Fallback: load into memory and parse
		parquetParser := parser.NewParquetParser(*parquetConfig, underlyingStream)
		parseErr = parquetParser.StreamRecords(ctx, reader, callback)
	default:
		return fmt.Errorf("unsupported file format: %s", s.config.FileFormat)
	}

	if parseErr != nil {
		return fmt.Errorf("failed to process file %s: %w", key, parseErr)
	}

	return nil
}

// getStreamReader returns a reader for streaming file data (S3-specific logic)
func (s *S3) getStreamReader(ctx context.Context, key string, fileSize int64) (io.Reader, error) {
	// Get the object from S3
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from S3: %w", err)
	}

	// Apply decompression if needed (auto-detect from file extension)
	reader, err := s.getReader(result.Body, key)
	if err != nil {
		result.Body.Close()
		return nil, fmt.Errorf("failed to create decompressed reader: %w", err)
	}

	return reader, nil
}

// processParquetFileStreamingWithParser uses S3 range requests with the parser package
func (s *S3) processParquetFileStreamingWithParser(ctx context.Context, stream types.StreamInterface, key string, fileSize int64, processFn abstract.BackfillMsgFn) error {
	logger.Infof("Processing Parquet file with streaming (S3 range requests): %s (size: %.2f MB)",
		key, float64(fileSize)/(1024*1024))

	// Create S3 range reader
	rangeReader := NewS3RangeReader(ctx, s.client, s.config.BucketName, key, fileSize)
	
	// Wrap reader to make it compatible with parser requirements
	wrapper := parser.NewParquetReaderWrapper(rangeReader, fileSize)

	// Create callback adapter
	callback := func(ctx context.Context, record map[string]any) error {
		return processFn(ctx, record)
	}

	// Use parser to stream records
	// Note: stream is StreamInterface, we need to get the underlying Stream for the parser
	underlyingStream := &types.Stream{
		Name:      stream.Name(),
		Namespace: stream.Namespace(),
		Schema:    stream.Schema(),
	}
	
	parquetParser := parser.NewParquetParser(*s.config.GetParquetConfig(), underlyingStream)
	return parquetParser.StreamRecords(ctx, wrapper, callback)
}

// getFileSize retrieves the file size from discovered files
// Returns 0 if file not found (fallback to non-streaming mode)
func (s *S3) getFileSize(streamName, fileKey string) int64 {
	files, exists := s.discoveredFiles[streamName]
	if !exists {
		return 0
	}

	for _, file := range files {
		if file.FileKey == fileKey {
			return file.Size
		}
	}

	return 0
}

// ============================================
// SECTION 4: Incremental-Specific Methods
// ============================================
// This section contains methods specific to incremental sync operations

// FetchMaxCursorValues returns the maximum LastModified timestamp for all files in the stream
// This is used by the abstract layer to track incremental sync progress
func (s *S3) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	streamName := stream.Name()
	
	files, exists := s.discoveredFiles[streamName]
	if !exists || len(files) == 0 {
		logger.Debugf("No files found for stream %s, returning nil cursor", streamName)
		return nil, nil, nil
	}

	// Find the latest LastModified timestamp among all files in this stream
	var maxLastModified string
	for _, file := range files {
		if file.LastModified > maxLastModified {
			maxLastModified = file.LastModified
		}
	}

	logger.Infof("Max cursor value for stream %s: %s (from %d files)", streamName, maxLastModified, len(files))
	
	// Return as primary cursor (secondary cursor not used for S3)
	return maxLastModified, nil, nil
}

// StreamIncrementalChanges is called by the abstract layer after backfill completes
// For S3, all incremental work is already done by GetOrSplitChunks + Backfill via cursor filtering:
//   1. GetOrSplitChunks filters files by cursor (empty for full-load, timestamp for incremental)
//   2. Backfill processes chunks in parallel via connection groups
//   3. This method becomes a no-op since all work is already complete
func (s *S3) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
	streamName := stream.Name()
	
	logger.Infof("Incremental sync already completed for stream %s via unified chunk processing", streamName)
	
	// All files have already been processed by the Backfill phase which:
	// - Called GetOrSplitChunks (with cursor-based filtering)
	// - Grouped files into optimized 2GB chunks
	// - Processed chunks in parallel via abstract layer connection groups
	//
	// No additional work needed here - cursor filtering ensures only new/modified files
	// were included in the chunks that were already processed.
	
	return nil
}

