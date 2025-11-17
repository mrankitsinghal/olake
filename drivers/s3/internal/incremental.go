package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// FetchMaxCursorValues returns the maximum LastModified timestamp for all files in the stream
func (s *S3) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	streamName := stream.Name()
	
	files, exists := s.discoveredFiles[streamName]
	if !exists || len(files) == 0 {
		return nil, nil, nil
	}

	// Find the latest LastModified timestamp among all files in this stream
	var maxLastModified string
	for _, file := range files {
		if file.LastModified > maxLastModified {
			maxLastModified = file.LastModified
		}
	}

	// Return as primary cursor (secondary cursor not used for S3)
	return maxLastModified, nil, nil
}

// StreamIncrementalChanges processes only new or modified files since last sync
func (s *S3) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
	streamName := stream.Name()
	files, exists := s.discoveredFiles[streamName]
	if !exists || len(files) == 0 {
		return fmt.Errorf("no files found for stream: %s", streamName)
	}

	logger.Infof("Checking for incremental changes for stream: %s (%d files)", streamName, len(files))

	// Get configured stream to access state
	configuredStream, ok := stream.(*types.ConfiguredStream)
	if !ok {
		logger.Warnf("Failed to cast to ConfiguredStream, syncing all files")
		return s.syncAllFiles(ctx, stream, files, cb)
	}

	// Get last synced timestamp from state (primary cursor)
	primaryCursor, _ := configuredStream.Cursor()
	lastSyncedTimestamp := s.state.GetCursor(configuredStream, primaryCursor)

	if lastSyncedTimestamp == nil {
		// First sync - process all files
		logger.Infof("No previous state for stream %s, syncing all %d files", streamName, len(files))
		return s.syncAllFiles(ctx, stream, files, cb)
	}

	// Parse last synced timestamp
	lastSynced, ok := lastSyncedTimestamp.(string)
	if !ok {
		logger.Warnf("Invalid cursor format in state for stream %s, syncing all files", streamName)
		return s.syncAllFiles(ctx, stream, files, cb)
	}

	// Filter files modified after last sync
	var filesToSync []FileObject
	for _, file := range files {
		if file.LastModified > lastSynced {
			filesToSync = append(filesToSync, file)
			logger.Debugf("File %s modified (%s > %s), will sync", file.FileKey, file.LastModified, lastSynced)
		}
	}

	if len(filesToSync) == 0 {
		logger.Infof("No new or modified files for stream %s (last synced: %s)", streamName, lastSynced)
		return nil
	}

	logger.Infof("Syncing %d modified files for stream %s (out of %d total)", len(filesToSync), streamName, len(files))
	return s.syncAllFiles(ctx, stream, filesToSync, cb)
}

// syncAllFiles processes a list of files sequentially
func (s *S3) syncAllFiles(ctx context.Context, stream types.StreamInterface, files []FileObject, cb abstract.BackfillMsgFn) error {
	for i, file := range files {
		logger.Infof("Processing file %d/%d: %s", i+1, len(files), file.FileKey)

		// Pass file metadata (size) in chunk for range requests
		chunk := types.Chunk{
			Min: file.FileKey,  // File key
			Max: file.Size,     // File size for range requests
		}
		if err := s.ChunkIterator(ctx, stream, chunk, cb); err != nil {
			return fmt.Errorf("failed to process file %s: %w", file.FileKey, err)
		}
	}

	return nil
}
