package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// StreamIncrementalChanges processes only new or modified files since last sync
func (s *S3) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb abstract.BackfillMsgFn) error {
	streamName := stream.Name()
	logger.Infof("Checking for incremental changes for file: %s", streamName)

	// Get the file object
	var fileObj *FileObject
	for i := range s.discoveredFiles {
		if s.discoveredFiles[i].FileKey == streamName {
			fileObj = &s.discoveredFiles[i]
			break
		}
	}

	if fileObj == nil {
		return fmt.Errorf("file not found: %s", streamName)
	}

	// Get configured stream to access state
	configuredStream, ok := stream.(*types.ConfiguredStream)
	if !ok {
		logger.Warnf("Failed to cast to ConfiguredStream, treating as new file")
		return s.ChunkIterator(ctx, stream, types.Chunk{Min: streamName, Max: nil}, cb)
	}

	// Get last synced ETag from state
	lastSyncedETag := s.state.GetCursor(configuredStream, "last_etag")
	if lastSyncedETag == nil {
		// No previous state, this is the first sync - treat as backfill
		logger.Infof("No previous state found for %s, treating as new file", streamName)
		// Process the file
		err := s.ChunkIterator(ctx, stream, types.Chunk{Min: streamName, Max: nil}, cb)
		if err != nil {
			return err
		}
		// Save state after successful sync
		s.state.SetCursor(configuredStream, "last_etag", fileObj.ETag)
		s.state.SetCursor(configuredStream, "last_modified", fileObj.LastModified)
		return nil
	}

	// Compare ETags
	lastETag, ok := lastSyncedETag.(string)
	if ok && lastETag == fileObj.ETag {
		logger.Infof("File %s has not changed (ETag match), skipping", streamName)
		return nil
	}

	logger.Infof("File %s has changed (ETag mismatch: %s != %s), syncing", streamName, lastETag, fileObj.ETag)
	// Process the file
	err := s.ChunkIterator(ctx, stream, types.Chunk{Min: streamName, Max: nil}, cb)
	if err != nil {
		return err
	}
	// Update state after successful sync
	s.state.SetCursor(configuredStream, "last_etag", fileObj.ETag)
	s.state.SetCursor(configuredStream, "last_modified", fileObj.LastModified)
	return nil
}
