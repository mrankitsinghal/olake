package driver

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
)

// S3 represents the S3 source driver
type S3 struct {
	client          *s3.Client
	config          *Config
	state           *types.State
	filePattern     *regexp.Regexp
	discoveredFiles []FileObject
}

// GetConfigRef returns a reference to the config struct
func (s *S3) GetConfigRef() abstract.Config {
	s.config = &Config{}
	return s.config
}

// Spec returns the configuration specification
func (s *S3) Spec() any {
	return Config{}
}

// Type returns the driver type identifier
func (s *S3) Type() string {
	return "s3"
}

// Setup initializes the S3 client and validates the configuration
func (s *S3) Setup(ctx context.Context) error {
	// Validate configuration
	if err := s.config.Validate(); err != nil {
		return fmt.Errorf("failed to validate config: %w", err)
	}

	// Compile file pattern regex if provided
	if s.config.FilePattern != "" {
		pattern, err := regexp.Compile(s.config.FilePattern)
		if err != nil {
			return fmt.Errorf("failed to compile file_pattern regex: %w", err)
		}
		s.filePattern = pattern
		logger.Infof("Using file pattern filter: %s", s.config.FilePattern)
	}

	// Configure AWS SDK with static credentials
	var cfg aws.Config
	var err error

	if s.config.Endpoint != "" {
		// Custom endpoint (e.g., MinIO, LocalStack)
		logger.Infof("Connecting to S3-compatible endpoint: %s", s.config.Endpoint)
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(s.config.Region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				s.config.AccessKeyID,
				s.config.SecretAccessKey,
				"",
			)),
		)
	} else {
		// Standard AWS S3
		logger.Infof("Connecting to AWS S3 in region: %s", s.config.Region)
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(s.config.Region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				s.config.AccessKeyID,
				s.config.SecretAccessKey,
				"",
			)),
		)
	}

	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	if s.config.Endpoint != "" {
		s.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(s.config.Endpoint)
			o.UsePathStyle = true // Required for MinIO and some S3-compatible services
		})
	} else {
		s.client = s3.NewFromConfig(cfg)
	}

	// Test connection by checking if bucket exists and is accessible
	logger.Infof("Testing connection to bucket: %s", s.config.BucketName)
	_, err = s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.config.BucketName),
	})
	if err != nil {
		return fmt.Errorf("failed to access bucket %s: %w", s.config.BucketName, err)
	}

	logger.Info("Successfully connected to S3")
	return nil
}

// SetupState sets the state reference for tracking sync progress
func (s *S3) SetupState(state *types.State) {
	s.state = state
}

// MaxConnections returns the maximum number of concurrent connections
func (s *S3) MaxConnections() int {
	return s.config.MaxThreads
}

// MaxRetries returns the maximum number of retry attempts
func (s *S3) MaxRetries() int {
	return s.config.RetryCount
}

// GetStreamNames discovers all files in the S3 bucket matching the configuration
func (s *S3) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Discovering files in bucket: %s with prefix: %s", s.config.BucketName, s.config.PathPrefix)

	var files []FileObject
	var continuationToken *string

	// List all objects with the given prefix
	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.config.BucketName),
			Prefix:            aws.String(s.config.PathPrefix),
			ContinuationToken: continuationToken,
		}

		result, err := s.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in bucket: %w", err)
		}

		// Filter and collect matching files
		for _, obj := range result.Contents {
			key := aws.ToString(obj.Key)

			// Skip directories (keys ending with /)
			if strings.HasSuffix(key, "/") {
				continue
			}

			// Apply file pattern filter if configured
			if s.filePattern != nil && !s.filePattern.MatchString(key) {
				logger.Debugf("Skipping file %s (does not match pattern)", key)
				continue
			}

			// Filter by file extension based on format
			if !s.matchesFileFormat(key) {
				logger.Debugf("Skipping file %s (does not match format)", key)
				continue
			}

			files = append(files, FileObject{
				FileKey:      key,
				Size:         aws.ToInt64(obj.Size),
				LastModified: obj.LastModified.Format("2006-01-02T15:04:05Z"),
				ETag:         strings.Trim(aws.ToString(obj.ETag), "\""),
			})
		}

		// Check if there are more results
		if !aws.ToBool(result.IsTruncated) {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	logger.Infof("Discovered %d files matching criteria", len(files))
	s.discoveredFiles = files

	// Return file keys as stream names
	streamNames := make([]string, len(files))
	for i, file := range files {
		streamNames[i] = file.FileKey
	}

	return streamNames, nil
}

// matchesFileFormat checks if a file key matches the configured file format
func (s *S3) matchesFileFormat(key string) bool {
	lowerKey := strings.ToLower(key)

	switch s.config.FileFormat {
	case FormatCSV:
		return strings.HasSuffix(lowerKey, ".csv") ||
			(s.config.Compression == CompressionGzip && strings.HasSuffix(lowerKey, ".csv.gz"))
	case FormatJSON:
		return strings.HasSuffix(lowerKey, ".json") ||
			strings.HasSuffix(lowerKey, ".jsonl") ||
			(s.config.Compression == CompressionGzip && (strings.HasSuffix(lowerKey, ".json.gz") || strings.HasSuffix(lowerKey, ".jsonl.gz")))
	case FormatParquet:
		return strings.HasSuffix(lowerKey, ".parquet")
	default:
		return false
	}
}

// ProduceSchema generates schema for a given file (stream)
func (s *S3) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	logger.Infof("Producing schema for file: %s", streamName)

	// Create stream with file key as name
	stream := types.NewStream(streamName, "s3", &s.config.BucketName)

	// Get file object for metadata
	var fileObj *FileObject
	for _, obj := range s.discoveredFiles {
		if obj.FileKey == streamName {
			fileObj = &obj
			break
		}
	}

	if fileObj == nil {
		return nil, fmt.Errorf("file not found in discovered files: %s", streamName)
	}

	// Infer schema based on file format
	switch s.config.FileFormat {
	case FormatCSV:
		return s.inferCSVSchema(ctx, stream, streamName)
	case FormatJSON:
		return s.inferJSONSchema(ctx, stream, streamName)
	case FormatParquet:
		return s.inferParquetSchema(ctx, stream, streamName)
	default:
		return nil, fmt.Errorf("unsupported file format: %s", s.config.FileFormat)
	}
}

// CDCSupported returns false as S3 does not support CDC
func (s *S3) CDCSupported() bool {
	return false
}

// PreCDC is not supported for S3
func (s *S3) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	return fmt.Errorf("CDC is not supported for S3 source")
}

// StreamChanges is not supported for S3
func (s *S3) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn abstract.CDCMsgFn) error {
	return fmt.Errorf("CDC is not supported for S3 source")
}

// PostCDC is not supported for S3
func (s *S3) PostCDC(ctx context.Context, stream types.StreamInterface, success bool) error {
	return fmt.Errorf("CDC is not supported for S3 source")
}

// CloseConnection closes the S3 client connection
func (s *S3) CloseConnection() {
	logger.Info("Closing S3 connection")
	// S3 client doesn't require explicit cleanup
}
