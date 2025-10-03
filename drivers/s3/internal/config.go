package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/constants"
)

// FileFormat represents the format of files in S3
type FileFormat string

const (
	FormatCSV     FileFormat = "csv"
	FormatJSON    FileFormat = "json"
	FormatParquet FileFormat = "parquet"
)

// CompressionType represents the compression type of files
type CompressionType string

const (
	CompressionNone CompressionType = "none"
	CompressionGzip CompressionType = "gzip"
	CompressionZip  CompressionType = "zip"
)

// Config represents the configuration for S3 source connector
type Config struct {
	// AWS Connection details
	BucketName      string `json:"bucket_name"`
	Region          string `json:"region"`
	PathPrefix      string `json:"path_prefix"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Endpoint        string `json:"endpoint"` // Optional: for S3-compatible services like MinIO

	// File format configuration
	FileFormat  FileFormat      `json:"file_format"`
	Compression CompressionType `json:"compression"`

	// CSV specific configuration
	Delimiter      string `json:"delimiter"`       // Default: ","
	HasHeader      bool   `json:"has_header"`      // Default: true
	SkipRows       int    `json:"skip_rows"`       // Number of rows to skip at the beginning
	QuoteCharacter string `json:"quote_character"` // Default: "\""

	// JSON specific configuration
	JSONLineDelimited bool `json:"json_line_delimited"` // Default: true (JSONL format)

	// Performance tuning
	MaxThreads int `json:"max_threads"` // Number of concurrent file processors
	BatchSize  int `json:"batch_size"`  // Number of records to batch before sending
	RetryCount int `json:"retry_count"` // Number of retries for failed operations

	// File filtering
	FilePattern string `json:"file_pattern"` // Regex pattern to filter files (optional)
}

// Validate validates the S3 configuration
func (c *Config) Validate() error {
	// Validate bucket name
	if c.BucketName == "" {
		return fmt.Errorf("bucket_name is required")
	}

	// Validate region (only if not using custom endpoint)
	if c.Endpoint == "" && c.Region == "" {
		return fmt.Errorf("region is required when not using custom endpoint")
	}

	// Validate credentials
	if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return fmt.Errorf("access_key_id and secret_access_key are required")
	}

	// Validate file format
	if c.FileFormat == "" {
		return fmt.Errorf("file_format is required (csv, json, or parquet)")
	}

	validFormat := false
	for _, format := range []FileFormat{FormatCSV, FormatJSON, FormatParquet} {
		if c.FileFormat == format {
			validFormat = true
			break
		}
	}
	if !validFormat {
		return fmt.Errorf("invalid file_format: must be csv, json, or parquet")
	}

	// Set default values
	if c.Compression == "" {
		c.Compression = CompressionNone
	}

	// Validate compression type
	validCompression := false
	for _, compression := range []CompressionType{CompressionNone, CompressionGzip, CompressionZip} {
		if c.Compression == compression {
			validCompression = true
			break
		}
	}
	if !validCompression {
		return fmt.Errorf("invalid compression: must be none, gzip, or zip")
	}

	// CSV specific validation
	if c.FileFormat == FormatCSV {
		if c.Delimiter == "" {
			c.Delimiter = ","
		}
		if c.QuoteCharacter == "" {
			c.QuoteCharacter = "\""
		}
		// Default to true if not specified
		if !c.HasHeader {
			c.HasHeader = true
		}
	}

	// JSON specific defaults
	if c.FileFormat == FormatJSON {
		if !c.JSONLineDelimited {
			c.JSONLineDelimited = true
		}
	}

	// Set default batch size
	if c.BatchSize <= 0 {
		c.BatchSize = 10000
	}

	// Set default thread count
	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	// Set default retry count
	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount
	}

	// Normalize path prefix (remove leading/trailing slashes)
	if c.PathPrefix != "" {
		c.PathPrefix = strings.Trim(c.PathPrefix, "/")
	}

	return nil
}

// FileObject represents a file object in S3
type FileObject struct {
	FileKey      string `json:"file_key"`
	Size         int64  `json:"size"`
	LastModified string `json:"last_modified"`
	ETag         string `json:"etag"`
}
