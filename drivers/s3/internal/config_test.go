package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/stretchr/testify/assert"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config with all required fields",
			config: Config{
				BucketName:      "test-bucket",
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				FileFormat:      FormatCSV,
			},
			wantErr: false,
		},
		{
			name: "missing bucket name",
			config: Config{
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				FileFormat:      FormatCSV,
			},
			wantErr: true,
			errMsg:  "bucket_name is required",
		},
		{
			name: "missing region without custom endpoint",
			config: Config{
				BucketName:      "test-bucket",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				FileFormat:      FormatCSV,
			},
			wantErr: true,
			errMsg:  "region is required when not using custom endpoint",
		},
		{
			name: "missing credentials",
			config: Config{
				BucketName: "test-bucket",
				Region:     "us-east-1",
				FileFormat: FormatCSV,
			},
			wantErr: true,
			errMsg:  "access_key_id and secret_access_key are required",
		},
		{
			name: "missing file format",
			config: Config{
				BucketName:      "test-bucket",
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
			},
			wantErr: true,
			errMsg:  "file_format is required",
		},
		{
			name: "invalid file format",
			config: Config{
				BucketName:      "test-bucket",
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				FileFormat:      "xml",
			},
			wantErr: true,
			errMsg:  "invalid file_format",
		},
		{
			name: "invalid compression type",
			config: Config{
				BucketName:      "test-bucket",
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				FileFormat:      FormatCSV,
				Compression:     "bzip2",
			},
			wantErr: true,
			errMsg:  "invalid compression",
		},
		{
			name: "valid config with custom endpoint",
			config: Config{
				BucketName:      "test-bucket",
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				FileFormat:      FormatJSON,
				Endpoint:        "http://localhost:9000",
			},
			wantErr: false,
		},
		{
			name: "valid config with compression",
			config: Config{
				BucketName:      "test-bucket",
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				FileFormat:      FormatCSV,
				Compression:     CompressionGzip,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	config := Config{
		BucketName:      "test-bucket",
		Region:          "us-east-1",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		FileFormat:      FormatCSV,
	}

	err := config.Validate()
	assert.NoError(t, err)

	// Check defaults
	assert.Equal(t, CompressionNone, config.Compression, "default compression should be 'none'")
	assert.Equal(t, ",", config.Delimiter, "default CSV delimiter should be ','")
	assert.Equal(t, "\"", config.QuoteCharacter, "default quote character should be '\"'")
	assert.Equal(t, true, config.HasHeader, "default has_header should be true")
	assert.Equal(t, 10000, config.BatchSize, "default batch size should be 10000")
	assert.Equal(t, constants.DefaultThreadCount, config.MaxThreads, "default max threads should match constant")
	assert.Equal(t, constants.DefaultRetryCount, config.RetryCount, "default retry count should match constant")
}

func TestConfigCSVDefaults(t *testing.T) {
	config := Config{
		BucketName:      "test-bucket",
		Region:          "us-east-1",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		FileFormat:      FormatCSV,
	}

	err := config.Validate()
	assert.NoError(t, err)

	assert.Equal(t, ",", config.Delimiter)
	assert.Equal(t, "\"", config.QuoteCharacter)
	assert.Equal(t, true, config.HasHeader)
}

func TestConfigJSONDefaults(t *testing.T) {
	config := Config{
		BucketName:      "test-bucket",
		Region:          "us-east-1",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		FileFormat:      FormatJSON,
	}

	err := config.Validate()
	assert.NoError(t, err)

	assert.Equal(t, true, config.JSONLineDelimited)
}

func TestConfigPathPrefixNormalization(t *testing.T) {
	tests := []struct {
		name           string
		pathPrefix     string
		expectedPrefix string
	}{
		{
			name:           "path with leading slash",
			pathPrefix:     "/data/files",
			expectedPrefix: "data/files",
		},
		{
			name:           "path with trailing slash",
			pathPrefix:     "data/files/",
			expectedPrefix: "data/files",
		},
		{
			name:           "path with both slashes",
			pathPrefix:     "/data/files/",
			expectedPrefix: "data/files",
		},
		{
			name:           "path without slashes",
			pathPrefix:     "data/files",
			expectedPrefix: "data/files",
		},
		{
			name:           "empty path",
			pathPrefix:     "",
			expectedPrefix: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				BucketName:      "test-bucket",
				Region:          "us-east-1",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				FileFormat:      FormatCSV,
				PathPrefix:      tt.pathPrefix,
			}

			err := config.Validate()
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedPrefix, config.PathPrefix)
		})
	}
}
