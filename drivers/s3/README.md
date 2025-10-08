# Olake S3 Source Driver

S3 source connector for Olake that enables ingesting data from Amazon S3 or S3-compatible storage (MinIO, LocalStack).

## Features

- ✅ CSV file support with header detection and type inference
- ✅ JSON support (line-delimited and array formats)
- ✅ Parquet file support with schema inference
- ✅ Gzip compression support
- ✅ Incremental sync with ETag-based change detection
- ✅ Configurable file pattern filtering
- ✅ Custom S3-compatible endpoints (MinIO, LocalStack)
- 🔜 Large file chunking (planned)

## Configuration

### Required Fields

- `bucket_name`: S3 bucket name
- `region`: AWS region (e.g., "us-east-1")
- `access_key_id`: AWS access key
- `secret_access_key`: AWS secret key
- `file_format`: File format - `csv`, `json`, or `parquet`

### Optional Fields

- `path_prefix`: S3 path prefix to filter files (default: "")
- `endpoint`: Custom S3 endpoint for MinIO/LocalStack (default: AWS S3)
- `compression`: Compression type - `none`, `gzip`, or `zip` (default: "none")
- `file_pattern`: Regex pattern to filter files (default: all files)
- `max_threads`: Number of concurrent file processors (default: 3)
- `batch_size`: Records per batch (default: 10000)
- `retry_count`: Number of retries for failed operations (default: 3)

### CSV-Specific Options

- `delimiter`: CSV delimiter (default: ",")
- `has_header`: Whether CSV has header row (default: true)
- `skip_rows`: Number of rows to skip at beginning (default: 0)
- `quote_character`: Quote character (default: "\"")

### JSON-Specific Options

- `json_line_delimited`: Use line-delimited JSON (JSONL) format (default: true)

## Quick Start

### 1. Start MinIO (for local testing)

```bash
cd drivers/s3
docker-compose up -d
```

Access MinIO Console at http://localhost:9001 (username: `minioadmin`, password: `minioadmin`)

### 2. Run Test Suite

```bash
cd drivers/s3
./test.sh
```

This will:
- Start MinIO
- Upload sample CSV and JSON files
- Build the S3 driver
- Test `spec`, `check`, and `discover` commands

### 3. Manual Testing

#### Spec Command
```bash
./olake spec
```

#### Check Connection
```bash
./olake check --config test-config/config.json
```

#### Discover Files
```bash
./olake discover --config test-config/config.json > test-config/streams.json
```

#### Sync Data
```bash
./olake sync \
  --config test-config/config.json \
  --destination test-config/writer.json \
  --catalog test-config/streams.json
```

## Example Configurations

### AWS S3 with CSV

```json
{
  "bucket_name": "my-data-bucket",
  "region": "us-east-1",
  "path_prefix": "exports/2024",
  "access_key_id": "YOUR_ACCESS_KEY",
  "secret_access_key": "YOUR_SECRET_KEY",
  "file_format": "csv",
  "has_header": true,
  "delimiter": ",",
  "max_threads": 5
}
```

### MinIO with JSON

```json
{
  "bucket_name": "analytics",
  "region": "us-east-1",
  "access_key_id": "minioadmin",
  "secret_access_key": "minioadmin",
  "endpoint": "http://localhost:9000",
  "file_format": "json",
  "json_line_delimited": true,
  "compression": "gzip",
  "file_pattern": ".*\\.json\\.gz$"
}
```

### S3 with File Pattern Filtering

```json
{
  "bucket_name": "logs",
  "region": "us-west-2",
  "path_prefix": "application-logs/2024",
  "access_key_id": "YOUR_ACCESS_KEY",
  "secret_access_key": "YOUR_SECRET_KEY",
  "file_format": "csv",
  "file_pattern": ".*-production-.*\\.csv$",
  "compression": "gzip"
}
```

## File Structure

```
drivers/s3/
├── internal/
│   ├── config.go          # Configuration and validation
│   ├── s3.go              # Main S3 driver implementation
│   ├── parsers.go         # File format parsers (CSV/JSON)
│   ├── backfill.go        # Full refresh logic
│   ├── incremental.go     # Incremental sync logic
│   ├── config_test.go     # Unit tests
├── testdata/              # Sample test files
│   ├── sample.csv
│   └── sample.json
├── test-config/           # Test configuration files
│   ├── config.json
│   └── writer.json
├── docker-compose.yml     # MinIO setup
├── test.sh                # Automated test script
├── main.go                # Driver entry point
└── README.md
```

## How It Works

1. **Discovery**: Lists all files in the specified S3 bucket/prefix matching the file format and pattern
2. **Schema Inference**: Reads the first 100 records to infer column types
3. **Backfill**: Streams file contents in batches, converting types based on inferred schema
4. **Incremental Sync**: Tracks file ETags to detect changes and skip unchanged files

## State Management

The driver tracks synced files using ETags:
- Files are identified by their S3 key (path)
- ETag is stored in state after successful sync
- On subsequent runs, only files with changed ETags are processed
- New files are automatically detected and synced

## Known Limitations

1. **Large Files**: Currently treats each file as a single chunk. For very large files (>1GB), consider splitting them before upload.
2. **Zip Compression**: Not yet implemented
3. **IAM Role Auth**: Only access key/secret key authentication supported
4. **Parquet Type Inference**: All parquet fields are currently read as strings for simplicity

## Backlog

- [ ] Implement file chunking for large files (>1GB)
- [ ] Improve Parquet type inference (currently all fields read as strings)
- [ ] Add Zip compression support
- [ ] Add IAM role-based authentication
- [ ] Add support for S3 Select for filtered queries
- [ ] Add multipart upload support for large files

## Troubleshooting

### Connection Issues

```bash
# Test MinIO connection
curl http://localhost:9000/minio/health/live

# Check bucket exists
docker exec olake-s3-test-minio mc ls local/olake-test-bucket
```

### File Not Discovered

- Check `path_prefix` matches your file location
- Verify `file_format` matches file extension
- Check `file_pattern` regex if specified
- Ensure files don't end with `/` (treated as directories)

### Schema Inference Issues

- Ensure CSV has consistent column counts
- For JSON, use consistent field names across records
- Check for malformed data in first 100 records

## Contributing

When adding new features:
1. Add unit tests in `*_test.go` files
2. Update this README
3. Test with MinIO locally before AWS S3
4. Follow existing code patterns in other drivers
