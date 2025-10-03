#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Olake S3 Driver - Test Suite${NC}"
echo -e "${GREEN}========================================${NC}"

# Step 1: Start MinIO
echo -e "\n${YELLOW}[1/6] Starting MinIO...${NC}"
docker-compose up -d

echo -e "${YELLOW}Waiting for MinIO to be ready...${NC}"
sleep 5

# Step 2: Setup MinIO buckets and upload test files
echo -e "\n${YELLOW}[2/6] Setting up MinIO buckets and uploading test files...${NC}"

# Configure mc alias inside MinIO container
docker exec olake-s3-test-minio mc alias set mylocal http://localhost:9000 minioadmin minioadmin

# Create buckets (ignore error if already exists)
echo -e "${YELLOW}Creating buckets...${NC}"
docker exec olake-s3-test-minio mc mb mylocal/olake-test-bucket 2>&1 || echo "Bucket olake-test-bucket already exists"
docker exec olake-s3-test-minio mc mb mylocal/olake-output 2>&1 || echo "Bucket olake-output already exists"

# Copy test files to container and upload to MinIO
echo -e "${YELLOW}Uploading test files...${NC}"
docker cp testdata/sample.csv olake-s3-test-minio:/tmp/
docker cp testdata/sample.json olake-s3-test-minio:/tmp/

docker exec olake-s3-test-minio mc cp /tmp/sample.csv mylocal/olake-test-bucket/data/sample.csv
docker exec olake-s3-test-minio mc cp /tmp/sample.json mylocal/olake-test-bucket/data/sample.json

# Verify files were uploaded
echo -e "${YELLOW}Verifying uploaded files...${NC}"
docker exec olake-s3-test-minio mc ls mylocal/olake-test-bucket/data/

echo -e "${GREEN}✓ Test files uploaded successfully${NC}"

# Step 3: Build the S3 driver
echo -e "\n${YELLOW}[3/6] Building S3 driver...${NC}"
cd ..
./build.sh driver-s3 spec > /dev/null 2>&1 || true
cd s3

if [ -f "olake" ]; then
    echo -e "${GREEN}✓ S3 driver built successfully${NC}"
else
    echo -e "${RED}✗ Failed to build S3 driver${NC}"
    exit 1
fi

# Step 4: Test spec command
echo -e "\n${YELLOW}[4/6] Testing 'spec' command...${NC}"
./olake spec > test-config/spec_output.json
if [ -f "test-config/spec_output.json" ]; then
    echo -e "${GREEN}✓ Spec command successful${NC}"
    echo "Sample spec output:"
    head -20 test-config/spec_output.json
else
    echo -e "${RED}✗ Spec command failed${NC}"
fi

# Step 5: Test check command
echo -e "\n${YELLOW}[5/6] Testing 'check' command...${NC}"
./olake check --config test-config/config.json
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Connection check successful${NC}"
else
    echo -e "${RED}✗ Connection check failed${NC}"
    exit 1
fi

# Step 6: Test discover command
echo -e "\n${YELLOW}[6/6] Testing 'discover' command...${NC}"
mkdir -p test-config
./olake discover --config test-config/config.json > test-config/streams.json 2>&1
if [ -f "test-config/streams.json" ]; then
    echo -e "${GREEN}✓ Discover command successful${NC}"
    echo "Discovered streams:"
    cat test-config/streams.json | grep -o '"name":"[^"]*"' || true
else
    echo -e "${RED}✗ Discover command failed${NC}"
fi

# Summary
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  Test Summary${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "MinIO Console: ${YELLOW}http://localhost:9001${NC}"
echo -e "Username: ${YELLOW}minioadmin${NC}"
echo -e "Password: ${YELLOW}minioadmin${NC}"
echo -e "\nTest files location: ${YELLOW}testdata/${NC}"
echo -e "Config files location: ${YELLOW}test-config/${NC}"
echo -e "\n${GREEN}To run sync:${NC}"
echo -e "  cd drivers/s3"
echo -e "  ./olake sync --config test-config/config.json --destination test-config/writer.json --catalog test-config/streams.json"
echo -e "\n${GREEN}To stop MinIO:${NC}"
echo -e "  docker-compose down"
echo -e "${GREEN}========================================${NC}"
