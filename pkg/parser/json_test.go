package parser

import (
	"context"
	"strings"
	"testing"

	"github.com/datazip-inc/olake/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInferJSONFieldType_NumbersAlwaysFloat64(t *testing.T) {
	tests := []struct {
		name         string
		values       []interface{}
		expectedType types.DataType
		description  string
	}{
		{
			name:         "integer numbers should be Float64",
			values:       []interface{}{1.0, 2.0, 3.0},
			expectedType: types.Float64,
			description:  "JSON integers should be inferred as Float64 to avoid Int64/Float64 conflicts",
		},
		{
			name:         "float numbers should be Float64",
			values:       []interface{}{1.5, 2.7, 3.9},
			expectedType: types.Float64,
			description:  "JSON floats should be inferred as Float64",
		},
		{
			name:         "mixed integer and float should be Float64",
			values:       []interface{}{1.0, 2.5, 3.0, 4.7},
			expectedType: types.Float64,
			description:  "Mixed JSON numbers should be inferred as Float64",
		},
		{
			name:         "boolean values",
			values:       []interface{}{true, false, true},
			expectedType: types.Bool,
			description:  "Boolean values should be inferred as Bool",
		},
		{
			name:         "string values",
			values:       []interface{}{"hello", "world", "test"},
			expectedType: types.String,
			description:  "String values should be inferred as String",
		},
		{
			name:         "object values",
			values:       []interface{}{map[string]interface{}{"key": "value"}},
			expectedType: types.String,
			description:  "Object values should be inferred as String (stored as JSON string)",
		},
		{
			name:         "array values",
			values:       []interface{}{[]interface{}{1, 2, 3}},
			expectedType: types.String,
			description:  "Array values should be inferred as String (stored as JSON string)",
		},
		{
			name:         "mixed types default to string",
			values:       []interface{}{"text", 123.0, true},
			expectedType: types.String,
			description:  "Mixed types should default to String",
		},
		{
			name:         "null values ignored",
			values:       []interface{}{nil, 1.0, nil, 2.0},
			expectedType: types.Float64,
			description:  "Null values should be ignored in type inference",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferJSONFieldType(tt.values)
			assert.Equal(t, tt.expectedType, result, tt.description)
		})
	}
}

func TestJSONParser_InferSchema_NumbersAsFloat64(t *testing.T) {
	jsonData := `{"id": 1, "name": "Alice", "price": 99.99, "quantity": 10, "active": true}`

	config := JSONConfig{
		LineDelimited: true,
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewJSONParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(jsonData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	// Check that integer numbers are inferred as Float64
	idType, err := result.Schema.GetType("id")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, idType, "id (integer) should be inferred as Float64")

	// Check that float numbers are inferred as Float64
	priceType, err := result.Schema.GetType("price")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, priceType, "price (float) should be inferred as Float64")

	// Check that quantity (integer) is also Float64
	quantityType, err := result.Schema.GetType("quantity")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, quantityType, "quantity (integer) should be inferred as Float64")

	// Check boolean
	activeType, err := result.Schema.GetType("active")
	require.NoError(t, err)
	assert.Equal(t, types.Bool, activeType, "active should be inferred as Bool")

	// Check string
	nameType, err := result.Schema.GetType("name")
	require.NoError(t, err)
	assert.Equal(t, types.String, nameType, "name should be inferred as String")
}

func TestJSONParser_InferSchema_JSONLFormat(t *testing.T) {
	jsonlData := `{"id": 1, "value": 100.5}
{"id": 2, "value": 200.75}
{"id": 3, "value": 300.25}`

	config := JSONConfig{
		LineDelimited: true,
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewJSONParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(jsonlData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	// Both id and value should be Float64
	idType, err := result.Schema.GetType("id")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, idType, "id should be Float64")

	valueType, err := result.Schema.GetType("value")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, valueType, "value should be Float64")
}

func TestJSONParser_InferSchema_JSONArrayFormat(t *testing.T) {
	jsonArrayData := `[
	{"id": 1, "score": 95.5},
	{"id": 2, "score": 87.25},
	{"id": 3, "score": 92.0}
]`

	config := JSONConfig{
		LineDelimited: false,
	}

	stream := types.NewStream("test", "test", nil)
	parser := NewJSONParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(jsonArrayData)

	result, err := parser.InferSchema(ctx, reader)
	require.NoError(t, err)

	// Both id and score should be Float64
	idType, err := result.Schema.GetType("id")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, idType, "id should be Float64")

	scoreType, err := result.Schema.GetType("score")
	require.NoError(t, err)
	assert.Equal(t, types.Float64, scoreType, "score should be Float64")
}

func TestJSONParser_StreamRecords_TypeConsistency(t *testing.T) {
	jsonlData := `{"id": 1, "count": 10, "price": 99.99}
{"id": 2, "count": 20, "price": 149.50}`

	config := JSONConfig{
		LineDelimited: true,
	}

	stream := types.NewStream("test", "test", nil)
	stream.UpsertField("id", types.Float64, true)
	stream.UpsertField("count", types.Float64, true)
	stream.UpsertField("price", types.Float64, true)

	parser := NewJSONParser(config, stream)

	ctx := context.Background()
	reader := strings.NewReader(jsonlData)

	records := []map[string]any{}
	callback := func(_ context.Context, record map[string]any) error {
		records = append(records, record)
		return nil
	}

	err := parser.StreamRecords(ctx, reader, callback)
	require.NoError(t, err)
	require.Len(t, records, 2)

	// Check that all numeric values are float64
	for i, record := range records {
		assert.IsType(t, float64(0), record["id"], "id in record %d should be float64", i)
		assert.IsType(t, float64(0), record["count"], "count in record %d should be float64", i)
		assert.IsType(t, float64(0), record["price"], "price in record %d should be float64", i)
	}
}

func TestInferJSONFieldType_PriorityOrder(t *testing.T) {
	tests := []struct {
		name         string
		values       []interface{}
		expectedType types.DataType
		description  string
	}{
		{
			name:         "Bool has highest priority",
			values:       []interface{}{true, false, true},
			expectedType: types.Bool,
			description:  "Bool should have priority over other types",
		},
		{
			name:         "Float64 has priority over Object/Array",
			values:       []interface{}{1.0, map[string]interface{}{"key": "value"}},
			expectedType: types.String, // Mixed types default to String
			description:  "Mixed number and object should default to String",
		},
		{
			name:         "Float64 has priority over String",
			values:       []interface{}{1.0, 2.0, 3.0},
			expectedType: types.Float64,
			description:  "All numbers should be Float64",
		},
		{
			name:         "Object/Array has priority over String",
			values:       []interface{}{map[string]interface{}{"key": "value"}},
			expectedType: types.String,
			description:  "Object should be stored as String (JSON string)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inferJSONFieldType(tt.values)
			assert.Equal(t, tt.expectedType, result, tt.description)
		})
	}
}
