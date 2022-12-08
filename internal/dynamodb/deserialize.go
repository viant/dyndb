package dynamodb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws/protocol/restjson"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	smithy "github.com/aws/smithy-go"
	smithyio "github.com/aws/smithy-go/io"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/francoispqt/gojay"
	"github.com/viant/dyndb/internal/exec"
	"io/ioutil"
	"strings"

	"github.com/aws/smithy-go/middleware"
	"io"
)

// DeserializeMiddleware provides the interface for middleware specific to the
// serialize step. Delegates to the next DeserializeHandler for further
// processing.
type DeserializeMiddleware struct {
	Output *ExecuteStatementOutput
}

//ID returns ID
func (m *DeserializeMiddleware) ID() string {
	return "OperationDeserializer"
}

//HandleDeserialize handle deserialize
func (m *DeserializeMiddleware) HandleDeserialize(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (out middleware.DeserializeOutput, metadata middleware.Metadata, err error) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	if err != nil {
		return out, metadata, err
	}
	var response, ok = out.RawResponse.(*smithyhttp.Response)
	if !ok {
		return out, metadata, &smithy.DeserializationError{Err: fmt.Errorf("unknown transport type %T", out.RawResponse)}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return out, metadata, handleExecuteStatementException(response, &metadata)
	}

	output := &dynamodb.ExecuteStatementOutput{}
	out.Result = output
	m.Output.ExecuteStatementOutput = output
	var buff [1024]byte
	ringBuffer := smithyio.NewRingBuffer(buff[:])
	body := io.TeeReader(response.Body, ringBuffer)
	data, err := ioutil.ReadAll(body)
	if err != nil && err != io.EOF {
		var snapshot bytes.Buffer
		io.Copy(&snapshot, ringBuffer)
		err = &smithy.DeserializationError{
			Err:      fmt.Errorf("failed to decode response body, %w", err),
			Snapshot: snapshot.Bytes(),
		}
		return out, metadata, err
	}
	m.Output.Data = data
	if err := gojay.Unmarshal(data, m.Output); err != nil && err != io.EOF {
		var snapshot bytes.Buffer
		_, _ = io.Copy(&snapshot, ringBuffer)
		err = &smithy.DeserializationError{
			Err:      fmt.Errorf("failed to decode response body, %w", err),
			Snapshot: snapshot.Bytes(),
		}
		return out, metadata, err
	}
	if err != nil {
		var snapshot bytes.Buffer
		_, _ = io.Copy(&snapshot, ringBuffer)
		err = &smithy.DeserializationError{
			Err:      fmt.Errorf("failed to decode response body, %w", err),
			Snapshot: snapshot.Bytes(),
		}
		return out, metadata, err
	}
	return out, metadata, err
}

//NewDeserializeMiddleware returns deserializer
func NewDeserializeMiddleware(aType *exec.Type) *DeserializeMiddleware {
	result := &DeserializeMiddleware{}
	result.Output = NewExecuteStatementOutput(aType)
	return result
}

func handleExecuteStatementException(response *smithyhttp.Response, metadata *middleware.Metadata) error {
	var errorBuffer bytes.Buffer
	if _, err := io.Copy(&errorBuffer, response.Body); err != nil {
		return &smithy.DeserializationError{Err: fmt.Errorf("failed to copy error response body, %w", err)}
	}
	errorBody := bytes.NewReader(errorBuffer.Bytes())
	errorCode := "UnknownError"
	errorMessage := errorCode
	code := response.Header.Get("X-Amzn-ErrorType")
	if len(code) != 0 {
		errorCode = restjson.SanitizeErrorCode(code)
	}
	var buff [1024]byte
	ringBuffer := smithyio.NewRingBuffer(buff[:])
	body := io.TeeReader(errorBody, ringBuffer)
	decoder := json.NewDecoder(body)
	decoder.UseNumber()
	code, message, err := restjson.GetErrorInfo(decoder)
	if err != nil {
		var snapshot bytes.Buffer
		io.Copy(&snapshot, ringBuffer)
		err = &smithy.DeserializationError{
			Err:      fmt.Errorf("failed to decode response body, %w", err),
			Snapshot: snapshot.Bytes(),
		}
		return err
	}

	errorBody.Seek(0, io.SeekStart)
	if len(code) != 0 {
		errorCode = restjson.SanitizeErrorCode(code)
	}
	if len(message) != 0 {
		errorMessage = message
	}

	switch {
	case strings.EqualFold("ConditionalCheckFailedException", errorCode):
		return awsAwsjson10_deserializeErrorConditionalCheckFailedException(response, errorBody)

	case strings.EqualFold("DuplicateItemException", errorCode):
		return awsAwsjson10_deserializeErrorDuplicateItemException(response, errorBody)

	case strings.EqualFold("InternalServerError", errorCode):
		return awsAwsjson10_deserializeErrorInternalServerError(response, errorBody)

	case strings.EqualFold("ItemCollectionSizeLimitExceededException", errorCode):
		return awsAwsjson10_deserializeErrorItemCollectionSizeLimitExceededException(response, errorBody)

	case strings.EqualFold("ProvisionedThroughputExceededException", errorCode):
		return awsAwsjson10_deserializeErrorProvisionedThroughputExceededException(response, errorBody)

	case strings.EqualFold("RequestLimitExceeded", errorCode):
		return awsAwsjson10_deserializeErrorRequestLimitExceeded(response, errorBody)

	case strings.EqualFold("ResourceNotFoundException", errorCode):
		return awsAwsjson10_deserializeErrorResourceNotFoundException(response, errorBody)

	case strings.EqualFold("TransactionConflictException", errorCode):
		return awsAwsjson10_deserializeErrorTransactionConflictException(response, errorBody)

	default:
		genericError := &smithy.GenericAPIError{
			Code:    errorCode,
			Message: errorMessage,
		}
		return genericError

	}
}
