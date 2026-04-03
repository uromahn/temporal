package gcloud

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dgryski/go-farm"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	archiverspb "go.temporal.io/server/api/archiver/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/gcloud/connector"
	"go.temporal.io/server/common/codec"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/proto"
)

func encode(message proto.Message) ([]byte, error) {
	encoder := codec.NewJSONPBEncoder()
	return encoder.Encode(message)
}

func constructHistoryFilenameMultipart(namespaceID, workflowID, runID string, version int64, partNumber int) string {
	combinedHash := constructHistoryFilenamePrefix(namespaceID, workflowID, runID)
	return fmt.Sprintf("%s_%v_%v.history", combinedHash, version, partNumber)
}

func constructHistoryFilenamePrefix(namespaceID, workflowID, runID string) string {
	return strings.Join([]string{hash(namespaceID), hash(workflowID), hash(runID)}, "")
}

func constructVisibilityFilenamePrefix(namespaceID, tag string) string {
	return fmt.Sprintf("%s/%s", namespaceID, tag)
}

func constructTimeBasedSearchKey(namespaceID, tag string, t time.Time, precision string) string {
	var timeFormat = ""
	switch precision {
	case PrecisionSecond:
		timeFormat = ":05"
		fallthrough
	case PrecisionMinute:
		timeFormat = ":04" + timeFormat
		fallthrough
	case PrecisionHour:
		timeFormat = "15" + timeFormat
		fallthrough
	case PrecisionDay:
		timeFormat = "2006-01-02T" + timeFormat
	}

	return fmt.Sprintf("%s_%s", constructVisibilityFilenamePrefix(namespaceID, tag), t.Format(timeFormat))
}

// constructWorkflowIdBasedSearchKey builds the GCS prefix for a workflowID-index query.
// The prefix uses the raw workflowID (not its hash) so filenames are human-readable.
// If a CloseTime + SearchPrecision are provided, the prefix is further narrowed by time.
func constructWorkflowIdBasedSearchKey(namespaceID string, parsedQuery *parsedQuery) string {
	// Use the raw workflowID so filenames are human-readable and the prefix
	// is a straightforward exact-match on the workflowID segment.
	prefix := constructVisibilityFilenamePrefix(namespaceID, indexKeyWorkflowID)
	prefix = fmt.Sprintf("%s_%s", prefix, *parsedQuery.workflowID)

	if !parsedQuery.closeTime.IsZero() && parsedQuery.searchPrecision != nil {
		var timeFormat = ""
		switch *parsedQuery.searchPrecision {
		case PrecisionSecond:
			timeFormat = ":05"
			fallthrough
		case PrecisionMinute:
			timeFormat = ":04" + timeFormat
			fallthrough
		case PrecisionHour:
			timeFormat = "15" + timeFormat
			fallthrough
		case PrecisionDay:
			timeFormat = "2006-01-02T" + timeFormat
		}
		prefix = fmt.Sprintf("%s_%s", prefix, parsedQuery.closeTime.Format(timeFormat))
	}
	return prefix
}

func hash(s string) (result string) {
	if s != "" {
		return fmt.Sprintf("%v", farm.Fingerprint64([]byte(s)))
	}
	return
}

func deserializeGetHistoryToken(bytes []byte) (*getHistoryToken, error) {
	token := &getHistoryToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func extractCloseFailoverVersion(filename string) (int64, int, error) {
	filenameParts := strings.FieldsFunc(filename, func(r rune) bool {
		return r == '_' || r == '.'
	})
	if len(filenameParts) != 4 {
		return -1, 0, errors.New("unknown filename structure")
	}

	failoverVersion, err := strconv.ParseInt(filenameParts[1], 10, 64)
	if err != nil {
		return -1, 0, err
	}

	highestPart, err := strconv.Atoi(filenameParts[2])
	return failoverVersion, highestPart, err
}

func serializeToken(token interface{}) ([]byte, error) {
	if token == nil {
		return nil, nil
	}
	return json.Marshal(token)
}

func decodeVisibilityRecord(data []byte) (*archiverspb.VisibilityRecord, error) {
	record := &archiverspb.VisibilityRecord{}
	encoder := codec.NewJSONPBEncoder()
	err := encoder.Decode(data, record)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func constructVisibilityFilename(namespace, workflowTypeName, workflowID, runID, tag string, t time.Time) string {
	prefix := constructVisibilityFilenamePrefix(namespace, tag)
	return fmt.Sprintf("%s_%s_%s_%s_%s.visibility", prefix, t.Format(time.RFC3339), hash(workflowTypeName), hash(workflowID), hash(runID))
}

// constructVisibilityWorkflowIDIndexFilename builds the filename for the workflowID-based
// index entry. The workflowID is stored in plain text (not hashed) for human readability,
// and workflowTypeName / runID are omitted — they are not needed for this index.
// Format: namespace/tag_<workflowID>_<closeTime>.visibility
func constructVisibilityWorkflowIDIndexFilename(namespace, workflowID, tag string, t time.Time) string {
	prefix := constructVisibilityFilenamePrefix(namespace, tag)
	return fmt.Sprintf("%s_%s_%s.visibility", prefix, workflowID, t.Format(time.RFC3339))
}

func deserializeQueryVisibilityToken(bytes []byte) (*queryVisibilityToken, error) {
	token := &queryVisibilityToken{}
	err := json.Unmarshal(bytes, token)
	return token, err
}

func convertToExecutionInfo(record *archiverspb.VisibilityRecord, saTypeMap searchattribute.NameTypeMap) (*workflowpb.WorkflowExecutionInfo, error) {
	searchAttributes, err := searchattribute.Parse(record.SearchAttributes, &saTypeMap)
	if err != nil {
		return nil, err
	}

	return &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: record.GetWorkflowId(),
			RunId:      record.GetRunId(),
		},
		Type: &commonpb.WorkflowType{
			Name: record.WorkflowTypeName,
		},
		StartTime:         record.StartTime,
		ExecutionTime:     record.ExecutionTime,
		CloseTime:         record.CloseTime,
		ExecutionDuration: record.ExecutionDuration,
		Status:            record.Status,
		HistoryLength:     record.HistoryLength,
		Memo:              record.Memo,
		SearchAttributes:  searchAttributes,
	}, nil
}

// newWorkflowIDPrecondition returns a filter that checks whether a filename contains
// the given workflowID hash (used for time-based index entries where the workflowID
// appears as a hash in position [3] of the underscore-split filename).
func newWorkflowIDPrecondition(workflowIDHash string) connector.Precondition {
	return func(subject interface{}) bool {

		if workflowIDHash == "" {
			return true
		}

		fileName, ok := subject.(string)
		if !ok {
			return false
		}

		if strings.Contains(fileName, workflowIDHash) {
			fileNameParts := strings.SplitN(fileName, "_", 5)
			if len(fileNameParts) != 5 {
				return true
			}
			return strings.Contains(fileName, fileNameParts[3])
		}

		return false
	}
}

func isRetryableError(err error) (retryable bool) {
	switch err.Error() {
	case connector.ErrBucketNotFound.Error(),
		archiver.ErrURISchemeMismatch.Error(),
		archiver.ErrInvalidURI.Error():
		retryable = false
	default:
		retryable = true
	}

	return
}
