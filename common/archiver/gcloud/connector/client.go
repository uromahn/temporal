//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_mock.go

package connector

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"

	"cloud.google.com/go/storage"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/config"
	"go.uber.org/multierr"
	"google.golang.org/api/iterator"
)

var (
	// ErrBucketNotFound is non retryable error that is thrown when the bucket doesn't exist
	ErrBucketNotFound = errors.New("bucket not found")
	errObjectNotFound = errors.New("object not found")
)

type (
	// Client is a wrapper around Google cloud storages client library.
	Client interface {
		Upload(ctx context.Context, URI archiver.URI, fileName string, file []byte) error
		Get(ctx context.Context, URI archiver.URI, file string) ([]byte, error)
		Query(ctx context.Context, URI archiver.URI, fileNamePrefix string) ([]string, error)
		QueryWithPagination(ctx context.Context, URI archiver.URI, fileNamePrefix string, pageSize int, pageToken string) ([]string, string, error)
		Exist(ctx context.Context, URI archiver.URI, fileName string) (bool, error)
	}

	storageWrapper struct {
		client GcloudStorageClient
	}
)

// NewClient return a Temporal gcloudstorage.Client based on default google service account creadentials (ScopeFullControl required).
// Bucket must be created by Iaas scripts, in other words, this library doesn't create the required Bucket.
// Optionaly you can set your credential path throught "GOOGLE_APPLICATION_CREDENTIALS" environment variable or through temporal config file.
// You can find more info about "Google Setting Up Authentication for Server to Server Production Applications" under the following link
// https://cloud.google.com/docs/authentication/production
func NewClient(ctx context.Context, config *config.GstorageArchiver) (Client, error) {
	if credentialsPath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); credentialsPath != "" {
		clientDelegate, err := newClientDelegateWithCredentials(ctx, credentialsPath)
		return &storageWrapper{client: clientDelegate}, err
	}

	if config.CredentialsPath != "" {
		clientDelegate, err := newClientDelegateWithCredentials(ctx, config.CredentialsPath)
		return &storageWrapper{client: clientDelegate}, err
	}

	clientDelegate, err := newDefaultClientDelegate(ctx)
	return &storageWrapper{client: clientDelegate}, err

}

// NewClientWithParams return a gcloudstorage.Client based on input parameters
func NewClientWithParams(clientD GcloudStorageClient) (Client, error) {
	return &storageWrapper{client: clientD}, nil
}

// Upload push a file to gcloud storage bucket (sinkPath)
// example:
// Upload(ctx, mockBucketHandleClient, "gs://my-bucket-cad/temporal_archival/development", "45273645-fileName.history", fileReader)
func (s *storageWrapper) Upload(ctx context.Context, URI archiver.URI, fileName string, file []byte) (err error) {
	bucket := s.client.Bucket(URI.Hostname())
	writer := bucket.Object(formatSinkPath(URI.Path()) + "/" + fileName).NewWriter(ctx)
	_, err = io.Copy(writer, bytes.NewReader(file))
	if err == nil {
		err = writer.Close()
	}

	return err
}

// Exist check if a bucket or an object exist
// If fileName is empty, then 'Exist' function will only check if the given bucket exist.
func (s *storageWrapper) Exist(ctx context.Context, URI archiver.URI, fileName string) (exists bool, err error) {
	bucket := s.client.Bucket(URI.Hostname())
	if _, err := bucket.Attrs(ctx); err != nil {
		return false, err
	}

	if fileName == "" {
		return true, nil
	}

	if _, err = bucket.Object(fileName).Attrs(ctx); err != nil {
		return false, errObjectNotFound
	}

	return true, nil
}

// Get retrieve a file
func (s *storageWrapper) Get(ctx context.Context, URI archiver.URI, fileName string) (fileContent []byte, err error) {
	bucket := s.client.Bucket(URI.Hostname())
	reader, err := bucket.Object(formatSinkPath(URI.Path()) + "/" + fileName).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = multierr.Combine(err, reader.Close())
	}()
	return io.ReadAll(reader)
}

// Query, retieves file names by provided storage query
func (s *storageWrapper) Query(ctx context.Context, URI archiver.URI, fileNamePrefix string) (fileNames []string, err error) {
	fileNames = make([]string, 0)
	bucket := s.client.Bucket(URI.Hostname())
	var attrs = new(storage.ObjectAttrs)
	it := bucket.Objects(ctx, &storage.Query{
		Prefix: formatSinkPath(URI.Path()) + "/" + fileNamePrefix,
	})

	for {
		attrs, err = it.Next()
		if err == iterator.Done {
			return fileNames, nil
		}
		fileNames = append(fileNames, attrs.Name)
	}

}

// QueryWithPagination retrieves filenames that match the provided prefix using native GCS pagination.
func (s *storageWrapper) QueryWithPagination(ctx context.Context, URI archiver.URI, fileNamePrefix string, pageSize int, pageToken string) ([]string, string, error) {
	bucket := s.client.Bucket(URI.Hostname())
	it := bucket.Objects(ctx, &storage.Query{
		Prefix: formatSinkPath(URI.Path()) + "/" + fileNamePrefix,
	})

	pager := iterator.NewPager(it, pageSize, pageToken)
	var objects []*storage.ObjectAttrs

	nextPageToken, err := pager.NextPage(&objects)
	if err != nil {
		return nil, "", err
	}

	resultSet := make([]string, 0, len(objects))
	for _, obj := range objects {
		resultSet = append(resultSet, obj.Name)
	}

	return resultSet, nextPageToken, nil
}

func formatSinkPath(sinkPath string) string {
	return sinkPath[1:]
}
