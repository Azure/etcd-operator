// Copyright 2017 The etcd-operator Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package abs

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/pborman/uuid"
)

const (
	// AzureBlobBlockChunkLimitInBytes 100MiB is the limit
	AzureBlobBlockChunkLimitInBytes = 100 * 1024 * 1024

	v1 = "v1/"
)

// ABS is a helper to wrap complex ABS logic
type ABS struct {
	container *storage.Container
	prefix    string
	client    *storage.BlobStorageClient
}

// New returns a new ABS object for a given container using credentials set in the environment
func New(container, accountName, accountKey, accountSASToken, prefix string) (*ABS, error) {
	var basicClient storage.Client
	var err error
	if len(accountSASToken) != 0 {
		// This piece code is to make accountSASToken compatible if customer pasted a URI instead
		ind := strings.IndexAny(accountSASToken, "?")
		if ind != -1 {
			accountSASToken = accountSASToken[ind+1:]
		}

		// Reference: https://github.com/Azure/azure-sdk-for-go/blob/eae258195456be76b2ec9ad2ee2ab63cdda365d9/storage/client_test.go#L313
		endpoint := fmt.Sprintf("http://%s.blob.core.windows.net/%s", accountName, container)
		basicClient, err = storage.NewAccountSASClientFromEndpointToken(endpoint, accountSASToken)
		if err != nil {
			return nil, fmt.Errorf("create ABS client (from SAS token) failed: %v", err)
		}
	} else {
		basicClient, err = storage.NewBasicClient(accountName, accountKey)
		if err != nil {
			return nil, fmt.Errorf("create ABS client (from storage account name and key) failed: %v", err)
		}
	}
	return NewFromClient(container, prefix, &basicClient)
}

// NewFromClient returns a new ABS object for a given container using the supplied storageClient
func NewFromClient(container, prefix string, storageClient *storage.Client) (*ABS, error) {
	client := storageClient.GetBlobService()
	containerRef := client.GetContainerReference(container)

	// Check if supplied container exists using ListBlob
	// if the blobs not there, we would expect to see 404
	_, err := containerRef.ListBlobs(storage.ListBlobsParameters{})
	if err != nil {
		return nil, fmt.Errorf("containerRef.ListBlobs failed. error: %v", err)
	}
	return &ABS{
		container: containerRef,
		prefix:    prefix,
		client:    &client,
	}, nil
}

// Put puts a chunk of data into a ABS container using the provided key for its reference
func (w *ABS) Put(key string, r io.Reader) error {
	blobName := path.Join(v1, w.prefix, key)
	blob := w.container.GetBlobReference(blobName)

	var buf bytes.Buffer
	if r != nil {
		_, err := io.Copy(&buf, r)
		if err != nil {
			return err
		}
		r = &buf
	}

	err := blob.CreateBlockBlob(&storage.PutBlobOptions{})
	if err != nil {
		return err
	}

	len := len(buf.Bytes())
	d := float64(len) / float64(AzureBlobBlockChunkLimitInBytes)
	chunkCount := int(math.Ceil(d))
	blocks := make([]storage.Block, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		blockID := base64.StdEncoding.EncodeToString([]byte(uuid.New()))
		blocks = append(blocks, storage.Block{ID: blockID, Status: storage.BlockStatusLatest})
		start := i * AzureBlobBlockChunkLimitInBytes
		end := (i + 1) * AzureBlobBlockChunkLimitInBytes
		if len < end {
			end = len
		}

		chunk := buf.Bytes()[start:end]
		err = blob.PutBlock(blockID, chunk, &storage.PutBlockOptions{})
		if err != nil {
			return err
		}
	}

	err = blob.PutBlockList(blocks, &storage.PutBlockListOptions{})
	if err != nil {
		return err
	}
	return nil
}

// Get gets the blob object specified by key from a ABS container
func (w *ABS) Get(key string) (io.ReadCloser, error) {
	blobName := path.Join(v1, w.prefix, key)
	blob := w.container.GetBlobReference(blobName)

	opts := &storage.GetBlobOptions{}
	return blob.Get(opts)
}

// Delete deletes the blob object specified by key from a ABS container
func (w *ABS) Delete(key string) error {
	blobName := path.Join(v1, w.prefix, key)
	blob := w.container.GetBlobReference(blobName)

	opts := &storage.DeleteBlobOptions{}
	return blob.Delete(opts)
}

// List lists all blobs in a given ABS container
func (w *ABS) List() ([]string, error) {
	_, l, err := w.list(w.prefix)
	return l, err
}

func (w *ABS) list(prefix string) (int64, []string, error) {
	params := storage.ListBlobsParameters{Prefix: path.Join(v1, prefix) + "/"}
	resp, err := w.container.ListBlobs(params)
	if err != nil {
		return -1, nil, err
	}

	keys := []string{}
	var size int64
	for _, blob := range resp.Blobs {
		k := (blob.Name)[len(resp.Prefix):]
		keys = append(keys, k)
		size += blob.Properties.ContentLength
	}

	return size, keys, nil
}

// TotalSize returns the total size of all blobs in a ABS container
func (w *ABS) TotalSize() (int64, error) {
	size, _, err := w.list(w.prefix)
	return size, err
}

// CopyPrefix copies all blobs with given prefix
func (w *ABS) CopyPrefix(from string) error {
	_, blobs, err := w.list(from)
	if err != nil {
		return err
	}
	for _, basename := range blobs {
		blobResource := w.container.GetBlobReference(basename)

		opts := storage.CopyOptions{}
		if err = blobResource.Copy(path.Join(w.container.Name, v1, from, basename), &opts); err != nil {
			return err
		}
	}
	return nil
}
