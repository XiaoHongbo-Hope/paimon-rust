/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package paimon_test

import (
	"encoding/binary"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"

	paimon "github.com/apache/paimon-rust/bindings/go"
)

func blobDescriptorV2(uri string, offset, length int64) []byte {
	result := make([]byte, 0, 29+len(uri))
	result = append(result, 2)
	result = binary.LittleEndian.AppendUint64(result, 0x424C4F4244455343)
	result = binary.LittleEndian.AppendUint32(result, uint32(len(uri)))
	result = append(result, uri...)
	result = binary.LittleEndian.AppendUint64(result, uint64(offset))
	result = binary.LittleEndian.AppendUint64(result, uint64(length))
	return result
}

func localFileURI(path string) string {
	return (&url.URL{Scheme: "file", Path: path}).String()
}

func TestBlobReaderReadBlobAndBatch(t *testing.T) {
	first, err := os.CreateTemp(t.TempDir(), "blob-first-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.WriteString("abcdefghij"); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := os.CreateTemp(t.TempDir(), "blob-second-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.WriteString("UVWXYZ"); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := paimon.NewBlobReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	value, err := reader.ReadBlob(blobDescriptorV2(localFileURI(first.Name()), 1, 3))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "bcd" {
		t.Fatalf("ReadBlob returned %q, want %q", value, "bcd")
	}

	values, err := reader.ReadBlobs([][]byte{
		blobDescriptorV2(localFileURI(second.Name()), 1, 3),
		blobDescriptorV2(localFileURI(first.Name()), 3, -1),
		blobDescriptorV2(localFileURI(first.Name()), 5, 0),
		blobDescriptorV2(localFileURI(first.Name()), 2, 4),
		blobDescriptorV2(localFileURI(first.Name()), 2, 4),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"VWX", "defghij", "", "cdef", "cdef"}
	for index, value := range values {
		if string(value) != want[index] {
			t.Fatalf("ReadBlobs result %d = %q, want %q", index, value, want[index])
		}
	}

	empty, err := reader.ReadBlobs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("empty batch returned %#v", empty)
	}
}

func TestBlobReaderFromTableOutlivesTable(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "blob-table-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("abcdefghij"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	table := openCopiedTestTable(t)
	reader, err := table.NewBlobReader()
	if err != nil {
		t.Fatal(err)
	}
	table.Close()
	defer reader.Close()

	value, err := reader.ReadBlob(blobDescriptorV2(localFileURI(file.Name()), 2, 4))
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "cdef" {
		t.Fatalf("ReadBlob returned %q, want %q", value, "cdef")
	}
}

func TestBlobReaderErrorsAndClose(t *testing.T) {
	reader, err := paimon.NewBlobReader(map[string]string{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := reader.ReadBlob(nil); err == nil {
		t.Fatal("expected invalid descriptor error")
	}

	missingURI := localFileURI(t.TempDir() + "/missing.blob")
	_, err = reader.ReadBlobs([][]byte{
		blobDescriptorV2(missingURI, 0, 1),
	})
	if err == nil {
		t.Fatal("expected missing object error")
	}
	if !strings.Contains(err.Error(), "input indices [0]") || !strings.Contains(err.Error(), missingURI) {
		t.Fatalf("error lacks descriptor context: %v", err)
	}

	reader.Close()
	reader.Close()
	if _, err := reader.ReadBlob(blobDescriptorV2(missingURI, 0, 0)); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("ReadBlob after Close returned %v, want ErrClosed", err)
	}
	if _, err := reader.ReadBlobs(nil); !errors.Is(err, paimon.ErrClosed) {
		t.Fatalf("ReadBlobs after Close returned %v, want ErrClosed", err)
	}
}
