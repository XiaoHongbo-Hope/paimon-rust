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

package paimon

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	libOnce sync.Once
	libPath string
	libErr  error
)

// loadEmbeddedLib decompresses the embedded shared library and writes it
// to a temp file. Called once via sync.Once.
func loadEmbeddedLib() error {
	libOnce.Do(func() {
		data, err := decompressLib(libPaimonZst)
		if err != nil {
			libErr = fmt.Errorf("paimon: failed to decompress embedded library: %w", err)
			return
		}
		libPath, err = writeTempExec(tempFilePattern(), data)
		if err != nil {
			libErr = fmt.Errorf("paimon: failed to write temp library: %w", err)
			return
		}
	})
	return libErr
}

func decompressLib(raw []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	return io.ReadAll(decoder)
}

func writeTempExec(pattern string, binary []byte) (path string, err error) {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", err
	}
	defer f.Close()
	defer func() {
		if err != nil {
			os.Remove(f.Name())
		}
	}()
	if _, err = f.Write(binary); err != nil {
		return "", err
	}
	if err = f.Chmod(0o700); err != nil {
		return "", err
	}
	return f.Name(), nil
}
