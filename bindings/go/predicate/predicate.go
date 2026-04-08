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

// Package predicate provides convenience functions for combining Paimon filter predicates.
package predicate

import (
	"fmt"

	paimon "github.com/apache/paimon-rust/bindings/go"
)

// Predicate is an alias for paimon.Predicate.
type Predicate = paimon.Predicate

// And combines two or more predicates with AND. Consumes all inputs
// (callers must NOT close them after this call).
func And(preds ...*Predicate) (*Predicate, error) {
	if len(preds) < 2 {
		return nil, fmt.Errorf("predicate.And requires at least 2 predicates, got %d", len(preds))
	}
	result := preds[0]
	for _, p := range preds[1:] {
		r, err := result.And(p)
		if err != nil {
			return nil, err
		}
		result = r
	}
	return result, nil
}

// Or combines two or more predicates with OR. Consumes all inputs
// (callers must NOT close them after this call).
func Or(preds ...*Predicate) (*Predicate, error) {
	if len(preds) < 2 {
		return nil, fmt.Errorf("predicate.Or requires at least 2 predicates, got %d", len(preds))
	}
	result := preds[0]
	for _, p := range preds[1:] {
		r, err := result.Or(p)
		if err != nil {
			return nil, err
		}
		result = r
	}
	return result, nil
}

// Not negates a predicate. Consumes the input
// (caller must NOT close it after this call).
func Not(p *Predicate) (*Predicate, error) {
	return p.Not()
}
