// Copyright © 2022 Meroxa, Inc.
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

package validator

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/go-playground/validator/v10"
	"go.uber.org/multierr"
)

// keyStructTag is a tag which contains a field's key.
const keyStructTag = "key"

var (
	once sync.Once

	// validate is a singleton instance of the validator.
	validate *validator.Validate
)

// Validate validates a struct.
func ValidateStruct(data any) error {
	once.Do(func() {
		validate = validator.New()
	})

	var err error

	validationErr := validate.Struct(data)
	if validationErr != nil {
		if errors.Is(validationErr, (*validator.InvalidValidationError)(nil)) {
			return fmt.Errorf("validate struct: %w", validationErr)
		}

		var validationErrs validator.ValidationErrors
		if errors.As(validationErr, &validationErrs) {
			for _, fieldErr := range validationErrs {
				fieldName := getFieldKey(data, fieldErr.StructField())

				if fieldErr.Tag() == "required" {
					err = multierr.Append(err, requiredErr(fieldName))
				}
			}
		}
	}

	//nolint:wrapcheck // since we use multierr here, we don't want to wrap the error
	return err
}

// requiredErr returns the formatted required error.
func requiredErr(name string) error {
	return fmt.Errorf("%q value must be set", name)
}

// getFieldKey returns a key ("key" tag) for the provided fieldName. If the "key" tag is not present,
// the function will return a fieldName.
func getFieldKey(data any, fieldName string) string {
	// if the data is not pointer or it's nil, return a fieldName.
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr && !val.IsNil() {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return fieldName
	}

	structField, ok := val.Type().FieldByName(fieldName)
	if !ok {
		return fieldName
	}

	fieldKey := structField.Tag.Get(keyStructTag)
	if fieldKey == "" {
		return fieldName
	}

	return fieldKey
}
