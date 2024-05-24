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

	"github.com/conduitio-labs/conduit-connector-hubspot/hubspot"
	"github.com/go-playground/validator/v10"
	"go.uber.org/multierr"
)

const (
	// keyStructTag is a tag which contains a field's key.
	keyStructTag = "key"
	// hubspotResourceTag is a tag for the [hubspotResource] validation.
	hubspotResourceTag = "hubspot_resource"
)

var (
	once sync.Once

	// validate is a singleton instance of the validator.
	validate *validator.Validate
)

// ValidateStruct validates a struct.
func ValidateStruct(data any) error {
	lazyInit()

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

				switch fieldErr.Tag() {
				case "required":
					err = multierr.Append(err, requiredErr(fieldName))
				case "gte":
					err = multierr.Append(err, gteErr(fieldName, fieldErr.Param()))
				case "lte":
					err = multierr.Append(err, lteErr(fieldName, fieldErr.Param()))
				case hubspotResourceTag:
					err = multierr.Append(err, hubspotResourceErr(fieldName))
				}
			}
		}
	}

	//nolint:wrapcheck // since we use multierr here, we don't want to wrap the error
	return err
}

// hubspotResource checks if a field's value is a supported HubSpot resource.
func hubspotResource(fl validator.FieldLevel) bool {
	_, ok := hubspot.ResourcesListPaths[fl.Field().String()]

	return ok
}

// requiredErr returns the formatted required error.
func requiredErr(name string) error {
	return fmt.Errorf("%q value must be set", name)
}

// gteErr returns the formatted gte error.
func gteErr(name, gte string) error {
	return fmt.Errorf("%q value must be greater than or equal to %s", name, gte)
}

// lteErr returns the formatted lte error.
func lteErr(name, lte string) error {
	return fmt.Errorf("%q value must be less than or equal to %s", name, lte)
}

// hubspotResourceErr returns the formatted hubspot_resource error.
func hubspotResourceErr(name string) error {
	return fmt.Errorf("%q value must be one of the supported HubSpot resources", name)
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

// lazyInit performs the initialization of a validator and registers custom validations.
func lazyInit() {
	once.Do(func() {
		validate = validator.New()

		if err := validate.RegisterValidation(hubspotResourceTag, hubspotResource); err != nil {
			panic(fmt.Errorf("register %q validation function: %w", hubspotResourceTag, err))
		}
	})
}
