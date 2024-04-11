/*
This package attempts to generate JSON schema from Go structs. It is a work in
progress. It is a small tool tailored to our specific needs without using a
bulky external API library. Still most of the API specification is done by hand
because it doesn't change that often and we add extra annotations in the OpenAPI
specification.
*/
package main

import (
	"fmt"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"strings"

	"github.com/semafind/semadb/models"
	"gopkg.in/yaml.v3"
)

const (
	typeObject  = "object"
	typeArray   = "array"
	typeString  = "string"
	typeNumber  = "number"
	typeBoolean = "boolean"
)

// Encapsulates a JSON schema definition
type schema struct {
	Ref                  string            `yaml:"$ref,omitempty"`
	Type                 string            `yaml:"type,omitempty"`
	Description          string            `yaml:"description,omitempty"`
	Properties           map[string]schema `yaml:"properties,omitempty"`
	Required             []string          `yaml:"required,omitempty"`
	Enum                 []string          `yaml:"enum,omitempty"`
	Minimum              float32           `yaml:"minimum,omitempty"`
	Maximum              float32           `yaml:"maximum,omitempty"`
	Pattern              string            `yaml:"pattern,omitempty"`
	MinLength            int               `yaml:"minLength,omitempty"`
	MaxLength            int               `yaml:"maxLength,omitempty"`
	MinItems             int               `yaml:"minItems,omitempty"`
	MaxItems             int               `yaml:"maxItems,omitempty"`
	AdditionalProperties *schema           `yaml:"additionalProperties,omitempty"`
}

func generateSchema(t reflect.Type, documentation map[string]string, schemas map[string]schema) error {
	s := schema{
		Description: "TOFILL",
		Properties:  make(map[string]schema),
	}
	if doc, ok := documentation[t.Name()]; ok {
		s.Description = doc
	}
	// ---------------------------
	for i := 0; i < t.NumField(); i++ {
		// A struct field
		// e.g. MyField string `json:"type" binding:"required,oneof=none binary product"`
		field := t.Field(i)
		// ---------------------------
		if !field.IsExported() {
			fmt.Printf("Skipping unexported field %s\n", field.Name)
			continue
		}
		// ---------------------------
		jsonTag, ok := field.Tag.Lookup("json")
		if !ok {
			return fmt.Errorf("field %s in %s has no json tag", field.Name, t.Name())
		}
		jsonName := strings.Split(jsonTag, ",")[0]
		// ---------------------------
		// e.g. "required,oneof=none binary product"
		fieldSchema := schema{
			Description: "TOFILL",
		}
		if doc, ok := documentation[t.Name()+"."+field.Name]; ok {
			fieldSchema.Description = doc
		}
		// ---------------------------
		// Process bindings
		bindingTag := field.Tag.Get("binding")
		bindings := strings.Split(bindingTag, ",")
		for _, binding := range bindings {
			switch {
			case len(binding) == 0:
				continue
			case binding == "dive":
				continue
			case binding == "required":
				s.Required = append(s.Required, jsonName)
			case strings.HasPrefix(binding, "oneof="):
				oneOf := strings.Split(binding, "=")[1]
				fieldSchema.Enum = strings.Split(oneOf, " ")
			case binding == "alphanum":
				fieldSchema.Pattern = "^[a-zA-Z0-9]*$"
			case strings.HasPrefix(binding, "min="):
				minValueS := strings.Split(binding, "=")[1]
				minValue, err := strconv.ParseFloat(minValueS, 32)
				if err != nil {
					return fmt.Errorf("min value %s is not a number", minValueS)
				}
				if field.Type.Kind() == reflect.String {
					fieldSchema.MinLength = int(minValue)
				} else {
					fieldSchema.Minimum = float32(minValue)
				}
			case strings.HasPrefix(binding, "max="):
				maxValueS := strings.Split(binding, "=")[1]
				maxValue, err := strconv.ParseFloat(maxValueS, 32)
				if err != nil {
					return fmt.Errorf("max value %s is not a number", maxValueS)
				}
				if field.Type.Kind() == reflect.String {
					fieldSchema.MaxLength = int(maxValue)
				} else {
					fieldSchema.Maximum = float32(maxValue)
				}
			default:
				return fmt.Errorf("unsupported binding %s", binding)
			}
		}
		// ---------------------------
		// Handle field type
		switch field.Type.Kind() {
		case reflect.String:
			fieldSchema.Type = typeString
		case reflect.Bool:
			fieldSchema.Type = typeBoolean
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fallthrough
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fallthrough
		case reflect.Float32, reflect.Float64:
			fieldSchema.Type = typeNumber
		case reflect.Struct:
			fieldSchema.Ref = fmt.Sprintf("#/components/schemas/%s", field.Type.Name())
			if err := generateSchema(field.Type, documentation, schemas); err != nil {
				return err
			}
		case reflect.Map:
			// e.g. map[string]string
			fieldSchema.Type = typeObject
			keyType := field.Type.Key()
			if keyType.Kind() != reflect.String {
				return fmt.Errorf("unsupported map key type %s", keyType.Kind())
			}
			valueType := field.Type.Elem()
			fieldSchema.AdditionalProperties = &schema{
				Ref: fmt.Sprintf("#/components/schemas/%s", valueType.Name()),
			}
			if err := generateSchema(valueType, documentation, schemas); err != nil {
				return err
			}
		case reflect.Pointer:
			// e.g. *float32
			elemType := field.Type.Elem()
			switch elemType.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				fallthrough
			case reflect.Float32, reflect.Float64:
				fieldSchema.Type = typeNumber
			case reflect.Struct:
				// e.g. *BinaryQuantizerParamaters
				fieldSchema.Ref = fmt.Sprintf("#/components/schemas/%s", elemType.Name())
				if err := generateSchema(elemType, documentation, schemas); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported pointer type %s", elemType.Kind())
			}
		default:
			return fmt.Errorf("unsupported type %s for %s", field.Type.Kind(), field.Name)
		}
		s.Properties[jsonName] = fieldSchema
	}
	schemas[t.Name()] = s
	return nil
}

// Attempts to extract struct and field documentation from Go source files. This
// is used as description in the JSON schema.
func parseDocumentation(directory string, documentation map[string]string) error {
	fset := token.NewFileSet()
	d, err := parser.ParseDir(fset, directory, nil, parser.ParseComments)
	if err != nil {
		panic(err)
	}
	for packageName, pkg := range d {
		// Package documentation gives access to struct documentation
		// the comments above type ... struct {}
		docPkg := doc.New(pkg, "./", doc.AllDecls)
		for _, t := range docPkg.Types {
			documentation[t.Name] = t.Doc
		}
		for fname, f := range pkg.Files {
			fmt.Println(packageName, fname)
			for _, decl := range f.Decls {
				typeSpec, ok := decl.(*ast.GenDecl)
				if !ok {
					continue
				}
				for _, spec := range typeSpec.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					structType, ok := typeSpec.Type.(*ast.StructType)
					if !ok {
						continue
					}
					structName := typeSpec.Name.Name
					for _, field := range structType.Fields.List {
						for _, name := range field.Names {
							// Here is the field documentation
							// Something like:
							// Documentation for my field
							// myfield int `json:"myfield" binding:"required"`
							documentation[structName+"."+name.Name] = field.Doc.Text()
						}
					}
				}
			}
		}
	}
	return nil
}

func main() {
	toEncode := []any{
		models.Quantizer{},
	}
	schemas := make(map[string]schema)
	// ---------------------------
	// Parse documentation
	documentation := make(map[string]string)
	if err := parseDocumentation("httpapi/v2", documentation); err != nil {
		panic(err)
	}
	if err := parseDocumentation("models", documentation); err != nil {
		panic(err)
	}
	// ---------------------------
	// Generate schema
	for _, model := range toEncode {
		t := reflect.TypeOf(model)
		err := generateSchema(t, documentation, schemas)
		if err != nil {
			panic(err)
		}
	}
	// ---------------------------
	yamlSchemas, _ := yaml.Marshal(schemas)
	fmt.Println(string(yamlSchemas))
}
