package dag

import (
	"encoding/json"
	"fmt"
	"io"
)

type Parser interface {
	Parse(reader io.Reader) (*Dag, error)
}

type JSONParser struct{}

func NewJSONParser() *JSONParser {
	return &JSONParser{}
}

func (p *JSONParser) Parse(reader io.Reader) (*Dag, error) {
	var dag Dag

	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(&dag); err != nil {
		return nil, fmt.Errorf("failed to parse json: %w", err)
	}

	validator := NewValidator()
	if err := validator.Validate(&dag); err != nil {
		return nil, fmt.Errorf("invalid dag config: %w", err)
	}

	return &dag, nil
}
