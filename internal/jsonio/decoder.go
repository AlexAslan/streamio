package jsonio

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"streamio/internal/record"
)

// Errors a JSON document can fail to decode with. They are deliberately specific about what this
// decoder accepts, because the honest answer is "flat JSON object" rather than "maybe malformed".
var (
	// ErrNotObject reports a line whose top-level value isn't a JSON object.
	ErrNotObject = errors.New("jsonio: document is not a JSON object")

	// ErrNestedValue reports an object or array under a field. Supporting it would mean inventing a
	// record.Record representation for arbitrary nesting, which nothing needs yet.
	ErrNestedValue = errors.New("jsonio: nested value not supported by Decoder")

	// ErrUnexpectedToken reports a token that cannot appear where it did, which means the document
	// isn't well-formed for this decoder's flat-object contract.
	ErrUnexpectedToken = errors.New("jsonio: unexpected token")
)

// ObjectDecoder decodes one flat JSON object document at a time into canonical records, reusable
// across many lines without per-line allocation.
type ObjectDecoder struct {
	dec *jsontext.Decoder
	src bytes.Reader
}

// NewObjectDecoder returns a reusable flat-object JSON decoder.
func NewObjectDecoder() *ObjectDecoder {
	d := &ObjectDecoder{}
	d.dec = jsontext.NewDecoder(&d.src)
	return d
}

// Decode refills rec from one JSON object document, preserving the document's field order.
func (d *ObjectDecoder) Decode(rec record.Record, doc []byte) (record.Record, error) {
	rec = rec.Reset()
	d.src.Reset(doc)
	d.dec.Reset(&d.src)

	open, err := d.dec.ReadToken()
	if err != nil {
		return rec, fmt.Errorf("%w: %w", ErrNotObject, err)
	}
	if open.Kind() != jsontext.KindBeginObject {
		return rec, fmt.Errorf("%w: starts with %v", ErrNotObject, open.Kind())
	}

	for {
		name, nameErr := d.dec.ReadToken()
		if nameErr != nil {
			return rec, nameErr
		}
		if name.Kind() == jsontext.KindEndObject {
			return rec, nil
		}
		if name.Kind() != jsontext.KindString {
			return rec, fmt.Errorf("%w: object name is %v", ErrUnexpectedToken, name.Kind())
		}

		field := name.String()
		value, valueErr := d.dec.ReadToken()
		if valueErr != nil {
			return rec, fmt.Errorf("field %q: %w", field, valueErr)
		}

		v, convErr := scalarFromToken(field, value)
		if convErr != nil {
			return rec, convErr
		}
		rec = rec.Append(field, v)
	}
}

// scalarFromToken classifies one JSON value token. A number is read as an int64 when it is one,
// since that is what the vast majority of NDJSON fields are and it avoids a lossy float round trip,
// and as a float64 otherwise — a fraction, an exponent, or a magnitude past int64.
func scalarFromToken(field string, tok jsontext.Token) (record.Value, error) {
	switch tok.Kind() {
	case jsontext.KindNull:
		return record.Null(), nil
	case jsontext.KindTrue, jsontext.KindFalse:
		return record.Bool(tok.Bool()), nil
	case jsontext.KindString:
		// tok.String() is the only exported string accessor jsontext.Token offers; it already
		// copies once internally, and there is no exported raw-bytes/zero-copy accessor to avoid
		// the second copy from the []byte conversion, so this stays a two-allocation path.
		return record.Bytes([]byte(tok.String())), nil
	case jsontext.KindNumber:
		if i, err := tok.Int(); err == nil {
			return record.Int64(i, record.SemanticNone), nil
		}
		f, err := tok.Float()
		if err != nil {
			return record.Value{}, fmt.Errorf("field %q: %w: %w", field, ErrUnexpectedToken, err)
		}
		return record.Float64(f), nil
	case jsontext.KindBeginObject, jsontext.KindBeginArray:
		return record.Value{}, fmt.Errorf("field %q: %w", field, ErrNestedValue)
	case jsontext.KindInvalid, jsontext.KindEndObject, jsontext.KindEndArray:
		return record.Value{}, fmt.Errorf("field %q: %w: %v", field, ErrUnexpectedToken, tok.Kind())
	default:
		return record.Value{}, fmt.Errorf("field %q: %w: %v", field, ErrUnexpectedToken, tok.Kind())
	}
}
