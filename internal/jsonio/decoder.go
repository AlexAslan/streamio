package jsonio

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"

	"github.com/AlexAslan/streamio/internal/record"
)

// Errors a JSON document can fail to decode with. They are deliberately specific about what this
// decoder accepts, because the honest answer is "an object, arbitrarily nested" rather than "maybe
// malformed".
var (
	// ErrNotObject reports a line whose top-level value isn't a JSON object.
	ErrNotObject = errors.New("jsonio: document is not a JSON object")

	// ErrUnexpectedToken reports a token that cannot appear where it did, which means the document
	// isn't well-formed for this decoder's object contract.
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

// Decode refills rec from one JSON object document, preserving the document's field order. A
// nested object or array decodes recursively into record.KindMap/record.KindList rather than being
// rejected, to whatever depth the document actually has.
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

	rec, err = d.decodeObjectBody(rec, "")
	if err != nil {
		return rec, err
	}

	if _, trailingErr := d.dec.ReadToken(); !errors.Is(trailingErr, io.EOF) {
		return rec, fmt.Errorf("%w: trailing data after closing brace", ErrUnexpectedToken)
	}
	return rec, nil
}

// decodeObjectBody reads name/value pairs until the object's closing brace, appending each as a
// field of rec. path names the object itself, for error messages naming a nested field's full
// location (e.g. "attributes.retries") rather than just its bare name.
func (d *ObjectDecoder) decodeObjectBody(rec record.Record, path string) (record.Record, error) {
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
		v, err := d.decodeValue(fieldPath(path, field))
		if err != nil {
			return rec, err
		}
		rec = rec.Append(field, v)
	}
}

// decodeListBody reads elements until the array's closing bracket, returning them in order. path
// names the array itself, for error messages naming a nested element's location.
func (d *ObjectDecoder) decodeListBody(path string) ([]record.Value, error) {
	var elements []record.Value
	for {
		if d.dec.PeekKind() == ']' {
			_, closeErr := d.dec.ReadToken()
			return elements, closeErr
		}

		v, err := d.decodeValue(path)
		if err != nil {
			return nil, err
		}
		elements = append(elements, v)
	}
}

// decodeValue reads one complete JSON value — scalar, nested object, or nested array — at the
// decoder's current position. path names the value's location, used only in error messages.
func (d *ObjectDecoder) decodeValue(path string) (record.Value, error) {
	tok, err := d.dec.ReadToken()
	if err != nil {
		return record.Value{}, fmt.Errorf("field %q: %w", path, err)
	}

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
		return numberFromToken(path, tok)
	case jsontext.KindBeginObject:
		nested, nestedErr := d.decodeObjectBody(nil, path)
		if nestedErr != nil {
			return record.Value{}, nestedErr
		}
		return record.Map(nested), nil
	case jsontext.KindBeginArray:
		elements, listErr := d.decodeListBody(path)
		if listErr != nil {
			return record.Value{}, listErr
		}
		return record.List(elements), nil
	case jsontext.KindInvalid, jsontext.KindEndObject, jsontext.KindEndArray:
		return record.Value{}, fmt.Errorf("field %q: %w: %v", path, ErrUnexpectedToken, tok.Kind())
	default:
		return record.Value{}, fmt.Errorf("field %q: %w: %v", path, ErrUnexpectedToken, tok.Kind())
	}
}

// numberFromToken classifies a JSON number token. It's read as an int64 when it is one, since that
// is what the vast majority of NDJSON fields are and it avoids a lossy float round trip, and as a
// float64 otherwise — a fraction, an exponent, or a magnitude past int64.
func numberFromToken(path string, tok jsontext.Token) (record.Value, error) {
	if i, err := tok.Int(); err == nil {
		return record.Int64(i, record.SemanticNone), nil
	}
	f, err := tok.Float()
	if err != nil {
		return record.Value{}, fmt.Errorf("field %q: %w: %w", path, ErrUnexpectedToken, err)
	}
	return record.Float64(f), nil
}

// fieldPath appends field to path, dot-joined, for a nested field's error-message location. An
// empty path (the document's own top-level fields) yields just field, with no leading dot.
func fieldPath(path, field string) string {
	if path == "" {
		return field
	}
	return path + "." + field
}
