// Package jsonio owns streamio's JSON output path: encoding canonical record.Records as JSON
// documents, and decoding NDJSON back into them.
//
// It writes JSON tokens straight to a jsontext.Encoder rather than marshalling a map, since
// reflection over a per-row map is the dominant cost this path avoids.
package jsonio

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"

	"github.com/AlexAslan/streamio/internal/formatio"
	"github.com/AlexAslan/streamio/internal/options"
	"github.com/AlexAslan/streamio/internal/record"
)

// errUnsupportedKind reports a record.Kind this encoder has no JSON representation for, which can
// only mean a Kind was added without teaching the encoders about it.
var errUnsupportedKind = errors.New("jsonio: unsupported value kind")

// encoder renders one record at a time as a self-contained JSON document. One encoder drives one
// decode worker and is not safe for concurrent use.
type encoder struct {
	enc *jsontext.Encoder

	// opts is set once and reused by every Reset call in encode, rather than a fresh
	// []jsontext.Options literal per record — encoder.EncodeBatch runs on the decode hot path.
	//
	// AllowDuplicateNames is required because writeMap preserves a Parquet Map(String,String)
	// column's entries verbatim, in source order — Parquet's Map has no uniqueness constraint, so
	// a row can legitimately carry the same key twice. Without this option jsontext's default RFC
	// 7493 uniqueness check would turn that into a hard encode error instead of the duplicate
	// member RFC 8259 itself leaves as unspecified (and that most JSON consumers, including
	// encoding/json's own map decoding, resolve as last-value-wins).
	opts []jsontext.Options

	// buf is the render destination, reused across every record so it grows to the widest one once
	// rather than per record. Each document is copied out of it before being returned.
	buf bytes.Buffer
}

// NewEncoder returns a JSON encoder for canonical records. cfg is unused; nothing about JSON
// output is configurable.
//
//nolint:ireturn // formatio.RecordEncoder is the constructor type streamio's format registry stores.
func NewEncoder(_ options.Config) (formatio.RecordEncoder, error) {
	opts := []jsontext.Options{jsontext.EscapeForHTML(true), jsontext.AllowDuplicateNames(true)}
	return &encoder{enc: jsontext.NewEncoder(io.Discard, opts...), opts: opts}, nil
}

// EncodeBatch renders every record in batch as its own JSON document, one []byte per record.
func (e *encoder) EncodeBatch(batch []record.Record) ([][]byte, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	docs := make([][]byte, 0, len(batch))
	for i := range batch {
		e.buf.Reset()
		if err := e.encode(batch[i]); err != nil {
			return nil, err
		}

		doc := make([]byte, e.buf.Len())
		copy(doc, e.buf.Bytes())
		docs = append(docs, doc)
	}

	return docs, nil
}

// encode renders rec into the encoder's buffer as a single-line JSON object.
func (e *encoder) encode(rec record.Record) error {
	e.enc.Reset(&e.buf, e.opts...)

	if err := e.enc.WriteToken(jsontext.BeginObject); err != nil {
		return err
	}
	for i := range rec {
		if err := e.writeField(rec[i]); err != nil {
			return err
		}
	}
	if err := e.enc.WriteToken(jsontext.EndObject); err != nil {
		return err
	}

	// jsontext terminates a top-level value with a newline. A document here is one element of a
	// stream whose framing belongs to the sink, not to the encoder, so trim it back off.
	e.trimTrailingNewline()
	return nil
}

// writeField writes one field's name followed by its value.
func (e *encoder) writeField(f record.Field) error {
	if err := e.enc.WriteToken(jsontext.String(f.Name)); err != nil {
		return err
	}
	return e.writeValue(f.Value)
}

// writeValue writes v, letting its Semantic decide how an otherwise ambiguous physical value is
// rendered — this is where classification made at decode time finally turns into bytes.
func (e *encoder) writeValue(v record.Value) error {
	switch v.Kind {
	case record.KindNull:
		return e.enc.WriteToken(jsontext.Null)
	case record.KindBool:
		return e.enc.WriteToken(jsontext.Bool(v.Bool))
	case record.KindInt64:
		return e.writeInt(v)
	case record.KindFloat64:
		if v.Semantic == record.SemanticFloat32 {
			return WriteFloat(e.enc, v.F64, Bits32)
		}
		return WriteFloat(e.enc, v.F64, Bits64)
	case record.KindBytes:
		return e.enc.WriteToken(jsontext.String(string(v.Str)))
	case record.KindMap:
		return e.writeMap(v.Map)
	case record.KindList:
		return e.writeList(v.List)
	default:
		return fmt.Errorf("%w: %v", errUnsupportedKind, v.Kind)
	}
}

// writeInt writes an integer value as whatever its Semantic says it really is: a date, an epoch
// timestamp at the precision it was stored with, an unsigned integer, or a plain signed one.
func (e *encoder) writeInt(v record.Value) error {
	switch v.Semantic {
	case record.SemanticDate:
		return e.enc.WriteToken(jsontext.String(Date(v.I64)))
	case record.SemanticTimestampMillis:
		return e.enc.WriteToken(jsontext.String(TimestampMillis(v.I64)))
	case record.SemanticTimestampMicros:
		return e.enc.WriteToken(jsontext.String(TimestampMicros(v.I64)))
	case record.SemanticTimestampNanos:
		return e.enc.WriteToken(jsontext.String(TimestampNanos(v.I64)))
	case record.SemanticUnsigned:
		//nolint:gosec // Deliberate: SemanticUnsigned means the I64 bits are a uint64.
		return e.enc.WriteToken(jsontext.Uint(uint64(v.I64)))
	case record.SemanticNone, record.SemanticFloat32:
		// SemanticFloat32 says nothing about an integer; treat it as the plain value it is rather
		// than failing on a tag that simply doesn't apply to this Kind.
		return e.enc.WriteToken(jsontext.Int(v.I64))
	default:
		return e.enc.WriteToken(jsontext.Int(v.I64))
	}
}

// writeMap writes a nested key/value group as a JSON object. Entries keep the order the decoder
// produced them in, which for a Parquet Map(String,String) is the order they were stored in.
func (e *encoder) writeMap(entries record.Record) error {
	if err := e.enc.WriteToken(jsontext.BeginObject); err != nil {
		return err
	}
	for i := range entries {
		if err := e.writeField(entries[i]); err != nil {
			return err
		}
	}
	return e.enc.WriteToken(jsontext.EndObject)
}

// writeList writes a nested ordered sequence as a JSON array, in its original element order.
func (e *encoder) writeList(elements []record.Value) error {
	if err := e.enc.WriteToken(jsontext.BeginArray); err != nil {
		return err
	}
	for i := range elements {
		if err := e.writeValue(elements[i]); err != nil {
			return err
		}
	}
	return e.enc.WriteToken(jsontext.EndArray)
}

// trimTrailingNewline truncates the buffer back past any newline jsontext appended after the
// document it holds.
func (e *encoder) trimTrailingNewline() {
	b := e.buf.Bytes()
	n := len(b)
	for n > 0 && b[n-1] == '\n' {
		n--
	}
	e.buf.Truncate(n)
}
