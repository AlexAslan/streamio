package record_test

import (
	"testing"

	"github.com/AlexAslan/streamio/internal/record"
)

// allocRuns is the sample size for every testing.AllocsPerRun assertion here. Large enough that a
// single stray allocation shows up as a non-zero average, small enough to stay fast.
const allocRuns = 1000

// maxUint64AsInt64 is math.MaxUint64 reinterpreted as an int64, the bit pattern a SemanticUnsigned
// Value carries for the largest uint64.
const maxUint64AsInt64 = -1

// TestValueConstructors checks each constructor tags the Kind/Semantic it claims and parks the
// payload in the field that Kind selects.
func TestValueConstructors(t *testing.T) {
	type args struct {
		check    func(record.Value) bool
		name     string
		value    record.Value
		wantKind record.Kind
		wantSem  record.Semantic
	}

	tests := []args{
		{
			name:     "null",
			value:    record.Null(),
			wantKind: record.KindNull,
			wantSem:  record.SemanticNone,
			check:    func(v record.Value) bool { return v.IsNull() },
		},
		{
			name:     "bool",
			value:    record.Bool(true),
			wantKind: record.KindBool,
			wantSem:  record.SemanticNone,
			check:    func(v record.Value) bool { return v.Bool && !v.IsNull() },
		},
		{
			name:     "plain int64",
			value:    record.Int64(42, record.SemanticNone),
			wantKind: record.KindInt64,
			wantSem:  record.SemanticNone,
			check:    func(v record.Value) bool { return v.I64 == 42 },
		},
		{
			name:     "date keeps the raw day count",
			value:    record.Int64(19723, record.SemanticDate),
			wantKind: record.KindInt64,
			wantSem:  record.SemanticDate,
			check:    func(v record.Value) bool { return v.I64 == 19723 },
		},
		{
			name:     "millisecond timestamp keeps the raw epoch value",
			value:    record.Int64(1704067200000, record.SemanticTimestampMillis),
			wantKind: record.KindInt64,
			wantSem:  record.SemanticTimestampMillis,
			check:    func(v record.Value) bool { return v.I64 == 1704067200000 },
		},
		{
			name:     "microsecond timestamp keeps its own unit",
			value:    record.Int64(1704067200000000, record.SemanticTimestampMicros),
			wantKind: record.KindInt64,
			wantSem:  record.SemanticTimestampMicros,
			check:    func(v record.Value) bool { return v.I64 == 1704067200000000 },
		},
		{
			name:     "nanosecond timestamp keeps its own unit",
			value:    record.Int64(1704067200000000000, record.SemanticTimestampNanos),
			wantKind: record.KindInt64,
			wantSem:  record.SemanticTimestampNanos,
			check:    func(v record.Value) bool { return v.I64 == 1704067200000000000 },
		},
		{
			name:     "unsigned keeps the bit pattern",
			value:    record.Int64(maxUint64AsInt64, record.SemanticUnsigned),
			wantKind: record.KindInt64,
			wantSem:  record.SemanticUnsigned,
			check:    func(v record.Value) bool { return uint64(v.I64) == ^uint64(0) },
		},
		{
			name:     "float64",
			value:    record.Float64(1.5),
			wantKind: record.KindFloat64,
			wantSem:  record.SemanticNone,
			check:    func(v record.Value) bool { return v.F64 == 1.5 },
		},
		{
			name:     "float32 widens but keeps its precision tag",
			value:    record.Float32(0.1),
			wantKind: record.KindFloat64,
			wantSem:  record.SemanticFloat32,
			check:    func(v record.Value) bool { return v.F64 == float64(float32(0.1)) },
		},
		{
			name:     "bytes",
			value:    record.Bytes([]byte("hello")),
			wantKind: record.KindBytes,
			wantSem:  record.SemanticNone,
			check:    func(v record.Value) bool { return string(v.Str) == "hello" },
		},
		{
			name:     "map carries its entries in order",
			value:    record.Map(record.Record{}.Append("k", record.Bytes([]byte("v")))),
			wantKind: record.KindMap,
			wantSem:  record.SemanticNone,
			check: func(v record.Value) bool {
				return len(v.Map) == 1 && v.Map[0].Name == "k" && string(v.Map[0].Value.Str) == "v"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value.Kind != tt.wantKind {
				t.Errorf("Kind = %v, want %v", tt.value.Kind, tt.wantKind)
			}
			if tt.value.Semantic != tt.wantSem {
				t.Errorf("Semantic = %v, want %v", tt.value.Semantic, tt.wantSem)
			}
			if !tt.check(tt.value) {
				t.Errorf("payload check failed for %+v", tt.value)
			}
		})
	}
}

// TestBytesDoesNotCopy pins the documented aliasing contract: Bytes references the caller's slice,
// so mutating the source is visible through the Value. Anything that starts copying here silently
// reintroduces a per-field allocation on the decode hot path.
func TestBytesDoesNotCopy(t *testing.T) {
	src := []byte("abc")
	v := record.Bytes(src)
	src[0] = 'z'

	if string(v.Str) != "zbc" {
		t.Fatalf("Value.Str = %q, want %q — Bytes copied its argument", v.Str, "zbc")
	}
}

// TestValueConstructorsDoNotAllocate is the load-bearing property of this package: a Value is a
// tagged union, so building one for a scalar must not allocate the way boxing into an any would.
func TestValueConstructorsDoNotAllocate(t *testing.T) {
	type args struct {
		build func() record.Value
		name  string
	}

	str := []byte("some string value")
	tests := []args{
		{name: "null", build: record.Null},
		{name: "bool", build: func() record.Value { return record.Bool(true) }},
		{name: "int64", build: func() record.Value { return record.Int64(42, record.SemanticNone) }},
		{name: "date", build: func() record.Value { return record.Int64(19723, record.SemanticDate) }},
		{name: "float64", build: func() record.Value { return record.Float64(1.5) }},
		{name: "bytes", build: func() record.Value { return record.Bytes(str) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sink record.Value
			got := testing.AllocsPerRun(allocRuns, func() { sink = tt.build() })
			if sink.Kind > record.KindMap {
				t.Fatalf("unexpected kind %v", sink.Kind)
			}
			if got != 0 {
				t.Errorf("got %.2f allocations per build, want 0", got)
			}
		})
	}
}

// TestRecordReuseDoesNotAllocate verifies the reuse discipline the decode path depends on: once a
// Record has grown to hold a row's fields, refilling it via Reset+Append must not allocate again.
func TestRecordReuseDoesNotAllocate(t *testing.T) {
	const fields = 8

	rec := make(record.Record, 0, fields)
	names := make([]string, fields)
	for i := range names {
		names[i] = string(rune('a' + i))
	}

	// Grow once outside the measured region so the first fill's allocation isn't counted.
	for i, name := range names {
		rec = rec.Append(name, record.Int64(int64(i), record.SemanticNone))
	}

	got := testing.AllocsPerRun(allocRuns, func() {
		rec = rec.Reset()
		for i, name := range names {
			rec = rec.Append(name, record.Int64(int64(i), record.SemanticNone))
		}
	})
	if got != 0 {
		t.Errorf("got %.2f allocations per row refill, want 0", got)
	}
	if len(rec) != fields {
		t.Errorf("len(rec) = %d, want %d", len(rec), fields)
	}
}

// TestRecordResetKeepsCapacity checks Reset truncates without discarding the backing array.
func TestRecordResetKeepsCapacity(t *testing.T) {
	rec := make(record.Record, 0, 4).
		Append("a", record.Int64(1, record.SemanticNone)).
		Append("b", record.Bool(false))

	reset := rec.Reset()
	if len(reset) != 0 {
		t.Errorf("len after Reset = %d, want 0", len(reset))
	}
	if cap(reset) != cap(rec) {
		t.Errorf("cap after Reset = %d, want %d", cap(reset), cap(rec))
	}
}

// TestRecordLookup covers hits, misses, and the documented first-wins rule for duplicate names.
func TestRecordLookup(t *testing.T) {
	type args struct {
		name    string
		lookup  string
		wantOK  bool
		wantI64 int64
	}

	rec := record.Record{}.
		Append("id", record.Int64(7, record.SemanticNone)).
		Append("name", record.Bytes([]byte("x"))).
		Append("id", record.Int64(99, record.SemanticNone))

	tests := []args{
		{name: "hit", lookup: "name", wantOK: true},
		{name: "first occurrence wins", lookup: "id", wantOK: true, wantI64: 7},
		{name: "miss", lookup: "absent", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, ok := rec.Lookup(tt.lookup)
			if ok != tt.wantOK {
				t.Fatalf("Lookup(%q) ok = %v, want %v", tt.lookup, ok, tt.wantOK)
			}
			if tt.wantI64 != 0 && v.I64 != tt.wantI64 {
				t.Errorf("Lookup(%q).I64 = %d, want %d", tt.lookup, v.I64, tt.wantI64)
			}
		})
	}
}

// TestKindString and TestSemanticString cover the diagnostic names, including the out-of-range
// guard both String methods carry.
func TestKindString(t *testing.T) {
	type args struct {
		want string
		kind record.Kind
	}

	tests := []args{
		{kind: record.KindNull, want: "null"},
		{kind: record.KindBool, want: "bool"},
		{kind: record.KindInt64, want: "int64"},
		{kind: record.KindFloat64, want: "float64"},
		{kind: record.KindBytes, want: "bytes"},
		{kind: record.KindMap, want: "map"},
		{kind: record.Kind(200), want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.kind.String(); got != tt.want {
				t.Errorf("Kind(%d).String() = %q, want %q", tt.kind, got, tt.want)
			}
		})
	}
}

func TestSemanticString(t *testing.T) {
	type args struct {
		want     string
		semantic record.Semantic
	}

	tests := []args{
		{semantic: record.SemanticNone, want: "none"},
		{semantic: record.SemanticDate, want: "date"},
		{semantic: record.SemanticTimestampMillis, want: "timestamp-millis"},
		{semantic: record.SemanticTimestampMicros, want: "timestamp-micros"},
		{semantic: record.SemanticTimestampNanos, want: "timestamp-nanos"},
		{semantic: record.SemanticUnsigned, want: "unsigned"},
		{semantic: record.SemanticFloat32, want: "float32"},
		{semantic: record.Semantic(200), want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.semantic.String(); got != tt.want {
				t.Errorf("Semantic(%d).String() = %q, want %q", tt.semantic, got, tt.want)
			}
		})
	}
}
