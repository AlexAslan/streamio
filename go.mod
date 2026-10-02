module github.com/AlexAslan/streamio

go 1.27

require (
	github.com/AlexAslan/parquet-go v0.32.1
	github.com/apache/arrow-go/v18 v18.8.0
	github.com/spf13/cobra v1.10.2
	golang.org/x/sync v0.23.0
)

require (
	github.com/andybalholm/brotli v1.2.3 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/parquet-go/bitpack v1.0.3 // indirect
	github.com/parquet-go/jsonlite v1.5.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.29 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	github.com/twpayne/go-geom v1.6.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

retract [v0.0.1, v0.0.5] // Pre-public development versions: Apache-2.0 licensed and built with a replace directive; use v0.0.6 or later.
