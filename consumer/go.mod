module delivery-consumer

go 1.26.0

require (
	github.com/danielgtaylor/huma/v2 v2.38.0
	github.com/go-chi/chi/v5 v5.3.0
	github.com/sammidev/delivery-kafka/schema v0.0.0-00010101000000-000000000000
	github.com/twmb/franz-go v1.18.1
)

require (
	github.com/fxamacker/cbor/v2 v2.9.1 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
)

replace github.com/sammidev/delivery-kafka/schema => ../schema
