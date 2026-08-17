module github.com/eduard-kolotushin/timeseries-baselines

go 1.26

require (
	github.com/eduard-kolotushin/timeseries v0.0.0
	github.com/eduard-kolotushin/timeseries-forecast v0.0.0
	github.com/segmentio/kafka-go v0.4.49
)

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)

replace github.com/eduard-kolotushin/timeseries => ../timeseries

replace github.com/eduard-kolotushin/timeseries-forecast => ../timeseries-forecast
