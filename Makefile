proto:
	protoc --gofast_out=. internal/protocol/commands.proto

.PHONY: proto
