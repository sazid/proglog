all:
	go build -o ./bin/server ./cmd/server/main.go

clean:
	rm -rf bin/

compile:
	protoc api/v1/*.proto \
			--go_out=. \
			--go_opt=paths=source_relative \
			--proto_path=.

test:
	go test -race ./...
