all:
	go build -o ./bin/server ./cmd/server/main.go

clean:
	rm -rf bin/
