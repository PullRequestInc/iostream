.PHONY: vet
vet:
	go vet ./...

.PHONY: build
build:
	go build ./...

.PHONY: test
test:
	go test ./...

.PHONY: check
check: vet build test
