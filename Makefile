build:
	for pkg in $(shell go list ./cmd/...); do go build $$pkg; done
