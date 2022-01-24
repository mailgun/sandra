.PHONY: integration-test
integration-test:
	go test ./... -race -timeout 1m --count=1
