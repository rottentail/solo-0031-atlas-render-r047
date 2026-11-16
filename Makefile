.PHONY: build vet run demo capacity
build:
	go build ./...
vet:
	go vet ./...
run:
	go run ./cmd/atlas
demo:
	python3 scripts/walkthrough.py --workflow submit-render --out var/demo
capacity:
	python3 scripts/capacity_validation.py --workdir var/capacity
