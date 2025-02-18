<p align="center">
  <img src="https://github.com/tobias-piotr/leshy/assets/49806746/c4b62d2b-e79b-4d1a-ad68-132876354051" width="300">
</p>

<p align="center">
  <img src="https://github.com/tobias-piotr/leshy/actions/workflows/ci.yml/badge.svg?branch=main" alt="Test">
  <img src="https://goreportcard.com/badge/github.com/tobias-piotr/leshy" alt="Report">
  <img src="https://gh.kaos.st/apache2.svg" alt="License">
</p>

# Leshy

DX-focused message queue, powered by Golang, SQLite and gRPC.

## Setting up

Generating protobuffs:

```bash
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/*.proto
```
