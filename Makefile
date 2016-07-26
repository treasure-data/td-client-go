.PHONY: all install test

all: test

install:
	go get github.com/ugorji/go/codec

test: install
	go test -v ./...
