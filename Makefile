.PHONY: all debugtest test

DEBUG_FLAGS = -gcflags "-N -l"

all:
	go build

debugtest:
	go build $(DEBUG_FLAGS)
	go test -c $(DEBUG_FLAGS)

test:
	go build
	go test
