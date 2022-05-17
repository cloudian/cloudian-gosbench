# Makefile for Cloudian gosbench
#
# USAGE:
#	make gosserver   	           # To generate a new gosbench-ctl binary
#	make gosdriver	               # To generate a new gosbench-ctl binary
#
include vars.make

PROJECT_NAME := cloudian-gosbench
export PROJECT_NAME

PROJECT_DIR := $(CURDIR)
export PROJECT_DIR

BUILD_CONTAINER := golang:1.17.1
export BUILD_CONTAINER

MOD_DIR := $(CURDIR)

# ISO8601 UTC build timestamp
export BUILDTIME := $(shell date --utc +%FT%TZ)

SUBDIRS = ./cmd/gosserver ./cmd/gosdriver

.PHONY: clean build lint

all: build

clean:
	for dir in $(SUBDIRS); do \
    	$(MAKE) -C $$dir clean; \
    done
	rm -rf $(BUILD)

#clean_cache:
#	sudo rm -rf $(BUILD)/golang/pkg

build: lint
	for dir in $(SUBDIRS); do \
    	$(MAKE) -C $$dir all; \
    done

lint:
	@echo "Running gofmt"
	sudo docker run --rm --name ${PROJECT_NAME}-compiler -v $(CURDIR):/go/src/${PROJECT_NAME} -w /go/src/${PROJECT_NAME} ${BUILD_CONTAINER} gofmt -s -w .
