BUILD := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))build
# $(shell mkdir -p $(BUILD)/golang/pkg)
$(shell mkdir -p $(BUILD)/bin)