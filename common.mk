SHELL := /bin/bash

# Fail early if GPHOME not found
ifeq ($(GPHOME),)
$(error GPHOME not defined)
endif

# Default PXF_HOME
PXF_HOME ?= $(GPHOME)/pxf
