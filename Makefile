SPDK_ROOT_DIR := /home/sophiawisdom/spdk
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

DIRS-$(CONFIG_FIO_PLUGIN) += fio_plugin

.PHONY: all clean main

all: main
clean: main

include $(SPDK_ROOT_DIR)/mk/spdk.subdirs.mk
