SPDK_ROOT_DIR := /home/sophiawisdom/spdk

APP = nvme_key nvme_key_init nvme_read_key_async nvme_write_key_async ../automated_interface

include $(SPDK_ROOT_DIR)/mk/nvme.libtest.mk

ifeq ($(OS),Linux)
SYS_LIBS += -laio
CFLAGS += -DHAVE_LIBAIO -I../../sillydb
endif

install:
	$(INSTALL_EXAMPLE) $(APP) -I~/sillydb

uninstall:
	$(UNINSTALL_EXAMPLE)
