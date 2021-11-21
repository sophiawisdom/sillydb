SPDK_ROOT_DIR := /home/sophiawisdom/spdk

APP = nvme nvme_init nvme_cleanup nvme_write nvme_read ../manual_interface

include $(SPDK_ROOT_DIR)/mk/nvme.libtest.mk

ifeq ($(OS),Linux)
SYS_LIBS += -laio
CFLAGS += -DHAVE_LIBAIO -I../../sillydb
endif

install:
	$(INSTALL_EXAMPLE) $(APP) -I~/sillydb

uninstall:
	$(UNINSTALL_EXAMPLE)
