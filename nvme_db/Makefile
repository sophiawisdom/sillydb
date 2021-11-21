SPDK_ROOT_DIR := /home/sophiawisdom/spdk

APP = main

include $(SPDK_ROOT_DIR)/mk/nvme.libtest.mk

ifeq ($(OS),Linux)
SYS_LIBS += -laio
CFLAGS += -DHAVE_LIBAIO
endif

install: $(APP)
	$(INSTALL_EXAMPLE)

uninstall:
	$(UNINSTALL_EXAMPLE)
