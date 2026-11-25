# Builds aesdsocket natively or with a cross compiler:
#   make                                   # native gcc
#   make CROSS_COMPILE=aarch64-none-linux-gnu-
# Yocto/Buildroot pass CC, CFLAGS and LDFLAGS through the environment.

ifeq ($(origin CC),default)
CC = $(CROSS_COMPILE)gcc
endif
CFLAGS ?= -g -O0
CFLAGS += -Wall -Werror
LDFLAGS ?=
LDLIBS += -pthread

TARGET = aesdsocket

all: $(TARGET)

$(TARGET): aesdsocket.c
	$(CC) $(CFLAGS) -pthread $^ -o $@ $(LDFLAGS) $(LDLIBS)

clean:
	rm -f $(TARGET) *.o

.PHONY: all clean
