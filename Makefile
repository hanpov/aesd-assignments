# Top-level convenience Makefile: builds the socket server in server/.
all:
	$(MAKE) -C server

clean:
	$(MAKE) -C server clean

.PHONY: all clean
