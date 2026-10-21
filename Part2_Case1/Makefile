# Makefile - Part II Case 1: fault injection, no session management
# IT305 Socket Programming Project, Group 006 + 043.
#
#   make                 build exactly two executables here: ./server and ./client
#   make strict          clean rebuild with -Werror (our zero-warning gate)
#   make SAN=address     AddressSanitizer + UBSan build   (run 'make clean' first)
#   make SAN=thread      ThreadSanitizer build            (run 'make clean' first)
#   make CC=clang        build with clang
#   make clean           remove objects and executables
#
# The shared modules (net, protocol, topic_map, ...) are copies of ../common/
# made by tools/sync_common.sh, so this folder builds on its own.

CC       = gcc
OPT     ?= -O2
WERROR  ?=
SAN     ?=

ifeq ($(SAN),address)
  SAN_FLAGS = -fsanitize=address,undefined -fno-omit-frame-pointer -g
endif
ifeq ($(SAN),thread)
  SAN_FLAGS = -fsanitize=thread -g
endif

CFLAGS   = -std=gnu11 -Wall -Wextra $(OPT) -pthread -MMD -MP $(SAN_FLAGS) $(WERROR)
LDFLAGS  = -pthread $(SAN_FLAGS)

COMMON      = util.o net.o protocol.o log.o topic_map.o transfer.o server_core.o fault.o
SERVER_OBJS = server.o $(COMMON)
CLIENT_OBJS = client.o $(COMMON)

all: server client

server: $(SERVER_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

client: $(CLIENT_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

strict:
	$(MAKE) clean
	$(MAKE) all WERROR=-Werror

clean:
	rm -f *.o *.d server client

-include $(wildcard *.d)

.PHONY: all strict clean
