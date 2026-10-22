# tests/unit/Makefile - unit tests for the shared modules in ../../common
#
#   make -C tests/unit run              build and run every test_*.c (no sanitizer)
#   make -C tests/unit run SAN=address  AddressSanitizer + UBSan (needs libasan, libubsan)
#   make -C tests/unit run SAN=thread   ThreadSanitizer (needs libtsan)
#
# The common modules are compiled into a static library, so a test links only
# the modules it actually uses (a module that does not exist yet is no problem).
CC      = gcc
SAN    ?=
BUILD  ?= build

ifeq ($(SAN),address)
  SAN_FLAGS = -fsanitize=address,undefined -fno-omit-frame-pointer
endif
ifeq ($(SAN),thread)
  SAN_FLAGS = -fsanitize=thread
endif

CFLAGS  = -std=gnu11 -Wall -Wextra -Werror -O1 -g -pthread -I../../common $(SAN_FLAGS)
LDFLAGS = -pthread $(SAN_FLAGS)

COMMON_SRCS = $(wildcard ../../common/*.c)
COMMON_OBJS = $(patsubst ../../common/%.c,$(BUILD)/obj/%.o,$(COMMON_SRCS))
TESTS       = $(patsubst %.c,$(BUILD)/%,$(wildcard test_*.c))

all: $(TESTS)

$(BUILD)/obj/%.o: ../../common/%.c $(wildcard ../../common/*.h)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILD)/libcommon.a: $(COMMON_OBJS)
	rm -f $@
	ar rcs $@ $^

$(BUILD)/test_%: test_%.c unit.h $(BUILD)/libcommon.a
	$(CC) $(CFLAGS) -o $@ $< $(BUILD)/libcommon.a $(LDFLAGS)

run: all
	@set -e; for t in $(TESTS); do echo "== $$t"; ./$$t; done; echo "ALL UNIT TESTS PASSED"

clean:
	rm -rf $(BUILD)

.PHONY: all run clean
