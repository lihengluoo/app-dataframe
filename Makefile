CC ?= cc
CFLAGS ?= -O2 -g

root_dir := $(abspath ../..)
llvm_root ?= $(root_dir)/dependencies/install/llvm21
clang := $(llvm_root)/bin/clang
wllvm ?= $(root_dir)/dependencies/install/wllvm/bin/wllvm
build_dir ?= build
ir_dir ?= $(build_dir)/ir

# Keep required flags outside CFLAGS: scripts/build.sh supplies its own
# capture flags while this Makefile supplies the language and thread flags.
compile_flags := -D_POSIX_C_SOURCE=200809L -std=c11 -Wall -Wextra -Werror \
                 -Wpedantic -Wstrict-prototypes -Wmissing-prototypes -pthread

.PHONY: all benchmark test ir clean

all: benchmark

benchmark: $(build_dir)/dataframe

test: benchmark
	python3 tests/test_dataframe.py $(build_dir)/dataframe

$(build_dir)/dataframe: dataframe.c dataframe.h | $(build_dir)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(compile_flags) dataframe.c $(LDFLAGS) $(LDLIBS) -lm -o $@

ir:
	"$(root_dir)/scripts/build.sh" --make-dir . \
	    --make-target benchmark --executable wllvm/dataframe --name dataframe \
	    --clang "$(clang)" --wllvm "$(wllvm)" --output-dir "$(ir_dir)"

$(build_dir):
	mkdir -p $@

clean:
	rm -rf "$(build_dir)" .dataframe.o*
