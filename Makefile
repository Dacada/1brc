CC = gcc
CFLAGS_OPT = -O3 -march=native -ftree-vectorize
CFLAGS_PRF = -O3 -march=native -ftree-vectorize -g -fno-omit-frame-pointer

SRC = analyze.c
BIN_OPT = analyze
BIN_PRF = analyze_prf

INPUT_TEST = measurements_short.txt
INPUT = measurements.txt
EXPECTED_OUTPUT = expected.txt
TEST_OUTPUT = test_output.txt
PERF_DATA = perf.data

.PHONY: all clean test perf run

all: $(BIN_OPT) $(BIN_PRF)

$(BIN_OPT): $(SRC)
	$(CC) $(CFLAGS_OPT) -o $@ $<

$(BIN_PRF): $(SRC)
	$(CC) $(CFLAGS_PRF) -o $@ $<

clean:
	rm -f $(BIN_OPT) $(BIN_PRF) $(TEST_OUTPUT) $(PERF_DATA) perf.data.old

test: $(TEST_OUTPUT) $(EXPECTED_OUTPUT)
	diff $(TEST_OUTPUT) $(EXPECTED_OUTPUT)

$(TEST_OUTPUT): $(BIN_OPT)
	./$(BIN_OPT) $(INPUT_TEST) > $(TEST_OUTPUT)

perf: $(PERF_DATA)
	perf report -i $(PERF_DATA)

$(PERF_DATA): $(BIN_PRF) $(INPUT)
	perf record --call-graph dwarf -o perf.data ./$(BIN_PRF) $(INPUT)

run: $(BIN_OPT) $(INPUT)
	./average_runtime.sh ./$(BIN_OPT) $(INPUT)