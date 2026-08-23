# journal/

The journal is the sequencer's product: a single, ordered, authoritative
record of every input a state machine has processed and every output it
produced, stored as a pair of memory-mapped files (a variable-length
data file plus a fixed-size index file) that any number of readers can
tail with zero copying and zero coordination with the writer. One
release-store of a "committed count," paired with an acquire-load on
every reader, is the entire synchronization protocol — no locks, no
reader registration, and a reader can never backpressure the writer.

Full details — the on-disk format, why it's two files, and why the
write order (data, then index, then the committed-count release-store)
is load-bearing rather than stylistic — are in
**[the specification, §6](../docs/specification.md#6-the-journal)**.
This library (`journal/`) is header-only and depends on nothing else in
this repository (§9.1).

## Testing

```sh
cmake --preset debug
cmake --build --preset debug
ctest --preset debug --output-on-failure
```

18 Google Test cases, across four files in `tests/`:

| File | Covers |
|---|---|
| `format_test.cpp` | Pure in-memory encode/decode of the record layout — no files involved. |
| `writer_reader_test.cpp` | Round trips (varying output counts, large inputs), O(1) random access by sequence number, reopen-and-continue, and the dense-sequence-number guard in `append()`. |
| `recovery_test.cpp` | A hand-crafted **torn write** (steps 1–2 of the write protocol replicated directly, deliberately skipping step 3) proves a crash mid-append is invisible and safely overwritten on the next real append; also bad magic, bad version, a truncated index file, and a mismatched file pair all fail loudly on open. |
| `concurrency_test.cpp` | A writer thread appending 20,000 records while two reader threads tail concurrently (one pure-spin, one spin-then-backoff) through independently-opened mappings — the cross-thread acquire/release claim in §6.3, checked rather than assumed. |

Run the same suite under ThreadSanitizer to check that last file's claim
for real, not just on x86's forgiving memory model:

```sh
cmake --preset tsan
cmake --build --preset tsan
ctest --preset tsan --output-on-failure
```

## Seeing it in action

`journal/` is a library, not a program, so its test suite doubles as
the clearest working demonstration — in particular
`WriterReader.ReopenResumesAtNextSequenceNumberAndStaysDense` (write,
close, reopen, keep appending densely) and
`Recovery.TornWriteIsInvisibleAndSafelyOverwritten` (simulate a crash,
recover, confirm nothing was lost or corrupted).

The shape of real usage — write a few records, then read them back —
is this:

```cpp
#include <sequencer/journal/reader.hpp>
#include <sequencer/journal/writer.hpp>

#include <string>
#include <vector>

using namespace sequencer;
using namespace sequencer::journal;

// Write.
JournalWriter writer("journal.data", "journal.index");

const std::string in = "hello";
const std::string out = "world";
Payload input(reinterpret_cast<const std::byte*>(in.data()), in.size());
Payload output(reinterpret_cast<const std::byte*>(out.data()), out.size());
std::vector<Payload> outputs = {output};

writer.append(writer.nextSequenceNumber(), input, outputs);
writer.flush();

// Read — any number of readers, colocated or reopened later, see the
// same records via the same two files.
JournalReader reader("journal.data", "journal.index");
RecordView record = reader.record(1);
// record.sequenceNumber(), record.input(), record.outputCount(), record.output(0)
```

## Benchmarking

```sh
cmake --preset release   # Release build, LTO enabled (§9.1)
cmake --build --preset release
./build/release/journal/benchmarks/journal_benchmark
```

Two Google Benchmark cases in `benchmarks/journal_benchmark.cpp`:
`BM_Append` measures the full three-step write protocol (§6.3) for a
64-byte input plus one 64-byte output; `BM_RecordViewRead` measures a
zero-copy `record()` lookup plus touching its input and output.

Last observed, Release build with LTO, on a 4-vCPU shared development
VM (noisy — CPU frequency scaling on, load average >5 from unrelated
processes; treat as a sanity check, not a real measurement — see
[Scope notes](../README.md#scope-notes) on why proper throughput/latency
numbers belong in the separate benchmarking repository instead):

```
Benchmark                  Time             CPU   Iterations UserCounters...
----------------------------------------------------------------------------
BM_Append                412 ns          282 ns      2654420 items_per_second=3.54318M/s
BM_RecordViewRead        17.6 ns         17.3 ns     44886961
```

412 ns is comfortably inside the specification's expectation (§12) that
a journal append costs "well under a microsecond."
