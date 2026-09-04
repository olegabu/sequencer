// Micro-benchmarks for the two operations specification.md §12 makes a
// specific claim about: "a journal append costs well under a
// microsecond." These are regression tripwires for local development
// (specification.md §9: "where they help catch regressions during
// development"), not the cross-process/cross-machine measurement that
// belongs in the separate benchmarking repository.

#include <sequencer/temp_dir.hpp>
#include <sequencer/journal/reader.hpp>
#include <sequencer/journal/writer.hpp>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>

namespace sequencer::journal {
namespace {


// A segment large enough that Google Benchmark's auto-selected
// iteration count for a sub-microsecond operation does not spend the
// run rolling: rollover is cheap but not free (it flushes, renames and
// maps), and BM_Append is meant to measure the append path rather than
// the roll path. Records here are tiny, so 256 bytes is ample.
JournalOptions benchmarkOptions() {
  JournalOptions options;
  options.recordsPerSegment = std::uint64_t{10} << 20;  // ~10M records per segment
  options.maxRecordBytes = 256;
  return options;
}

void BM_Append(benchmark::State& state) {
  const std::filesystem::path dir = sequencer::makeTempDir("sequencer_journal_bench");
  JournalWriter writer(dir / "journal", benchmarkOptions());

  const std::string inputStr(64, 'i');
  const std::string outputStr(64, 'o');
  const Payload input(reinterpret_cast<const std::byte*>(inputStr.data()), inputStr.size());
  const Payload output(reinterpret_cast<const std::byte*>(outputStr.data()), outputStr.size());
  std::vector<Payload> outputs = {output};

  std::uint64_t seq = 1;
  for (auto _ : state) {
    writer.append(seq, input, outputs);
    ++seq;
  }

  state.SetItemsProcessed(static_cast<std::int64_t>(seq - 1));
  std::filesystem::remove_all(dir);
}
BENCHMARK(BM_Append);

void BM_RecordViewRead(benchmark::State& state) {
  const std::filesystem::path dir = sequencer::makeTempDir("sequencer_journal_bench");
  {
    JournalWriter writer(dir / "journal", benchmarkOptions());
    const std::string inputStr(64, 'i');
    const std::string outputStr(64, 'o');
    const Payload input(reinterpret_cast<const std::byte*>(inputStr.data()), inputStr.size());
    const Payload output(reinterpret_cast<const std::byte*>(outputStr.data()), outputStr.size());
    std::vector<Payload> outputs = {output};
    for (std::uint64_t seq = 1; seq <= 100000; ++seq) {
      writer.append(seq, input, outputs);
    }
    writer.flush(false);
  }

  JournalReader reader(dir / "journal");
  std::uint64_t seq = 1;
  for (auto _ : state) {
    RecordView view = reader.record(seq);
    benchmark::DoNotOptimize(view.input().data());
    benchmark::DoNotOptimize(view.output(0).data());
    seq = (seq % 100000) + 1;
  }

  std::filesystem::remove_all(dir);
}
BENCHMARK(BM_RecordViewRead);

}  // namespace
}  // namespace sequencer::journal

BENCHMARK_MAIN();
