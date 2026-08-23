// Micro-benchmarks for the two operations specification.md §12 makes a
// specific claim about: "a journal append costs well under a
// microsecond." These are regression tripwires for local development
// (specification.md §9: "where they help catch regressions during
// development"), not the cross-process/cross-machine measurement that
// belongs in the separate benchmarking repository.

#include <sequencer/journal/reader.hpp>
#include <sequencer/journal/writer.hpp>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>

namespace sequencer::journal {
namespace {

std::filesystem::path makeTempDir() {
  std::string tmpl = (std::filesystem::temp_directory_path() / "sequencer_journal_bench_XXXXXX").string();
  if (::mkdtemp(tmpl.data()) == nullptr) {
    std::abort();
  }
  return tmpl;
}

// Sized generously enough to cover Google Benchmark's auto-selected
// iteration count for a sub-microsecond operation without exhausting
// the reservation mid-run (mapped_file.hpp: fixed reservation, never
// remapped).
JournalOptions benchmarkOptions() {
  JournalOptions options;
  options.maxDataFileBytes = std::uint64_t{4} << 30;  // 4 GiB
  options.maxIndexEntries = std::uint64_t{10} << 20;  // ~10M records
  return options;
}

void BM_Append(benchmark::State& state) {
  const std::filesystem::path dir = makeTempDir();
  JournalWriter writer(dir / "journal.data", dir / "journal.index", benchmarkOptions());

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
  const std::filesystem::path dir = makeTempDir();
  {
    JournalWriter writer(dir / "journal.data", dir / "journal.index", benchmarkOptions());
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

  JournalReader reader(dir / "journal.data", dir / "journal.index");
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
