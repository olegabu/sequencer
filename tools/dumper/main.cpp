// tools/dumper — specification.md §9: "a genuinely standalone binary:
// dumps raw journal records (sequence numbers, lengths, a hex or text
// preview) for human inspection, without interpreting payload meaning
// — needs no application code, unlike replay." Reads any journal
// directly via the header-only journal/ library — no StateMachine, no
// braft, no application dependency of any kind.

#include <sequencer/journal/reader.hpp>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

DEFINE_string(data_dir, "", "Directory containing journal.data/journal.index to dump (required)");
DEFINE_uint64(from, 1, "First sequence number to dump (inclusive)");
DEFINE_uint64(to, 0, "Last sequence number to dump (inclusive); 0 means the journal's committed count");
DEFINE_int32(preview_bytes, 64,
             "Maximum bytes to preview per field, as text if printable or hex otherwise; 0 disables "
             "the preview (lengths are always shown)");

namespace {

// Text if every previewed byte is printable ASCII, hex otherwise —
// "a hex or text preview" per the component's own charter above. Never
// interprets what the bytes *mean* — just whether they happen to be
// printable.
std::string preview(sequencer::Payload data, int maxBytes) {
  if (maxBytes <= 0 || data.empty()) {
    return "";
  }
  const std::size_t n = std::min(data.size(), static_cast<std::size_t>(maxBytes));

  bool printable = true;
  for (std::size_t i = 0; i < n; ++i) {
    const auto c = static_cast<unsigned char>(data[i]);
    if (!(std::isprint(c) || c == '\t')) {
      printable = false;
      break;
    }
  }

  std::ostringstream out;
  if (printable) {
    out << '"' << std::string(reinterpret_cast<const char*>(data.data()), n) << '"';
  } else {
    out << "0x" << std::hex << std::setfill('0');
    for (std::size_t i = 0; i < n; ++i) {
      out << std::setw(2) << static_cast<unsigned>(static_cast<unsigned char>(data[i]));
    }
  }
  if (n < data.size()) {
    out << "...";
  }
  return out.str();
}

}  // namespace

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_data_dir.empty()) {
    std::cerr << "dumper: --data_dir is required" << std::endl;
    return 1;
  }

  try {
    sequencer::journal::JournalReader reader(FLAGS_data_dir + "/journal.data",
                                              FLAGS_data_dir + "/journal.index");
    const std::uint64_t committed = reader.committedCount();
    const std::uint64_t from = std::max<std::uint64_t>(FLAGS_from, 1);
    const std::uint64_t to = FLAGS_to == 0 ? committed : std::min(FLAGS_to, committed);

    std::cout << "journal: " << FLAGS_data_dir << " committedCount=" << committed << std::endl;
    for (std::uint64_t seq = from; seq <= to; ++seq) {
      const sequencer::journal::RecordView record = reader.record(seq);
      std::cout << "seq=" << record.sequenceNumber() << " input_len=" << record.input().size()
                << " input=" << preview(record.input(), FLAGS_preview_bytes)
                << " output_count=" << record.outputCount() << std::endl;
      for (std::uint16_t i = 0; i < record.outputCount(); ++i) {
        const sequencer::Payload out = record.output(i);
        std::cout << "  output[" << i << "] len=" << out.size() << " "
                  << preview(out, FLAGS_preview_bytes) << std::endl;
      }
    }
  } catch (const std::exception& e) {
    std::cerr << "dumper: " << e.what() << std::endl;
    return 1;
  }
  return 0;
}
