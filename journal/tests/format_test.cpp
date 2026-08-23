// Pure in-memory encode/decode tests — no file I/O. These pin down the
// on-disk field layout (specification.md §6.2) independent of the
// writer/reader's file handling, which writer_reader_test.cpp covers.

#include <sequencer/journal/format.hpp>
#include <sequencer/journal/record_view.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace sequencer::journal {
namespace {

Payload bytesOf(const std::string& s) {
  return Payload(reinterpret_cast<const std::byte*>(s.data()), s.size());
}

TEST(Format, HeaderAndEntryHaveNoImplicitPadding) {
  EXPECT_EQ(sizeof(IndexHeader), 24u);
  EXPECT_EQ(sizeof(IndexEntry), 16u);
}

TEST(Format, RecordEncodedSizeMatchesLayout) {
  // Payload is a non-owning span, so every string it points into must
  // outlive the calls that read that Payload — hence named locals
  // rather than bytesOf("literal") passed straight into a vector that
  // is read on a later statement (a dangling temporary otherwise, since
  // the temporary std::string bound to bytesOf's parameter is destroyed
  // at the end of the vector-initializing statement).
  const std::string input = "hello";
  const std::string a = "a";
  const std::string bb = "bb";
  const std::string ccc = "ccc";
  std::vector<Payload> outputs = {bytesOf(a), bytesOf(bb), bytesOf(ccc)};
  const std::size_t expected = 8 + 4 + input.size() + 2 + (4 + 1) + (4 + 2) + (4 + 3);
  EXPECT_EQ(recordEncodedSize(bytesOf(input), outputs), expected);
}

TEST(Format, EncodeThenViewRoundTripsWithMultipleOutputs) {
  const std::string input = "state-machine-input";
  const std::string out0Str = "out0";
  const std::string out1Str = "";
  const std::string out2Str = "out2-longer";
  const std::vector<Payload> outputs = {bytesOf(out0Str), bytesOf(out1Str), bytesOf(out2Str)};

  const std::size_t size = recordEncodedSize(bytesOf(input), outputs);
  std::vector<std::byte> buffer(size);
  encodeRecord(buffer.data(), /*sequenceNumber=*/42, bytesOf(input), outputs);

  RecordView view(buffer.data(), static_cast<std::uint32_t>(size));
  EXPECT_EQ(view.sequenceNumber(), 42u);

  const Payload viewedInput = view.input();
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(viewedInput.data()), viewedInput.size()),
            input);

  ASSERT_EQ(view.outputCount(), outputs.size());
  for (std::uint16_t i = 0; i < outputs.size(); ++i) {
    const Payload got = view.output(i);
    const Payload want = outputs[i];
    EXPECT_EQ(got.size(), want.size());
    EXPECT_TRUE(std::equal(got.begin(), got.end(), want.begin(), want.end()));
  }

  EXPECT_EQ(view.rawBytes().size(), size);
}

TEST(Format, EncodeThenViewRoundTripsWithZeroOutputs) {
  const std::string input = "no-outputs";
  const std::vector<Payload> outputs;

  const std::size_t size = recordEncodedSize(bytesOf(input), outputs);
  std::vector<std::byte> buffer(size);
  encodeRecord(buffer.data(), /*sequenceNumber=*/1, bytesOf(input), outputs);

  RecordView view(buffer.data(), static_cast<std::uint32_t>(size));
  EXPECT_EQ(view.sequenceNumber(), 1u);
  EXPECT_EQ(view.outputCount(), 0u);
}

TEST(Format, EncodeThenViewRoundTripsWithEmptyInput) {
  const std::string onlyOutput = "only-output";
  const std::vector<Payload> outputs = {bytesOf(onlyOutput)};
  const std::size_t size = recordEncodedSize(Payload{}, outputs);
  std::vector<std::byte> buffer(size);
  encodeRecord(buffer.data(), /*sequenceNumber=*/7, Payload{}, outputs);

  RecordView view(buffer.data(), static_cast<std::uint32_t>(size));
  EXPECT_EQ(view.input().size(), 0u);
  ASSERT_EQ(view.outputCount(), 1u);
  const Payload out0 = view.output(0);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(out0.data()), out0.size()), "only-output");
}

}  // namespace
}  // namespace sequencer::journal
