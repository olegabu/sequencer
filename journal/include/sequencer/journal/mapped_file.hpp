#pragma once

// A minimal MAP_SHARED file mapping, sized once at open time and never
// remapped.
//
// The journal's writer never grows a live mapping: JournalOptions
// (writer.hpp) fixes a maximum data-file and index-file size up front,
// and that maximum is reserved as one mmap call, backed by a sparse file
// (ftruncate to the full reserved length; the filesystem allocates
// blocks lazily as pages are actually written). This sidesteps the hard
// problem of remapping while lock-free readers may hold the old mapping
// — at the cost of choosing a size limit in advance. See writer.hpp for
// the resulting JournalOptions defaults and how to size them for a real
// deployment.

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <system_error>
#include <utility>

namespace sequencer::journal {

class MappedFile {
 public:
  MappedFile() = default;

  // Creates a new file at `path` (fails if it already exists), extends
  // it to `length` bytes (sparse — see class comment), and maps it
  // MAP_SHARED, read-write.
  static MappedFile createNew(const std::filesystem::path& path, std::size_t length) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
      throw std::system_error(errno, std::generic_category(),
                               "open(O_CREAT|O_EXCL) failed: " + path.string());
    }
    if (::ftruncate(fd, static_cast<off_t>(length)) != 0) {
      int err = errno;
      ::close(fd);
      throw std::system_error(err, std::generic_category(), "ftruncate failed: " + path.string());
    }
    return MappedFile(fd, length, /*readOnly=*/false);
  }

  // Opens an existing file at `path` and maps it. By default that means
  // the file's whole on-disk size, which for a journal segment is its
  // full reservation rather than the bytes actually written.
  //
  // `mapAtMost` caps that. It exists for readers of SEALED segments
  // (§6.5), where the reservation is deliberately far larger than the
  // data — a segment reserves recordsPerSegment * maxRecordBytes so the
  // record count always ends it first, and real records are typically
  // orders of magnitude smaller than maxRecordBytes. A reader that
  // mapped every sealed segment's full reservation would consume
  // address space proportional to the reservation rather than to the
  // data, and run out long before disk did. A sealed segment's used
  // length is exactly known from its last index entry, so it can be
  // mapped at that instead. Zero means "no cap".
  static MappedFile openExisting(const std::filesystem::path& path, bool readOnly,
                                  std::size_t mapAtMost = 0) {
    int fd = ::open(path.c_str(), readOnly ? O_RDONLY : O_RDWR);
    if (fd < 0) {
      throw std::system_error(errno, std::generic_category(), "open failed: " + path.string());
    }
    struct stat st{};
    if (::fstat(fd, &st) != 0) {
      int err = errno;
      ::close(fd);
      throw std::system_error(err, std::generic_category(), "fstat failed: " + path.string());
    }
    std::size_t length = static_cast<std::size_t>(st.st_size);
    if (mapAtMost > 0 && mapAtMost < length) {
      length = mapAtMost;
    }
    return MappedFile(fd, length, readOnly);
  }

  MappedFile(const MappedFile&) = delete;
  MappedFile& operator=(const MappedFile&) = delete;

  MappedFile(MappedFile&& other) noexcept { *this = std::move(other); }

  MappedFile& operator=(MappedFile&& other) noexcept {
    if (this != &other) {
      reset();
      base_ = std::exchange(other.base_, nullptr);
      length_ = std::exchange(other.length_, 0);
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }

  ~MappedFile() { reset(); }

  std::byte* data() noexcept { return static_cast<std::byte*>(base_); }
  const std::byte* data() const noexcept { return static_cast<const std::byte*>(base_); }
  std::size_t size() const noexcept { return length_; }

  // Flushes the mapping's dirty pages to disk. `async=true` (the
  // default per §5.1: "the journal is asynchronously flushed to disk by
  // default") schedules the write without blocking; readers observe
  // writes via the mapping immediately regardless of flush timing —
  // this governs local crash-survival only, never reader visibility.
  void flush(bool async = true) {
    if (::msync(base_, length_, async ? MS_ASYNC : MS_SYNC) != 0) {
      throw std::system_error(errno, std::generic_category(), "msync failed");
    }
  }

 private:
  MappedFile(int fd, std::size_t length, bool readOnly) : length_(length), fd_(fd) {
    int prot = readOnly ? PROT_READ : (PROT_READ | PROT_WRITE);
    base_ = ::mmap(nullptr, length, prot, MAP_SHARED, fd, 0);
    if (base_ == MAP_FAILED) {
      int err = errno;
      ::close(fd_);
      fd_ = -1;
      throw std::system_error(err, std::generic_category(), "mmap failed");
    }
  }

  void reset() noexcept {
    if (base_ != nullptr) {
      ::munmap(base_, length_);
      base_ = nullptr;
    }
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
    length_ = 0;
  }

  void* base_ = nullptr;
  std::size_t length_ = 0;
  int fd_ = -1;
};

}  // namespace sequencer::journal
