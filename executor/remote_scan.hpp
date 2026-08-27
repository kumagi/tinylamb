/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXECUTOR_REMOTE_SCAN_HPP
#define TINYLAMB_EXECUTOR_REMOTE_SCAN_HPP

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "executor/data_chunk.hpp"
#include "executor/executor_base.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

// Thread-safe channel for streaming DataChunk batches between distributed
// workers or background threads.
class RemoteChannel {
 public:
  RemoteChannel() = default;

  void Push(DataChunk chunk);
  void Push(Row row, RowPosition rp = RowPosition());
  void Close();

  bool Pop(DataChunk* destination);
  bool PopRow(Row* row, RowPosition* rp = nullptr);
  [[nodiscard]] bool IsClosed() const;
  [[nodiscard]] size_t BufferedChunks() const;

 private:
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<DataChunk> queue_;
  std::deque<std::pair<Row, RowPosition>> row_queue_;
  bool closed_{false};
};

// RemoteScan executor that receives and streams DataChunks / Rows across
// a RemoteChannel or pre-buffered distributed chunks.
class RemoteScan : public ExecutorBase {
 public:
  RemoteScan(Schema schema, std::shared_ptr<RemoteChannel> channel);
  RemoteScan(Schema schema, std::vector<DataChunk> pre_buffered_chunks);

  ~RemoteScan() override = default;

  bool Next(Row* dst, RowPosition* rp) override;
  size_t NextBatch(DataChunk* destination,
                   size_t max_rows = kDefaultVectorSize) override;
  void Dump(std::ostream& o, int indent) const override;
  void Explain(std::ostream& o, int indent) const override;

  [[nodiscard]] const Schema& GetSchema() const { return schema_; }
  [[nodiscard]] std::shared_ptr<RemoteChannel> Channel() const {
    return channel_;
  }

 private:
  Schema schema_;
  std::shared_ptr<RemoteChannel> channel_;
  std::vector<DataChunk> buffered_chunks_;
  size_t chunk_idx_{0};
  size_t row_in_chunk_idx_{0};
};

}  // namespace tinylamb

#endif  // TINYLAMB_EXECUTOR_REMOTE_SCAN_HPP
