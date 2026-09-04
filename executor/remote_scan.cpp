/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "executor/remote_scan.hpp"

#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "executor/data_chunk.hpp"
#include "page/row_position.hpp"
#include "type/row.hpp"
#include "type/schema.hpp"

namespace tinylamb {

void RemoteChannel::Push(DataChunk chunk) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_) {
    return;
  }
  queue_.push_back(std::move(chunk));
  cv_.notify_one();
}

void RemoteChannel::Push(Row row, RowPosition rp) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (closed_) {
    return;
  }
  row_queue_.emplace_back(std::move(row), rp);
  cv_.notify_one();
}

void RemoteChannel::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  closed_ = true;
  cv_.notify_all();
}

bool RemoteChannel::IsClosed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return closed_ && queue_.empty() && row_queue_.empty();
}

size_t RemoteChannel::BufferedChunks() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return queue_.size();
}

bool RemoteChannel::Pop(DataChunk* destination) {
  assert(destination != nullptr);
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [this] { return !queue_.empty() || closed_; });
  if (queue_.empty()) {
    return false;
  }
  *destination = std::move(queue_.front());
  queue_.pop_front();
  return true;
}

bool RemoteChannel::PopRow(Row* row, RowPosition* rp) {
  assert(row != nullptr);
  std::unique_lock<std::mutex> lock(mutex_);
  cv_.wait(lock, [this] { return !row_queue_.empty() || closed_; });
  if (row_queue_.empty()) {
    return false;
  }
  *row = std::move(row_queue_.front().first);
  if (rp != nullptr) {
    *rp = row_queue_.front().second;
  }
  row_queue_.pop_front();
  return true;
}

RemoteScan::RemoteScan(Schema schema, std::shared_ptr<RemoteChannel> channel)
    : schema_(std::move(schema)), channel_(std::move(channel)) {}

RemoteScan::RemoteScan(Schema schema,
                       std::vector<DataChunk> pre_buffered_chunks)
    : schema_(std::move(schema)),
      buffered_chunks_(std::move(pre_buffered_chunks)) {}

bool RemoteScan::Next(Row* dst, RowPosition* rp) {
  assert(dst != nullptr);
  if (channel_) {
    // If we have an active chunk in buffered_chunks_, read from it
    if (chunk_idx_ < buffered_chunks_.size()) {
      auto& chunk = buffered_chunks_[chunk_idx_];
      if (row_in_chunk_idx_ < chunk.Size()) {
        *dst = chunk.RowAt(row_in_chunk_idx_);
        if (rp != nullptr) {
          *rp = chunk.PositionAt(row_in_chunk_idx_);
        }
        ++row_in_chunk_idx_;
        return true;
      }
      ++chunk_idx_;
      row_in_chunk_idx_ = 0;
    }

    // Try popping next chunk from channel
    DataChunk next_chunk(schema_, 1024);
    if (channel_->Pop(&next_chunk)) {
      buffered_chunks_.push_back(std::move(next_chunk));
      auto& chunk = buffered_chunks_.back();
      if (chunk.Size() > 0) {
        *dst = chunk.RowAt(0);
        if (rp != nullptr) {
          *rp = chunk.PositionAt(0);
        }
        row_in_chunk_idx_ = 1;
        return true;
      }
    }

    // Try popping single row if available
    return channel_->PopRow(dst, rp);
  }

  // Pre-buffered chunks
  while (chunk_idx_ < buffered_chunks_.size()) {
    auto& chunk = buffered_chunks_[chunk_idx_];
    if (row_in_chunk_idx_ < chunk.Size()) {
      *dst = chunk.RowAt(row_in_chunk_idx_);
      if (rp != nullptr) {
        *rp = chunk.PositionAt(row_in_chunk_idx_);
      }
      ++row_in_chunk_idx_;
      return true;
    }
    ++chunk_idx_;
    row_in_chunk_idx_ = 0;
  }
  return false;
}

size_t RemoteScan::NextBatch(DataChunk* destination, size_t max_rows) {
  if (destination == nullptr || max_rows == 0) {
    return 0;
  }

  if (channel_) {
    DataChunk chunk(schema_, max_rows);
    if (channel_->Pop(&chunk)) {
      *destination = std::move(chunk);
      return destination->Size();
    }
  }

  // Fallback to row iteration into destination
  destination->Reset(schema_, max_rows);
  size_t count = 0;
  Row row;
  RowPosition rp;
  while (count < max_rows && Next(&row, &rp)) {
    destination->Append(row, rp);
    ++count;
  }
  return count;
}

void RemoteScan::Dump(std::ostream& o, int /*indent*/) const {
  o << "RemoteScan(schema=" << schema_.Name() << ")";
}

void RemoteScan::Explain(std::ostream& o, int indent) const { Dump(o, indent); }

}  // namespace tinylamb
