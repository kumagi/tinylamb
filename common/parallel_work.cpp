/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/parallel_work.hpp"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <exception>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "common/platform/cpu_affinity.hpp"
#include "common/query_parallelism.hpp"

namespace tinylamb {
namespace {
thread_local bool running_parallel_task = false;

class TaskScope {
 public:
  TaskScope() : previous_(std::exchange(running_parallel_task, true)) {}
  TaskScope(const TaskScope&) = delete;
  TaskScope& operator=(const TaskScope&) = delete;
  TaskScope(TaskScope&&) = delete;
  TaskScope& operator=(TaskScope&&) = delete;
  ~TaskScope() { running_parallel_task = previous_; }

 private:
  bool previous_;
};

Status Invoke(const std::function<void(size_t)>& task, size_t index) {
  try {
    TaskScope scope;
    task(index);
    return Status::kSuccess;
  } catch (const std::exception& error) {
    return StatusError(StatusCode::kRuntimeError, error.what());
  } catch (...) {
    return StatusError(StatusCode::kRuntimeError, "parallel task failed");
  }
}

struct TaskBatch {
  const std::function<void(size_t)>& task;
  size_t count;
  size_t next{1};    // guarded by the pool mutex
  size_t remaining;  // guarded by the completion mutex
  QueryParallelismBudget budget{query_parallelism_budget};
  std::optional<platform::CpuSet> affinity{platform::CpuSet::CurrentThread()};
  std::mutex mutex{};
  std::condition_variable done{};
  Status error{Status::kSuccess};
};

class ParallelPool {
 public:
  ParallelPool() = default;
  ParallelPool(const ParallelPool&) = delete;
  ParallelPool& operator=(const ParallelPool&) = delete;
  ParallelPool(ParallelPool&&) = delete;
  ParallelPool& operator=(ParallelPool&&) = delete;
  ~ParallelPool() {
    {
      std::scoped_lock lock(mutex_);
      stopping_ = true;
    }
    ready_.notify_all();
    threads_.clear();
  }

  bool Submit(TaskBatch* batch) {
    std::scoped_lock lock(mutex_);
    const size_t helpers = std::min<size_t>(
        batch->count - 1, std::max(1U, std::thread::hardware_concurrency()));
    try {
      while (threads_.size() < helpers) {
        threads_.emplace_back([this] { Work(); });
      }
      batches_.push_back(batch);
    } catch (...) {
      // No work was published; the caller can execute all indices serially.
      return false;
    }
    for (size_t index = 0; index < helpers; ++index) {
      ready_.notify_one();
    }
    return true;
  }

 private:
  void Work() {
    std::optional<platform::CpuSet> current_affinity;
    for (;;) {
      TaskBatch* batch = nullptr;
      size_t index = 0;
      {
        std::unique_lock lock(mutex_);
        ready_.wait(lock, [&] { return stopping_ || !batches_.empty(); });
        if (batches_.empty()) {
          return;
        }
        batch = batches_.front();
        index = batch->next++;
        if (batch->next == batch->count) {
          batches_.pop_front();
        }
      }
      Status error{Status::kSuccess};
      if (batch->affinity && batch->affinity != current_affinity) {
        if (batch->affinity->ApplyToCurrentThread()) {
          current_affinity = batch->affinity;
        } else {
          error = StatusError(StatusCode::kRuntimeError,
                              "cannot apply parallel task CPU affinity");
        }
      }
      if (error.ok()) {
        const auto budget = batch->budget;
        ScopedQueryParallelismBudget scope(budget.outstanding, budget.cpu_slots,
                                           budget.snapshot_reads);
        error = Invoke(batch->task, index);
      }
      // Notify under this lock: after the final unlock the coordinator may
      // destroy the batch, so the helper must never touch it again.
      std::scoped_lock lock(batch->mutex);
      if (batch->error.ok() && !error.ok()) {
        batch->error = error;
      }
      if (--batch->remaining == 0) {
        batch->done.notify_one();
      }
    }
  }

  std::mutex mutex_;
  std::condition_variable ready_;
  std::deque<TaskBatch*> batches_;
  std::vector<std::jthread> threads_;
  bool stopping_{false};
};
}  // namespace

Status RunParallelTasks(size_t count, const std::function<void(size_t)>& task) {
  if (count > 1 && !running_parallel_task) {
    TaskBatch batch{.task = task, .count = count, .remaining = count - 1};
    static ParallelPool pool;
    if (pool.Submit(&batch)) {
      const Status caller_error = Invoke(task, 0);
      std::unique_lock lock(batch.mutex);
      batch.done.wait(lock, [&] { return batch.remaining == 0; });
      return caller_error.ok() ? batch.error : caller_error;
    }
  }
  Status error{Status::kSuccess};
  for (size_t index = 0; index < count; ++index) {
    const Status status = Invoke(task, index);
    if (error.ok() && !status.ok()) {
      error = status;
    }
  }
  return error;
}
}  // namespace tinylamb
