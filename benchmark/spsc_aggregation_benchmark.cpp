/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
// Isolated COUNT/SUM experiment, not a SQL executor or a production queue API.
#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/platform/cpu_affinity.hpp"
#include "executor/detail/flat_int_key_map.hpp"

namespace {
using Clock = std::chrono::steady_clock;
struct State {
  uint64_t count{0};
  int64_t sum{0};
  bool operator==(const State&) const = default;
};
struct Row {
  int64_t key;
  int64_t value;
};
struct Partial {
  int64_t key;
  State state;
};
static_assert(sizeof(Row) == 16 && std::endian::native == std::endian::little);
using Map = tinylamb::relational_detail::FlatIntKeyMap<State>;
constexpr size_t kBatchEntries = 256;
struct Batch {
  size_t size{0};
  std::array<Partial, kBatchEntries> entries{};
};

uint64_t Mix(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27U)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31U);
}
void Merge(Map& map, const Partial& p) {
  State* s = map.TryEmplace(p.key);
  s->count += p.state.count;
  s->sum += p.state.sum;
}
int64_t Evaluate(int64_t value, size_t work) {
  if (work == 0) {
    return value;
  }
  auto x = static_cast<uint64_t>(value);
  for (size_t i = 0; i < work; ++i) {
    x = Mix(x);
  }
  return static_cast<int64_t>(x & 2047U) - 1024;
}

// Fixed-size, allocation-free local preaggregation. All transport variants
// use this same table, hash, flush threshold, and partial-state representation.
class LocalTable {
 public:
  explicit LocalTable(size_t limit)
      : slots_(std::bit_ceil(limit * 2)), limit_(limit) {
    touched_.reserve(limit);
  }
  void Add(int64_t key, int64_t value) {
    size_t pos = Mix(static_cast<uint64_t>(key)) & (slots_.size() - 1);
    while (slots_[pos].epoch == epoch_ && slots_[pos].key != key) {
      pos = (pos + 1) & (slots_.size() - 1);
    }
    Slot& slot = slots_[pos];
    if (slot.epoch != epoch_) {
      slot = Slot{.key = key, .state = {}, .epoch = epoch_};
      touched_.push_back(pos);
    }
    ++slot.state.count;
    slot.state.sum += value;
  }
  [[nodiscard]] bool Full() const { return touched_.size() == limit_; }
  template <typename Emit>
  void Flush(Emit&& emit) {
    for (const size_t pos : touched_) {
      emit(Partial{.key = slots_[pos].key, .state = slots_[pos].state});
    }
    touched_.clear();
    if (++epoch_ == 0) {
      for (Slot& slot : slots_) {
        slot.epoch = 0;
      }
      epoch_ = 1;
    }
  }

 private:
  struct Slot {
    int64_t key{0};
    State state{};
    uint64_t epoch{0};
  };
  std::vector<Slot> slots_;
  size_t limit_;
  std::vector<size_t> touched_;
  uint64_t epoch_{1};
};

// Exactly one producer and one consumer. Each Try/Commit call is bounded:
// no allocation, CAS, mutex or retry loop. A full/empty queue returns nullptr.
// The caller's retry/backpressure loop is NOT wait-free. Batch payload work
// and hash-table operations are also outside this queue-operation claim.
class Ring {
 public:
  explicit Ring(size_t capacity) : mask_(capacity - 1), slots_(capacity) {
    if (!std::has_single_bit(capacity)) {
      throw std::invalid_argument("ring capacity must be a power of two");
    }
  }
  Ring(const Ring&) = delete;
  Ring& operator=(const Ring&) = delete;
  Ring(Ring&&) = delete;
  Ring& operator=(Ring&&) = delete;
  ~Ring() = default;
  Batch* TryWrite() {
    if (writer_.local - writer_.peer == slots_.size()) {
      writer_.peer = reader_.published.load(std::memory_order_acquire);
      if (writer_.local - writer_.peer == slots_.size()) {
        return nullptr;
      }
    }
    return &slots_[writer_.local & mask_];
  }
  void CommitWrite() {
    writer_.published.store(++writer_.local, std::memory_order_release);
  }
  const Batch* TryRead() {
    if (reader_.local == reader_.peer) {
      reader_.peer = writer_.published.load(std::memory_order_acquire);
      if (reader_.local == reader_.peer) {
        return nullptr;
      }
    }
    return &slots_[reader_.local & mask_];
  }
  void CommitRead() {
    reader_.published.store(++reader_.local, std::memory_order_release);
  }

 private:
  static_assert(std::atomic<uint64_t>::is_always_lock_free);
  struct alignas(64) Cursor {
    std::atomic<uint64_t> published{0};
    uint64_t local{0};
    uint64_t peer{0};
  };
  Cursor writer_;
  Cursor reader_;
  size_t mask_;
  std::vector<Batch> slots_;

 public:
  std::atomic<bool> done{false};
};

struct Options {
  size_t rows{1000000};
  size_t groups{65536};
  size_t threads{8};
  size_t local_groups{4096};
  size_t ring_batches{8};
  size_t batch_entries{kBatchEntries};
  size_t work{0};
  size_t morsel_rows{65536};
  size_t repeats{5};
  bool skew{false};
  bool reverse{false};
  bool self_test{false};
  std::string input;
  std::vector<int> cpus;
};
void Pin(const Options& o, size_t lane) {
  if (!o.cpus.empty() && !tinylamb::platform::CpuSet::Single(o.cpus[lane])
                              .ApplyToCurrentThread()) {
    throw std::runtime_error("CPU affinity failed");
  }
}
void Backoff(uint64_t& polls) {
  if ((++polls & 63U) == 0) {
    std::this_thread::yield();
  }
}
struct alignas(64) WorkerStats {
  uint64_t partials{0};
  uint64_t full_polls{0};
  uint64_t batches{0};
};
struct Result {
  double wall_ms{0};
  double cpu_ms{0};
  uint64_t partials{0};
  uint64_t full_polls{0};
  uint64_t empty_polls{0};
  uint64_t batches{0};
  size_t transport_capacity_bytes{0};
  size_t producers{0};
  size_t consumers{0};
  std::vector<Map> maps;
};

template <typename Emit>
void Produce(const std::vector<Row>& input, const Options& o, size_t worker,
             size_t workers, LocalTable& local, Emit&& emit) {
  const size_t begin = input.size() * worker / workers;
  const size_t end = input.size() * (worker + 1) / workers;
  size_t until_flush = o.morsel_rows;
  for (size_t i = begin; i < end; ++i) {
    local.Add(input[i].key, Evaluate(input[i].value, o.work));
    if (--until_flush == 0 || local.Full()) {
      local.Flush(emit);
      until_flush = o.morsel_rows;
    }
  }
  local.Flush(emit);
}

Result Run(const std::string& mode, const Options& o,
           const std::vector<Row>& input, const std::vector<State>& expected) {
  const bool pipeline = mode == "spsc_n1";
  const bool partitioned = mode == "partitioned_merge";
  const size_t producers = pipeline ? o.threads - 1 : o.threads;
  const size_t partitions = partitioned ? o.threads : 1;
  Result result;
  result.producers = producers;
  result.consumers = partitions;
  result.maps.resize(partitions);
  std::vector<size_t> counts(partitions);
  for (size_t key = 0; key < expected.size(); ++key) {
    if (expected[key].count != 0) {
      ++counts[Mix(key) % partitions];
    }
  }
  for (size_t i = 0; i < partitions; ++i) {
    result.maps[i].Reserve(counts[i]);
  }
  std::vector<LocalTable> locals;
  locals.reserve(producers);
  std::vector<WorkerStats> stats(producers);
  std::vector<std::unique_ptr<Ring>> rings;
  std::vector<std::vector<std::vector<Partial>>> buffered(producers);
  for (size_t w = 0; w < producers; ++w) {
    locals.emplace_back(o.local_groups);
    if (pipeline) {
      rings.push_back(std::make_unique<Ring>(o.ring_batches));
    } else {
      buffered[w].resize(partitions);
      // Reserve a fair uniform estimate; skew may grow individual partitions.
      for (auto& bucket : buffered[w]) {
        bucket.reserve((input.size() / producers / partitions) + 1);
      }
    }
  }
  std::barrier ready(static_cast<std::ptrdiff_t>(o.threads));
  std::barrier phase(static_cast<std::ptrdiff_t>(o.threads));
  std::atomic<bool> start{false};
  std::vector<std::jthread> team;
  const auto produce = [&](size_t w) {
    if (pipeline) {
      Ring& ring = *rings[w];
      Batch* batch = nullptr;
      Produce(input, o, w, producers, locals[w], [&](const Partial& partial) {
        if (batch == nullptr) {
          while ((batch = ring.TryWrite()) == nullptr) {
            Backoff(stats[w].full_polls);
          }
          batch->size = 0;
        }
        batch->entries[batch->size++] = partial;
        ++stats[w].partials;
        if (batch->size == o.batch_entries) {
          ring.CommitWrite();
          batch = nullptr;
          ++stats[w].batches;
        }
      });
      if (batch != nullptr) {
        ring.CommitWrite();
        ++stats[w].batches;
      }
      ring.done.store(true, std::memory_order_release);
    } else {
      Produce(input, o, w, producers, locals[w], [&](const Partial& partial) {
        const size_t part =
            partitioned ? Mix(static_cast<uint64_t>(partial.key)) % partitions
                        : 0;
        buffered[w][part].push_back(partial);
        ++stats[w].partials;
      });
    }
  };
  const auto merge_partition = [&](size_t part) {
    for (const auto& worker : buffered) {
      for (const Partial& partial : worker[part]) {
        Merge(result.maps[part], partial);
      }
    }
  };
  const auto lane = [&](size_t w) {
    Pin(o, pipeline ? w + 1 : w);
    ready.arrive_and_wait();
    start.wait(false, std::memory_order_acquire);
    produce(w);
    if (!pipeline) {
      phase.arrive_and_wait();
      if (partitioned) {
        merge_partition(w);
      }
    }
  };
  for (size_t w = pipeline ? 0 : 1; w < producers; ++w) {
    team.emplace_back(lane, w);
  }
  Pin(o, 0);
  ready.arrive_and_wait();
  const std::clock_t cpu_begin = std::clock();
  const auto begin = Clock::now();
  start.store(true, std::memory_order_release);
  start.notify_all();
  if (pipeline) {
    std::vector<bool> closed(producers, false);
    size_t remaining = producers;
    while (remaining != 0) {
      bool progressed = false;
      for (size_t w = 0; w < producers; ++w) {
        if (closed[w]) {
          continue;
        }
        const Batch* batch = rings[w]->TryRead();
        if (batch == nullptr &&
            rings[w]->done.load(std::memory_order_acquire)) {
          // The acquire of done publishes every preceding tail commit.
          batch = rings[w]->TryRead();
          if (batch == nullptr) {
            closed[w] = true;
            --remaining;
            continue;
          }
        }
        if (batch == nullptr) {
          continue;
        }
        for (size_t i = 0; i < batch->size; ++i) {
          Merge(result.maps[0], batch->entries[i]);
        }
        rings[w]->CommitRead();
        progressed = true;
      }
      if (!progressed && remaining != 0) {
        Backoff(result.empty_polls);
      }
    }
  } else {
    produce(0);
    phase.arrive_and_wait();
    merge_partition(0);
  }
  team.clear();  // joins before timing stops; input and result validation
                 // excluded
  result.wall_ms =
      std::chrono::duration<double, std::milli>(Clock::now() - begin).count();
  result.cpu_ms =
      1000.0 * static_cast<double>(std::clock() - cpu_begin) / CLOCKS_PER_SEC;
  for (size_t w = 0; w < producers; ++w) {
    result.partials += stats[w].partials;
    result.full_polls += stats[w].full_polls;
    result.batches += stats[w].batches;
    if (!pipeline) {
      for (const auto& bucket : buffered[w]) {
        result.transport_capacity_bytes += bucket.capacity() * sizeof(Partial);
      }
    }
  }
  if (pipeline) {
    result.transport_capacity_bytes =
        producers * o.ring_batches * sizeof(Batch);
  }
  return result;
}

void Verify(Result& result, const std::vector<State>& expected) {
  size_t seen = 0;
  std::vector<bool> seen_keys(expected.size(), false);
  for (Map& map : result.maps) {
    map.ForEach([&](int64_t key, const State& state) {
      if (key < 0 || static_cast<size_t>(key) >= expected.size() ||
          state != expected[static_cast<size_t>(key)] || state.count == 0 ||
          seen_keys[static_cast<size_t>(key)]) {
        throw std::runtime_error("aggregate result mismatch");
      }
      seen_keys[static_cast<size_t>(key)] = true;
      ++seen;
    });
  }
  const auto groups = static_cast<size_t>(std::ranges::count_if(
      expected, [](const State& state) { return state.count != 0; }));
  if (seen != groups) {
    throw std::runtime_error("group count mismatch");
  }
}

void SelfTest() {
  Ring ring(2);
  if (ring.TryRead() != nullptr) {
    throw std::runtime_error("new queue is not empty");
  }
  for (size_t i = 0; i < 2; ++i) {
    Batch* b = ring.TryWrite();
    if (b == nullptr) {
      throw std::runtime_error("early full queue");
    }
    b->size = 1;
    b->entries[0] = Partial{.key = static_cast<int64_t>(i),
                            .state = State{.count = 1, .sum = -1}};
    ring.CommitWrite();
  }
  if (ring.TryWrite() != nullptr) {
    throw std::runtime_error("full queue accepted write");
  }
  for (size_t i = 0; i < 2; ++i) {
    const Batch* b = ring.TryRead();
    if (b == nullptr || !std::cmp_equal(b->entries[0].key, i)) {
      throw std::runtime_error("FIFO mismatch");
    }
    ring.CommitRead();
  }
  constexpr int64_t kTransfers = 100000;
  std::jthread producer([&] {
    uint64_t polls = 0;
    for (int64_t i = 0; i < kTransfers; ++i) {
      Batch* b = nullptr;
      while ((b = ring.TryWrite()) == nullptr) {
        Backoff(polls);
      }
      b->size = 1;
      b->entries[0] = Partial{.key = i, .state = State{.count = 1, .sum = -i}};
      ring.CommitWrite();
    }
  });
  uint64_t polls = 0;
  for (int64_t i = 0; i < kTransfers; ++i) {
    const Batch* b = nullptr;
    while ((b = ring.TryRead()) == nullptr) {
      Backoff(polls);
    }
    if (b->size != 1 || b->entries[0].key != i ||
        b->entries[0].state.sum != -i) {
      throw std::runtime_error("concurrent FIFO mismatch");
    }
    ring.CommitRead();
  }
  producer.join();
  for (const size_t n : {size_t{0}, size_t{1}, size_t{31}, size_t{10003}}) {
    Options o;
    o.threads = 4;
    o.local_groups = 7;
    o.ring_batches = 2;
    o.batch_entries = 3;
    o.morsel_rows = 19;
    std::vector<Row> rows;
    std::vector<State> expected(101);
    for (size_t i = 0; i < n; ++i) {
      const size_t key = Mix(i) % expected.size();
      const int64_t value = static_cast<int64_t>(i % 19) - 9;
      rows.push_back(Row{.key = static_cast<int64_t>(key), .value = value});
      ++expected[key].count;
      expected[key].sum += value;
    }
    for (const std::string mode :
         {"barrier_n1", "spsc_n1", "partitioned_merge"}) {
      auto result = Run(mode, o, rows, expected);
      Verify(result, expected);
    }
  }
  std::cout << "self-test passed\n";
}

size_t Number(const std::string& text) {
  size_t used = 0;
  const auto value = std::stoull(text, &used);
  if (used != text.size() || text.starts_with('-') || value > SIZE_MAX) {
    throw std::invalid_argument("invalid unsigned option");
  }
  return static_cast<size_t>(value);
}
Options Parse(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; ++i) {
    const std::string key = argv[i];
    if (key == "--self-test") {
      o.self_test = true;
    } else if (key == "--skew") {
      o.skew = true;
    } else if (key == "--reverse") {
      o.reverse = true;
    } else {
      if (++i >= argc) {
        throw std::invalid_argument("missing option value");
      }
      const std::string value = argv[i];
      if (key == "--input") {
        o.input = value;
      } else if (key == "--cpus") {
        size_t begin = 0;
        do {
          const size_t end = value.find(',', begin);
          const size_t cpu = Number(value.substr(begin, end - begin));
          if (cpu > static_cast<size_t>(std::numeric_limits<int>::max())) {
            throw std::invalid_argument("CPU out of range");
          }
          o.cpus.push_back(static_cast<int>(cpu));
          if (end == std::string::npos) {
            break;
          }
          begin = end + 1;
        } while (begin < value.size());
      } else if (key == "--rows") {
        o.rows = Number(value);
      } else if (key == "--groups") {
        o.groups = Number(value);
      } else if (key == "--threads") {
        o.threads = Number(value);
      } else if (key == "--local-groups") {
        o.local_groups = Number(value);
      } else if (key == "--ring-batches") {
        o.ring_batches = Number(value);
      } else if (key == "--batch-entries") {
        o.batch_entries = Number(value);
      } else if (key == "--morsel-rows") {
        o.morsel_rows = Number(value);
      } else if (key == "--work") {
        o.work = Number(value);
      } else if (key == "--repeats") {
        o.repeats = Number(value);
      } else {
        throw std::invalid_argument("unknown option " + key);
      }
    }
  }
  if (o.threads < 2 || o.threads > 256 || o.groups == 0 ||
      o.groups > 10000000 || o.rows > 100000000 || o.local_groups == 0 ||
      o.local_groups > 1048576 || !std::has_single_bit(o.ring_batches) ||
      o.ring_batches > 1024 || o.batch_entries == 0 ||
      o.batch_entries > kBatchEntries || o.repeats == 0 || o.work > 1000 ||
      o.morsel_rows == 0 || (!o.cpus.empty() && o.cpus.size() != o.threads)) {
    throw std::invalid_argument("option outside experiment limits");
  }
  return o;
}
}  // namespace

int main(int argc, char** argv) {
  try {
    const Options o = Parse(argc, argv);
    if (o.self_test) {
      SelfTest();
      return 0;
    }
    std::vector<Row> input;
    if (o.input.empty()) {
      input.reserve(o.rows);
      for (size_t i = 0; i < o.rows; ++i) {
        const uint64_t hash = Mix(i);
        const size_t key = o.skew && hash % 10 != 0
                               ? (hash >> 8U) % std::min<size_t>(64, o.groups)
                               : (hash >> 8U) % o.groups;
        input.push_back(
            Row{.key = static_cast<int64_t>(key),
                .value = static_cast<int64_t>(Mix(hash) % 2001) - 1000});
      }
    } else {
      std::ifstream file(o.input, std::ios::binary | std::ios::ate);
      if (!file || file.tellg() < 0 ||
          static_cast<size_t>(file.tellg()) % sizeof(Row) != 0) {
        throw std::runtime_error("invalid binary input");
      }
      if (static_cast<size_t>(file.tellg()) / sizeof(Row) > 100000000) {
        throw std::runtime_error("input exceeds experiment row bound");
      }
      input.resize(static_cast<size_t>(file.tellg()) / sizeof(Row));
      file.seekg(0);
      file.read(reinterpret_cast<char*>(input.data()),
                static_cast<std::streamsize>(input.size() * sizeof(Row)));
      if (!file) {
        throw std::runtime_error("input read failed");
      }
    }
    std::vector<State> expected(o.groups);
    for (const Row& row : input) {
      if (row.key < 0 || std::cmp_greater_equal(row.key, o.groups) ||
          row.value < -1000000000 || row.value > 1000000000) {
        throw std::runtime_error("input outside exact int64 experiment range");
      }
      State& s = expected[static_cast<size_t>(row.key)];
      ++s.count;
      s.sum += Evaluate(row.value, o.work);
    }
    const auto distinct = static_cast<size_t>(std::ranges::count_if(
        expected, [](const State& s) { return s.count != 0; }));
    std::vector<std::string> modes{"barrier_n1", "spsc_n1",
                                   "partitioned_merge"};
    for (size_t trial = 0; trial <= o.repeats; ++trial) {
      auto order = modes;
      std::rotate(
          order.begin(),
          order.begin() + static_cast<std::ptrdiff_t>(trial % order.size()),
          order.end());
      if (o.reverse) {
        std::ranges::reverse(order);
      }
      for (const auto& mode : order) {
        auto r = Run(mode, o, input, expected);
        Verify(r, expected);
        std::cout << R"({"mode":")" << mode << R"(","trial":)" << trial
                  << ",\"warmup\":" << (trial == 0 ? "true" : "false")
                  << ",\"rows\":" << input.size() << ",\"groups\":" << distinct
                  << ",\"threads\":" << o.threads
                  << ",\"producers\":" << r.producers
                  << ",\"consumers\":" << r.consumers << ",\"work\":" << o.work
                  << ",\"wall_ms\":" << r.wall_ms << ",\"cpu_ms\":" << r.cpu_ms
                  << ",\"partials\":" << r.partials
                  << ",\"batches\":" << r.batches
                  << ",\"full_polls\":" << r.full_polls
                  << ",\"empty_polls\":" << r.empty_polls
                  << ",\"transport_capacity_bytes\":"
                  << r.transport_capacity_bytes
                  << ",\"retained_partial_bytes\":"
                  << (mode == "spsc_n1" ? 0 : r.partials * sizeof(Partial))
                  << ",\"verified\":true}\n"
                  << std::flush;
      }
    }
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
