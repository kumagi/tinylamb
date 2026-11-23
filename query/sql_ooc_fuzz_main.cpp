/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */

// Out-of-process driver for the OOC session fuzzer.  One process runs one
// session, so a crash, a sanitizer abort or a hang costs exactly one session
// and still leaves a reproducible trace behind (every op is appended to the
// trace file before it executes).  scripts/ooc_fuzz.py runs campaigns of
// these, buckets the findings and minimizes them.
//
//   sql_ooc_fuzz gen --seed N [--seconds S] [--scale K] [--flavor NAME]
//                    [--trace FILE] [--op-timeout S] [--verbose]
//   sql_ooc_fuzz replay FILE [--lenient] [--op-timeout S] [--verbose]
//   sql_ooc_fuzz minimize FILE --out FILE [--seconds S]
//   sql_ooc_fuzz flavors
//
// Exit status: 0 every check held, 1 finding (FINDING:/SIGNATURE: lines on
// stdout), 2 usage or malformed input, 3 an op exceeded --op-timeout.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/platform/synced_stream.hpp"
#include "query/sql_ooc_fuzzer.hpp"

namespace {

using tinylamb::OocProfile;
using tinylamb::OocRunOptions;
using tinylamb::OocStats;
using tinylamb::OocTrace;

int Usage() {
  std::cerr << "usage:\n"
               "  sql_ooc_fuzz gen --seed N [--seconds S] [--scale K] "
               "[--flavor NAME] [--trace FILE] [--op-timeout S] [--verbose]\n"
               "  sql_ooc_fuzz replay FILE [--lenient] [--op-timeout S] "
               "[--verbose]\n"
               "  sql_ooc_fuzz minimize FILE --out FILE [--seconds S]\n"
               "  sql_ooc_fuzz flavors\n";
  return 2;
}

struct Args {
  std::string command;
  std::string file;
  std::string trace_path;
  std::string out_path;
  std::string flavor;
  uint64_t seed{0};
  double seconds{0};
  double op_timeout{0};
  int scale{0};
  bool verbose{false};
  bool lenient{false};
};

// Only a known option name means "the previous flag has no value". A value
// that merely starts with `--` (a hyphen-leading --out path or --flavor name)
// is data, not a flag, and must reach the option that consumes it.
bool IsOptionFlag(std::string_view arg) {
  return arg == "--verbose" || arg == "--lenient" || arg == "--seed" ||
         arg == "--seconds" || arg == "--scale" || arg == "--flavor" ||
         arg == "--trace" || arg == "--out" || arg == "--op-timeout";
}

bool ParseArgs(int argc, char** argv, Args* a) {
  if (argc < 2) {
    return false;
  }
  a->command = argv[1];
  for (int i = 2; i < argc; ++i) {
    const std::string_view arg = argv[i];
    auto value = [&]() -> const char* {
      // A missing value must not silently swallow the next option (e.g.
      // `--seed --verbose` would otherwise yield seed 0 and drop --verbose).
      if (i + 1 >= argc || IsOptionFlag(argv[i + 1])) {
        return nullptr;
      }
      return argv[++i];
    };
    if (arg == "--verbose") {
      a->verbose = true;
    } else if (arg == "--lenient") {
      a->lenient = true;
    } else if (arg == "--seed" || arg == "--seconds" || arg == "--scale" ||
               arg == "--flavor" || arg == "--trace" || arg == "--out" ||
               arg == "--op-timeout") {
      const char* v = value();
      if (v == nullptr) {
        return false;
      }
      if (arg == "--seed") {
        a->seed = std::strtoull(v, nullptr, 10);
      } else if (arg == "--seconds") {
        a->seconds = std::strtod(v, nullptr);
      } else if (arg == "--scale") {
        a->scale = static_cast<int>(std::strtol(v, nullptr, 10));
      } else if (arg == "--flavor") {
        a->flavor = v;
      } else if (arg == "--trace") {
        a->trace_path = v;
      } else if (arg == "--out") {
        a->out_path = v;
      } else {
        a->op_timeout = std::strtod(v, nullptr);
      }
    } else if (!arg.starts_with("--") && a->file.empty()) {
      a->file = std::string(arg);
    } else {
      return false;
    }
  }
  return true;
}

// Flags an op that runs longer than the timeout: a spill operator waiting
// for memory it will never get, or a livelock in the eviction path.
class Watchdog {
 public:
  explicit Watchdog(double timeout_seconds) : timeout_(timeout_seconds) {
    if (timeout_ > 0) {
      thread_ = std::thread([this]() { Run(); });
    }
  }
  Watchdog(const Watchdog&) = delete;
  Watchdog& operator=(const Watchdog&) = delete;
  Watchdog(Watchdog&&) = delete;
  Watchdog& operator=(Watchdog&&) = delete;
  ~Watchdog() {
    stop_.store(true, std::memory_order_release);
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  void Tick(const std::string& op) {
    std::scoped_lock lock(mu_);
    op_ = op;
    started_ = std::chrono::steady_clock::now();
    armed_ = true;
  }
  void Disarm() {
    std::scoped_lock lock(mu_);
    armed_ = false;
  }

 private:
  void Run() {
    while (!stop_.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      {
        // Hold mu_ from the timeout decision through the exit. Disarm() must
        // either win the lock first (we then see armed_ == false and skip) or
        // not run at all: releasing the lock between the check and _Exit let
        // a late Disarm() cancel a finding only after it was printed, giving
        // a false hang for an op that actually completed.
        std::scoped_lock lock(mu_);
        if (!armed_ || std::chrono::duration<double>(
                           std::chrono::steady_clock::now() - started_)
                               .count() < timeout_) {
          continue;
        }
        const std::string kind = op_.substr(0, op_.find(' '));
        {
          // A disarmed watchdog can still be inside this block while the main
          // thread prints a report; osyncstream serialises the two writers so
          // a SIGNATURE line is never interleaved with other stdout output.
          tinylamb::platform::SyncedOStream out(std::cout);
          out << "FINDING: op ran longer than " << timeout_
              << " s (hang or livelock): " << op_.substr(0, 400) << "\n"
              << "SIGNATURE: hang / " << kind << "\n"
              << std::flush;
        }
        std::_Exit(3);
      }
    }
  }

  double timeout_;
  std::mutex mu_;
  std::string op_;
  std::chrono::steady_clock::time_point started_;
  bool armed_{false};
  std::atomic<bool> stop_{false};
  std::thread thread_;
};

void PrintStats(const OocStats& stats) {
  tinylamb::platform::SyncedOStream out(std::cout);
  out << "STATS: " << stats.Summary() << "\n";
  out << "SHAPES:";
  for (const auto& [name, n] : stats.shapes) {
    out << " " << name << "=" << n;
  }
  out << "\nOPERATORS:";
  for (const auto& [name, n] : stats.operators) {
    out << " " << name << "=" << n;
  }
  out << "\n";
  for (const auto& [name, n] : stats.routes) {
    out << "ROUTE: " << n << " " << name << "\n";
  }
}

int Report(const std::string& report, const OocStats& stats) {
  PrintStats(stats);
  tinylamb::platform::SyncedOStream out(std::cout);
  if (report.empty()) {
    out << "OK\n";
    return 0;
  }
  out << "FINDING: " << report << (report.ends_with('\n') ? "" : "\n")
      << "SIGNATURE: " << tinylamb::OocFailureSignature(report) << "\n";
  return 1;
}

bool LoadTrace(const std::string& path, uint64_t* seed, OocTrace* trace,
               std::string* summary) {
  std::ifstream file(path);
  if (!file) {
    std::cerr << "cannot read " << path << "\n";
    return false;
  }
  const std::string text((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());
  if (!tinylamb::ParseOocTest(text, seed, trace, summary)) {
    std::cerr << "malformed trace file " << path << "\n";
    return false;
  }
  return true;
}

int RunGen(const Args& a) {
  OocRunOptions options;
  options.verbose = a.verbose;
  options.budget_seconds = a.seconds;
  options.scale = a.scale;
  if (!a.flavor.empty()) {
    const std::vector<std::string> names = tinylamb::OocFlavorNames();
    for (size_t i = 0; i < names.size(); ++i) {
      if (names[i] == a.flavor) {
        options.flavor = static_cast<int>(i);
      }
    }
    if (options.flavor < 0) {
      std::cerr << "unknown flavor " << a.flavor << "\n";
      return 2;
    }
  }
  std::ofstream trace_file;
  // T3295: open (and validate) the trace up front -- a bad --trace-path
  // used to leave an empty/absent trace while gen still reported OK.
  if (!a.trace_path.empty()) {
    trace_file.open(a.trace_path, std::ios::trunc);
    if (!trace_file) {
      std::cerr << "cannot open --trace " << a.trace_path << "\n";
      return 2;
    }
  }
  Watchdog watchdog(a.op_timeout);
  options.on_profile = [&](const OocProfile& profile) {
    if (trace_file.is_open()) {
      // Write into the already-open stream: re-opening here would set
      // failbit from a second callback onward.
      trace_file << tinylamb::SerializeOocHeader(a.seed, profile) << std::flush;
    }
  };
  options.on_op = [&](const std::string& op) {
    if (trace_file.is_open()) {
      trace_file << op << "\n" << std::flush;
    }
    watchdog.Tick(op);
  };
  OocStats stats;
  OocTrace trace;
  const std::string report =
      tinylamb::RunOocSession(nullptr, 0, a.seed, options, &stats, &trace);
  watchdog.Disarm();
  const int rc = Report(report, stats);
  if (!a.trace_path.empty()) {
    trace_file.close();
    if (!trace_file) {
      std::cerr << "failed writing --trace " << a.trace_path << "\n";
      return rc == 0 ? 1 : rc;
    }
  }
  return rc;
}

int RunReplay(const Args& a) {
  uint64_t seed = 0;
  OocTrace trace;
  std::string summary;
  if (a.file.empty() || !LoadTrace(a.file, &seed, &trace, &summary)) {
    return 2;
  }
  OocRunOptions options;
  options.verbose = a.verbose;
  options.lenient = a.lenient;
  Watchdog watchdog(a.op_timeout);
  options.on_op = [&](const std::string& op) { watchdog.Tick(op); };
  OocStats stats;
  const std::string report = tinylamb::ReplayOocTrace(trace, options, &stats);
  watchdog.Disarm();
  return Report(report, stats);
}

int RunMinimize(const Args& a) {
  uint64_t seed = 0;
  OocTrace trace;
  std::string summary;
  if (a.file.empty() || a.out_path.empty() ||
      !LoadTrace(a.file, &seed, &trace, &summary)) {
    return 2;
  }
  OocRunOptions lenient;
  lenient.lenient = true;
  Watchdog watchdog(a.op_timeout);
  lenient.on_op = [&](const std::string& op) { watchdog.Tick(op); };
  const std::string report = tinylamb::ReplayOocTrace(trace, lenient, nullptr);
  if (report.empty()) {
    watchdog.Disarm();
    std::cout << "OK (the trace does not reproduce in-process)\n";
    return 0;
  }
  std::string final_report;
  // Minimize replays many candidate traces; pass the same watchdog into it so
  // --op-timeout keeps guarding every op, not just the first reproduction.
  const OocTrace minimized = tinylamb::MinimizeOocTrace(
      trace, report, a.seconds > 0 ? a.seconds : 300, &final_report,
      [&](const std::string& op) { watchdog.Tick(op); });
  watchdog.Disarm();
  std::string one_line = final_report;
  for (char& c : one_line) {
    if (c == '\n') {
      c = ' ';
    }
  }
  {
    std::ofstream out(a.out_path);
    if (!out) {
      std::cerr << "cannot write minimized trace to " << a.out_path << "\n";
      return 1;
    }
    out << tinylamb::SerializeOocTest(seed, minimized, one_line.substr(0, 600));
    if (!out) {
      std::cerr << "failed writing minimized trace to " << a.out_path << "\n";
      return 1;
    }
  }
  std::cout << "MINIMIZED: " << trace.ops.size() << " -> "
            << minimized.ops.size() << " ops, written to " << a.out_path
            << "\nFINDING: " << final_report
            << (final_report.ends_with('\n') ? "" : "\n")
            << "SIGNATURE: " << tinylamb::OocFailureSignature(final_report)
            << "\n";
  return 1;
}

}  // namespace

int main(int argc, char** argv) {
  Args a;
  if (!ParseArgs(argc, argv, &a)) {
    return Usage();
  }
  if (a.command == "gen") {
    return RunGen(a);
  }
  if (a.command == "replay") {
    return RunReplay(a);
  }
  if (a.command == "minimize") {
    return RunMinimize(a);
  }
  if (a.command == "flavors") {
    for (const std::string& name : tinylamb::OocFlavorNames()) {
      std::cout << name << "\n";
    }
    return 0;
  }
  return Usage();
}
