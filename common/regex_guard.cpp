/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/regex_guard.hpp"

#include <algorithm>
#include <cstring>
#include <iterator>
#include <memory>
#include <utility>
#ifdef TINYLAMB_HAVE_RE2
#include <re2/re2.h>
#endif

namespace tinylamb {
namespace {
bool IsAsciiAlphaNumeric(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
         (c >= '0' && c <= '9');
}

struct RewritePiece {
  bool is_capture = false;
  uint8_t capture_index = 0;
  std::string literal;
};

class LinearPrefixVM {
 public:
  enum class StepKind : uint8_t {
    kLiteral = 0,
    kOptLiteral = 1,
    kCharClassOne = 2,
    kOptCharClass = 3,
    kCharClassPlus = 4,
    kCharClassStar = 5,
    kCaptureStart = 6,
    kCaptureEnd = 7,
  };

  struct Step {
    StepKind kind = StepKind::kLiteral;
    std::string literal;
    regex_guard_detail::CharSet charset{};
    int excluded_byte = -1;
    uint8_t capture_id = 0;
  };

  static std::optional<LinearPrefixVM> TryCompile(std::string_view pattern,
                                                  size_t expected_mark_count) {
    if (pattern.size() < 2 || pattern.front() != '^') {
      return std::nullopt;
    }
    for (size_t k = 0; k < pattern.size(); ++k) {
      if (static_cast<unsigned char>(pattern[k]) >= 128 ||
          pattern.substr(k, 2) == "[:") {
        return std::nullopt;
      }
    }

    LinearPrefixVM vm;
    size_t pos = 1;
    uint8_t next_capture = 1;
    size_t opt_count = 0;
    bool saw_end_anchor = false;
    if (!ParseSequence(pattern, pos, /*end_on_rparen=*/false, vm.steps_,
                       next_capture, opt_count, saw_end_anchor,
                       vm.accept_prefix_)) {
      return std::nullopt;
    }
    if (!saw_end_anchor || pos != pattern.size() ||
        std::cmp_not_equal(next_capture - 1, expected_mark_count) ||
        opt_count > 6 || vm.steps_.size() > 32) {
      return std::nullopt;
    }

    // Verify every greedy character-class repetition has a disjoint follow-set
    // so greedy consumption can never steal bytes needed by subsequent atoms.
    for (size_t idx = 0; idx < vm.steps_.size(); ++idx) {
      const Step& step = vm.steps_[idx];
      if (step.kind != StepKind::kCharClassPlus &&
          step.kind != StepKind::kCharClassStar) {
        continue;
      }
      regex_guard_detail::CharSet follow_set;
      for (size_t j = idx + 1; j < vm.steps_.size(); ++j) {
        const Step& next = vm.steps_[j];
        if (next.kind == StepKind::kCaptureStart ||
            next.kind == StepKind::kCaptureEnd) {
          continue;
        }
        if (next.kind == StepKind::kOptLiteral) {
          follow_set.set(static_cast<unsigned char>(next.literal.front()));
          continue;
        }
        if (next.kind == StepKind::kOptCharClass ||
            next.kind == StepKind::kCharClassStar) {
          follow_set |= next.charset;
          continue;
        }
        if (next.kind == StepKind::kLiteral) {
          follow_set.set(static_cast<unsigned char>(next.literal.front()));
          break;
        }
        if (next.kind == StepKind::kCharClassOne ||
            next.kind == StepKind::kCharClassPlus) {
          follow_set |= next.charset;
          break;
        }
      }
      if ((step.charset & follow_set).any()) {
        return std::nullopt;
      }
    }

    std::array<std::string_view, 10> empty_captures{};
    if (vm.Match("", empty_captures)) {
      return std::nullopt;
    }
    return vm;
  }

  bool Match(std::string_view input,
             std::array<std::string_view, 10>& captures) const noexcept {
    captures.fill("");
    std::array<size_t, 10> cap_starts{};
    if (!ExecuteFrom(0, 0, input, cap_starts, captures)) {
      return false;
    }
    captures[0] = input;
    return true;
  }

 private:
  static bool IsQuantifierOrBrace(char c) {
    return c == '?' || c == '*' || c == '+' || c == '{' || c == '}';
  }

  static void AppendLiteralByte(std::vector<Step>& steps, char c) {
    if (!steps.empty() && steps.back().kind == StepKind::kLiteral) {
      steps.back().literal.push_back(c);
      return;
    }
    Step s;
    s.kind = StepKind::kLiteral;
    s.literal.push_back(c);
    steps.push_back(std::move(s));
  }

  static void AppendSteps(std::vector<Step>& dst, std::vector<Step>& src) {
    for (Step& s : src) {
      if (s.kind == StepKind::kLiteral && !dst.empty() &&
          dst.back().kind == StepKind::kLiteral) {
        dst.back().literal += s.literal;
      } else {
        dst.push_back(std::move(s));
      }
    }
  }

  static int SingleExcludedByte(const regex_guard_detail::CharSet& cs) {
    if (cs.count() != 255) {
      return -1;
    }
    for (int b = 0; b < 256; ++b) {
      if (!cs.test(static_cast<size_t>(b))) {
        return b;
      }
    }
    return -1;
  }

  static bool ParseClassAtom(std::string_view pattern, size_t& pos,
                             unsigned char* out) {
    const size_t n = pattern.size();
    if (pos >= n || pattern[pos] == '[') {
      return false;
    }
    if (pattern[pos] == '\\') {
      ++pos;
      if (pos >= n || IsAsciiAlphaNumeric(pattern[pos])) {
        return false;
      }
      *out = static_cast<unsigned char>(pattern[pos++]);
      return true;
    }
    *out = static_cast<unsigned char>(pattern[pos++]);
    return true;
  }

  static bool ParseSequence(std::string_view pattern, size_t& pos,
                            bool end_on_rparen, std::vector<Step>& steps,
                            uint8_t& next_capture, size_t& opt_count,
                            bool& saw_end_anchor, bool& accept_prefix) {
    const size_t n = pattern.size();
    while (pos < n) {
      const char c = pattern[pos];
      if (end_on_rparen && c == ')') {
        return true;
      }
      if (c == '$') {
        if (end_on_rparen || pos + 1 != n) {
          return false;
        }
        accept_prefix = false;
        saw_end_anchor = true;
        pos = n;
        return true;
      }
      if (!end_on_rparen && pos + 8 == n &&
          pattern.substr(pos, 8) == R"([\s\S]*$)") {
        accept_prefix = true;
        saw_end_anchor = true;
        pos = n;
        return true;
      }
      if (c == '(') {
        if (pattern.substr(pos, 3) == "(?:") {
          pos += 3;
          std::vector<Step> inner;
          bool dummy_anchor = false;
          bool dummy_prefix = false;
          if (!ParseSequence(pattern, pos, /*end_on_rparen=*/true, inner,
                             next_capture, opt_count, dummy_anchor,
                             dummy_prefix) ||
              pos >= n || pattern[pos] != ')') {
            return false;
          }
          ++pos;
          if (pos < n && pattern[pos] == '?') {
            ++pos;
            if (pos < n && IsQuantifierOrBrace(pattern[pos])) {
              return false;
            }
            if (inner.size() != 1 || inner[0].kind != StepKind::kLiteral ||
                inner[0].literal.empty()) {
              return false;
            }
            Step opt;
            opt.kind = StepKind::kOptLiteral;
            opt.literal = std::move(inner[0].literal);
            steps.push_back(std::move(opt));
            ++opt_count;
          } else if (pos < n && IsQuantifierOrBrace(pattern[pos])) {
            return false;
          } else {
            AppendSteps(steps, inner);
          }
          continue;
        }
        if (pos + 1 >= n || pattern[pos + 1] == '?') {
          return false;
        }
        ++pos;
        if (next_capture > 9) {
          return false;
        }
        const uint8_t cap_id = next_capture++;
        Step cap_start;
        cap_start.kind = StepKind::kCaptureStart;
        cap_start.capture_id = cap_id;
        steps.push_back(std::move(cap_start));
        bool dummy_anchor = false;
        bool dummy_prefix = false;
        if (!ParseSequence(pattern, pos, /*end_on_rparen=*/true, steps,
                           next_capture, opt_count, dummy_anchor,
                           dummy_prefix) ||
            pos >= n || pattern[pos] != ')') {
          return false;
        }
        ++pos;
        if (pos < n && IsQuantifierOrBrace(pattern[pos])) {
          return false;
        }
        Step cap_end;
        cap_end.kind = StepKind::kCaptureEnd;
        cap_end.capture_id = cap_id;
        steps.push_back(std::move(cap_end));
        continue;
      }
      if (c == '[') {
        ++pos;
        const bool negated = (pos < n && pattern[pos] == '^');
        if (negated) {
          ++pos;
        }
        if (pos >= n || pattern[pos] == ']') {
          return false;
        }
        regex_guard_detail::CharSet cs;
        while (pos < n && pattern[pos] != ']') {
          unsigned char lo = 0;
          if (!ParseClassAtom(pattern, pos, &lo)) {
            return false;
          }
          if (pos + 1 < n && pattern[pos] == '-' && pattern[pos + 1] != ']') {
            ++pos;
            unsigned char hi = 0;
            if (!ParseClassAtom(pattern, pos, &hi) || lo > hi) {
              return false;
            }
            regex_guard_detail::AddRange(cs, lo, hi);
          } else {
            cs.set(lo);
          }
        }
        if (pos >= n || pattern[pos] != ']') {
          return false;
        }
        ++pos;
        if (negated) {
          cs.flip();
        }
        if (cs.none()) {
          return false;
        }
        Step cls;
        cls.charset = cs;
        cls.excluded_byte = SingleExcludedByte(cs);
        if (pos < n && (pattern[pos] == '+' || pattern[pos] == '*' ||
                        pattern[pos] == '?')) {
          const char q = pattern[pos++];
          if (pos < n && IsQuantifierOrBrace(pattern[pos])) {
            return false;
          }
          if (q == '+') {
            cls.kind = StepKind::kCharClassPlus;
          } else if (q == '*') {
            cls.kind = StepKind::kCharClassStar;
          } else {
            cls.kind = StepKind::kOptCharClass;
            ++opt_count;
          }
        } else if (pos < n && IsQuantifierOrBrace(pattern[pos])) {
          return false;
        } else {
          cls.kind = StepKind::kCharClassOne;
        }
        steps.push_back(std::move(cls));
        continue;
      }
      if (c == '.' || c == '^' || c == '|' || c == ')' || c == ']' ||
          IsQuantifierOrBrace(c)) {
        return false;
      }
      char lit = 0;
      if (c == '\\') {
        ++pos;
        if (pos >= n || IsAsciiAlphaNumeric(pattern[pos])) {
          return false;
        }
        lit = pattern[pos++];
      } else {
        lit = pattern[pos++];
      }
      if (pos < n &&
          (pattern[pos] == '?' || pattern[pos] == '+' || pattern[pos] == '*')) {
        const char q = pattern[pos++];
        if (pos < n && IsQuantifierOrBrace(pattern[pos])) {
          return false;
        }
        if (q == '?') {
          Step opt;
          opt.kind = StepKind::kOptLiteral;
          opt.literal.push_back(lit);
          steps.push_back(std::move(opt));
          ++opt_count;
        } else {
          Step rep;
          rep.kind =
              (q == '+') ? StepKind::kCharClassPlus : StepKind::kCharClassStar;
          rep.charset.set(static_cast<unsigned char>(lit));
          steps.push_back(std::move(rep));
        }
      } else if (pos < n && IsQuantifierOrBrace(pattern[pos])) {
        return false;
      } else {
        AppendLiteralByte(steps, lit);
      }
    }
    return !end_on_rparen;
  }

  [[nodiscard]] static size_t ScanCharClass(const Step& step,
                                            std::string_view input,
                                            size_t pos) noexcept {
    const size_t n = input.size();
    if (pos >= n) {
      return pos;
    }
    if (step.excluded_byte >= 0) {
      const void* found =
          std::memchr(input.data() + pos, step.excluded_byte, n - pos);
      return found != nullptr
                 ? static_cast<size_t>(static_cast<const char*>(found) -
                                       input.data())
                 : n;
    }
    while (pos < n &&
           step.charset.test(static_cast<unsigned char>(input[pos]))) {
      ++pos;
    }
    return pos;
  }

  bool ExecuteFrom(size_t step_idx, size_t pos, std::string_view input,
                   std::array<size_t, 10>& cap_starts,
                   std::array<std::string_view, 10>& captures) const noexcept {
    const size_t n = input.size();
    while (step_idx < steps_.size()) {
      const Step& step = steps_[step_idx];
      switch (step.kind) {
        case StepKind::kLiteral: {
          const size_t m = step.literal.size();
          if (n - pos < m ||
              std::memcmp(input.data() + pos, step.literal.data(), m) != 0) {
            return false;
          }
          pos += m;
          ++step_idx;
          break;
        }
        case StepKind::kOptLiteral: {
          const size_t m = step.literal.size();
          if (n - pos >= m &&
              std::memcmp(input.data() + pos, step.literal.data(), m) == 0) {
            const auto saved_starts = cap_starts;
            const auto saved_caps = captures;
            if (ExecuteFrom(step_idx + 1, pos + m, input, cap_starts,
                            captures)) {
              return true;
            }
            cap_starts = saved_starts;
            captures = saved_caps;
          }
          ++step_idx;
          break;
        }
        case StepKind::kCharClassOne: {
          if (pos >= n ||
              !step.charset.test(static_cast<unsigned char>(input[pos]))) {
            return false;
          }
          ++pos;
          ++step_idx;
          break;
        }
        case StepKind::kOptCharClass: {
          if (pos < n &&
              step.charset.test(static_cast<unsigned char>(input[pos]))) {
            const auto saved_starts = cap_starts;
            const auto saved_caps = captures;
            if (ExecuteFrom(step_idx + 1, pos + 1, input, cap_starts,
                            captures)) {
              return true;
            }
            cap_starts = saved_starts;
            captures = saved_caps;
          }
          ++step_idx;
          break;
        }
        case StepKind::kCharClassPlus: {
          const size_t end_pos = ScanCharClass(step, input, pos);
          if (end_pos == pos) {
            return false;
          }
          pos = end_pos;
          ++step_idx;
          break;
        }
        case StepKind::kCharClassStar: {
          pos = ScanCharClass(step, input, pos);
          ++step_idx;
          break;
        }
        case StepKind::kCaptureStart: {
          cap_starts[step.capture_id] = pos;
          ++step_idx;
          break;
        }
        case StepKind::kCaptureEnd: {
          const size_t start = cap_starts[step.capture_id];
          captures[step.capture_id] = input.substr(start, pos - start);
          ++step_idx;
          break;
        }
      }
    }
    return accept_prefix_ || pos == n;
  }

  std::vector<Step> steps_;
  bool accept_prefix_ = false;
};

#ifdef TINYLAMB_HAVE_RE2
std::optional<std::string> LinearPattern(std::string_view pattern) {
  std::string result;
  bool in_class = false;
  for (size_t i = 0; i < pattern.size(); ++i) {
    const char c = pattern[i];
    if (static_cast<unsigned char>(c) >= 128 || pattern.substr(i, 2) == "[:") {
      return std::nullopt;
    }
    if (!in_class && pattern.substr(i, 6) == R"([\s\S])") {
      result += "(?s:.)";
      i += 5;
      continue;
    }
    if (c == '\\') {
      if (++i == pattern.size()) {
        return std::nullopt;
      }
      const char escaped = pattern[i];
      // Escape classes and backreferences differ between ECMAScript and
      // RE2. Keep the reference evaluator unless the escape is punctuation.
      if (IsAsciiAlphaNumeric(escaped)) {
        return std::nullopt;
      }
      result += '\\';
      result += escaped;
      continue;
    }
    if (c == '[') {
      in_class = true;
    }
    if (c == ']') {
      in_class = false;
    }
    // ECMAScript dot also excludes CR, unlike RE2's default dot.
    if (c == '.' && !in_class) {
      return std::nullopt;
    }
    result += c;
  }
  return result;
}

bool IsAnchoredSingleMatchPattern(std::string_view pattern) {
  if (pattern.size() < 2 || pattern.front() != '^') {
    return false;
  }
  bool in_class = false;
  size_t paren_depth = 0;
  bool ended_with_anchor = false;
  for (size_t i = 1; i < pattern.size(); ++i) {
    const char c = pattern[i];
    if (c == '\\') {
      if (++i == pattern.size()) {
        return false;
      }
      continue;
    }
    if (in_class) {
      if (c == ']') {
        in_class = false;
      }
      continue;
    }
    if (c == '[') {
      in_class = true;
      continue;
    }
    if (c == '(') {
      if (i + 1 < pattern.size() && pattern[i + 1] == '?' &&
          pattern.substr(i, 3) != "(?:") {
        return false;
      }
      ++paren_depth;
      continue;
    }
    if (c == ')') {
      if (paren_depth == 0) {
        return false;
      }
      --paren_depth;
      continue;
    }
    if (c == '|' && paren_depth == 0) {
      return false;
    }
    if (c == '$') {
      if (paren_depth == 0 && i + 1 == pattern.size()) {
        ended_with_anchor = true;
      } else {
        return false;
      }
    }
  }
  return !in_class && paren_depth == 0 && ended_with_anchor;
}
#endif

struct ReplacementProgram {
  std::regex compiled;
  std::string format;
  std::vector<RewritePiece> rewrite_pieces;
  std::optional<uint8_t> single_capture;
  uint8_t nsubmatch = 0;
  std::optional<LinearPrefixVM> prefix_vm;
#ifdef TINYLAMB_HAVE_RE2
  std::unique_ptr<RE2> linear;
  bool anchored_both = false;
#endif
};

void ApplyRewritePieces(const std::vector<RewritePiece>& pieces,
                        const std::array<std::string_view, 10>& captures,
                        std::string* out) {
  out->clear();
  size_t total = 0;
  for (const RewritePiece& piece : pieces) {
    total += piece.is_capture ? captures[piece.capture_index].size()
                              : piece.literal.size();
  }
  out->reserve(total);
  for (const RewritePiece& piece : pieces) {
    if (piece.is_capture) {
      out->append(captures[piece.capture_index]);
    } else {
      out->append(piece.literal);
    }
  }
}

StatusOr<ReplacementProgram> CompileReplacement(std::string_view pattern,
                                                std::string_view replacement) {
  ASSIGN_OR_RETURN(std::regex, compiled,
                   CompileCheckedRegex(std::string(pattern)));
  const size_t mark_count = compiled.mark_count();
  std::string format;
  format.reserve(replacement.size());
  std::vector<RewritePiece> rewrite_pieces;
  uint8_t nsubmatch = 0;
  auto push_literal_char = [&](char ch) {
    if (!rewrite_pieces.empty() && !rewrite_pieces.back().is_capture) {
      rewrite_pieces.back().literal.push_back(ch);
    } else {
      rewrite_pieces.push_back(RewritePiece{.is_capture = false,
                                            .capture_index = 0,
                                            .literal = std::string(1, ch)});
    }
  };

  for (size_t i = 0; i < replacement.size(); ++i) {
    const char c = replacement[i];
    if (c == '$') {
      format += "$$";
      push_literal_char('$');
    } else if (c != '\\') {
      format += c;
      push_literal_char(c);
    } else {
      if (++i == replacement.size()) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "trailing backslash in regular expression replacement");
      }
      const char escaped = replacement[i];
      if (escaped == '\\') {
        format += '\\';
        push_literal_char('\\');
      } else if (escaped >= '0' && escaped <= '9' &&
                 std::cmp_less_equal(escaped - '0', mark_count)) {
        const auto cap_idx = static_cast<uint8_t>(escaped - '0');
        nsubmatch = std::max<uint8_t>(nsubmatch, cap_idx + 1U);
        rewrite_pieces.push_back(RewritePiece{
            .is_capture = true, .capture_index = cap_idx, .literal = {}});
        format += '$';
        if (escaped == '0') {
          format += '&';
        } else {
          format += '0';
          format += escaped;
        }
      } else {
        return StatusError(StatusCode::kInvalidArgument,
                           "invalid regular expression replacement escape");
      }
    }
  }

  std::optional<uint8_t> single_capture;
  if (rewrite_pieces.size() == 1 && rewrite_pieces[0].is_capture) {
    single_capture = rewrite_pieces[0].capture_index;
  }

  std::optional<LinearPrefixVM> prefix_vm =
      LinearPrefixVM::TryCompile(pattern, mark_count);

  ReplacementProgram program{
      .compiled = std::move(compiled),
      .format = std::move(format),
      .rewrite_pieces = std::move(rewrite_pieces),
      .single_capture = single_capture,
      .nsubmatch = nsubmatch,
      .prefix_vm = std::move(prefix_vm),
#ifdef TINYLAMB_HAVE_RE2
      .linear = nullptr,
      .anchored_both = false,
#endif
  };
#ifdef TINYLAMB_HAVE_RE2
  // Latin1 preserves the byte-oriented contract. Unsupported patterns and
  // empty-matching programs retain the checked ECMAScript implementation.
  const auto linear_pattern = LinearPattern(pattern);
  if (linear_pattern) {
    RE2::Options options;
    options.set_encoding(RE2::Options::EncodingLatin1);
    options.set_log_errors(false);
    options.set_max_mem(1 << 20);
    auto linear = std::make_unique<RE2>(*linear_pattern, options);
    if (linear->ok() && !RE2::FullMatch("", *linear)) {
      std::string error;
      if (!linear->CheckRewriteString(replacement, &error)) {
        return StatusError(StatusCode::kInvalidArgument, error);
      }
      program.anchored_both = IsAnchoredSingleMatchPattern(pattern);
      program.linear = std::move(linear);
    }
  }
#endif
  return program;
}

StatusOr<const ReplacementProgram*> LookupOrCompileReplacement(
    std::string_view pattern, std::string_view replacement,
    std::optional<ReplacementProgram>* temporary) {
  struct Entry {
    std::string pattern;
    std::string replacement;
    ReplacementProgram program;
  };
  static thread_local std::array<std::optional<Entry>, 8> cache;
  static thread_local size_t next = 0;
  for (const auto& entry : cache) {
    if (entry && entry->pattern == pattern &&
        entry->replacement == replacement) {
      return &entry->program;
    }
  }
  ASSIGN_OR_RETURN(ReplacementProgram, compiled,
                   CompileReplacement(pattern, replacement));
  if (pattern.size() <= 4096 && replacement.size() <= 4096) {
    auto& entry =
        cache[next].emplace(Entry{.pattern = std::string(pattern),
                                  .replacement = std::string(replacement),
                                  .program = std::move(compiled)});
    next = (next + 1) % cache.size();
    return &entry.program;
  }
  return &temporary->emplace(std::move(compiled));
}
}  // namespace

StatusOr<std::string_view> ReplaceCheckedRegexView(std::string_view input,
                                                   std::string_view pattern,
                                                   std::string_view replacement,
                                                   std::string* scratch) try {
  if (scratch == nullptr) {
    return StatusError(StatusCode::kInvalidArgument,
                       "null scratch buffer in regular expression replacement");
  }
  if (input.empty()) {
    input = "";
  }
  std::optional<ReplacementProgram> temporary;
  ASSIGN_OR_RETURN(
      const ReplacementProgram*, program,
      LookupOrCompileReplacement(pattern, replacement, &temporary));

  if (program->prefix_vm.has_value()) {
    std::array<std::string_view, 10> captures{};
    if (!program->prefix_vm->Match(input, captures)) {
      return input;
    }
    if (program->single_capture.has_value()) {
      return captures[*program->single_capture];
    }
    ApplyRewritePieces(program->rewrite_pieces, captures, scratch);
    return std::string_view(*scratch);
  }

#ifdef TINYLAMB_HAVE_RE2
  if (program->linear) {
    const absl::string_view input_sp(input.data(), input.size());
    if (program->anchored_both) {
      std::array<absl::string_view, 10> submatches{};
      if (!program->linear->Match(input_sp, 0, input_sp.size(),
                                  RE2::ANCHOR_BOTH, submatches.data(),
                                  program->nsubmatch)) {
        return input;
      }
      if (program->single_capture.has_value()) {
        const absl::string_view sub = submatches[*program->single_capture];
        return sub.empty() ? std::string_view("", 0)
                           : std::string_view(sub.data(), sub.size());
      }
      std::array<std::string_view, 10> captures{};
      for (size_t i = 0; i < program->nsubmatch; ++i) {
        const absl::string_view sub = submatches[i];
        captures[i] = sub.empty() ? std::string_view("", 0)
                                  : std::string_view(sub.data(), sub.size());
      }
      ApplyRewritePieces(program->rewrite_pieces, captures, scratch);
      return std::string_view(*scratch);
    }
    scratch->assign(input.data(), input.size());
    if (RE2::GlobalReplace(
            scratch, *program->linear,
            absl::string_view(replacement.data(), replacement.size())) == 0) {
      return input;
    }
    return std::string_view(*scratch);
  }
#endif

  scratch->clear();
  std::regex_replace(std::back_inserter(*scratch), input.begin(), input.end(),
                     program->compiled, program->format);
  return std::string_view(*scratch);
} catch (...) {
  return StatusError(StatusCode::kInvalidArgument,
                     "regular expression replacement failed");
}

StatusOr<std::string> ReplaceCheckedRegex(std::string_view input,
                                          std::string_view pattern,
                                          std::string_view replacement) try {
  std::string scratch;
  ASSIGN_OR_RETURN(
      std::string_view, view,
      ReplaceCheckedRegexView(input, pattern, replacement, &scratch));
  if (!scratch.empty() && view.data() == scratch.data() &&
      view.size() == scratch.size()) {
    return scratch;
  }
  return std::string(view);
} catch (...) {
  return StatusError(StatusCode::kInvalidArgument,
                     "regular expression replacement failed");
}

}  // namespace tinylamb
