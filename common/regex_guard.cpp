/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/regex_guard.hpp"

#include <algorithm>
#include <bit>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <stdexcept>
#include <utility>

#include "common/string_util.hpp"
#ifdef TINYLAMB_HAVE_RE2
#include <re2/re2.h>
#endif

namespace tinylamb {
namespace {

// P464 test hook: full CompileReplacement() executions (cache misses) on
// this thread.  Exposed via ReplacementProgramFullCompilesForTest().
thread_local size_t g_replacement_full_compiles = 0;
std::string Utf8RegexPattern(std::string_view pattern) {
  // std::regex searches bytes. An unescaped wildcard in a STRING pattern
  // consumes a full code point; in a BYTES pattern it consumes one byte.
  constexpr std::string_view dot =
      R"((?:[\x00-\x09\x0B\x0C\x0E-\x7F]|[\xC2-\xDF][\x80-\xBF]|[\xE0-\xEF][\x80-\xBF]{2}|[\xF0-\xF4][\x80-\xBF]{3}))";
  std::string converted;
  // Count only the unescaped dots outside character classes that will expand.
  size_t dots = 0;
  bool counting_in_class = false;
  for (size_t i = 0; i < pattern.size(); ++i) {
    const char ch = pattern[i];
    if (ch == '\\' && i + 1 < pattern.size()) {
      ++i;  // the escaped character is copied verbatim below
    } else if (ch == '.' && !counting_in_class) {
      ++dots;
    } else if (ch == '[') {
      counting_in_class = true;
    } else if (ch == ']') {
      counting_in_class = false;
    }
  }
  constexpr size_t growth = dot.size() - 1;
  if (pattern.size() > converted.max_size() ||
      dots > (converted.max_size() - pattern.size()) / growth) {
    throw std::length_error("expanded regex is too large");
  }
  converted.reserve(pattern.size() + (dots * growth));
  bool in_class = false;
  for (size_t i = 0; i < pattern.size(); ++i) {
    const char ch = pattern[i];
    if (ch == '\\' && i + 1 < pattern.size()) {
      converted.append(pattern.substr(i, 2));
      ++i;
    } else if (ch == '.' && !in_class) {
      converted.append(dot);
    } else {
      converted.push_back(ch);
      if (ch == '[') {
        in_class = true;
      } else if (ch == ']') {
        in_class = false;
      }
    }
  }
  return converted;
}

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

  [[nodiscard]] std::optional<DelimitedRegexCapture> DescribeDelimitedCapture(
      uint8_t capture) const {
    if (!accept_prefix_ || capture == 0 || steps_.size() < 4) {
      return std::nullopt;
    }
    const size_t end = steps_.size();
    const Step& open = steps_[end - 4];
    const Step& run = steps_[end - 3];
    const Step& close = steps_[end - 2];
    const Step& stop = steps_[end - 1];
    if (open.kind != StepKind::kCaptureStart || open.capture_id != capture ||
        run.kind != StepKind::kCharClassPlus || run.excluded_byte < 0 ||
        close.kind != StepKind::kCaptureEnd || close.capture_id != capture ||
        stop.kind != StepKind::kLiteral || stop.literal.size() != 1 ||
        std::cmp_not_equal(stop.literal.front(), run.excluded_byte)) {
      return std::nullopt;
    }
    DelimitedRegexCapture description{
        .prefixes = {""}, .delimiter = static_cast<uint8_t>(run.excluded_byte)};
    for (const Step& step : std::span(steps_).first(end - 4)) {
      if (step.kind == StepKind::kLiteral) {
        for (auto& prefix : description.prefixes) {
          prefix += step.literal;
        }
      } else if (step.kind == StepKind::kOptLiteral &&
                 description.prefixes.size() < 8) {
        std::vector<std::string> alternatives;
        alternatives.reserve(description.prefixes.size());
        for (const auto& prefix : description.prefixes) {
          alternatives.push_back(prefix + step.literal);
          alternatives.push_back(prefix);
        }
        description.prefixes = std::move(alternatives);
      } else {
        return std::nullopt;
      }
    }
    return description;
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
          if (step.capture_id >= cap_starts.size()) {
            return false;
          }
          cap_starts[step.capture_id] = pos;
          ++step_idx;
          break;
        }
        case StepKind::kCaptureEnd: {
          if (step.capture_id >= cap_starts.size()) {
            return false;
          }
          const size_t start = cap_starts[step.capture_id];
          if (start > pos || pos > n) {
            return false;
          }
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
  result.reserve(pattern.size());
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

Status ApplyRewritePieces(const std::vector<RewritePiece>& pieces,
                          const std::array<std::string_view, 10>& captures,
                          std::string* out) {
  out->clear();
  size_t total = 0;
  for (const RewritePiece& piece : pieces) {
    const size_t bytes = piece.is_capture ? captures[piece.capture_index].size()
                                          : piece.literal.size();
    if (bytes > out->max_size() - total) {
      return StatusError(StatusCode::kTooBigData,
                         "regular expression replacement is too large");
    }
    total += bytes;
  }
  out->reserve(total);
  for (const RewritePiece& piece : pieces) {
    if (piece.is_capture) {
      out->append(captures[piece.capture_index]);
    } else {
      out->append(piece.literal);
    }
  }
  return Status::kSuccess;
}

StatusOr<ReplacementProgram> CompileReplacement(std::string_view pattern,
                                                std::string_view replacement) {
  ASSIGN_OR_RETURN(std::regex, compiled,
                   CompileCheckedRegex(std::string(pattern)));
  const size_t mark_count = compiled.mark_count();
  std::string format;
  // Each capture reference grows by one backslash and one brace; reserving
  // twice the input avoids repeated reallocations for replacement-heavy work.
  format.reserve(replacement.size() * 2);
  std::vector<RewritePiece> rewrite_pieces;
  rewrite_pieces.reserve(replacement.size());
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
    size_t bytes = 0;
  };
  // P464: the old gate dropped programs over 4096 bytes of pattern *or*
  // replacement from the cache, forcing a full recompile (nested-repetition
  // scan + std::regex + RE2) per row for a stable large replacement
  // template.  Cache any program whose conservative size estimate fits the
  // per-thread budget, evicting round-robin-oldest entries as needed.
  static thread_local std::array<std::optional<Entry>, 8> cache;
  static thread_local size_t head = 0;
  static thread_local size_t count = 0;
  static thread_local size_t used = 0;
  for (const auto& entry : cache) {
    if (entry && entry->pattern == pattern &&
        entry->replacement == replacement) {
      return &entry->program;
    }
  }
  ASSIGN_OR_RETURN(ReplacementProgram, compiled,
                   CompileReplacement(pattern, replacement));
  ++g_replacement_full_compiles;
  size_t bytes = pattern.size() + replacement.size() + compiled.format.size() +
                 (size_t{1} << 10);
#ifdef TINYLAMB_HAVE_RE2
  if (compiled.linear != nullptr) {
    // The compile path caps RE2 at max_mem = 1 MiB; book the full cap.
    bytes += size_t{1} << 20;
  }
#endif
  // 8 entries x (1 MiB RE2 cap + text) worst case.  The entries outlive any
  // single query (thread-local), so they are budgeted here rather than
  // charged to the per-query kQuery pool, which small OOC budgets would
  // wedge.
  constexpr size_t kProgramCacheBudget = size_t{8} << 20;
  if (bytes <= kProgramCacheBudget) {
    // Evict the oldest entries only while a slot or bytes are needed.
    while (count == cache.size() || used + bytes > kProgramCacheBudget) {
      auto& slot = cache[head];
      used -= slot->bytes;
      slot.reset();
      head = (head + 1) % cache.size();
      --count;
    }
    auto& entry = cache[(head + count) % cache.size()].emplace(
        Entry{.pattern = std::string(pattern),
              .replacement = std::string(replacement),
              .program = std::move(compiled),
              .bytes = bytes});
    used += bytes;
    ++count;
    return &entry.program;
  }
  return &temporary->emplace(std::move(compiled));
}

// Conservative regex AST + bitset Thompson NFA for REGEXP_CONTAINS.
// Lean proofs:
//   - lean/TinylambOpt/RegexMandatoryLiteral.lean (mandatory_search_sound)
//   - lean/TinylambOpt/RegexBitsetNfa.lean +
//     lean/TinylambOpt/ThompsonSearch.lean (compiled_search_iff)
struct FastRegexAstNode {
  enum class Kind {
    kEmpty,
    kAnchor,
    kLiteral,  // non-empty pure literal string
    kCharSet,  // single 7-bit ASCII byte class
    kConcat,
    kAlt,
    kRepeat,  // min_rep..max_rep (max_rep == -1 => unbounded)
  };
  Kind kind{Kind::kEmpty};
  std::string literal;
  std::array<uint64_t, 2> charset{};  // bits 0..127
  int min_rep{1};
  int max_rep{1};
  std::vector<FastRegexAstNode> children;
};

struct FastRegexParser {
  std::string_view pat;
  size_t pos{0};
  bool aborted{false};

  static void SetAsciiBit(std::array<uint64_t, 2>& cs, uint8_t b) noexcept {
    if (b < 64) {
      cs[0] |= uint64_t{1} << b;
    } else {
      cs[1] |= uint64_t{1} << (b - 64);
    }
  }

  static void SetAsciiRange(std::array<uint64_t, 2>& cs, uint8_t lo,
                            uint8_t hi) noexcept {
    for (unsigned c = lo; c <= hi && c < 128; ++c) {
      SetAsciiBit(cs, static_cast<uint8_t>(c));
    }
  }

  [[nodiscard]] bool Parse(FastRegexAstNode* out) {
    *out = ParseAlt();
    return !(aborted || pos != pat.size());
  }

  FastRegexAstNode ParseAlt() {
    std::vector<FastRegexAstNode> branches;
    branches.push_back(ParseConcat());
    while (!aborted && pos < pat.size() && pat[pos] == '|') {
      ++pos;
      branches.push_back(ParseConcat());
    }
    if (branches.size() == 1) {
      return std::move(branches[0]);
    }
    FastRegexAstNode node;
    node.kind = FastRegexAstNode::Kind::kAlt;
    node.children = std::move(branches);
    return node;
  }

  FastRegexAstNode ParseConcat() {
    std::vector<FastRegexAstNode> items;
    while (!aborted && pos < pat.size() && pat[pos] != '|' && pat[pos] != ')') {
      FastRegexAstNode piece = ParsePiece();
      if (aborted) {
        return {};
      }
      if (piece.kind == FastRegexAstNode::Kind::kEmpty) {
        continue;
      }
      if (!items.empty() &&
          items.back().kind == FastRegexAstNode::Kind::kLiteral &&
          piece.kind == FastRegexAstNode::Kind::kLiteral) {
        items.back().literal += piece.literal;
      } else {
        items.push_back(std::move(piece));
      }
    }
    if (items.empty()) {
      FastRegexAstNode empty;
      empty.kind = FastRegexAstNode::Kind::kEmpty;
      return empty;
    }
    if (items.size() == 1) {
      return std::move(items[0]);
    }
    FastRegexAstNode node;
    node.kind = FastRegexAstNode::Kind::kConcat;
    node.children = std::move(items);
    return node;
  }

  static bool ParseDecimalInt(std::string_view sv, size_t* p, int* out) {
    if (*p >= sv.size() || sv[*p] < '0' || sv[*p] > '9') {
      return false;
    }
    int64_t val = 0;
    while (*p < sv.size() && sv[*p] >= '0' && sv[*p] <= '9') {
      val = (val * 10) + (sv[*p] - '0');
      if (val > 1000) {
        return false;
      }
      ++(*p);
    }
    *out = static_cast<int>(val);
    return true;
  }

  FastRegexAstNode ParsePiece() {
    FastRegexAstNode atom = ParseAtom();
    if (aborted) {
      return {};
    }
    if (pos < pat.size()) {
      const char q = pat[pos];
      if (q == '*' || q == '+' || q == '?') {
        if (atom.kind == FastRegexAstNode::Kind::kAnchor) {
          aborted = true;
          return {};
        }
        ++pos;
        if (pos < pat.size() && pat[pos] == '?') {
          ++pos;  // lazy modifier preserves REGEXP_CONTAINS existence
        }
        if (pos < pat.size() && (pat[pos] == '*' || pat[pos] == '+' ||
                                 pat[pos] == '?' || pat[pos] == '{')) {
          aborted = true;  // stacked quantifier edge case
          return {};
        }
        FastRegexAstNode rep;
        rep.kind = FastRegexAstNode::Kind::kRepeat;
        rep.min_rep = (q == '+') ? 1 : 0;
        rep.max_rep = (q == '?') ? 1 : -1;
        rep.children.push_back(std::move(atom));
        return rep;
      }
      if (q == '{') {
        if (atom.kind == FastRegexAstNode::Kind::kAnchor) {
          aborted = true;
          return {};
        }
        size_t p = pos + 1;
        int lo = 0;
        if (!ParseDecimalInt(pat, &p, &lo)) {
          aborted = true;
          return {};
        }
        int hi = lo;
        if (p < pat.size() && pat[p] == ',') {
          ++p;
          if (p < pat.size() && pat[p] == '}') {
            hi = -1;
          } else if (!ParseDecimalInt(pat, &p, &hi)) {
            aborted = true;
            return {};
          }
        }
        if (p >= pat.size() || pat[p] != '}' || (hi != -1 && hi < lo)) {
          aborted = true;
          return {};
        }
        pos = p + 1;
        if (pos < pat.size() && pat[pos] == '?') {
          ++pos;
        }
        FastRegexAstNode rep;
        rep.kind = FastRegexAstNode::Kind::kRepeat;
        rep.min_rep = lo;
        rep.max_rep = hi;
        rep.children.push_back(std::move(atom));
        return rep;
      }
    }
    return atom;
  }

  static bool IsPunctEscape(char e) noexcept {
    switch (e) {
      case '\\':
      case '.':
      case '-':
      case '/':
      case '_':
      case '+':
      case '*':
      case '?':
      case '(':
      case ')':
      case '[':
      case ']':
      case '{':
      case '}':
      case '^':
      case '$':
      case '|':
      case ':':
      case ';':
      case ',':
      case '<':
      case '>':
      case '\'':
      case '"':
      case '!':
      case '@':
      case '#':
      case '%':
      case '&':
      case '=':
      case '~':
      case '`':
      case ' ':
        return true;
      default:
        return false;
    }
  }

  static bool ParseEscapeAtom(char esc, FastRegexAstNode* out) {
    if (esc == 'n' || esc == 'r' || esc == 't' || esc == 'f' || esc == 'v' ||
        IsPunctEscape(esc)) {
      char ch = esc;
      if (esc == 'n') { ch = '\n';
}
      if (esc == 'r') { ch = '\r';
}
      if (esc == 't') { ch = '\t';
}
      if (esc == 'f') { ch = '\f';
}
      if (esc == 'v') { ch = '\v';
}
      out->kind = FastRegexAstNode::Kind::kLiteral;
      out->literal.assign(1, ch);
      return true;
    }
    std::array<uint64_t, 2> cs{};
    switch (esc) {
      case 'd':
        SetAsciiRange(cs, '0', '9');
        break;
      case 'D':
        SetAsciiRange(cs, 0, 127);
        cs[0] &= ~((uint64_t{1} << ('9' + 1)) - (uint64_t{1} << '0'));
        break;
      case 'w':
        SetAsciiRange(cs, '0', '9');
        SetAsciiRange(cs, 'A', 'Z');
        SetAsciiBit(cs, '_');
        SetAsciiRange(cs, 'a', 'z');
        break;
      case 'W': {
        std::array<uint64_t, 2> w{};
        SetAsciiRange(w, '0', '9');
        SetAsciiRange(w, 'A', 'Z');
        SetAsciiBit(w, '_');
        SetAsciiRange(w, 'a', 'z');
        cs[0] = ~w[0];
        cs[1] = ~w[1];
        break;
      }
      case 's':
        for (char c : {' ', '\t', '\n', '\v', '\f', '\r'}) {
          SetAsciiBit(cs, static_cast<uint8_t>(c));
        }
        break;
      case 'S': {
        std::array<uint64_t, 2> s{};
        for (char c : {' ', '\t', '\n', '\v', '\f', '\r'}) {
          SetAsciiBit(s, static_cast<uint8_t>(c));
        }
        cs[0] = ~s[0];
        cs[1] = ~s[1];
        break;
      }
      default:
        return false;
    }
    out->kind = FastRegexAstNode::Kind::kCharSet;
    out->charset = cs;
    return true;
  }

  FastRegexAstNode ParseAtom() {
    if (pos >= pat.size()) {
      aborted = true;
      return {};
    }
    const char c = pat[pos];
    if (static_cast<unsigned char>(c) >= 128U) {
      aborted = true;
      return {};
    }
    if (c == '(') {
      ++pos;
      if (pos < pat.size() && pat[pos] == '?') {
        if (pos + 1 < pat.size() && pat[pos + 1] == ':') {
          pos += 2;
        } else {
          aborted = true;  // flags or lookaround
          return {};
        }
      }
      FastRegexAstNode inner = ParseAlt();
      if (aborted || pos >= pat.size() || pat[pos] != ')') {
        aborted = true;
        return {};
      }
      ++pos;
      return inner;
    }
    if (c == '[') {
      ++pos;
      bool negated = false;
      if (pos < pat.size() && pat[pos] == '^') {
        negated = true;
        ++pos;
      }
      std::array<uint64_t, 2> cs{};
      bool first = true;
      while (pos < pat.size() && (pat[pos] != ']' || first)) {
        first = false;
        const auto uc = static_cast<unsigned char>(pat[pos]);
        if (uc >= 128U) {
          aborted = true;
          return {};
        }
        if (pat[pos] == '[' && pos + 1 < pat.size() &&
            (pat[pos + 1] == ':' || pat[pos + 1] == '.' ||
             pat[pos + 1] == '=')) {
          aborted = true;
          return {};
        }
        bool single_char = false;
        uint8_t ch_val = 0;
        if (pat[pos] == '\\') {
          ++pos;
          if (pos >= pat.size()) {
            aborted = true;
            return {};
          }
          const char esc = pat[pos++];
          FastRegexAstNode tmp;
          if (!ParseEscapeAtom(esc, &tmp)) {
            aborted = true;
            return {};
          }
          if (tmp.kind == FastRegexAstNode::Kind::kLiteral) {
            single_char = true;
            ch_val = static_cast<uint8_t>(tmp.literal[0]);
          } else {
            cs[0] |= tmp.charset[0];
            cs[1] |= tmp.charset[1];
          }
        } else {
          single_char = true;
          ch_val = static_cast<uint8_t>(pat[pos++]);
        }
        if (single_char) {
          if (pos + 1 < pat.size() && pat[pos] == '-' && pat[pos + 1] != ']') {
            ++pos;  // consume '-'
            uint8_t end_val = 0;
            if (pat[pos] == '\\') {
              ++pos;
              if (pos >= pat.size()) {
                aborted = true;
                return {};
              }
              FastRegexAstNode tmp;
              if (!ParseEscapeAtom(pat[pos++], &tmp) ||
                  tmp.kind != FastRegexAstNode::Kind::kLiteral) {
                aborted = true;
                return {};
              }
              end_val = static_cast<uint8_t>(tmp.literal[0]);
            } else {
              const auto end_uc =
                  static_cast<unsigned char>(pat[pos++]);
              if (end_uc >= 128U) {
                aborted = true;
                return {};
              }
              end_val = end_uc;
            }
            if (ch_val > end_val) {
              aborted = true;
              return {};
            }
            SetAsciiRange(cs, ch_val, end_val);
          } else {
            SetAsciiBit(cs, ch_val);
          }
        }
      }
      if (pos >= pat.size() || pat[pos] != ']') {
        aborted = true;
        return {};
      }
      ++pos;
      if (negated) {
        cs[0] = ~cs[0];
        cs[1] = ~cs[1];
      }
      FastRegexAstNode node;
      node.kind = FastRegexAstNode::Kind::kCharSet;
      node.charset = cs;
      return node;
    }
    if (c == '.') {
      ++pos;
      FastRegexAstNode node;
      node.kind = FastRegexAstNode::Kind::kCharSet;
      node.charset[0] = ~((uint64_t{1} << '\n') | (uint64_t{1} << '\r'));
      node.charset[1] = ~uint64_t{0};
      return node;
    }
    if (c == '^' || c == '$') {
      ++pos;
      FastRegexAstNode anchor;
      anchor.kind = FastRegexAstNode::Kind::kAnchor;
      return anchor;
    }
    if (c == '\\') {
      ++pos;
      if (pos >= pat.size()) {
        aborted = true;
        return {};
      }
      const char esc = pat[pos++];
      FastRegexAstNode node;
      if (!ParseEscapeAtom(esc, &node)) {
        aborted = true;
        return {};
      }
      return node;
    }
    if (c == '*' || c == '+' || c == '?' || c == '{' || c == '}' || c == ']') {
      aborted = true;
      return {};
    }
    ++pos;
    FastRegexAstNode node;
    node.kind = FastRegexAstNode::Kind::kLiteral;
    node.literal.assign(1, c);
    return node;
  }
};

std::vector<std::string> ExtractMandatoryLiterals(
    const FastRegexAstNode& node) {
  switch (node.kind) {
    case FastRegexAstNode::Kind::kEmpty:
    case FastRegexAstNode::Kind::kAnchor:
    case FastRegexAstNode::Kind::kCharSet:
      return {};
    case FastRegexAstNode::Kind::kLiteral:
      return node.literal.empty() ? std::vector<std::string>{}
                                  : std::vector<std::string>{node.literal};
    case FastRegexAstNode::Kind::kConcat: {
      std::vector<std::string> res;
      for (const FastRegexAstNode& ch : node.children) {
        auto sub = ExtractMandatoryLiterals(ch);
        res.insert(res.end(), std::make_move_iterator(sub.begin()),
                   std::make_move_iterator(sub.end()));
      }
      return res;
    }
    case FastRegexAstNode::Kind::kAlt: {
      if (node.children.empty()) {
        return {};
      }
      std::vector<std::string> res = ExtractMandatoryLiterals(node.children[0]);
      for (size_t i = 1; i < node.children.size() && !res.empty(); ++i) {
        const std::vector<std::string> rhs =
            ExtractMandatoryLiterals(node.children[i]);
        std::erase_if(res, [&](const std::string& lit) {
          return std::ranges::find(rhs, lit) == rhs.end();
        });
      }
      return res;
    }
    case FastRegexAstNode::Kind::kRepeat:
      if (node.min_rep >= 1 && !node.children.empty()) {
        return ExtractMandatoryLiterals(node.children[0]);
      }
      return {};
  }
  return {};
}

struct BitsetThompsonNfa {
  static constexpr size_t kMaxStates = 64;
  uint64_t start_mask{0};
  uint64_t accept_mask{0};
  bool start_can_accept{false};
  std::array<uint64_t, 128> byte_consume_mask{};
  std::array<uint64_t, kMaxStates> after_consume{};

  [[nodiscard]] bool ContainsAscii(std::string_view input) const noexcept {
    if (start_can_accept) {
      return true;
    }
    uint64_t active = start_mask;
    for (const char ch : input) {
      const auto b = static_cast<uint8_t>(ch);
      uint64_t matched = active & byte_consume_mask[b];
      // Inject start_mask at every byte boundary (`ThompsonSearch.lean`).
      uint64_t next = start_mask;
      while (matched != 0) {
        const auto s = static_cast<unsigned>(std::countr_zero(matched));
        next |= after_consume[s];
        matched &= (matched - 1);
      }
      if ((next & accept_mask) != 0) {
        return true;
      }
      active = next;
    }
    return false;
  }
};

struct NfaCompiler {
  size_t num_states{0};
  std::array<uint64_t, 64> eps_adj{};
  std::array<uint8_t, 64> consume_target{};
  std::array<uint64_t, 128> byte_consume_mask{};

  struct Frag {
    uint8_t entry{0};
    uint8_t exit{0};
    bool ok{false};
  };

  std::optional<uint8_t> NewState() noexcept {
    if (num_states >= 64) {
      return std::nullopt;
    }
    return static_cast<uint8_t>(num_states++);
  }

  Frag Compile(const FastRegexAstNode& node) {
    switch (node.kind) {
      case FastRegexAstNode::Kind::kAnchor:
        return {};  // anchors cannot be checked unanchored
      case FastRegexAstNode::Kind::kEmpty: {
        auto s = NewState();
        if (!s) { return {};
}
        return {.entry = *s, .exit = *s, .ok = true};
      }
      case FastRegexAstNode::Kind::kLiteral: {
        if (node.literal.empty()) {
          auto s = NewState();
          if (!s) { return {};
}
          return {.entry = *s, .exit = *s, .ok = true};
        }
        auto first = NewState();
        if (!first) { return {};
}
        uint8_t cur = *first;
        for (const char ch : node.literal) {
          auto next = NewState();
          if (!next) { return {};
}
          byte_consume_mask[static_cast<uint8_t>(ch)] |= uint64_t{1} << cur;
          consume_target[cur] = *next;
          cur = *next;
        }
        return {.entry = *first, .exit = cur, .ok = true};
      }
      case FastRegexAstNode::Kind::kCharSet: {
        auto u = NewState();
        auto v = NewState();
        if (!u || !v) { return {};
}
        const uint64_t bit = uint64_t{1} << *u;
        for (size_t b = 0; b < 64; ++b) {
          if ((node.charset[0] & (uint64_t{1} << b)) != 0) {
            byte_consume_mask[b] |= bit;
          }
          if ((node.charset[1] & (uint64_t{1} << b)) != 0) {
            byte_consume_mask[b + 64] |= bit;
          }
        }
        consume_target[*u] = *v;
        return {.entry = *u, .exit = *v, .ok = true};
      }
      case FastRegexAstNode::Kind::kConcat: {
        if (node.children.empty()) {
          auto s = NewState();
          if (!s) { return {};
}
          return {.entry = *s, .exit = *s, .ok = true};
        }
        Frag acc = Compile(node.children[0]);
        if (!acc.ok) { return {};
}
        for (size_t i = 1; i < node.children.size(); ++i) {
          Frag rhs = Compile(node.children[i]);
          if (!rhs.ok) { return {};
}
          eps_adj[acc.exit] |= uint64_t{1} << rhs.entry;
          acc.exit = rhs.exit;
        }
        return acc;
      }
      case FastRegexAstNode::Kind::kAlt: {
        auto entry = NewState();
        auto exit = NewState();
        if (!entry || !exit) { return {};
}
        for (const FastRegexAstNode& ch : node.children) {
          Frag sub = Compile(ch);
          if (!sub.ok) { return {};
}
          eps_adj[*entry] |= uint64_t{1} << sub.entry;
          eps_adj[sub.exit] |= uint64_t{1} << *exit;
        }
        return {.entry = *entry, .exit = *exit, .ok = true};
      }
      case FastRegexAstNode::Kind::kRepeat: {
        if (node.children.empty()) {
          return {};
        }
        const FastRegexAstNode& child = node.children[0];
        if (node.min_rep == 0 && node.max_rep == -1) {
          // Star: child*
          auto entry = NewState();
          auto exit = NewState();
          if (!entry || !exit) { return {};
}
          Frag sub = Compile(child);
          if (!sub.ok) { return {};
}
          eps_adj[*entry] |=
              (uint64_t{1} << sub.entry) | (uint64_t{1} << *exit);
          eps_adj[sub.exit] |=
              (uint64_t{1} << sub.entry) | (uint64_t{1} << *exit);
          return {.entry = *entry, .exit = *exit, .ok = true};
        }
        if (node.min_rep == 1 && node.max_rep == -1) {
          // Plus: child+
          auto exit = NewState();
          if (!exit) { return {};
}
          Frag sub = Compile(child);
          if (!sub.ok) { return {};
}
          eps_adj[sub.exit] |=
              (uint64_t{1} << sub.entry) | (uint64_t{1} << *exit);
          return {.entry = sub.entry, .exit = *exit, .ok = true};
        }
        if (node.min_rep == 0 && node.max_rep == 1) {
          // Opt: child?
          auto entry = NewState();
          auto exit = NewState();
          if (!entry || !exit) { return {};
}
          Frag sub = Compile(child);
          if (!sub.ok) { return {};
}
          eps_adj[*entry] |=
              (uint64_t{1} << sub.entry) | (uint64_t{1} << *exit);
          eps_adj[sub.exit] |= uint64_t{1} << *exit;
          return {.entry = *entry, .exit = *exit, .ok = true};
        }
        // Bounded {m, n} / {m,}: expand up to a modest count while < 64 states.
        const int bound = (node.max_rep >= 0) ? node.max_rep : node.min_rep + 1;
        if (bound > 8) {
          return {};
        }
        auto start = NewState();
        if (!start) { return {};
}
        Frag cur{.entry = *start, .exit = *start, .ok = true};
        for (int i = 0; i < node.min_rep; ++i) {
          Frag copy = Compile(child);
          if (!copy.ok) { return {};
}
          eps_adj[cur.exit] |= uint64_t{1} << copy.entry;
          cur.exit = copy.exit;
        }
        if (node.max_rep < 0) {
          // Remaining is child*
          auto star_entry = NewState();
          auto star_exit = NewState();
          if (!star_entry || !star_exit) { return {};
}
          Frag sub = Compile(child);
          if (!sub.ok) { return {};
}
          eps_adj[*star_entry] |=
              (uint64_t{1} << sub.entry) | (uint64_t{1} << *star_exit);
          eps_adj[sub.exit] |=
              (uint64_t{1} << sub.entry) | (uint64_t{1} << *star_exit);
          eps_adj[cur.exit] |= uint64_t{1} << *star_entry;
          cur.exit = *star_exit;
          return cur;
        }
        for (int i = node.min_rep; i < node.max_rep; ++i) {
          auto opt_entry = NewState();
          auto opt_exit = NewState();
          if (!opt_entry || !opt_exit) { return {};
}
          Frag sub = Compile(child);
          if (!sub.ok) { return {};
}
          eps_adj[*opt_entry] |=
              (uint64_t{1} << sub.entry) | (uint64_t{1} << *opt_exit);
          eps_adj[sub.exit] |= uint64_t{1} << *opt_exit;
          eps_adj[cur.exit] |= uint64_t{1} << *opt_entry;
          cur.exit = *opt_exit;
        }
        return cur;
      }
    }
    return {};
  }

  std::optional<BitsetThompsonNfa> Build(const FastRegexAstNode& root) {
    Frag frag = Compile(root);
    if (!frag.ok) {
      return std::nullopt;
    }
    std::array<uint64_t, 64> closure{};
    for (size_t s = 0; s < num_states; ++s) {
      uint64_t reached = uint64_t{1} << s;
      uint64_t frontier = reached;
      while (frontier != 0) {
        const auto u = static_cast<unsigned>(std::countr_zero(frontier));
        frontier &= (frontier - 1);
        const uint64_t next = eps_adj[u] & ~reached;
        reached |= next;
        frontier |= next;
      }
      closure[s] = reached;
    }
    BitsetThompsonNfa nfa;
    nfa.start_mask = closure[frag.entry];
    nfa.accept_mask = uint64_t{1} << frag.exit;
    nfa.start_can_accept = (nfa.start_mask & nfa.accept_mask) != 0;
    nfa.byte_consume_mask = byte_consume_mask;
    for (size_t s = 0; s < num_states; ++s) {
      const uint8_t tgt = consume_target[s];
      if (tgt < num_states) {
        nfa.after_consume[s] = closure[tgt];
      }
    }
    return nfa;
  }
};

struct ContainsProgram {
  std::string pattern;
  std::regex regex;
  std::string mandatory_literal;
  std::optional<BitsetThompsonNfa> nfa;
  uint64_t stamp{0};
};

bool IsAllAscii(std::string_view input) noexcept {
  uint8_t acc = 0;
  for (const char ch : input) {
    acc |= static_cast<uint8_t>(ch);
  }
  return (acc & 0x80U) == 0;
}

ContainsProgram BuildContainsProgram(std::string_view pattern,
                                     std::regex compiled_regex) {
  ContainsProgram prog{
      .pattern = std::string(pattern),
      .regex = std::move(compiled_regex),
      .mandatory_literal = {},
      .nfa = std::nullopt,
      .stamp = 0,
  };
  FastRegexParser parser{.pat = pattern, .pos = 0, .aborted = false};
  FastRegexAstNode root;
  if (parser.Parse(&root)) {
    std::vector<std::string> mand = ExtractMandatoryLiterals(root);
    for (std::string& lit : mand) {
      if (lit.size() > prog.mandatory_literal.size()) {
        prog.mandatory_literal = std::move(lit);
      }
    }
    NfaCompiler compiler;
    prog.nfa = compiler.Build(root);
  }
  return prog;
}

bool ExecuteContainsProgram(const ContainsProgram& prog,
                            std::string_view input) {
  if (!prog.mandatory_literal.empty() &&
      input.find(prog.mandatory_literal) == std::string_view::npos) {
    return false;
  }
  if (prog.nfa.has_value() && IsAllAscii(input)) {
    return prog.nfa->ContainsAscii(input);
  }
  return std::regex_search(input.begin(), input.end(), prog.regex);
}

}  // namespace

StatusOr<bool> CheckedRegexContains(std::string_view input,
                                    std::string_view pattern) try {
  if (FastRegexDisabledFlag().load(std::memory_order_relaxed)) {
    ASSIGN_OR_RETURN(std::regex, re, CompileCheckedRegex(std::string(pattern)));
    return std::regex_search(input.begin(), input.end(), re);
  }

  static thread_local std::array<std::optional<ContainsProgram>, 16> cache;
  static thread_local uint64_t clock = 0;
  for (std::optional<ContainsProgram>& slot : cache) {
    if (slot.has_value() && slot->pattern == pattern) {
      slot->stamp = ++clock;
      return ExecuteContainsProgram(*slot, input);
    }
  }

  ASSIGN_OR_RETURN(std::regex, re, CompileCheckedRegex(std::string(pattern)));
  ContainsProgram built = BuildContainsProgram(pattern, std::move(re));
  if (pattern.size() <= 4096) {
    size_t victim = 0;
    for (size_t i = 0; i < cache.size(); ++i) {
      if (!cache[i].has_value()) {
        victim = i;
        break;
      }
      if (cache[i]->stamp < cache[victim]->stamp) {
        victim = i;
      }
    }
    built.stamp = ++clock;
    cache[victim] = std::move(built);
    return ExecuteContainsProgram(*cache[victim], input);
  }
  return ExecuteContainsProgram(built, input);
} catch (const std::bad_alloc&) {
  return StatusError(StatusCode::kMemoryLimitExceeded,
                     "REGEXP_CONTAINS ran out of memory");
} catch (...) {
  return StatusError(StatusCode::kRuntimeError,
                     "regular expression evaluation failed unexpectedly");
}

StatusOr<int64_t> CheckedRegexPosition(std::string_view input,
                                       std::string_view pattern,
                                       int64_t position, int64_t occurrence,
                                       int64_t return_position,
                                       bool byte_mode) try {
  if (position <= 0 || occurrence <= 0) {
    return StatusError(StatusCode::kInvalidArgument,
                       "REGEXP_INSTR position and occurrence must be positive");
  }
  if (return_position != 0 && return_position != 1) {
    return StatusError(StatusCode::kInvalidArgument,
                       "REGEXP_INSTR occurrence_position must be 0 or 1");
  }
  if (pattern.empty()) {
    return int64_t{0};
  }

  // Validate repetition on the source pattern. The wildcard expansion has
  // one disjoint, fixed-width alternative for each possible lead byte.
  ASSIGN_OR_RETURN(
      std::regex, compiled,
      byte_mode || pattern.find('.') == std::string_view::npos
          ? CompileCheckedRegex(std::string(pattern))
          : regex_guard_detail::CompileCheckedPattern<Utf8RegexPattern>(
                std::string(pattern)));
  const size_t group = compiled.mark_count();
  if (group > 1) {
    return StatusError(StatusCode::kInvalidArgument,
                       "REGEXP_INSTR supports at most one capturing group");
  }
  if (std::cmp_greater(position, input.size())) {
    return int64_t{0};
  }
  auto start = static_cast<size_t>(position - 1);
  if (!byte_mode) {
    start = 0;
    for (int64_t i = 1; i < position && start < input.size(); ++i) {
      start += Utf8SequenceLength(static_cast<unsigned char>(input[start]));
    }
    if (start >= input.size()) {
      return int64_t{0};
    }
  }

  const char* const begin = input.data();
  std::cregex_iterator current(begin + start, begin + input.size(), compiled);
  const std::cregex_iterator end;
  for (; current != end && occurrence > 1; --occurrence) {
    ++current;
  }
  if (current == end || !(*current)[group].matched) {
    return int64_t{0};
  }
  const auto& match = (*current)[group];
  const char* const boundary =
      return_position == 0 ? match.first : match.second;
  if (byte_mode) {
    return static_cast<int64_t>(boundary - begin) + 1;
  }
  return static_cast<int64_t>(std::count_if(
             begin, boundary,
             [](char ch) {
               return (static_cast<unsigned char>(ch) & 0xC0U) != 0x80U;
             })) +
         1;
} catch (const std::bad_alloc&) {
  return StatusError(StatusCode::kMemoryLimitExceeded,
                     "REGEXP_INSTR ran out of memory");
} catch (const std::regex_error&) {
  return StatusError(StatusCode::kInvalidArgument, "REGEXP_INSTR failed");
}

StatusOr<std::optional<DelimitedRegexCapture>> DescribeDelimitedRegexCapture(
    std::string_view pattern, std::string_view replacement) try {
  std::optional<ReplacementProgram> temporary;
  ASSIGN_OR_RETURN(
      const ReplacementProgram*, program,
      LookupOrCompileReplacement(pattern, replacement, &temporary));
  if (!program->prefix_vm || !program->single_capture) {
    return std::nullopt;
  }
  return program->prefix_vm->DescribeDelimitedCapture(*program->single_capture);
} catch (const std::bad_alloc&) {
  return StatusError(StatusCode::kMemoryLimitExceeded,
                     "regex capture description ran out of memory");
}

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
    RETURN_IF_FAIL(ApplyRewritePieces(program->rewrite_pieces, captures, scratch));
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
      RETURN_IF_FAIL(ApplyRewritePieces(program->rewrite_pieces, captures, scratch));
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
} catch (const std::bad_alloc&) {
  // T3722 (ADR 0012): an allocation failure is OOM, not an invalid
  // pattern/replacement; keep the SQLSTATE mapping honest.
  return StatusError(StatusCode::kMemoryLimitExceeded,
                     "regular expression replacement ran out of memory");
} catch (...) {
  return StatusError(StatusCode::kRuntimeError,
                     "regular expression replacement failed unexpectedly");
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
} catch (const std::bad_alloc&) {
  // T3722 (ADR 0012): OOM, not a caller error.
  return StatusError(StatusCode::kMemoryLimitExceeded,
                     "regular expression replacement ran out of memory");
} catch (...) {
  return StatusError(StatusCode::kRuntimeError,
                     "regular expression replacement failed unexpectedly");
}

// P464 test hook: number of full replacement-program compiles (cache misses)
// executed on this thread.  Used by the regex_guard tests to pin the
// budgeted cache behavior for >4096-byte replacement templates.
size_t ReplacementProgramFullCompilesForTest() {
  return g_replacement_full_compiles;
}

}  // namespace tinylamb
