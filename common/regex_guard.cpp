/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "common/regex_guard.hpp"

#include <iterator>
#include <memory>
#include <utility>
#ifdef TINYLAMB_HAVE_RE2
#include <re2/re2.h>
#endif

namespace tinylamb {
namespace {
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
      if ((escaped >= 'a' && escaped <= 'z') ||
          (escaped >= 'A' && escaped <= 'Z') ||
          (escaped >= '0' && escaped <= '9')) {
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
#endif

struct ReplacementProgram {
  std::regex compiled;
  std::string format;
#ifdef TINYLAMB_HAVE_RE2
  std::unique_ptr<RE2> linear;
#endif
};

StatusOr<ReplacementProgram> CompileReplacement(std::string_view pattern,
                                                std::string_view replacement) {
  ASSIGN_OR_RETURN(std::regex, compiled,
                   CompileCheckedRegex(std::string(pattern)));
  std::string format;
  format.reserve(replacement.size());
  for (size_t i = 0; i < replacement.size(); ++i) {
    const char c = replacement[i];
    if (c == '$') {
      format += "$$";
    } else if (c != '\\') {
      format += c;
    } else {
      if (++i == replacement.size()) {
        return StatusError(
            StatusCode::kInvalidArgument,
            "trailing backslash in regular expression replacement");
      }
      const char escaped = replacement[i];
      if (escaped == '\\') {
        format += '\\';
      } else if (escaped >= '0' && escaped <= '9' &&
                 std::cmp_less_equal(escaped - '0', compiled.mark_count())) {
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
  ReplacementProgram program{
      .compiled = std::move(compiled),
      .format = std::move(format),
#ifdef TINYLAMB_HAVE_RE2
      .linear = nullptr,
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
      program.linear = std::move(linear);
    }
  }
#endif
  return program;
}
}  // namespace

StatusOr<std::string> ReplaceCheckedRegex(std::string_view input,
                                          std::string_view pattern,
                                          std::string_view replacement) try {
  struct Entry {
    std::string pattern;
    std::string replacement;
    ReplacementProgram program;
  };
  static thread_local std::array<std::optional<Entry>, 8> cache;
  static thread_local size_t next = 0;
  const ReplacementProgram* program = nullptr;
  std::optional<ReplacementProgram> temporary;
  for (const auto& entry : cache) {
    if (entry && entry->pattern == pattern &&
        entry->replacement == replacement) {
      program = &entry->program;
      break;
    }
  }
  if (program == nullptr) {
    ASSIGN_OR_RETURN(ReplacementProgram, compiled,
                     CompileReplacement(pattern, replacement));
    if (pattern.size() <= 4096 && replacement.size() <= 4096) {
      auto& entry =
          cache[next].emplace(Entry{.pattern = std::string(pattern),
                                    .replacement = std::string(replacement),
                                    .program = std::move(compiled)});
      program = &entry.program;
      next = (next + 1) % cache.size();
    } else {
      program = &temporary.emplace(std::move(compiled));
    }
  }
#ifdef TINYLAMB_HAVE_RE2
  if (program->linear) {
    std::string result(input);
    RE2::GlobalReplace(&result, *program->linear, replacement);
    return result;
  }
#endif
  // Default-constructed empty views may have a null pointer. Give the regex
  // iterator implementation a valid empty range without copying the input.
  if (input.empty()) {
    input = "";
  }
  std::string result;
  std::regex_replace(std::back_inserter(result), input.begin(), input.end(),
                     program->compiled, program->format);
  return result;
} catch (...) {
  return StatusError(StatusCode::kInvalidArgument,
                     "regular expression replacement failed");
}

}  // namespace tinylamb
