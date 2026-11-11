/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#include "query/googlesql_ast.hpp"

#include <charconv>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/constants.hpp"
#include "common/status_or.hpp"

namespace tinylamb {
namespace {

bool ParseLocation(std::string& label, size_t* start, size_t* end) {
  if (label.empty() || label.back() != ']') {
    return false;
  }
  const size_t open = label.rfind(" [");
  if (open == std::string::npos) {
    return false;
  }
  const size_t dash = label.find('-', open + 2);
  if (dash == std::string::npos) {
    return false;
  }
  const char* first = label.data() + open + 2;
  const char* middle = label.data() + dash;
  const char* last = label.data() + label.size() - 1;
  size_t parsed_start = 0;
  size_t parsed_end = 0;
  const auto start_result = std::from_chars(first, middle, parsed_start);
  const auto end_result = std::from_chars(middle + 1, last, parsed_end);
  if (start_result.ec != std::errc() || start_result.ptr != middle ||
      end_result.ec != std::errc() || end_result.ptr != last ||
      parsed_end < parsed_start) {
    return false;
  }
  *start = parsed_start;
  *end = parsed_end;
  label.resize(open);
  return true;
}

std::unique_ptr<GoogleSqlAstNode> ParseNode(std::string label) {
  auto node = std::make_unique<GoogleSqlAstNode>();
  ParseLocation(label, &node->start, &node->end);
  const size_t open = label.find('(');
  if (open != std::string::npos && label.back() == ')') {
    node->kind = label.substr(0, open);
    // Attribute-style labels separate the name from the detail with a
    // space ("WithClause (recursive)"); trim so kind equality keeps working.
    while (!node->kind.empty() &&
           (node->kind.back() == ' ' || node->kind.back() == '\t')) {
      node->kind.pop_back();
    }
    node->detail = label.substr(open + 1, label.size() - open - 2);
  } else {
    node->kind = std::move(label);
  }
  return node;
}

// Literal contents may contain parentheses and text resembling a location.
// Find their SQL closing quote before interpreting the dump's suffix.
bool LabelContinues(std::string_view label) {
  const size_t first = label.find_first_not_of(' ');
  if (first == std::string_view::npos) return false;
  label.remove_prefix(first);
  if (label.starts_with("StringLiteral(") ||
      label.starts_with("StringLiteralComponent(") ||
      label.starts_with("BytesLiteral(") ||
      label.starts_with("BytesLiteralComponent(")) {
    size_t cursor = label.find('(') + 1;
    while (cursor < label.size() &&
           (label[cursor] == 'r' || label[cursor] == 'R' ||
            label[cursor] == 'b' || label[cursor] == 'B'))
      ++cursor;
    if (cursor >= label.size()) return true;
    const char quote = label[cursor];
    if (quote != '\'' && quote != '"') return false;
    const bool triple = cursor + 2 < label.size() &&
                        label[cursor + 1] == quote &&
                        label[cursor + 2] == quote;
    const size_t quote_size = triple ? 3 : 1;
    cursor += quote_size;
    for (; cursor < label.size(); ++cursor) {
      if (label[cursor] == '\\') {
        ++cursor;
      } else if (label[cursor] == quote &&
                 (!triple ||
                  (cursor + 2 < label.size() && label[cursor + 1] == quote &&
                   label[cursor + 2] == quote))) {
        return false;
      }
    }
    return true;
  }
  size_t open = 0;
  size_t close = 0;
  for (const char c : label) {
    if (c == '(')
      ++open;
    else if (c == ')')
      ++close;
  }
  return open > close;
}

}  // namespace

const GoogleSqlAstNode* GoogleSqlAstNode::Child(std::string_view child_kind,
                                                size_t occurrence) const {
  for (const auto& child : children) {
    if (child->kind == child_kind && occurrence-- == 0) {
      return child.get();
    }
  }
  return nullptr;
}

std::vector<const GoogleSqlAstNode*> GoogleSqlAstNode::Children(
    std::string_view child_kind) const {
  std::vector<const GoogleSqlAstNode*> result;
  for (const auto& child : children) {
    if (child->kind == child_kind) {
      result.push_back(child.get());
    }
  }
  return result;
}

StatusOr<std::unique_ptr<GoogleSqlAstNode>> GoogleSqlAstParser::Parse(
    std::string_view dump) {
  std::unique_ptr<GoogleSqlAstNode> root;
  std::vector<GoogleSqlAstNode*> parents;
  size_t previous_depth = 0;
  size_t cursor = 0;
  while (cursor < dump.size()) {
    size_t line_end = dump.find('\n', cursor);
    if (line_end == std::string_view::npos) {
      line_end = dump.size();
    }
    std::string full_line = std::string(dump.substr(cursor, line_end - cursor));
    if (!full_line.empty() && full_line.back() == '\r') {
      full_line.pop_back();
    }
    cursor = line_end + 1;
    if (full_line.empty()) {
      continue;
    }

    while (LabelContinues(full_line) && cursor < dump.size()) {
      size_t next_line_end = dump.find('\n', cursor);
      if (next_line_end == std::string_view::npos) {
        next_line_end = dump.size();
      }
      std::string_view next_line = dump.substr(cursor, next_line_end - cursor);
      if (!next_line.empty() && next_line.back() == '\r') {
        next_line.remove_suffix(1);
      }
      cursor = next_line_end + 1;
      full_line.push_back('\n');
      full_line.append(next_line);
    }

    size_t spaces = 0;
    while (spaces < full_line.size() && full_line[spaces] == ' ') {
      ++spaces;
    }
    if (spaces % 2 != 0 || spaces == full_line.size()) {
      return Status::kUnknown;
    }
    const size_t depth = spaces / 2;
    if (depth >= kMaxGoogleSqlAstDepth) {
      return StatusError(StatusCode::kInvalidArgument,
                         "GoogleSQL AST depth limit exceeded");
    }
    auto node = ParseNode(full_line.substr(spaces));
    GoogleSqlAstNode* raw = node.get();
    if (depth == 0) {
      if (root) {
        return Status::kUnknown;
      }
      root = std::move(node);
    } else {
      // A child may descend only one level per line.  Attaching at
      // parents[depth - 1] without that bound lets a malformed jump (e.g.
      // depths 0,1,2,1,3) graft the node onto a stale ancestor two levels
      // up, silently producing a corrupted tree instead of kUnknown.
      if (depth > parents.size() || depth > previous_depth + 1) {
        return Status::kUnknown;
      }
      parents[depth - 1]->children.push_back(std::move(node));
    }
    if (parents.size() <= depth) {
      parents.resize(depth + 1);
    }
    parents[depth] = raw;
    parents.resize(depth + 1);
    previous_depth = depth;
  }
  if (!root) {
    return Status::kUnknown;
  }
  return root;
}

}  // namespace tinylamb
