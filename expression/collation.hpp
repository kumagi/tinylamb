/** Copyright 2026 KUMAZAKI Hiroki. Licensed under Apache-2.0. */
#ifndef TINYLAMB_EXPRESSION_COLLATION_HPP
#define TINYLAMB_EXPRESSION_COLLATION_HPP

#include <optional>
#include <vector>

#include "expression/expression.hpp"
#include "type/collation.hpp"

namespace tinylamb {

// The collation an expression *statically* asks for: `COLLATE(x, 'und:ci')`
// (or any other non-'binary' spec) is case-insensitive and
// `COLLATE(x, 'binary')` is binary.
//
// Every other shape is kNone, including a COLLATE whose collator is not a
// string literal (GoogleSQL rejects that at the frontend, but a hand-built
// expression can still reach here) and a COLLATE nested under an operator.
// kNone means "the engine default", so a caller that needs the resolved
// collation of a *value* must use CollationOf(value) instead: the tag on the
// evaluated value is what actually governs the comparison.
[[nodiscard]] Collation CollationOfExpression(const Expression& expression);

// A COLLATE clause over a SELECT-list ordinal: `ORDER BY 1 COLLATE 'und:ci'`
// parses into COLLATE(<integer>, 'spec'), and the ordinal is only resolved
// once the SELECT list is expanded. The two halves therefore have to travel
// together until then — resolving the ordinal alone would leave a constant sort
// key that silently orders nothing.
struct CollatedOrdinal {
  int64_t ordinal{0};
  std::string spec;
};

[[nodiscard]] std::optional<CollatedOrdinal> AsCollatedOrdinal(
    const Expression& key);

// Wraps `expression` in the COLLATE(expression, 'spec') call a collator spec
// means. The same spelling COLLATE() itself resolves, so the static and the
// evaluated answer agree.
[[nodiscard]] Expression ApplyCollationSpec(Expression expression,
                                            std::string_view spec);

// Re-applies the collator of an already-parsed key to a substituted
// expression. `ORDER BY 1 COLLATE 'und:ci'` collates the SELECT-list item the
// ordinal names, and the substitution happens after the COLLATE clause was
// parsed, so the wrapper has to move with the operand. A key that carries no
// collator is returned unchanged.
[[nodiscard]] Expression ReapplyCollation(Expression resolved,
                                          const Expression& parsed_key);

// The collation governing an ORDER BY term list. A sort compares its keys
// left to right and any key may carry the collator, so the whole ordering is
// case-insensitive as soon as one term asks for it.
[[nodiscard]] Collation CollationOfOrdering(
    const std::vector<Expression>& keys);

}  // namespace tinylamb

#endif  // TINYLAMB_EXPRESSION_COLLATION_HPP
