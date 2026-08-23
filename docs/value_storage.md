# Value storage (mid-term design)

## Current model

`Value` is a tagged struct: a `ValueType` discriminator plus a `union` for
scalar payloads and a `std::string` member for `kVarChar`. Rows store
`std::vector<Value>` inline in heap-allocated `Row` objects.

## Hot-path contract

While this layout remains in place:

1. **No extra allocations on comparison or arithmetic** for fixed-width types
   (`kInt64`, `kDouble`, `kDate`, null). String compares use the existing
   `std::string` buffer only.
2. **New executor / scan code must not copy strings** when a `string_view` or
   column reference suffices.
3. **Aggregates and joins** should prefer integer/double paths; avoid building
   intermediate `Value` strings in inner loops.

## Mid-term direction

| Area | Today | Target |
|------|-------|--------|
| Scalars | `union` in `Value` | `std::variant`-style layout or separate column arrays |
| Strings | Owned in `Value` | Arena or row-page slot storage; `Value` holds offset/length |
| NULL | `ValueType::kNull` | Separate null bitmap per column batch |

Migration will be incremental: columnar batches (`DataChunk`) can adopt the new
representation first; row-oriented APIs keep a compatibility `Value` view.

## Non-goals (for now)

- Changing on-disk row encoding without a format version bump.
- Eager migration of every expression evaluator path to variant.
