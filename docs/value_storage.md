# Value storage

Current implementation, checked against `type/value.{hpp,cpp}` on 2026-11-11.

## In-memory model

`Value` carries a `ValueType`, a scalar/string-view union, a compact collation
or unsigned tag, and `OwnedValuePayload`. VARCHAR cells hold `std::string_view`;
they do not embed a `std::string`. The optional owner points to a reference-counted
string or immutable array payload. Copies share owned payloads; mutation detaches
shared strings. Scalar overwrite can retain an owner until reuse/destruction.
`Row` stores `std::vector<Value>`. `DataChunk` also supports typed column storage,
selection vectors and NULL metadata.

A string view may be borrowed from a scan/arena or owned through the payload.
Borrowed views must not outlive their source. Retaining a row beyond a batch or
page lifetime requires the appropriate ownership/materialization path. Never
infer ownership merely from `ValueType::kVarChar` or move an owning string out
while another live view still depends on it.

## Hot-path contract

- Fixed-width comparison/arithmetic should not allocate.
- Scans and expressions should use views while their backing storage is alive.
- Joins and aggregates should prefer typed paths and avoid constructing strings
  in inner loops; fallback semantics must still match AST evaluation.
- Mutation must invalidate cached string hashes and preserve shared owners.
- NULL carries no readable scalar union member; check its tag first.

## Persistence

Object layout, pointers and reference counts are not the disk format.
`Value::Serialize` encodes a payload under its supplied type; row serialization
adds schema/null information. ARRAY payloads contain a count, element SQL type,
and per-element null/type/payload fields. `Row::DeserializeBounded` is the
bounded entry point for untrusted serialized rows. Persisted row values and
memcomparable index keys have different encodings and are not interchangeable.

Storage compatibility changes are allowed for the current development task.
Any new encoding still needs an explicit version/reader contract, malformed-input
checks and round-trip tests; do not silently reinterpret old bytes. Native
`Value` size and internal ownership choices are not a persistence guarantee.

## Explicit INTERVAL domain (audit correction, 2026-11-11)

A VARCHAR-shaped spelling is never sufficient to infer INTERVAL semantics.
Explicit interval values use semantic tag 4, retain field-wise
(months, days, nanoseconds) comparison, and have memcomparable key tag 7 with
three signed, sign-flipped big-endian 64-bit fields. Plain VARCHAR keeps byte
identity and byte ordering even for strings such as `2-0 0 0:0:0`.

Column encoding uses bit 3 for INTERVAL, bits 4–5 for integer width
(0=64, 1=8, 2=16, 3=32), and bit 7 for unsignedness; bits 0–2 remain ValueType.
The self-describing Value and ARRAY-element type byte uses bits 0–2 for
ValueType and bits 3–7 for the semantic tag (0=none, 1=binary, 2=CI,
3=unsigned, 4=interval). Old untagged values remain readable; older readers
reject the tagged encodings. Recreate old interval tables that had only VARCHAR
metadata; their previous implicit string interpretation is no longer valid.

Schema-based row decoding restores the interval tag. Batch columns use boxed
values for intervals, and spill serialization retains semantic tags. Typed
VARCHAR scan filters and PAX builders decline interval schemas until they can
preserve that domain; ordinary row execution remains available.
