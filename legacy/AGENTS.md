# `legacy/` — archived SQL parser sources (do not build on)

See `parser/AGENTS.md`: `legacy/parser/*.cpp` (tokenizer, recursive-descent +
Pratt parsers) serve historical unit tests only. The canonical frontend is
`query/`. If a test here fails, consider migrating the test to `query/`
instead of fixing the archived code.
