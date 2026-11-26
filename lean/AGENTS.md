# Working on Lean proofs

- Write Lean module documentation, declaration docstrings, and proof comments
  in English. Put a module doc comment immediately after the imports in each
  new or updated Lean file. For a proof, state the claim, its relevant
  assumptions, and what it allows SQL execution to change while preserving the
  modeled result. For a helper, aggregation module, or audit, explain its role
  and the SQL behavior it supports. Keep existing Japanese README prose in
  Japanese.
- Start with [the reusable lemma index](README.md#既存の補題を探す) and search
  `TinylambOpt/` for lemmas about the same model before proving a new helper.
- Share lemmas through a module for the model they depend on. The proofs of
  individual goals can import that module directly.
- Run `lake build` from this directory and use `scripts/verify_lean_goals.sh`
  to check registered proofs and counterexamples.
