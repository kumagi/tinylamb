import Std.Tactic

/-! Lazy evaluation of dictionary predicates: evaluate a predicate only on
    the dictionary values actually referenced by selected rows, cache the
    result per key, and stop at the same first error as sequential evaluation.

    Reference computation: `evalSeq` evaluates `pred gen (dict code)` for
    every selected `(row, code)` in physical row order, emits the results
    until the first `fail`, and reports that row as the first error.
    Optimized computation: `evalLazy` looks up the cache key `(generation,
    code)` first; on a miss it evaluates the predicate and installs the
    result, on a hit it reuses the cached value. `NULL` is represented by
    `Option` in the dictionary and generations are part of the cache key, so
    neither can be confused with a cached boolean.

    Hypotheses realised by the caller (see
    `executor/detail/typed_stream_aggregate_dict.cpp`):
      * the predicate depends only on the dictionary value and generation and
        is deterministic (`pred` is a pure function);
      * the cache is initially sound: every cached entry equals the
        predicate's result (`CacheSound`);
      * a cache entry is keyed by `(generation, code)`, so a new dictionary
        generation cannot reuse a stale result.

    `evalOne_twice_is_hit` shows a repeated reference is served from the
    cache; `evalLazy_cache_sound` keeps every installed entry equal to the
    predicate result; `lazy_dictionary_sound` proves identical output and
    identical first-error row. `evalSeq_ignores_unused_bad_code` is the CAST
    boundary example: a malformed dictionary entry that no selected row
    references never produces an error. -/

namespace TinylambOpt.LazyDictionary

/-- Result of applying a predicate to a dictionary value. `null` is a value,
    `fail` carries an error code (e.g. a failed CAST). -/
inductive PredResult where
  | bit (b : Bool)
  | null
  | fail (code : Nat)

/-- A cache maps `(generation, dictionary code)` to a result; `none` is a
    miss. -/
abbrev Cache := (Nat × Nat) → Option PredResult

def CacheSound (gen : Nat) (dict : Nat → Option α)
    (pred : Nat → Option α → PredResult) (cache : Cache) : Prop :=
  ∀ code v, cache (gen, code) = some v → v = pred gen (dict code)

def updateCache (cache : Cache) (key : Nat × Nat) (v : PredResult) : Cache :=
  fun k => if k = key then some v else cache k

/-- One cache probe: evaluate only when the key is absent. -/
def evalOne (gen : Nat) (dict : Nat → Option α)
    (pred : Nat → Option α → PredResult) (cache : Cache) (code : Nat) :
    PredResult × Cache :=
  match cache (gen, code) with
  | some v => (v, cache)
  | none => (pred gen (dict code), updateCache cache (gen, code) (pred gen (dict code)))

theorem evalOne_value (gen : Nat) (dict : Nat → Option α)
    (pred : Nat → Option α → PredResult) (cache : Cache)
    (hc : CacheSound gen dict pred cache) (code : Nat) :
    (evalOne gen dict pred cache code).1 = pred gen (dict code) := by
  dsimp [evalOne]
  split
  next v h => exact hc code v h
  next _ => rfl

theorem evalOne_sound (gen : Nat) (dict : Nat → Option α)
    (pred : Nat → Option α → PredResult) (cache : Cache)
    (hc : CacheSound gen dict pred cache) (code : Nat) :
    CacheSound gen dict pred (evalOne gen dict pred cache code).2 := by
  dsimp [evalOne]
  split
  next v h => exact hc
  next h =>
    intro code' w hw
    by_cases same : code' = code
    · subst code'
      simp [updateCache] at hw
      simp [hw]
    · have hw' : cache (gen, code') = some w := by
        simpa [updateCache, same] using hw
      exact hc code' w hw'

/-- Sequential reference evaluator over selected `(row, code)` pairs. -/
def evalSeq (gen : Nat) (dict : Nat → Option α)
    (pred : Nat → Option α → PredResult) :
    List (Nat × Nat) → List (Nat × PredResult) × Option Nat
  | [] => ([], none)
  | (row, code) :: rest =>
      match pred gen (dict code) with
      | PredResult.fail _ => ([], some row)
      | v => let (out, err) := evalSeq gen dict pred rest
             ((row, v) :: out, err)

/-- Optimized evaluator with caching; returns output, first error row and
    the final cache. -/
def evalLazy (gen : Nat) (dict : Nat → Option α)
    (pred : Nat → Option α → PredResult) (cache : Cache) :
    List (Nat × Nat) → List (Nat × PredResult) × Option Nat × Cache
  | [] => ([], none, cache)
  | (row, code) :: rest =>
      let (v, cache') := evalOne gen dict pred cache code
      match v with
      | PredResult.fail _ => ([], some row, cache')
      | _ =>
          let (out, err, cache'') := evalLazy gen dict pred cache' rest
          ((row, v) :: out, err, cache'')

/-- A second reference to the same key is a cache hit: the predicate is not
    evaluated again. -/
theorem evalOne_twice_is_hit (gen : Nat) (dict : Nat → Option α)
    (pred : Nat → Option α → PredResult) (cache : Cache)
    (hc : CacheSound gen dict pred cache) (code : Nat) :
    (evalOne gen dict pred cache code).2 (gen, code) =
      some (pred gen (dict code)) := by
  dsimp [evalOne]
  split
  next v h => simpa [h] using congrArg some (hc code v h)
  next h => simp [updateCache]

theorem evalLazy_cache_sound (gen : Nat) (dict : Nat → Option α)
    (pred : Nat → Option α → PredResult) (cache : Cache)
    (hc : CacheSound gen dict pred cache) (rows : List (Nat × Nat)) :
    CacheSound gen dict pred (evalLazy gen dict pred cache rows).2.2 := by
  induction rows generalizing cache with
  | nil => simpa [evalLazy] using hc
  | cons pair rest ih =>
      rcases pair with ⟨row, code⟩
      dsimp [evalLazy]
      cases hres : (evalOne gen dict pred cache code).1 with
      | fail e => exact evalOne_sound gen dict pred cache hc code
      | bit b =>
          apply ih
          exact evalOne_sound gen dict pred cache hc code
      | null =>
          apply ih
          exact evalOne_sound gen dict pred cache hc code

theorem lazy_dictionary_sound (gen : Nat) (dict : Nat → Option α)
    (pred : Nat → Option α → PredResult) (cache : Cache)
    (hc : CacheSound gen dict pred cache) (rows : List (Nat × Nat)) :
    let (outL, errL, _) := evalLazy gen dict pred cache rows
    let (outR, errR) := evalSeq gen dict pred rows
    outL = outR ∧ errL = errR := by
  induction rows generalizing cache with
  | nil => simp [evalLazy, evalSeq]
  | cons pair rest ih =>
      rcases pair with ⟨row, code⟩
      dsimp [evalLazy, evalSeq]
      have hv := evalOne_value gen dict pred cache hc code
      rw [← hv]
      cases hres : (evalOne gen dict pred cache code).1 with
      | fail e => simp
      | bit b =>
          simp
          exact ih (evalOne gen dict pred cache code).2 (evalOne_sound gen dict pred cache hc code)
      | null =>
          simp
          exact ih (evalOne gen dict pred cache code).2 (evalOne_sound gen dict pred cache hc code)

/-- A malformed dictionary item causes no error unless a selected row
    actually references it (the "unused bad numeric string" boundary case). -/
theorem evalSeq_ignores_unused_bad_code (gen : Nat) (dict : Nat → Option α)
    (pred : Nat → Option α → PredResult) (rows : List (Nat × Nat))
    (badCode : Nat)
    (hfailsOnly : ∀ code e, pred gen (dict code) = PredResult.fail e → code = badCode)
    (hunused : ∀ pair ∈ rows, pair.2 ≠ badCode) :
    (evalSeq gen dict pred rows).2 = none := by
  induction rows with
  | nil => simp [evalSeq]
  | cons pair rest ih =>
      rcases pair with ⟨row, code⟩
      have hne : code ≠ badCode := hunused (row, code) (by simp)
      have hrest : ∀ p ∈ rest, p.2 ≠ badCode := by
        intro p hp; exact hunused p (by simp [hp])
      by_cases hf : ∃ e, pred gen (dict code) = PredResult.fail e
      · rcases hf with ⟨e, he⟩
        exact False.elim (hne (hfailsOnly code e he))
      · have hnf : ∀ e, pred gen (dict code) ≠ PredResult.fail e := by
          intro e he; exact hf ⟨e, he⟩
        cases hres : pred gen (dict code) with
        | fail e => exact False.elim (hnf e hres)
        | bit b => simpa [evalSeq, hres] using ih hrest
        | null => simpa [evalSeq, hres] using ih hrest

/-- The lazy evaluator has the same "no error for unreferenced bad value"
    behaviour as the sequential reference. -/
theorem lazy_dictionary_ignores_unused_bad_code (gen : Nat) (dict : Nat → Option α)
    (pred : Nat → Option α → PredResult) (cache : Cache)
    (hc : CacheSound gen dict pred cache) (rows : List (Nat × Nat))
    (badCode : Nat)
    (hfailsOnly : ∀ code e, pred gen (dict code) = PredResult.fail e → code = badCode)
    (hunused : ∀ pair ∈ rows, pair.2 ≠ badCode) :
    (evalLazy gen dict pred cache rows).2.1 = none := by
  have hs := lazy_dictionary_sound gen dict pred cache hc rows
  have hr := evalSeq_ignores_unused_bad_code gen dict pred rows badCode hfailsOnly hunused
  dsimp at hs
  exact hs.2.trans hr

end TinylambOpt.LazyDictionary
