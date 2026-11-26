import TinylambOpt.ResearchGoals.EngineDecisions.MVCC
import TinylambOpt.ResearchGoals.ExecutionStrategies.VersionedIndexes

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Proved

- `OverlayCache` — A batch of workers can reuse a transaction-private lookup cache over
  a fixed snapshot by patching each hot key once with the *last* staged edit.
  Negative cache hits are patched as well as positive ones.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.EngineDecisions.OverlayCache

open TinylambOpt.ResearchGoals.EngineDecisions.MVCC.OverlayCache

theorem lookup_filterMap_fun {α β : Type} [BEq α] [LawfulBEq α]
    (l : List α) (g : α → Option β) (a : α) :
    (l.filterMap (fun k => (g k).map (k, ·))).lookup a = (if a ∈ l then g a else none) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hax : a = x
      · subst x
        cases hg : g a <;> simp [List.lookup_cons, hg, ih]
      · have hmem : (a ∈ x :: xs) ↔ (a ∈ xs) := by simp [hax]
        have hbeq : (a == x) = false := by simpa [hax]
        cases hgx : g x <;> simp [List.lookup_cons, hgx, hbeq, hmem, ih]

theorem patch_lookup (patch : List Edit) (a : Nat) :
    ((patch.map Prod.fst).filterMap (fun k => (patch.lookup k).map (k, ·))).lookup a = patch.lookup a := by
  rw [lookup_filterMap_fun]
  by_cases h : a ∈ patch.map Prod.fst
  · simp [h]
  · have hnone : patch.lookup a = none := by
      cases hl : patch.lookup a with
      | none => rfl
      | some v =>
          rw [List.lookup_eq_some_iff] at hl
          obtain ⟨pre, suf, hpatch, _⟩ := hl
          exact False.elim (h (by rw [hpatch]; simp))
    simp [h, hnone]

theorem lookup_filterMap_eraseDups {α β : Type} [BEq α] [LawfulBEq α]
    (l : List α) (g : α → Option β) (a : α) :
    ((l.eraseDups).filterMap (fun k => (g k).map (k, ·))).lookup a =
    (l.filterMap (fun k => (g k).map (k, ·))).lookup a := by
  rw [lookup_filterMap_fun, lookup_filterMap_fun]
  simp [List.mem_eraseDups]

theorem compact_lookup (patch : List Edit) (a : Nat) :
    ((patch.map Prod.fst).eraseDups.filterMap fun k => (patch.lookup k).map (k, ·)).lookup a = patch.lookup a := by
  calc
    ((patch.map Prod.fst).eraseDups.filterMap fun k => (patch.lookup k).map (k, ·)).lookup a
        = ((patch.map Prod.fst).filterMap fun k => (patch.lookup k).map (k, ·)).lookup a := by
            exact lookup_filterMap_eraseDups (patch.map Prod.fst) (fun k => patch.lookup k) a
    _ = patch.lookup a := patch_lookup patch a

theorem sound : Goal := by
  intro history snap old δ requests
  apply congrArg (fun f => requests.map f)
  funext a
  dsimp only [retain]
  rw [compact_lookup δ.reverse a]
  dsimp [TinylambOpt.ResearchGoals.EngineDecisions.MVCC.OverlayCache.read]
  cases hp : δ.reverse.lookup a <;> simp [hp, List.lookup_append]

end TinylambOpt.ResearchProofs.EngineDecisions.OverlayCache

-- C++ references to this model or its proof.
-- transaction/mvcc_decisions.hpp
