import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# DerivedIpKeysAfterGrouping

Grouping on a key together with deterministic functions of that key preserves the same
grouping as grouping on the key alone.

The executor can hash ClientIP alone and compute the derived IP columns when emitting
each group.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.DerivedIpKeysAfterGrouping
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking
open TinylambOpt.ResearchGoals.QueryRewrites

def wide (i : Int) := (i, i - 1, i - 2, i - 3)

theorem wide_inj : Function.Injective wide := by
  intro a b h
  exact congrArg Prod.fst h

theorem count_map_wide (xs : List Int) (i : Int) : (xs.map wide).count (wide i) = xs.count i := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : x == i
      · have heq : x = i := beq_iff_eq.mp h
        have hw : wide x == wide i := beq_iff_eq.mpr (by rw [heq])
        simp [List.count_cons, h, hw, ih]
      · have hwne : ¬ (wide x == wide i) = true := by
          intro hw
          apply h
          apply beq_iff_eq.mpr
          exact wide_inj (beq_iff_eq.mp hw)
        simp [List.count_cons, h, hwne, ih]

theorem filter_map_wide (xs : List Int) (x : Int) :
    (xs.map wide).filter (fun b => !b == wide x) = (xs.filter (fun y => !y == x)).map wide := by
  induction xs with
  | nil => simp
  | cons y ys ih =>
      by_cases hy : y == x
      · have heq : y = x := beq_iff_eq.mp hy
        have hw : wide y == wide x := beq_iff_eq.mpr (by rw [heq])
        simp [hy, hw, ih]
      · have hwne : ¬ (wide y == wide x) = true := by
          intro hw
          apply hy
          apply beq_iff_eq.mpr
          exact wide_inj (beq_iff_eq.mp hw)
        simp [hy, hwne, ih]

theorem eraseDups_map_wide (xs : List Int) : (xs.map wide).eraseDups = xs.eraseDups.map wide := by
  let motive : Nat → Prop := fun n => ∀ xs : List Int, xs.length = n → (xs.map wide).eraseDups = xs.eraseDups.map wide
  exact (Nat.strongRecOn xs.length (motive := motive) (fun n ih => by
    intro xs hlen
    cases xs with
    | nil => simp
    | cons x xs =>
        simp only [List.length_cons] at hlen
        have hsublen : (xs.filter (fun y => !y == x)).length < n := by
          have hle := List.length_filter_le (fun y => !y == x) xs
          omega
        have hsub := ih (xs.filter (fun y => !y == x)).length hsublen (xs.filter (fun y => !y == x)) rfl
        simp only [List.map_cons]
        rw [List.eraseDups_cons, filter_map_wide, hsub]
        rw [List.eraseDups_cons]
        simp only [List.map_cons])) xs rfl

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.DerivedIpKeysAfterGrouping.Goal := by
  intro ips w
  change frequency (ips.map wide) = (frequency ips).map (fun (i, n) => (wide i, n))
  unfold frequency
  rw [eraseDups_map_wide]
  simp [count_map_wide]
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.DerivedIpKeysAfterGrouping
