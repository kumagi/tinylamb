import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
A 16-bit bucket-local ordinal is safe after rebasing by a wide bucket base
whenever the bucket's final count fits in 16 bits: the lane's rank is strictly
smaller than that count, so truncation never changes the destination.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_NarrowOffsetAfterRebase

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

theorem drop_eq_cons_of_getElem?_eq_some {α : Type} (xs : List α) (i : Nat) (v : α)
    (h : xs[i]? = some v) : ∃ t, xs.drop i = v :: t := by
  induction xs generalizing i with
  | nil => simp at h
  | cons a xs ih =>
      cases i with
      | zero =>
          simp at h
          subst v
          exact ⟨xs, rfl⟩
      | succ i =>
          simp at h
          obtain ⟨t, ht⟩ := ih i h
          exact ⟨t, by simp [ht]⟩

theorem active_split (xs : List (Option Nat)) (i k : Nat)
    (h : xs[i]? = some (some k)) :
    active xs = active (xs.take i) ++ k :: active (xs.drop i).tail := by
  obtain ⟨t, ht⟩ := drop_eq_cons_of_getElem?_eq_some xs i (some k) h
  have hx : xs = xs.take i ++ xs.drop i := (List.take_append_drop i xs).symm
  calc
    active xs = active (xs.take i ++ xs.drop i) := congrArg active hx
    _ = active (xs.take i) ++ active (xs.drop i) := by
      simp only [active]
      rw [List.filterMap_append]
    _ = active (xs.take i) ++ k :: active (xs.drop i).tail := by rw [ht]; simp [active]

theorem rank_lt_count (xs : List (Option Nat)) (i k : Nat)
    (h : xs[i]? = some (some k)) : rank xs i k < (active xs).count k := by
  rw [active_split xs i k h, List.count_append]
  simp [rank]

theorem sound : NarrowOffsetAfterRebase.Goal := by
  intro xs i k hxi hcount
  have hlt : rank xs i k < 65536 := by
    have hr := rank_lt_count xs i k hxi
    omega
  have hmod : rank xs i k % 65536 = rank xs i k := Nat.mod_eq_of_lt hlt
  simp [dst, hxi, hmod]

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_NarrowOffsetAfterRebase
