import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-! Dropping k packed rows is the same as dropping source bits through the k-th selected ordinal and packing the remainder. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_PackThenDropPrefix

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.PackThenDropPrefix

theorem filter_fst_map_succ (l : List (Bool × Nat)) :
    List.map Prod.snd (List.filter Prod.fst (l.map (fun p => (p.1, p.2 + 1)))) =
      List.map (fun j => j + 1) (List.map Prod.snd (List.filter Prod.fst l)) := by
  induction l with
  | nil => simp
  | cons p l ih =>
      rcases p with ⟨b, j⟩
      cases b <;> simp [ih]

theorem positions_cons (b : Bool) (m : List Bool) :
    positions (b :: m) = (if b then 0 :: (positions m).map (fun j => j + 1) else (positions m).map (fun j => j + 1)) := by
  cases b <;> simp [positions, List.zipIdx_cons, List.zipIdx_succ, filter_fst_map_succ, Function.comp_def]

theorem filterMap_congr {α β : Type} (f g : α → Option β) (l : List α) (h : ∀ x ∈ l, f x = g x) :
    l.filterMap f = l.filterMap g := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y = g y := by intro y hy; exact h y (by simp [hy])
      rw [List.filterMap_cons, List.filterMap_cons, hx, ih ht]

theorem shifted_filterMap (xs : List Nat) (m : List Bool) :
    (positions m).filterMap ((fun i => xs[i]?) ∘ (fun j => j + 1)) =
      (positions m).filterMap (fun j => xs[j + 1]?) := by
  apply filterMap_congr
  intro j hj
  simp

theorem compress_cons_true (xs : List Nat) (m : List Bool) :
    compress xs (true :: m) = (xs[0]?).toList ++ compress (xs.drop 1) m := by
  unfold compress gather
  rw [positions_cons]
  simp [List.filterMap_cons, shifted_filterMap]
  cases hxs : xs[0]? <;> simp

theorem compress_cons_false (xs : List Nat) (m : List Bool) :
    compress xs (false :: m) = compress (xs.drop 1) m := by
  unfold compress gather
  rw [positions_cons]
  simp [shifted_filterMap]

theorem sound : PackThenDropPrefix.Goal := by
  intro xs m k i hlen hpos
  induction m generalizing xs k i with
  | nil =>
      simp [positions] at hpos
  | cons b t ih =>
      have hlen' : (xs.drop 1).length = t.length := by
        simp at hlen ⊢
        omega
      cases b with
      | false =>
          have hp : ((positions t)[k]?).map (fun j => j + 1) = some i := by
            simpa [positions_cons] using hpos
          cases hopt : (positions t)[k]? with
          | none => simp [hopt] at hp
          | some j =>
              have hi : i = j + 1 := by
                simp [hopt] at hp
                omega
              subst i
              rw [compress_cons_false]
              have hrec := ih (xs.drop 1) k j hlen' hopt
              simpa [List.drop_drop, Nat.add_comm] using hrec
      | true =>
          cases k with
          | zero =>
              simp [positions_cons] at hpos
              subst i
              simp
          | succ k =>
              have hp : ((positions t)[k]?).map (fun j => j + 1) = some i := by
                simpa [positions_cons] using hpos
              cases hopt : (positions t)[k]? with
              | none => simp [hopt] at hp
              | some j =>
                  have hi : i = j + 1 := by
                    simp [hopt] at hp
                    omega
                  subst i
                  have hxpos : 0 < xs.length := by
                    simp at hlen
                    omega
                  have hx0 : xs[0]? = some (xs[0]'hxpos) := List.getElem?_eq_getElem hxpos
                  rw [compress_cons_true, hx0]
                  rw [List.drop_succ_cons]
                  have hrec := ih (xs.drop 1) k j hlen' hopt
                  simpa [List.drop_drop, Nat.add_comm] using hrec

end TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_PackThenDropPrefix
