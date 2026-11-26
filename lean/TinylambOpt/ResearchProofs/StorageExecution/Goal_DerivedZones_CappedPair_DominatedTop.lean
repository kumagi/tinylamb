import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_DominatedTop

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.CappedPair

-- A brute-force scan over small bounds/pages/witness lists (t < 3, coordinates < 4,
-- |p| <= 2, |seen| <= 2, k <= 3, restricted to hypothesis-satisfying inputs)
-- found no counterexample.

private def desc (a b : Nat) : Bool := a ≥ b

attribute [local instance] boolRelToRel

private theorem desc_trans : ∀ a b c : Nat, desc a b → desc b c → desc a c := by
  intro a b c hab hbc
  simp [desc] at hab hbc ⊢
  omega

private theorem desc_total : ∀ a b : Nat, desc a b || desc b a := by
  intro a b
  simp [desc]
  omega

private theorem nat_min_le_min {a b c d : Nat} (h1 : a ≤ c) (h2 : b ≤ d) :
    min a b ≤ min c d := by
  apply Nat.le_min.mpr
  constructor
  · exact Nat.le_trans (Nat.min_le_left a b) h1
  · exact Nat.le_trans (Nat.min_le_right a b) h2

private theorem score_le_hi (t : Nat) (a b : Bounds) (x y : Nat)
    (hx : inside a x) (hy : inside b y) :
    kernel.score t (x, y) ≤ (kernel.bounds t a b).hi := by
  change min t (min x y) ≤ min t (min a.hi b.hi)
  exact nat_min_le_min (Nat.le_refl t) (nat_min_le_min hx.2 hy.2)

private theorem scores_le_hi (t : Nat) (p : List (Option Row)) (a b : Bounds)
    (hc : covers p a b) : ∀ v ∈ scores kernel t p, v ≤ (kernel.bounds t a b).hi := by
  intro v hv
  rw [scores, List.mem_filterMap] at hv
  rcases hv with ⟨r, hrp, hrv⟩
  rcases r with _ | ⟨x, y⟩
  · simp at hrv
  · simp [kernel] at hrv
    subst v
    exact score_le_hi t a b x y (hc (some (x, y)) hrp x y rfl).1
      (hc (some (x, y)) hrp x y rfl).2

private theorem head_gt_of_count {x : Nat} {xs : List Nat} {U : Nat}
    (hX : (x :: xs).Pairwise desc)
    (hpos : 0 < (x :: xs).countP (fun z => U < z)) : U < x := by
  by_cases hxlt : U < x
  · exact hxlt
  · have hxle : x ≤ U := by omega
    have hall : ∀ z ∈ x :: xs, z ≤ U := by
      intro z hz
      rcases List.mem_cons.mp hz with hzx | hz
      · subst z
        exact hxle
      · have hrel : desc x z := List.rel_of_pairwise_cons hX hz
        simp [desc] at hrel
        exact Nat.le_trans hrel hxle
    have hzero : (x :: xs).countP (fun z => U < z) = 0 := by
      rw [List.countP_eq_zero]
      intro z hz
      have hzle := hall z hz
      simp [show ¬ U < z by omega]
    omega

private theorem merge_take_of_gap {U : Nat} : ∀ (k : Nat) (X Y : List Nat),
    X.Pairwise desc →
    (∀ v ∈ Y, v ≤ U) →
    k ≤ X.countP (fun z => U < z) →
    (List.merge X Y desc).take k = X.take k := by
  intro k
  induction k with
  | zero =>
      intro X Y hX hY hbig
      simp
  | succ k ih =>
      intro X Y hX hY hbig
      cases X with
      | nil => simp at hbig
      | cons x xs =>
          have hxgt : U < x := head_gt_of_count hX (by omega)
          have hkxs : k ≤ xs.countP (fun z => U < z) := by
            simp [hxgt] at hbig
            omega
          cases Y with
          | nil => simp
          | cons y ys =>
              have hyle : y ≤ U := hY y (by simp)
              have hge : desc x y := by simp [desc]; omega
              rw [List.cons_merge_cons, ite_eq_left hge]
              simp only [List.take_succ_cons]
              exact congrArg (fun l : List Nat => x :: l) (ih xs (y :: ys) hX.tail (by
                intro v hv
                exact hY v (by simp [hv])) hkxs)

theorem sound_DominatedTop : CappedPair.DominatedTop.Goal := by
  intro t p a b seen k hc hk
  let U := (kernel.bounds t a b).hi
  let X := seen.mergeSort desc
  let Y := (scores kernel t p).mergeSort desc
  have hXs : X.Pairwise desc := by
    dsimp [X]
    exact List.pairwise_mergeSort desc_trans desc_total seen
  have hYs : Y.Pairwise desc := by
    dsimp [Y]
    exact List.pairwise_mergeSort desc_trans desc_total (scores kernel t p)
  have hYle : ∀ v ∈ Y, v ≤ U := by
    intro v hv
    have hv' : v ∈ scores kernel t p := by
      simpa [Y] using hv
    dsimp [U]
    exact scores_le_hi t p a b hc v hv'
  have hs1 : ((seen ++ scores kernel t p).mergeSort desc).Pairwise desc :=
    List.pairwise_mergeSort desc_trans desc_total (seen ++ scores kernel t p)
  have hs2 : (List.merge X Y desc).Pairwise desc :=
    List.pairwise_merge desc_trans desc_total X Y hXs hYs
  have hperm0 : List.Perm ((seen ++ scores kernel t p).mergeSort desc) (seen ++ scores kernel t p) :=
    List.mergeSort_perm _ _
  have hperm1 : List.Perm (seen ++ scores kernel t p) (X ++ Y) := by
    dsimp [X, Y]
    exact ((List.mergeSort_perm seen desc).symm.append
      (List.mergeSort_perm (scores kernel t p) desc).symm)
  have hperm2 : List.Perm (X ++ Y) (List.merge X Y desc) :=
    (List.merge_perm_append desc).symm
  have hperm : List.Perm ((seen ++ scores kernel t p).mergeSort desc) (List.merge X Y desc) :=
    hperm0.trans (hperm1.trans hperm2)
  have heq : (seen ++ scores kernel t p).mergeSort desc = List.merge X Y desc :=
    List.Perm.eq_of_pairwise
      (by
        intro x y hx hy hxy hyx
        simp [desc] at hxy hyx
        omega)
      hs1 hs2 hperm
  change ((seen ++ scores kernel t p).mergeSort desc).take k = (seen.mergeSort desc).take k
  rw [heq]
  change (List.merge X Y desc).take k = X.take k
  have hkX : k ≤ X.countP (fun z => U < z) := by
    have hcX : X.countP (fun z => (kernel.bounds t a b).hi < z) =
        seen.countP (fun z => (kernel.bounds t a b).hi < z) := by
      dsimp [X]
      exact (List.mergeSort_perm seen desc).countP_eq
        (fun z => (kernel.bounds t a b).hi < z)
    have hkX' : k ≤ X.countP (fun z => (kernel.bounds t a b).hi < z) := by
      omega
    simpa [U] using hkX'
  exact merge_take_of_gap k X Y hXs hYle hkX

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_CappedPair_DominatedTop
