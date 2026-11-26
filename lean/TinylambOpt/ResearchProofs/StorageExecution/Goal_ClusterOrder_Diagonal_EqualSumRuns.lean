import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `Diagonal.EqualSumRuns` — `∀ rs, (rle ((physical key rs).map (fun (x,y) =>
  x+y))).length = (rs.map (fun (x,y) => x+y)).eraseDups.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Diagonal_EqualSumRuns

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Diagonal

private theorem rle_cons (x : Nat) (xs : List Nat) :
    rle (x :: xs) = match rle xs with
      | [] => [(x, 1)]
      | (y, k) :: ys => if x == y then (x, k + 1) :: ys else (x, 1) :: (y, k) :: ys := by
  unfold rle
  rw [List.foldr_cons]
  split <;> simp_all

private theorem rle_head : ∀ (x : Nat) (xs : List Nat), ∃ k rest, rle (x :: xs) = (x, k) :: rest := by
  intro x xs
  induction xs generalizing x with
  | nil => exact ⟨1, [], by rw [rle_cons]; rfl⟩
  | cons y ys ih =>
      obtain ⟨k, rest, h⟩ := ih y
      by_cases hxy : x == y
      · refine ⟨k + 1, rest, ?_⟩
        rw [rle_cons, h]
        simp [hxy]
      · refine ⟨1, (y, k) :: rest, ?_⟩
        rw [rle_cons, h]
        simp [hxy]

private theorem rle_mem : ∀ (l : List Nat) (v : Nat), v ∈ (rle l).map Prod.fst ↔ v ∈ l := by
  intro l
  induction l with
  | nil => intro v; simp [rle]
  | cons x xs ih =>
      intro v
      cases xs with
      | nil => simp [rle_cons, rle]
      | cons y ys =>
          obtain ⟨k, rest, h⟩ := rle_head y ys
          have ihy := ih v
          rw [h] at ihy
          rw [rle_cons, h]
          by_cases hxy : x == y
          · have hxeq : x = y := by simpa using hxy
            subst x
            simpa using ihy
          · simp only [ite_false, hxy, Bool.false_eq_true, List.mem_map, Prod.exists,
              List.mem_cons, Prod.mk.injEq, exists_eq_right, List.map_cons] at ihy ⊢
            constructor
            · intro hv
              rcases hv with hvx | hvy | hvrest
              · exact Or.inl hvx
              · exact Or.inr (Or.inl hvy)
              · have hres := ihy.mp (Or.inr hvrest)
                rcases hres with hvy' | hys
                · subst v
                  exact Or.inr (Or.inl rfl)
                · exact Or.inr (Or.inr hys)
            · intro hv
              rcases hv with hvx | hvy | hys
              · exact Or.inl hvx
              · exact Or.inr (Or.inl hvy)
              · have hres := ihy.mpr (Or.inr hys)
                rcases hres with hvy' | hvrest
                · exact Or.inr (Or.inl hvy')
                · exact Or.inr (Or.inr hvrest)

private theorem head_le_of_pairwise {a : Nat} {l : List Nat} (hs : (a :: l).Pairwise (· ≤ ·))
    {z : Nat} (hz : z ∈ a :: l) : a ≤ z := by
  cases hz with
  | head => exact Nat.le_refl a
  | tail _ hm => exact (List.pairwise_cons.mp hs).1 z hm

private theorem rle_values_pairwise (l : List Nat) (hs : l.Pairwise (· ≤ ·)) :
    ((rle l).map Prod.fst).Pairwise (· < ·) := by
  induction l with
  | nil => simp [rle]
  | cons x xs ih =>
      cases xs with
      | nil => simp [rle_cons, rle]
      | cons y ys =>
          have hs_tail : (y :: ys).Pairwise (· ≤ ·) := (List.pairwise_cons.mp hs).2
          have hxy : x ≤ y := (List.pairwise_cons.mp hs).1 y (by simp)
          obtain ⟨k, rest, h⟩ := rle_head y ys
          have ihy := ih hs_tail
          rw [h] at ihy
          rw [rle_cons, h]
          by_cases hEq : x == y
          · have hxeq : x = y := by simpa using hEq
            subst x
            simp [hEq] at ihy ⊢
            exact ihy
          · have hxne : x ≠ y := by simpa using hEq
            have hxlt : x < y := by omega
            simp [hEq] at ihy ⊢
            constructor
            · constructor
              · exact hxlt
              · intro a k' ha
                have hmemr : (a, k') ∈ rle (y :: ys) := by
                  rw [h]
                  exact List.mem_cons_of_mem (y, k) ha
                have hmem : a ∈ y :: ys :=
                  (rle_mem (y :: ys) a).mp (List.mem_map.mpr ⟨(a, k'), hmemr, rfl⟩)
                have hya : y ≤ a := head_le_of_pairwise hs_tail hmem
                omega
            · exact ihy

private theorem eraseDups_nodup : ∀ l : List Nat, l.eraseDups.Nodup := by
  intro l
  refine (Nat.strongRecOn (motive := fun n => ∀ l : List Nat, l.length = n → l.eraseDups.Nodup)
    l.length ?_ l rfl)
  intro n ih l hlen
  cases l with
  | nil => simp [List.eraseDups]
  | cons a as =>
      rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro hmem
        have hf : a ∈ as.filter (fun b => ! b == a) := List.mem_eraseDups.mp hmem
        have hp := (List.mem_filter.mp hf).2
        simp at hp
      · apply ih (as.filter (fun b => ! b == a)).length
        · simp only [List.length_cons] at hlen
          have hle := List.length_filter_le (fun b : Nat => ! b == a) as
          omega
        · rfl

private def tagLe (x y : Row × Nat) : Bool :=
  decide (let a := key x.1; let b := key y.1
    a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2))))

private theorem tagLe_trans (x y z : Row × Nat) :
    tagLe x y = true → tagLe y z = true → tagLe x z = true := by
  intro h1 h2
  simp only [tagLe, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at h1 h2 ⊢
  omega

private theorem tagLe_total (x y : Row × Nat) : (tagLe x y || tagLe y x) = true := by
  simp only [tagLe, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq]
  omega

private theorem perm_eq_sorted (rs : List Row) :
    perm key rs = (rs.zipIdx.mergeSort tagLe).map Prod.snd := by
  rfl

private theorem physical_eq_sorted_fst (rs : List Row) :
    physical key rs = (rs.zipIdx.mergeSort tagLe).map Prod.fst := by
  unfold physical
  rw [perm_eq_sorted]
  apply List.ext_getElem
  · simp
  · intro i hi1 hi2
    have hi_s : i < (rs.zipIdx.mergeSort tagLe).length := by simpa using hi2
    let p : Row × Nat := (rs.zipIdx.mergeSort tagLe)[i]'hi_s
    have hpmem : p ∈ rs.zipIdx.mergeSort tagLe := by
      exact List.getElem_mem (by simpa [p] using hi_s)
    have hporig : p ∈ rs.zipIdx := (List.mergeSort_perm _ _).mem_iff.mp hpmem
    have hb := List.mem_zipIdx' hporig
    simp only [List.getElem_map]
    dsimp [p] at hb ⊢
    rw [List.getElem?_eq_getElem hb.1, ← hb.2]
    simp

private theorem physical_perm (rs : List Row) : (physical key rs).Perm rs := by
  rw [physical_eq_sorted_fst]
  have h1 := (List.mergeSort_perm rs.zipIdx tagLe).map Prod.fst
  simpa using h1

private theorem row_sum_le_of_tagLe {x y : Row × Nat} (h : tagLe x y = true) :
    x.1.1 + x.1.2 ≤ y.1.1 + y.1.2 := by
  rcases x with ⟨⟨xa, xb⟩, xi⟩
  rcases y with ⟨⟨ya, yb⟩, yi⟩
  simp only [tagLe, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at h
  dsimp [key] at h ⊢
  omega

private theorem physical_sorted (rs : List Row) :
    (physical key rs).Pairwise (fun a b : Row => a.1 + a.2 ≤ b.1 + b.2) := by
  rw [physical_eq_sorted_fst]
  have hs := List.pairwise_mergeSort tagLe_trans tagLe_total rs.zipIdx
  exact hs.map Prod.fst (by intro x y h; exact row_sum_le_of_tagLe h)

theorem sound_EqualSumRuns : EqualSumRuns.Goal := by
  intro rs
  let l : List Nat := (physical key rs).map (fun p : Row => p.1 + p.2)
  have hs : l.Pairwise (fun a b : Nat => a ≤ b) := by
    exact (physical_sorted rs).map (fun p : Row => p.1 + p.2) (by
      intro a b h
      exact h)
  have hpair : ((rle l).map Prod.fst).Pairwise (fun a b : Nat => a < b) := rle_values_pairwise l hs
  have hdnodup : ((rle l).map Prod.fst).Nodup := hpair.imp (by intro a b h; omega)
  let target : List Nat := (rs.map (fun p : Row => p.1 + p.2)).eraseDups
  have htnodup : target.Nodup := eraseDups_nodup _
  have hsumperm : l.Perm (rs.map (fun p : Row => p.1 + p.2)) := by
    dsimp [l]
    exact (physical_perm rs).map (fun p : Row => p.1 + p.2)
  have hperm : ((rle l).map Prod.fst).Perm target := by
    apply List.perm_iff_count.mpr
    intro v
    rw [hdnodup.count, htnodup.count]
    have hmem1 := rle_mem l v
    have hmem2 : v ∈ target ↔ v ∈ rs.map (fun p : Row => p.1 + p.2) := by
      simp [target]
    have hmem3 : v ∈ l ↔ v ∈ rs.map (fun p : Row => p.1 + p.2) := hsumperm.mem_iff
    by_cases hv1 : v ∈ (rle l).map Prod.fst
    · have hv2 : v ∈ target := hmem2.mpr (hmem3.mp (hmem1.mp hv1))
      simp [hv1, hv2]
    · have hv2 : v ∉ target := by
        intro ht
        exact hv1 (hmem1.mpr (hmem3.mpr (hmem2.mp ht)))
      simp [hv1, hv2]
  have hlen := hperm.length_eq
  simpa [l, target] using hlen

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Diagonal_EqualSumRuns
