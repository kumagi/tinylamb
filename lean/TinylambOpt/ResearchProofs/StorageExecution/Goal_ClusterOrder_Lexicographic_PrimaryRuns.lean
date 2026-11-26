import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `Lexicographic.PrimaryRuns` — `∀ rs, (rle ((physical key rs).map Prod.fst)).length =
  (rs.map Prod.fst).eraseDups.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Lexicographic_PrimaryRuns

open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Lexicographic

private theorem rowAt_of_mem_zipIdx {rs : List Row} {x : Row × Nat} (hx : x ∈ rs.zipIdx) :
    rs[x.2]?.getD (0, 0) = x.1 := by
  have hget : rs[x.2]? = some x.1 := List.mem_zipIdx_iff_getElem?.mp hx
  simp [hget]

private theorem perm_perm_range (rs : List Row) : (perm id rs).Perm (List.range rs.length) := by
  unfold perm
  have hmap := (List.mergeSort_perm rs.zipIdx (fun (x : Row × Nat) (y : Row × Nat) =>
    let a := id x.1
    let b := id y.1
    a.1 < b.1 || (a.1 == b.1 && (a.2 < b.2 || (a.2 == b.2 && x.2 ≤ y.2))))).map Prod.snd
  have hz : rs.zipIdx.map Prod.snd = List.range rs.length := by
    rw [List.zipIdx_map_snd]
    exact List.range_eq_range'.symm
  simpa [hz] using hmap

private theorem physical_perm (rs : List Row) : (physical id rs).Perm rs := by
  unfold physical
  have h1 := (perm_perm_range rs).map (fun i => rs[i]?.getD (0, 0))
  have h2 : (List.range rs.length).map (fun i => rs[i]?.getD (0, 0)) = rs := by
    symm
    apply List.ext_getElem
    · simp
    · intro i h1 h2
      simp only [List.length_map, List.length_range] at h2
      simp [List.getElem?_eq_getElem h2]
  simpa [h2] using h1

private def tagLe (x y : Row × Nat) : Bool :=
  x.1.1 < y.1.1 ||
    (x.1.1 == y.1.1 && (x.1.2 < y.1.2 || (x.1.2 == y.1.2 && x.2 ≤ y.2)))

private theorem tagLe_trans (x y z : Row × Nat) :
    tagLe x y = true → tagLe y z = true → tagLe x z = true := by
  intro h1 h2
  simp only [tagLe, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at h1 h2 ⊢
  omega

private theorem tagLe_total (x y : Row × Nat) : (tagLe x y || tagLe y x) = true := by
  simp only [tagLe, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq]
  omega

private theorem pairwise_of_tagLe {rs : List Row} {L : List (Row × Nat)}
    (hmem : ∀ x ∈ L, x ∈ rs.zipIdx)
    (hs : L.Pairwise (fun a b : Row × Nat => tagLe a b = true)) :
    L.Pairwise
      (fun a b : Row × Nat => (rs[a.2]?.getD (0, 0)).1 ≤ (rs[b.2]?.getD (0, 0)).1) := by
  induction hs with
  | nil => simp
  | @cons a l hhead htail ih =>
      apply List.Pairwise.cons
      · intro b hb
        have ht : tagLe a b = true := hhead b hb
        have ha_mem : a ∈ rs.zipIdx := hmem a List.mem_cons_self
        have hb_mem : b ∈ rs.zipIdx := hmem b (List.mem_cons_of_mem a hb)
        have hrowa := rowAt_of_mem_zipIdx ha_mem
        have hrowb := rowAt_of_mem_zipIdx hb_mem
        simp only [tagLe, decide_eq_true_eq, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq] at ht
        have hab : a.1.1 ≤ b.1.1 := by omega
        simpa [hrowa, hrowb] using hab
      · exact ih (fun x hx => hmem x (List.mem_cons_of_mem a hx))

private theorem sorted_merged_fst (rs : List Row) :
    ((rs.zipIdx.mergeSort tagLe).map (fun p : Row × Nat => (rs[p.2]?.getD (0, 0)).1)).Pairwise
      (fun a b : Nat => a ≤ b) := by
  rw [List.pairwise_map]
  exact pairwise_of_tagLe (rs := rs)
    (fun x hx => (List.mergeSort_perm rs.zipIdx tagLe).mem_iff.mp hx)
    (List.pairwise_mergeSort tagLe_trans tagLe_total rs.zipIdx)

private theorem col_pairwise (rs : List Row) :
    ((physical id rs).map Prod.fst).Pairwise (fun a b : Nat => a ≤ b) := by
  unfold physical perm
  rw [List.map_map, List.map_map]
  exact sorted_merged_fst rs

private theorem rle_length_cons (x : Nat) (xs : List Nat) :
    (rle (x :: xs)).length =
      match rle xs with
      | [] => 1
      | (y, _k) :: _ys => if x == y then (rle xs).length else (rle xs).length + 1 := by
  change (match rle xs with
    | [] => [(x, 1)]
    | (y, k) :: ys => if x == y then (x, k + 1) :: ys else (x, 1) :: rle xs).length = _
  cases h : rle xs with
  | nil => simp
  | cons p ps =>
      by_cases hxp : x == p.1 <;> simp [hxp]

private theorem rle_head_fst_of_cons (a : Nat) (xs : List Nat) :
    ∃ k ps, rle (a :: xs) = (a, k) :: ps := by
  induction xs generalizing a with
  | nil => simp [rle]
  | cons b ys ih =>
      rcases ih b with ⟨k, ps, hk⟩
      refine ⟨if a == b then k + 1 else 1, if a == b then ps else (b, k) :: ps, ?_⟩
      change (match rle (b :: ys) with
        | [] => [(a, 1)]
        | (y, c) :: ys' => if a == y then (a, c + 1) :: ys' else (a, 1) :: rle (b :: ys)) =
          (a, if a == b then k + 1 else 1) :: (if a == b then ps else (b, k) :: ps)
      rw [hk]
      by_cases h : a == b
      · have heq : a = b := by simpa using h
        subst b
        simp
      · simp [h]

private theorem rle_length_cons_eq_of_eq (a b : Nat) (ys : List Nat) (heq : a = b) :
    (rle (a :: b :: ys)).length = (rle (b :: ys)).length := by
  rw [rle_length_cons]
  obtain ⟨k, ps, hk⟩ := rle_head_fst_of_cons b ys
  rw [hk]
  simp [heq]

private theorem rle_length_cons_ne_of_ne (a b : Nat) (ys : List Nat) (hne : a ≠ b) :
    (rle (a :: b :: ys)).length = (rle (b :: ys)).length + 1 := by
  rw [rle_length_cons]
  obtain ⟨k, ps, hk⟩ := rle_head_fst_of_cons b ys
  rw [hk]
  simp [hne]

private theorem sorted_rle_length (l : List Nat) (hs : l.Pairwise (fun a b : Nat => a ≤ b)) :
    (rle l).length = l.eraseDups.length := by
  induction l with
  | nil => simp [rle]
  | cons a xs ih =>
      cases xs with
      | nil => simp [rle, List.eraseDups_cons]
      | cons b ys =>
          have hhead1 : ∀ c ∈ b :: ys, a ≤ c := (List.pairwise_cons.mp hs).1
          have hhead2 : ∀ c ∈ ys, b ≤ c := (List.pairwise_cons.mp (List.pairwise_cons.mp hs).2).1
          have htail : (b :: ys).Pairwise (fun x y : Nat => x ≤ y) := (List.pairwise_cons.mp hs).2
          have ih' := ih htail
          by_cases hab : a == b
          · have heq : a = b := by simpa using hab
            rw [rle_length_cons_eq_of_eq a b ys heq, ih']
            simp [List.eraseDups_cons, heq]
          · have hne : a ≠ b := by intro he; exact hab (by simp [he])
            have halt : ∀ c ∈ b :: ys, c ≠ a := by
              intro c hc hca
              have hbc : b ≤ c := by
                rcases List.mem_cons.mp hc with hcb | hcys
                · simp [hcb]
                · exact hhead2 c hcys
              have hba : b ≤ a := by simpa [hca] using hbc
              have hab_le : a ≤ b := hhead1 b List.mem_cons_self
              exact hne (Nat.le_antisymm hab_le hba)
            have hfilt : (b :: ys).filter (fun c => ! c == a) = b :: ys := by
              apply List.filter_eq_self.mpr
              intro c hc
              simp [halt c hc]
            rw [rle_length_cons_ne_of_ne a b ys hne, ih']
            simp [List.eraseDups_cons, hfilt]

private theorem eraseDups_length_swap (a b : Nat) (l : List Nat) :
    (a :: b :: l).eraseDups.length = (b :: a :: l).eraseDups.length := by
  have hrem : l.removeAll [a, b] = l.removeAll [b, a] := by
    unfold List.removeAll
    apply List.filter_congr
    intro x hx
    simp [or_comm]
  by_cases h : a == b
  · have heq : a = b := by simpa using h
    simp [heq]
  · have hne : a ≠ b := by intro he; exact h (by simp [he])
    have hba : b ≠ a := fun he => hne he.symm
    rw [show a :: b :: l = [a, b] ++ l by rfl, show b :: a :: l = [b, a] ++ l by rfl]
    rw [List.eraseDups_append, List.eraseDups_append]
    simp [List.eraseDups_cons, beq_iff_eq, hne, hba, hrem]

private theorem perm_eraseDups_length {l₁ l₂ : List Nat} (p : l₁.Perm l₂) :
    l₁.eraseDups.length = l₂.eraseDups.length := by
  have hmain : ∀ {a b : List Nat}, a.Perm b →
      ∀ f : Nat → Bool, (a.filter f).eraseDups.length = (b.filter f).eraseDups.length := by
    intro a b p
    refine List.Perm.rec
      (motive := fun a b _ => ∀ f : Nat → Bool, (a.filter f).eraseDups.length = (b.filter f).eraseDups.length)
      ?_ ?_ ?_ ?_ p
    · intro f
      rfl
    · intro x l1 l2 p ih f
      by_cases hx : f x = true
      · rw [List.filter_cons_of_pos hx, List.filter_cons_of_pos hx]
        rw [List.eraseDups_cons, List.eraseDups_cons]
        have ih' := ih (fun y => f y && ! y == x)
        simpa [List.filter_filter, Bool.and_comm, Bool.and_left_comm, Bool.and_assoc] using ih'
      · rw [List.filter_cons_of_neg hx, List.filter_cons_of_neg hx]
        exact ih f
    · intro x y l f
      by_cases hx : f x = true <;> by_cases hy : f y = true
      · rw [List.filter_cons_of_pos hy, List.filter_cons_of_pos hx]
        rw [List.filter_cons_of_pos hx, List.filter_cons_of_pos hy]
        exact eraseDups_length_swap y x (l.filter f)
      · rw [List.filter_cons_of_neg hy, List.filter_cons_of_pos hx]
        rw [List.filter_cons_of_pos hx, List.filter_cons_of_neg hy]
      · rw [List.filter_cons_of_pos hy, List.filter_cons_of_neg hx]
        rw [List.filter_cons_of_neg hx, List.filter_cons_of_pos hy]
      · rw [List.filter_cons_of_neg hy, List.filter_cons_of_neg hx]
        rw [List.filter_cons_of_neg hx, List.filter_cons_of_neg hy]
    · intro la lb lc p1 p2 ih1 ih2 f
      exact (ih1 f).trans (ih2 f)
  have htrue : ∀ l : List Nat, l.filter (fun _ => true) = l := by
    intro l
    induction l with
    | nil => rfl
    | cons a l ih => simp [ih]
  simpa [htrue] using hmain p (fun _ => true)

theorem sound_PrimaryRuns : PrimaryRuns.Goal := by
  intro rs
  let col := (physical id rs).map Prod.fst
  have hs : col.Pairwise (fun a b : Nat => a ≤ b) := by
    dsimp [col]
    exact col_pairwise rs
  have hp : col.Perm (rs.map Prod.fst) := by
    dsimp [col]
    exact (physical_perm rs).map Prod.fst
  calc
    (rle col).length = col.eraseDups.length := sorted_rle_length col hs
    _ = (rs.map Prod.fst).eraseDups.length := perm_eraseDups_length hp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Lexicographic_PrimaryRuns
