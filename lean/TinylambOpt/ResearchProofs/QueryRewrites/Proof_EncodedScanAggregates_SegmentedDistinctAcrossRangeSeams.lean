import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_SegmentedDistinctAcrossRangeSeams

open TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates


/-!
# SegmentedDistinctAcrossRangeSeams

Distinct counts over adjacent sorted user slices add except for one possible
duplicate at the seam. Counting equal-adjacent pairs makes the correction
exact, so the executor can merge per-range distinct counts without rescanning
either range.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false


private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : (l : List α) → l.eraseDups.Nodup
  | [] => List.Pairwise.nil
  | x :: l => by
      rw [List.eraseDups_cons]
      refine List.Pairwise.cons (fun b hb => ?_) (nodup_eraseDups (l.filter (fun b => !b == x)))
      intro hxb
      have h2 := List.mem_filter.mp (List.mem_eraseDups.mp hb)
      rw [hxb] at h2
      simp at h2
  termination_by l => l.length
  decreasing_by
    all_goals
      refine Nat.lt_of_le_of_lt (List.length_filter_le ..) ?_
      exact Nat.lt_succ_self _

private theorem nodup_length_le_of_mem {α : Type} [BEq α] [LawfulBEq α] {l₁ l₂ : List α}
    (hnd : l₁.Nodup) (hm : ∀ x ∈ l₁, x ∈ l₂) : l₁.length ≤ l₂.length := by
  induction l₁ generalizing l₂ with
  | nil => simp
  | cons a as ih =>
      rcases List.nodup_cons.mp hnd with ⟨haas, hasnd⟩
      have ha2 : a ∈ l₂ := hm a (by simp)
      have hasub : ∀ x ∈ as, x ∈ l₂.erase a := by
        intro x hx
        have hx2 : x ∈ l₂ := hm x (by simp [hx])
        have hxa : x ≠ a := by
          intro h
          apply haas
          simpa [h] using hx
        simpa [List.mem_erase_of_ne hxa] using hx2
      have ih' := ih hasnd hasub
      have hlen : (l₂.erase a).length = l₂.length - 1 := by
        rw [List.length_erase]
        simp [ha2]
      have hpos : 0 < l₂.length := by
        rw [List.length_pos_iff]
        intro h
        subst l₂
        simp at ha2
      simp only [List.length_cons]
      omega

private theorem nodup_length_eq_of_mem_iff {α : Type} [BEq α] [LawfulBEq α]
    {l₁ l₂ : List α} (h₁ : l₁.Nodup) (h₂ : l₂.Nodup)
    (hm : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.length = l₂.length := by
  apply Nat.le_antisymm
  · exact nodup_length_le_of_mem h₁ (fun x hx => (hm x).mp hx)
  · exact nodup_length_le_of_mem h₂ (fun x hx => (hm x).mpr hx)

private theorem eraseDups_cons_length (ys : List Nat) (x : Nat) :
    (x :: ys).eraseDups.length = ys.eraseDups.length + (if x ∈ ys then 0 else 1) := by
  by_cases hxy : x ∈ ys
  · have hlen := nodup_length_eq_of_mem_iff
      (l₁ := (x :: ys).eraseDups) (l₂ := ys.eraseDups)
      (nodup_eraseDups (x :: ys)) (nodup_eraseDups ys) (fun z => by
        constructor
        · intro hz
          rcases List.mem_cons.mp ((List.mem_eraseDups).mp hz) with rfl | hzys
          · exact (List.mem_eraseDups).mpr hxy
          · exact (List.mem_eraseDups).mpr hzys
        · intro hz
          apply (List.mem_eraseDups).mpr
          exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mp hz)))
    rw [hlen, ite_eq_left hxy, Nat.add_zero]
  · have hnody : (x :: ys.eraseDups).Nodup := by
      refine List.nodup_cons.mpr ⟨?_, nodup_eraseDups ys⟩
      intro hm
      exact hxy ((List.mem_eraseDups).mp hm)
    have hlen := nodup_length_eq_of_mem_iff
      (l₁ := (x :: ys).eraseDups) (l₂ := x :: ys.eraseDups)
      (nodup_eraseDups (x :: ys)) hnody (fun z => by
        constructor
        · intro hz
          have hz' : z = x ∨ z ∈ ys := List.mem_cons.mp ((List.mem_eraseDups).mp hz)
          rcases hz' with rfl | hzys
          · exact List.mem_cons_self
          · exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mpr hzys))
        · intro hz
          apply (List.mem_eraseDups).mpr
          rcases List.mem_cons.mp hz with rfl | hzys
          · exact List.mem_cons_self
          · exact List.mem_cons.mpr (Or.inr ((List.mem_eraseDups).mp hzys)))
    rw [hlen, List.length_cons, ite_eq_right hxy]

private def adjEq (l : List Nat) : Nat := (l.zip (l.drop 1)).countP (fun p => p.1 == p.2)

private theorem adjEq_cons (x : Nat) (ys : List Nat) :
    adjEq (x :: ys) = adjEq ys + (if ys.head? == some x then 1 else 0) := by
  cases ys with
  | nil => simp [adjEq]
  | cons y zs =>
      by_cases hxy : x = y
      · subst y
        simp [adjEq, List.countP_cons]
      · have hyx : y ≠ x := by intro h; exact hxy h.symm
        have h1 : (x == y) = false := by simp [hxy]
        have h2 : (y == x) = false := by simp [hyx]
        simp [adjEq, List.countP_cons, h1, h2]

private theorem adjEq_le_length (l : List Nat) : adjEq l ≤ l.length := by
  unfold adjEq
  have h := List.countP_le_length (p := fun p : Nat × Nat => p.1 == p.2)
    (l := l.zip (l.drop 1))
  simp only [List.length_zip, List.length_drop] at h
  omega

private theorem head_le_of_mem_sorted {x y : Nat} {ys : List Nat}
    (h : (x :: ys).Pairwise (· ≤ ·)) (hy : y ∈ x :: ys) : x ≤ y := by
  obtain ⟨hall, _⟩ := List.pairwise_cons.mp h
  rcases List.mem_cons.mp hy with rfl | hy
  · omega
  · exact hall y hy

private theorem eraseDups_length_sorted (l : List Nat) (h : l.Pairwise (· ≤ ·)) :
    l.eraseDups.length = l.length - adjEq l := by
  induction l with
  | nil => simp [adjEq]
  | cons x ys ih =>
      have hys : ys.Pairwise (· ≤ ·) := h.of_cons
      have hxle : ∀ y ∈ ys, x ≤ y := (List.pairwise_cons.mp h).1
      rw [eraseDups_cons_length]
      rw [ih hys]
      rw [adjEq_cons]
      by_cases hxmem : x ∈ ys
      · have hind : (if ys.head? == some x then 1 else 0) = 1 := by
          cases ys with
          | nil => simp at hxmem
          | cons y zs =>
              have hyx : y ≤ x := head_le_of_mem_sorted h.of_cons hxmem
              have hxy : x ≤ y := hxle y (by simp)
              have hyeq : y = x := by omega
              simp [hyeq]
        rw [ite_eq_left hxmem, hind]
        have hb := adjEq_le_length ys
        simp only [List.length_cons]
        omega
      · have hind : (if ys.head? == some x then 1 else 0) = 0 := by
          cases ys with
          | nil => simp
          | cons y zs =>
              by_cases hyx : y == x
              · have hyeq : y = x := by simpa using hyx
                have : x ∈ y :: zs := by simp [hyeq]
                exact absurd this hxmem
              · simp [hyx]
        rw [ite_eq_right hxmem, hind]
        have hb := adjEq_le_length ys
        simp only [List.length_cons]
        omega

private def seam (l1 l2 : List Nat) : Nat :=
  match l1.getLast?, l2.head? with
  | some a, some b => if a == b then 1 else 0
  | _, _ => 0

private theorem seam_eq_if (l1 l2 : List Nat) :
    seam l1 l2 = if l1.getLast?.isSome && l1.getLast? == l2.head? then 1 else 0 := by
  cases h1 : l1.getLast? with
  | none =>
      cases h2 : l2.head? <;> simp [seam, h1]
  | some a =>
      cases h2 : l2.head? with
      | none => simp [seam, h1, h2]
      | some b =>
          by_cases hab : a == b
          · have haeq : a = b := by simpa using hab
            subst b
            simp [seam, h1, h2]
          · simp [seam, h1, h2, hab]

private theorem seam_cons_cons (x y : Nat) (ys l2 : List Nat) :
    seam (x :: y :: ys) l2 = seam (y :: ys) l2 := by
  have hlast : (x :: y :: ys).getLast? = (y :: ys).getLast? := by
    rw [List.getLast?_cons]
    cases h : (y :: ys).getLast? with
    | none =>
        exfalso
        exact absurd (List.getLast?_eq_none_iff.mp h) (by simp)
    | some z => simp [h]
  simp [seam, hlast]

private theorem adjEq_append (l1 l2 : List Nat) :
    adjEq (l1 ++ l2) = adjEq l1 + adjEq l2 + seam l1 l2 := by
  induction l1 with
  | nil =>
      cases l2 with
      | nil => simp [adjEq, seam]
      | cons z zs => simp [adjEq, seam]
  | cons x xs ih =>
      cases xs with
      | nil =>
          cases l2 with
          | nil => simp [adjEq, adjEq_cons, seam]
          | cons z zs =>
              rw [show (x :: []) ++ (z :: zs) = x :: z :: zs by rfl]
              rw [adjEq_cons]
              by_cases h : x = z
              · subst z
                simp [adjEq, seam]
              · have hz : z ≠ x := by intro hz; exact h hz.symm
                simp [adjEq, seam, h, hz]
      | cons y ys =>
          rw [show (x :: y :: ys) ++ l2 = x :: ((y :: ys) ++ l2) by rfl]
          rw [adjEq_cons x ((y :: ys) ++ l2)]
          rw [ih]
          rw [adjEq_cons x (y :: ys)]
          have hhead : (((y :: ys) ++ l2).head?) = (y :: ys).head? := by simp
          rw [hhead]
          rw [seam_cons_cons]
          omega

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.SegmentedDistinctAcrossRangeSeams.Goal := by
  intro a b h
  have hab := List.pairwise_append.mp h
  rw [eraseDups_length_sorted (a ++ b) h]
  rw [eraseDups_length_sorted a hab.1]
  rw [eraseDups_length_sorted b hab.2.1]
  rw [adjEq_append]
  rw [seam_eq_if]
  simp only [List.length_append]
  have hab_adj := adjEq_le_length (a ++ b)
  have ha_adj := adjEq_le_length a
  have hb_adj := adjEq_le_length b
  have hsum_le : adjEq a + adjEq b + (if a.getLast?.isSome && a.getLast? == b.head? then 1 else 0) ≤
      (a ++ b).length := by
    rw [← seam_eq_if]
    rw [← adjEq_append]
    exact hab_adj
  have hdeq : adjEq (a ++ b) = adjEq a + adjEq b +
      (if a.getLast?.isSome && a.getLast? == b.head? then 1 else 0) := by
    rw [adjEq_append, seam_eq_if]
  have hlenab : (a ++ b).length = a.length + b.length := by simp
  omega
end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_SegmentedDistinctAcrossRangeSeams
