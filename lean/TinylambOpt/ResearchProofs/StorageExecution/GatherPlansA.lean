import TinylambOpt.ResearchGoals.StorageExecution.GatherPlans

namespace TinylambOpt.ResearchProofs.StorageExecution.GatherPlansA

open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.GatherPlans
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

/-! Shared facts about normalization, exhaustive covering plans, and the
boundary dynamic program.  The device-specific goals at the end are instances
of these generic results. -/

private theorem eraseDups_nodup_aux {α : Type} [BEq α] [LawfulBEq α]
    (n : Nat) (xs : List α) (hlen : xs.length = n) :
    xs.eraseDups.Nodup := by
  induction n generalizing xs with
  | zero =>
    cases xs with
    | nil => simp
    | cons x xs => simp at hlen
  | succ n ih =>
    cases xs with
    | nil => simp at hlen
    | cons x xs =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro hx
        have hxf : x ∈ List.filter (fun b => !b == x) xs := List.mem_eraseDups.mp hx
        have hx' := List.mem_filter.mp hxf
        have hxx : x ∈ xs.eraseDups := List.mem_eraseDups.mpr hx'.1
        have hbeq : (x == x) = true := by simp
        simp [hbeq] at hx'
      · exact ih (List.filter (fun b => !b == x) xs)
          (by simp only [List.length_filter_le]; omega)

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup :=
  eraseDups_nodup_aux xs.length xs rfl

private theorem points_mem (xs : List Nat) (x : Nat) :
    x ∈ points xs ↔ x ∈ xs := by
  simp [points]

private theorem points_nodup (xs : List Nat) : (points xs).Nodup := by
  unfold points
  exact (List.mergeSort_perm xs.eraseDups (fun a b : Nat => decide (a ≤ b))).nodup_iff.mpr
    (eraseDups_nodup xs)

private theorem pairwise_lt_of_le_nodup {xs : List Nat}
    (hle : xs.Pairwise (fun a b => a ≤ b)) (hnd : xs.Nodup) :
    xs.Pairwise (fun a b => a < b) := by
  induction xs with
  | nil => exact .nil
  | cons x xs ih =>
    rw [List.pairwise_cons] at hle ⊢
    constructor
    · intro b hb
      have hne : x ≠ b := by
        intro heq
        subst b
        exact (List.nodup_cons.mp hnd).1 hb
      have hle' := hle.1 b hb
      omega
    · exact ih hle.2 (List.nodup_cons.mp hnd).2

private theorem points_sorted (xs : List Nat) :
    (points xs).Pairwise (fun a b => a < b) := by
  unfold points
  refine pairwise_lt_of_le_nodup ?_ ?_
  · exact List.pairwise_mergeSort
      (fun a b c hab hbc => by simpa using Nat.le_trans hab hbc)
      (fun a b => by simpa using Nat.le_total a b)
      xs.eraseDups
  · exact (List.mergeSort_perm xs.eraseDups (fun a b : Nat => a ≤ b)).nodup_iff.mpr
      (eraseDups_nodup xs)

private theorem covers_nonempty (xs : List Nat) : ∃ ps, ps ∈ covers xs := by
  induction xs with
  | nil => exact ⟨[], by simp [covers]⟩
  | cons x xs ih =>
    obtain ⟨q, hq⟩ := ih
    cases q with
    | nil =>
      exact ⟨[(x, x)], by
        rw [covers]
        exact List.mem_flatMap.mpr ⟨[], hq, by simp⟩⟩
    | cons e rest =>
      exact ⟨(x, x) :: e :: rest, by
        rw [covers]
        exact List.mem_flatMap.mpr ⟨e :: rest, hq, by simp⟩⟩

/-- An exact recursive specification of the plans enumerated by `covers`. -/
private inductive Parts : List Nat → List Extent → Prop
  | nil : Parts [] []
  | singleton : (x : Nat) → (xs : List Nat) → (rest : List Extent) →
      Parts xs rest → Parts (x :: xs) ((x, x) :: rest)
  | bridge : (x : Nat) → (xs : List Nat) → (d : Nat) → (b : Nat) →
      (rest : List Extent) → xs[d]? = some b →
      Parts (xs.drop (d + 1)) rest → Parts (x :: xs) ((x, b) :: rest)

private theorem covers_parts {xs : List Nat} {ps : List Extent}
    (h : ps ∈ covers xs) : Parts xs ps := by
  induction xs generalizing ps with
  | nil =>
    simp only [covers, List.mem_singleton] at h
    subst ps
    exact .nil
  | cons x xs ih =>
    change ps ∈ (covers xs).flatMap (fun q => match q with
      | [] => [[(x, x)]]
      | (_, b) :: rest => [(x, x) :: q, (x, b) :: rest]) at h
    rw [List.mem_flatMap] at h
    obtain ⟨q, hq, hps⟩ := h
    cases q with
    | nil =>
      simp only [List.mem_singleton] at hps
      subst ps
      exact .singleton x xs [] .nil
    | cons e rest =>
      simp only [List.mem_cons, List.mem_singleton] at hps
      rcases hps with rfl | rfl
      · exact .singleton x xs (e :: rest) (ih hq)
      · exact .bridge x xs 0 e.2 rest (by simp) (ih hq)

private theorem parts_covers_aux (n : Nat) (xs : List Nat) (ps : List Extent)
    (hlen : xs.length = n) (h : Parts xs ps) : ps ∈ covers xs := by
  induction n generalizing xs ps with
  | zero =>
    cases xs with
    | nil =>
      cases h
      simp [covers]
    | cons x xs => simp at hlen
  | succ n ih =>
    cases h with
    | nil => simp [covers]
    | singleton x xs rest hrest =>
      have hr : rest ∈ covers xs := ih xs rest (by simp at hlen; omega) hrest
      rw [covers]
      exact List.mem_flatMap.mpr ⟨rest, hr, by simp⟩
    | bridge x xs d b rest hget hrest =>
      cases xs with
      | nil => simp at hget
      | cons a tail =>
        have hp : Parts (a :: tail) ((a, b) :: rest) := by
          cases d with
          | zero =>
            simp only [List.getElem?_cons_zero, Option.some.injEq] at hget
            subst b
            exact .singleton a tail rest hrest
          | succ d =>
            exact .bridge a tail d b rest (by simpa using hget) hrest
        have hq : (a, b) :: rest ∈ covers (a :: tail) :=
          ih (a :: tail) ((a, b) :: rest) (by simp at hlen; omega) hp
        rw [covers]
        exact List.mem_flatMap.mpr ⟨(a, b) :: rest, hq, by simp⟩

private theorem parts_covers {xs : List Nat} {ps : List Extent}
    (h : Parts xs ps) : ps ∈ covers xs :=
  parts_covers_aux xs.length xs ps rfl h

private theorem covers_iff_parts {xs : List Nat} {ps : List Extent} :
    ps ∈ covers xs ↔ Parts xs ps :=
  ⟨covers_parts, parts_covers⟩

private theorem parts_endpoints {xs : List Nat} {ps : List Extent}
    (h : Parts xs ps) : ∀ e ∈ ps, e.1 ∈ xs ∧ e.2 ∈ xs := by
  cases h with
  | nil => simp
  | singleton x xs rest hrest =>
    intro e he
    simp only [List.mem_cons, Prod.mk.injEq] at he
    rcases he with rfl | he
    · exact ⟨by simp, by simp⟩
    · obtain ⟨h1, h2⟩ := parts_endpoints hrest e he
      exact ⟨List.mem_cons_of_mem x h1, List.mem_cons_of_mem x h2⟩
  | bridge x xs d b rest hget hrest =>
    intro e he
    simp only [List.mem_cons, Prod.mk.injEq] at he
    rcases he with rfl | he
    · exact ⟨by simp, List.mem_cons_of_mem x (List.mem_iff_getElem?.mpr ⟨d, hget⟩)⟩
    · obtain ⟨h1, h2⟩ := parts_endpoints hrest e he
      exact ⟨List.mem_cons_of_mem x (List.mem_of_mem_drop h1),
        List.mem_cons_of_mem x (List.mem_of_mem_drop h2)⟩

/-- A compact recurrence for the iterative `boundaryCost` implementation. -/
private def seg (n off : Nat) (edge : Nat → Nat → Nat) : Nat :=
  match n with
  | 0 => 0
  | m + 1 =>
    ((List.range (m + 1)).map (fun d =>
      edge off (off + d + 1) + seg (m - d) (off + d + 1) edge)).min?.getD 0

private def coverMin (f : Price) (xs : List Nat) : Nat :=
  ((covers xs).map (cost f)).min?.getD 0

private theorem coverMin_le {f : Price} {xs : List Nat} {ps : List Extent}
    (h : ps ∈ covers xs) : coverMin f xs ≤ cost f ps := by
  obtain ⟨q, hq⟩ := covers_nonempty xs
  have hne : (covers xs).map (cost f) ≠ [] := by
    intro he
    have : cost f q ∈ (covers xs).map (cost f) := List.mem_map.mpr ⟨q, hq, rfl⟩
    simp [he] at this
  unfold coverMin
  cases hm : ((covers xs).map (cost f)).min? with
  | none => exact False.elim (hne (List.min?_eq_none_iff.mp hm))
  | some n =>
    have hmin := (List.min?_eq_some_iff.mp hm).2 (cost f ps)
      (List.mem_map.mpr ⟨ps, h, rfl⟩)
    exact hmin

private theorem coverMin_attained {f : Price} {xs : List Nat} :
    ∃ ps, ps ∈ covers xs ∧ cost f ps = coverMin f xs := by
  obtain ⟨q, hq⟩ := covers_nonempty xs
  have hne : (covers xs).map (cost f) ≠ [] := by
    intro he
    have : cost f q ∈ (covers xs).map (cost f) := List.mem_map.mpr ⟨q, hq, rfl⟩
    simp [he] at this
  unfold coverMin
  cases hm : ((covers xs).map (cost f)).min? with
  | none => exact False.elim (hne (List.min?_eq_none_iff.mp hm))
  | some n =>
    obtain ⟨hn, _⟩ := List.min?_eq_some_iff.mp hm
    obtain ⟨ps, hps, rfl⟩ := List.mem_map.mp hn
    exact ⟨ps, hps, rfl⟩

end TinylambOpt.ResearchProofs.StorageExecution.GatherPlansA
