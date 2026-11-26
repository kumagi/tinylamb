import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `Vertical.PageBudget` — Count addressed bytes once and bound a gather by both the
  physical frame and the naive per-bit read count.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_PageBudget

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical

private theorem eraseDups_nodupNat : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by simp
  | a :: l => by
      rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro h
        have h1 : a ∈ List.filter (fun b => ! b == a) l := List.mem_eraseDups.mp h
        simp at h1
      · exact eraseDups_nodupNat (List.filter (fun b => ! b == a) l)
termination_by l => l.length
decreasing_by
  simp only [List.length_cons]
  have hle := List.length_filter_le (fun b => ! b == a) l
  omega

private theorem sum_map_le_mul_length (k : Nat) (l : List Nat) (f : Nat → Nat)
    (h : ∀ x ∈ l, f x ≤ k) : (l.map f).sum ≤ l.length * k := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have ha : f a ≤ k := h a (by simp)
      have hl : ∀ x ∈ l, f x ≤ k := by
        intro x hx
        exact h x (by simp [hx])
      have ih' := ih hl
      calc
        f a + (l.map f).sum ≤ k + l.length * k := Nat.add_le_add ha ih'
        _ = (l.length + 1) * k := by
          rw [Nat.succ_mul]
          exact Nat.add_comm _ _

private theorem footprint_len_le (n w i : Nat) : (footprint layout n w i).length ≤ w := by
  unfold footprint lanes
  have hnodup := eraseDups_nodupNat (((List.range w).map (fun b => layout.bit n w i b / 8)))
  have hsub : ((List.range w).map (fun b => layout.bit n w i b / 8)).eraseDups ⊆
      (List.range w).map (fun b => layout.bit n w i b / 8) := by
    intro x hx
    exact List.mem_eraseDups.mp hx
  have hle := hnodup.length_le_of_subset hsub
  simpa using hle

theorem sound_PageBudget : Vertical.PageBudget.Goal := by
  intro n w rows hrows
  change (addressSet layout n w rows).length ≤
    min (byteCount (n * w)) (rows.eraseDups.length * w)

  by_cases hn : n = 0
  · subst n
    have hrows_empty : rows = [] := by
      cases rows with
      | nil => rfl
      | cons i is =>
          have : i < 0 := hrows i (by simp)
          omega
    subst rows
    simp [addressSet, byteCount]
  by_cases hw : w = 0
  · subst w
    have hfp : ∀ i, footprint layout n 0 i = [] := by
      intro i
      simp [footprint, lanes]
    have hflat : rows.flatMap (footprint layout n 0) = [] := by
      induction rows with
      | nil => rfl
      | cons i is ih => simp [hfp]
    simp [addressSet, hflat, byteCount]

  have hbound1 : (addressSet layout n w rows).length ≤ byteCount (n * w) := by
    have hsub : addressSet layout n w rows ⊆ List.range (byteCount (n * w)) := by
      intro x hx
      rw [List.mem_range]
      unfold addressSet at hx
      have hx' : x ∈ (rows.flatMap (footprint layout n w)).eraseDups := hx
      rw [List.mem_eraseDups] at hx'
      rw [List.mem_flatMap] at hx'
      rcases hx' with ⟨i, hi_rows, hxfp⟩
      have hi : i < n := hrows i hi_rows
      unfold footprint lanes at hxfp
      rw [List.mem_eraseDups] at hxfp
      rw [List.mem_map] at hxfp
      rcases hxfp with ⟨b, hb_range, hbx⟩
      subst x
      have hb : b < w := List.mem_range.mp hb_range
      have hmul : (b + 1) * n ≤ w * n := Nat.mul_le_mul_right n (Nat.succ_le_of_lt hb)
      have hlt : b * n + i < w * n := by
        calc
          b * n + i < b * n + n := Nat.add_lt_add_left hi (b * n)
          _ = (b + 1) * n := by rw [Nat.succ_mul]
          _ ≤ w * n := hmul
      have hlt' : b * n + i < n * w := by simpa [Nat.mul_comm] using hlt
      have harith : (b * n + i) / 8 < (n * w + 7) / 8 := by omega
      simpa [layout, byteCount] using harith
    have hnodup := eraseDups_nodupNat (rows.flatMap (footprint layout n w))
    have hle := hnodup.length_le_of_subset hsub
    simpa [addressSet] using hle

  have hbound2 : (addressSet layout n w rows).length ≤ rows.eraseDups.length * w := by
    have hsub : addressSet layout n w rows ⊆ rows.eraseDups.flatMap (footprint layout n w) := by
      intro x hx
      unfold addressSet at hx
      rw [List.mem_eraseDups] at hx
      rw [List.mem_flatMap] at hx
      rcases hx with ⟨i, hi_rows, hxfp⟩
      rw [List.mem_flatMap]
      exact ⟨i, List.mem_eraseDups.mpr hi_rows, hxfp⟩
    have hnodup := eraseDups_nodupNat (rows.flatMap (footprint layout n w))
    have hle := hnodup.length_le_of_subset hsub
    have hsum : (rows.eraseDups.flatMap (footprint layout n w)).length ≤
        rows.eraseDups.length * w := by
      rw [List.length_flatMap]
      apply sum_map_le_mul_length w
      intro i hi
      exact footprint_len_le n w i
    exact Nat.le_trans hle hsum

  exact Nat.le_min.mpr ⟨hbound1, hbound2⟩

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_PageBudget
