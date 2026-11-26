import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_PageBudget

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow

private theorem mem_lanes {b w : Nat} : b ∈ lanes w ↔ b < w := by
  simp [lanes, List.mem_range]

-- eraseDups helpers (missing in Lean 4.36 Init).
private theorem length_eraseDups_le [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.length ≤ l.length := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons]
      simp only [List.length_cons]
      have h1 : (as.filter fun b => !b == a).eraseDups.length ≤ (as.filter fun b => !b == a).length :=
        length_eraseDups_le (as.filter fun b => !b == a)
      have h2 : (as.filter fun b => !b == a).length ≤ as.length := List.length_filter_le _ _
      omega
termination_by l.length
decreasing_by
  simp_wf
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem nodup_eraseDups [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons]
      rw [List.nodup_cons]
      refine ⟨?_, nodup_eraseDups (as.filter fun b => !b == a)⟩
      intro hmem
      rw [List.mem_eraseDups] at hmem
      rw [List.mem_filter] at hmem
      have h := hmem.2
      simp at h
termination_by l.length
decreasing_by
  simp_wf
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

-- Generic bound: if every addressed byte lies inside the declared physical
-- frame, the address set is bounded by the frame and by one byte per lane.
private theorem pageBudget_of_bound (l : Layout)
    (hbound : ∀ n w i b, i < n → b < w → l.bit n w i b / 8 < byteCount (l.capacity n w)) :
    pageBudget l := by
  intro n w rows hrows
  refine Nat.le_min.mpr ⟨?_, ?_⟩
  · have hsub : (rows.flatMap (footprint l n w)).eraseDups ⊆ List.range (byteCount (l.capacity n w)) := by
      intro x hx
      rw [List.mem_eraseDups, List.mem_flatMap] at hx
      rcases hx with ⟨i, hi, hxi⟩
      unfold footprint at hxi
      rw [List.mem_eraseDups, List.mem_map] at hxi
      rcases hxi with ⟨b, hb, rfl⟩
      have hb' : b < w := by simpa [mem_lanes] using hb
      exact List.mem_range.mpr (hbound n w i b (hrows i hi) hb')
    have hnodup1 : (addressSet l n w rows).Nodup := nodup_eraseDups _
    calc (addressSet l n w rows).length
        ≤ (List.range (byteCount (l.capacity n w))).length := hnodup1.length_le_of_subset hsub
      _ = byteCount (l.capacity n w) := List.length_range
  · unfold addressSet
    have hsub_rows : ((rows.flatMap (footprint l n w)).eraseDups) ⊆
        ((rows.eraseDups.flatMap (footprint l n w)).eraseDups) := by
      intro x hx
      rw [List.mem_eraseDups, List.mem_flatMap] at hx
      rcases hx with ⟨i, hi, hxi⟩
      rw [List.mem_eraseDups, List.mem_flatMap]
      exact ⟨i, List.mem_eraseDups.mpr hi, hxi⟩
    have hnodup1 := nodup_eraseDups (rows.flatMap (footprint l n w))
    calc ((rows.flatMap (footprint l n w)).eraseDups).length
        ≤ ((rows.eraseDups.flatMap (footprint l n w)).eraseDups).length :=
          hnodup1.length_le_of_subset hsub_rows
      _ ≤ (rows.eraseDups.flatMap (footprint l n w)).length := length_eraseDups_le _
      _ = (rows.eraseDups.map (fun i => (footprint l n w i).length)).sum := List.length_flatMap ..
      _ ≤ (rows.eraseDups.map (fun _ => w)).sum := by
          have hle : ∀ i ∈ rows.eraseDups, (footprint l n w i).length ≤ w := by
            intro i _
            unfold footprint
            calc ((lanes w).map (fun b => l.bit n w i b / 8)).eraseDups.length
                ≤ ((lanes w).map (fun b => l.bit n w i b / 8)).length := length_eraseDups_le _
              _ = w := by simp [lanes]
          have hsum_le : ∀ (l : List Nat) (f g : Nat → Nat),
              (∀ x ∈ l, f x ≤ g x) → (l.map f).sum ≤ (l.map g).sum := by
            intro l f g h
            induction l with
            | nil => simp
            | cons a as ih =>
                simp only [List.map_cons, List.sum_cons]
                exact Nat.add_le_add (h a (List.mem_cons_self ..)) (ih (fun x hx => h x (List.mem_cons_of_mem _ hx)))
          exact hsum_le rows.eraseDups (fun i => (footprint l n w i).length) (fun _ => w) hle
      _ = rows.eraseDups.length * w := by
          have hsum : ∀ (l : List Nat) (c : Nat), (l.map (fun _ => c)).sum = l.length * c := by
            intro l c; induction l with
            | nil => simp
            | cons a as ih => simp [ih, Nat.add_mul, Nat.add_comm]
          rw [hsum]

-- Byte-aligned rows: bit i*align 8 w + b (b < w) always lies within the
-- n*align 8 w bit frame, hence its byte index is within the frame's byteCount.
private theorem hbound8 : ∀ n w i b, i < n → b < w →
    layout.bit n w i b / 8 < byteCount (layout.capacity n w) := by
  intro n w i b hi hb
  have hwk : w ≤ align 8 w := by unfold align; omega
  have hkdecomp : align 8 w / 8 * 8 = align 8 w := by
    have hm : align 8 w % 8 = 0 := by unfold align; omega
    have := Nat.div_add_mod (align 8 w) 8
    omega
  have hnk : n * align 8 w = (n * (align 8 w / 8)) * 8 := by
    rw [← hkdecomp]
    simp [Nat.mul_comm, Nat.mul_left_comm]
  have hbc : byteCount (layout.capacity n w) = n * (align 8 w / 8) := by
    change (n * align 8 w + 7) / 8 = n * (align 8 w / 8)
    rw [hnk]
    omega
  have hbitdiv : layout.bit n w i b / 8 = i * (align 8 w / 8) + b / 8 := by
    change (i * align 8 w + b) / 8 = i * (align 8 w / 8) + b / 8
    rw [show i * align 8 w + b = (i * (align 8 w / 8)) * 8 + b by
      rw [← hkdecomp]
      simp [Nat.mul_comm, Nat.mul_left_comm]]
    omega
  rw [hbitdiv, hbc]
  have hbdiv : b / 8 < align 8 w / 8 := by omega
  have hstep : i * (align 8 w / 8) + align 8 w / 8 ≤ n * (align 8 w / 8) := by
    have := Nat.mul_le_mul_right (align 8 w / 8) (show i + 1 ≤ n by omega)
    simpa [Nat.add_mul, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using this
  omega

theorem sound_PageBudget : ByteAlignedRow.PageBudget.Goal := by
  exact pageBudget_of_bound layout hbound8

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_PageBudget
