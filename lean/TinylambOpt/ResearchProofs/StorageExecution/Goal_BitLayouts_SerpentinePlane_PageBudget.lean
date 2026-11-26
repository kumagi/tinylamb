import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentinePlane_PageBudget

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane

/-! Reversing the ordinal order on odd bit planes permutes the bit cells
inside each plane, so every cell still lies below the declared capacity and a
row still contributes at most one byte per bit plane.  Thus the page budget
argument is the same as for row-major layout, once the bit-cell bound is
reestablished for this layout. -/

private theorem eraseDups_nodup (l : List Nat) : l.eraseDups.Nodup := by
  generalize hl : l.length = k
  induction k using Nat.strongRecOn generalizing l with
  | ind k ih =>
      cases l with
      | nil => simp
      | cons a t =>
          rw [List.eraseDups_cons]
          constructor
          · intro b hb
            have hbf : b ∈ t.filter (fun x => !x == a) := List.mem_eraseDups.mp hb
            have hbne : (!b == a) = true := (List.mem_filter.mp hbf).2
            intro hba
            subst b
            simp at hbne
          · have hle : (t.filter (fun x => !x == a)).length ≤ t.length := List.length_filter_le _ _
            simp at hl
            have hlt : (t.filter (fun x => !x == a)).length < k := by omega
            exact ih _ hlt _ rfl

private theorem eraseDups_length_le (l : List Nat) : l.eraseDups.length ≤ l.length := by
  exact (eraseDups_nodup l).length_le_of_subset (by
    intro a ha
    exact List.mem_eraseDups.mp ha)

private theorem mem_footprint (n w i j : Nat) :
    j ∈ footprint layout n w i ↔ ∃ b, b < w ∧ (layout.bit n w i b)/8 = j := by
  simp [footprint, lanes, layout, List.mem_eraseDups]

private theorem footprint_length_le (n w i : Nat) : (footprint layout n w i).length ≤ w := by
  unfold footprint lanes
  simpa using (eraseDups_length_le (((List.range w).map (fun b => layout.bit n w i b / 8))))

private theorem flatMap_footprint_length_le (n w : Nat) (rows : List Nat) :
    (rows.flatMap (footprint layout n w)).length ≤ rows.length * w := by
  induction rows with
  | nil => simp
  | cons i t ih =>
      simp only [List.flatMap_cons, List.length_append]
      have hfi := footprint_length_le n w i
      simpa [Nat.succ_mul, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc] using Nat.add_le_add hfi ih

private theorem div_bound (m : Nat) (hm : 1 ≤ m) : (m-1)/8 < (m+7)/8 := by
  omega

private theorem bit_lt_capacity (n w i b : Nat) (hi : i < n) (hb : b < w) :
    layout.bit n w i b < n*w := by
  have hb1 : b + 1 ≤ w := by omega
  have hmul : (b+1)*n ≤ w*n := Nat.mul_le_mul_right n hb1
  have hsucc : b*n + n = (b+1)*n := by simp [Nat.succ_mul]
  have hcomm : w*n = n*w := Nat.mul_comm w n
  by_cases he : b % 2 = 0
  · have hsum : b*n + i < b*n + n := Nat.add_lt_add_left hi (b*n)
    simp [layout, he]
    omega
  · have hsub : n - 1 - i < n := by omega
    have hsum : b*n + (n-1-i) < b*n + n := Nat.add_lt_add_left hsub (b*n)
    simp [layout, he]
    omega

theorem sound_PageBudget : SerpentinePlane.PageBudget.Goal := by
  intro n w rows hrows
  change ((rows.flatMap (footprint layout n w)).eraseDups).length ≤
    min (byteCount (layout.capacity n w)) (rows.eraseDups.length * w)
  rw [Nat.le_min]
  constructor
  · have hsub : (rows.flatMap (footprint layout n w)).eraseDups ⊆
        List.range (byteCount (layout.capacity n w)) := by
      intro j hj
      have hflat : j ∈ rows.flatMap (footprint layout n w) := List.mem_eraseDups.mp hj
      rcases List.mem_flatMap.mp hflat with ⟨i, hi, hji⟩
      have hin : i < n := hrows i hi
      rcases (mem_footprint n w i j).mp hji with ⟨b, hb, heq⟩
      subst j
      simp only [List.mem_range]
      change (layout.bit n w i b) / 8 < (n*w + 7) / 8
      have hlt : layout.bit n w i b < n*w := bit_lt_capacity n w i b hin hb
      have hle : (layout.bit n w i b)/8 ≤ (n*w-1)/8 := Nat.div_le_div_right (by omega)
      have hbnd := div_bound (n*w) (by omega)
      omega
    have hlen := (eraseDups_nodup _).length_le_of_subset hsub
    simpa using hlen
  · have hsub : (rows.flatMap (footprint layout n w)).eraseDups ⊆
        rows.eraseDups.flatMap (footprint layout n w) := by
      intro j hj
      have hflat : j ∈ rows.flatMap (footprint layout n w) := List.mem_eraseDups.mp hj
      rcases List.mem_flatMap.mp hflat with ⟨i, hi, hji⟩
      have hi' : i ∈ rows.eraseDups := List.mem_eraseDups.mpr hi
      exact List.mem_flatMap.mpr ⟨i, hi', hji⟩
    have hlen := (eraseDups_nodup _).length_le_of_subset hsub
    calc
      ((rows.flatMap (footprint layout n w)).eraseDups).length
          ≤ (rows.eraseDups.flatMap (footprint layout n w)).length := hlen
      _ ≤ rows.eraseDups.length * w := flatMap_footprint_length_le n w rows.eraseDups

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentinePlane_PageBudget
