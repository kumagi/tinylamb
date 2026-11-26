import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `TileTranspose.PageBudget` — Count addressed bytes once and bound a gather by both
  the physical frame and the naive per-bit read count.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_PageBudget

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose

/-! In a tiled transpose the lane of significance `b` of row `i` lives in byte
`i/8*w + b`, so a single row contributes exactly `w` bytes.  The tile padding
`align 8 n` keeps every cell inside the declared capacity.  Hence both bounds
of the page budget follow from counting distinct bytes per distinct row. -/

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

private theorem align8_eq (n : Nat) : align 8 n = ((n + 7) / 8) * 8 := by
  unfold align
  have h : n + 8 - 1 = n + 7 := by omega
  rw [h]

private theorem ceil_succ (n : Nat) (hn : 1 ≤ n) : (n - 1) / 8 + 1 = (n + 7) / 8 := by
  omega

private theorem align8_mul (n w : Nat) : ((n + 7) / 8) * 8 * w = 8 * (((n + 7) / 8) * w) := by
  ac_rfl

private theorem byteCount_capacity (n w : Nat) :
    byteCount (layout.capacity n w) = ((n + 7) / 8) * w := by
  simp only [layout, align8_eq, byteCount]
  rw [align8_mul]
  omega

private theorem bit_lt_capacity (n w i b : Nat) (hi : i < n) (hb : b < w) :
    layout.bit n w i b < align 8 n * w := by
  rw [align8_eq]
  let q := (i / 8) * w + b
  have hle_i : i ≤ n - 1 := by omega
  have hdiv : i / 8 ≤ (n - 1) / 8 := Nat.div_le_div_right hle_i
  have hw : 1 ≤ w := by omega
  have hb_le : b ≤ w - 1 := by omega
  have hmul : (i / 8) * w ≤ ((n - 1) / 8) * w := Nat.mul_le_mul_right w hdiv
  have h1 : (i / 8) * w + b ≤ ((n - 1) / 8) * w + (w - 1) := Nat.add_le_add hmul hb_le
  have hq1 : q + 1 ≤ ((n - 1) / 8 + 1) * w := by
    dsimp [q]
    calc
      (i / 8) * w + b + 1 ≤ ((n - 1) / 8) * w + (w - 1) + 1 := Nat.succ_le_succ h1
      _ = ((n - 1) / 8) * w + w := by rw [Nat.add_assoc, Nat.sub_add_cancel hw]
      _ = ((n - 1) / 8 + 1) * w := by rw [Nat.succ_mul]
  have hn : 1 ≤ n := by omega
  have hq2 : q + 1 ≤ ((n + 7) / 8) * w := by
    rwa [ceil_succ n hn] at hq1
  have hbit : layout.bit n w i b = 8 * q + i % 8 := by
    dsimp [q]
    simp only [layout]
    rw [Nat.mul_add]
    ac_rfl
  have hlt1 : 8 * q + i % 8 < 8 * (q + 1) := by
    rw [Nat.mul_succ]
    have hmod : i % 8 < 8 := Nat.mod_lt i (by decide)
    omega
  have hle2 : 8 * (q + 1) ≤ 8 * (((n + 7) / 8) * w) := Nat.mul_le_mul_left 8 hq2
  calc
    layout.bit n w i b = 8 * q + i % 8 := hbit
    _ < 8 * (q + 1) := hlt1
    _ ≤ 8 * (((n + 7) / 8) * w) := hle2
    _ = ((n + 7) / 8) * 8 * w := by ac_rfl

theorem sound_PageBudget : TileTranspose.PageBudget.Goal := by
  intro n w rows hrows
  change ((rows.flatMap (footprint layout n w)).eraseDups).length ≤
    min (byteCount (layout.capacity n w)) (rows.eraseDups.length * w)
  rw [Nat.le_min]
  constructor
  · rw [byteCount_capacity]
    have hsub : (rows.flatMap (footprint layout n w)).eraseDups ⊆
        List.range (((n + 7) / 8) * w) := by
      intro j hj
      have hflat : j ∈ rows.flatMap (footprint layout n w) := List.mem_eraseDups.mp hj
      rcases List.mem_flatMap.mp hflat with ⟨i, hi, hji⟩
      have hin : i < n := hrows i hi
      rcases (mem_footprint n w i j).mp hji with ⟨b, hb, heq⟩
      subst j
      simp only [List.mem_range]
      have hlt : layout.bit n w i b < align 8 n * w := bit_lt_capacity n w i b hin hb
      rw [align8_eq] at hlt
      rw [align8_mul] at hlt
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_PageBudget
