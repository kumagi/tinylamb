import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_PageBudget

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble

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
    j ∈ footprint layout n w i ↔ ∃ b, b < w ∧ layout.bit n w i b / 8 = j := by
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

/-- Every occupied bit cell of a valid row lies strictly below n*w. -/
private theorem bit_lt (n w i b : Nat) (hi : i < n) (hb : b < w) :
    layout.bit n w i b < n*w := by
  change (if b < 4 then i * min 4 w + b else n * min 4 w + (b - 4) * n + i) < n*w
  by_cases hb4 : b < 4
  · simp only [ite_eq_left hb4]
    by_cases hw4 : w ≤ 4
    · have hmin : min 4 w = w := Nat.min_eq_right hw4
      rw [hmin]
      have h1 : i*w + b < i*w + w := Nat.add_lt_add_left hb (i*w)
      have h2 : i*w + w = (i+1)*w := by rw [Nat.succ_mul]
      have h3 : (i+1)*w ≤ n*w := Nat.mul_le_mul_right w (by omega)
      exact Nat.lt_of_lt_of_le (by simpa [h2] using h1) h3
    · have hwge : 4 ≤ w := by omega
      have hmin : min 4 w = 4 := Nat.min_eq_left hwge
      rw [hmin]
      calc
        i*4 + b < i*4 + 4 := Nat.add_lt_add_left hb4 (i*4)
        _ = (i+1)*4 := by rw [Nat.succ_mul]
        _ ≤ n*4 := Nat.mul_le_mul_right 4 (by omega)
        _ ≤ n*w := Nat.mul_le_mul_left n hwge
  · simp only [ite_eq_right hb4]
    have hwge : 4 ≤ w := by omega
    have hmin : min 4 w = 4 := Nat.min_eq_left hwge
    rw [hmin]
    have hb4le : 4 ≤ b := by omega
    have hle4 : n*4 ≤ b*n := by
      calc n*4 = 4*n := Nat.mul_comm _ _
           _ ≤ b*n := Nat.mul_le_mul_right n hb4le
    have hsub : (b-4)*n = b*n - n*4 := by
      rw [Nat.sub_mul, Nat.mul_comm 4 n]
    have heq : n*4 + (b-4)*n = b*n := by
      rw [hsub]
      exact Nat.add_sub_of_le hle4
    rw [heq]
    have h1 : b*n + i < (b+1)*n := by
      rw [Nat.succ_mul]
      exact Nat.add_lt_add_left hi (b*n)
    have h2 : (b+1)*n ≤ n*w := by
      calc (b+1)*n ≤ w*n := Nat.mul_le_mul_right n (by omega)
           _ = n*w := Nat.mul_comm _ _
    exact Nat.lt_of_lt_of_le h1 h2

theorem sound_PageBudget : SplitNibble.PageBudget.Goal := by
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
      change layout.bit n w i b / 8 < byteCount (n*w)
      have hbit := bit_lt n w i b hin hb
      have hnpos : 0 < n := by omega
      have hwpos : 0 < w := by omega
      have hmpos : 0 < n*w := Nat.mul_pos hnpos hwpos
      have hle : layout.bit n w i b / 8 ≤ (n*w - 1)/8 :=
        Nat.div_le_div_right (by omega)
      have hbnd := div_bound (n*w) hmpos
      simpa [byteCount] using (Nat.lt_of_le_of_lt hle hbnd)
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SplitNibble_PageBudget
