import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_PlanePredicate

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical

private theorem mem_lanes {b w : Nat} : b ∈ lanes w ↔ b < w := by
  simp [lanes, List.mem_range]

private def step (n i : Nat) (x acc c : Nat) : Nat :=
  if x.testBit c then acc ||| 2 ^ (c * n + i) else acc

private def row (n w i : Nat) (x acc : Nat) : Nat :=
  (lanes w).foldl (step n i x) acc

-- ORing a bit cell into a fold sets that bit iff some lane contributed it.
private theorem testBit_fold_step (n i : Nat) (x acc p : Nat) (bs : List Nat) :
    (bs.foldl (step n i x) acc).testBit p =
      (acc.testBit p || bs.any (fun b => x.testBit b && decide (b * n + i = p))) := by
  induction bs generalizing acc with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.any_cons]
      rw [ih]
      dsimp [step]
      by_cases hx : x.testBit b
      · rw [ite_eq_left hx, Nat.testBit_or, Nat.testBit_two_pow]
        by_cases heq : b * n + i = p
        · subst heq
          simp [hx]
        · simp [hx, heq]
      · rw [ite_eq_right hx]
        simp [hx]

private theorem row_testBit (n w i x acc p : Nat) :
    (row n w i x acc).testBit p =
      (acc.testBit p || (lanes w).any (fun b => x.testBit b && decide (b * n + i = p))) := by
  unfold row
  exact testBit_fold_step n i x acc p (lanes w)

private theorem fold_row_testBit (n w : Nat) (l : List (Nat × Nat)) (acc p : Nat) :
    (l.foldl (fun a q => row n w q.2 q.1 a) acc).testBit p =
      (acc.testBit p ||
        l.any (fun q => (lanes w).any (fun b => q.1.testBit b && decide (b * n + q.2 = p)))) := by
  induction l generalizing acc with
  | nil => simp
  | cons q qs ih =>
      simp only [List.foldl_cons, List.any_cons]
      rw [ih]
      rw [row_testBit]
      simp only [Bool.or_assoc]

private theorem image_testBit (w : Nat) (xs : List Nat) (p : Nat) :
    (image layout w xs).testBit p =
      xs.zipIdx.any
        (fun q => (lanes w).any (fun b => q.1.testBit b && decide (b * xs.length + q.2 = p))) := by
  unfold image layout
  change ((xs.zipIdx).foldl (fun a q => row xs.length w q.2 q.1 a) 0).testBit p = _
  rw [fold_row_testBit]
  simp

-- Vertical bit cells are injective for ordinals below the row count.
private theorem cell_injective {n c b i j : Nat} (hi : i < n) (hj : j < n)
    (heq : c * n + j = b * n + i) : c = b ∧ j = i := by
  have hn : 0 < n := by omega
  have hcdiv : (c * n + j) / n = c := by
    rw [Nat.add_comm, Nat.add_mul_div_right j c hn, Nat.div_eq_of_lt hj, Nat.zero_add]
  have hbdiv : (b * n + i) / n = b := by
    rw [Nat.add_comm, Nat.add_mul_div_right i b hn, Nat.div_eq_of_lt hi, Nat.zero_add]
  have hcb : c = b := by
    rw [← hcdiv, ← hbdiv, heq]
  subst c
  exact ⟨rfl, by omega⟩

-- The bit written for row i at significance b is exactly xs[i]'s bit b.
private theorem image_bit_at (w : Nat) (xs : List Nat) (i b : Nat)
    (hi : i < xs.length) (hb : b < w) :
    (image layout w xs).testBit (b * xs.length + i) = (xs[i]?.getD 0).testBit b := by
  rw [image_testBit]
  rw [Bool.eq_iff_iff]
  constructor
  · intro h
    rw [List.any_eq_true] at h
    rcases h with ⟨⟨x, j⟩, hjmem, hj⟩
    rw [List.any_eq_true] at hj
    rcases hj with ⟨c, hc, hcond⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
    have hcw : c < w := by simpa [mem_lanes] using hc
    have hjlt : j < xs.length := by
      have := List.snd_lt_of_mem_zipIdx hjmem
      simpa [List.length_zipIdx] using this
    have hxmem : xs[j]? = some x := List.mem_zipIdx_iff_getElem?.mp hjmem
    have hx : x = xs[j]?.getD 0 := by
      rw [List.getElem?_eq_getElem hjlt] at hxmem
      rw [List.getElem?_eq_getElem hjlt]
      simpa using hxmem.symm
    have hinj := cell_injective (n := xs.length) hi hjlt hcond.2
    have hcb : c = b := hinj.1
    have hji : j = i := hinj.2
    subst c
    subst j
    rw [← hx]
    exact hcond.1
  · intro h
    rw [List.any_eq_true]
    refine ⟨(xs[i]?.getD 0, i), ?_, ?_⟩
    · rw [List.mem_zipIdx_iff_getElem?]
      rw [List.getElem?_eq_getElem hi]
      simp
    · rw [List.any_eq_true]
      refine ⟨b, by simp [mem_lanes, hb], ?_⟩
      simp [h]

private theorem atByte_little (n k j : Nat) (hj : j < k) :
    atByte (little n k) j = (n / 256 ^ j) % 256 := by
  unfold atByte little
  have hj' : j < ((List.range k).map (fun i => (n / 256 ^ i) % 256)).length := by
    simp [List.length_map, List.length_range, hj]
  rw [List.getElem?_eq_getElem hj']
  simp

private theorem little_byte_bit (v k j r : Nat) (hj : j < k) (hr : r < 8) :
    (atByte (little v k) j).testBit r = v.testBit (r + 8 * j) := by
  rw [atByte_little v k j hj]
  have h256 : 256 = 2 ^ 8 := by decide
  have hpow : 256 ^ j = 2 ^ (8 * j) := by rw [h256, Nat.pow_mul]
  rw [hpow, h256, Nat.testBit_mod_two_pow]
  simp [hr]
  rw [Nat.testBit_div_two_pow]

private theorem file_testBit (w : Nat) (xs : List Nat) (j r : Nat)
    (hj : j < byteCount (layout.capacity xs.length w)) (hr : r < 8) :
    (atByte (file layout w xs) j).testBit r = (image layout w xs).testBit (j * 8 + r) := by
  unfold file
  rw [little_byte_bit _ _ j r hj hr]
  congr 1
  omega

theorem sound_PlanePredicate : Vertical.PlanePredicate.Goal := by
  intro w xs b hb
  unfold planeRows
  dsimp [layout]
  apply List.filter_congr
  intro i hi
  simp only [List.mem_range] at hi
  let p := b * xs.length + i
  change (atByte (file layout w xs) (p / 8)).testBit (p % 8) = (xs[i]?.getD 0).testBit b
  have hn : 0 < xs.length := by omega
  have hbsucc : b + 1 ≤ w := Nat.succ_le_of_lt hb
  have hmul : (b + 1) * xs.length ≤ xs.length * w := by
    rw [Nat.mul_comm xs.length w]
    exact Nat.mul_le_mul_right xs.length hbsucc
  have hpnext : b * xs.length + i < (b + 1) * xs.length := by
    rw [Nat.succ_mul]
    omega
  have hp : p < xs.length * w := by
    dsimp [p]
    exact Nat.lt_of_lt_of_le hpnext hmul
  have hj : p / 8 < byteCount (layout.capacity xs.length w) := by
    dsimp [layout, byteCount]
    omega
  rw [file_testBit w xs (p / 8) (p % 8) hj (Nat.mod_lt p (by decide : 0 < 8))]
  have hdecomp : p / 8 * 8 + p % 8 = p := by
    have := Nat.div_add_mod p 8
    omega
  rw [hdecomp]
  dsimp [p]
  exact image_bit_at w xs i b hi hb

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_PlanePredicate
