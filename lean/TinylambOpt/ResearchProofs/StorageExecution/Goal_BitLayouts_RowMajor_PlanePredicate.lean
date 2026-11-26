import TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_Nonoverlap

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `RowMajor.PlanePredicate` — BIT_TEST can produce matching row IDs from a single
  physical bit per row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_PlanePredicate

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor
open TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_Nonoverlap

private def step (w i : Nat) (x acc c : Nat) : Nat :=
  if x.testBit c then acc ||| 2 ^ (i * w + c) else acc

private def row (w i : Nat) (x acc : Nat) : Nat :=
  (lanes w).foldl (step w i x) acc

private theorem testBit_fold_step (w i : Nat) (x acc p : Nat) (bs : List Nat) :
    (bs.foldl (step w i x) acc).testBit p =
      (acc.testBit p || bs.any (fun b => x.testBit b && decide (i * w + b = p))) := by
  induction bs generalizing acc with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.any_cons]
      rw [ih]
      dsimp [step]
      by_cases hx : x.testBit b
      · rw [ite_eq_left hx, Nat.testBit_or, Nat.testBit_two_pow]
        by_cases heq : i * w + b = p
        · subst heq
          simp [hx]
        · simp [hx, heq]
      · rw [ite_eq_right hx]
        simp [hx]

private theorem row_testBit (w i x acc p : Nat) :
    (row w i x acc).testBit p =
      (acc.testBit p || (lanes w).any (fun b => x.testBit b && decide (i * w + b = p))) := by
  unfold row
  exact testBit_fold_step w i x acc p (lanes w)

private theorem fold_row_testBit (w : Nat) (l : List (Nat × Nat)) (acc p : Nat) :
    (l.foldl (fun a q => row w q.2 q.1 a) acc).testBit p =
      (acc.testBit p ||
        l.any (fun q => (lanes w).any (fun b => q.1.testBit b && decide (q.2 * w + b = p)))) := by
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
        (fun q => (lanes w).any (fun b => q.1.testBit b && decide (q.2 * w + b = p))) := by
  unfold image layout
  change ((xs.zipIdx).foldl (fun a q => row w q.2 q.1 a) 0).testBit p = _
  rw [fold_row_testBit]
  simp

private theorem rowMajor_nonoverlap : nonoverlap layout := by
  intro n w i j a b hi hj ha hb
  exact sound_Nonoverlap n w i j a b hi hj ha hb

private theorem image_bit_at (w : Nat) (xs : List Nat) (i b : Nat)
    (hi : i < xs.length) (hb : b < w) :
    (image layout w xs).testBit (layout.bit xs.length w i b) =
      (xs[i]?.getD 0).testBit b := by
  rw [image_testBit]
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    rw [List.any_eq_true] at h
    rcases h with ⟨⟨x, j⟩, hjmem, hj⟩
    rw [List.any_eq_true] at hj
    rcases hj with ⟨a, ha, hcond⟩
    simp only [Bool.and_eq_true, decide_eq_true_eq] at hcond
    have haw : a < w := by simpa [lanes] using ha
    have hjlt : j < xs.length := by
      have := List.snd_lt_of_mem_zipIdx hjmem
      simpa [List.length_zipIdx] using this
    have hxmem : xs[j]? = some x := List.mem_zipIdx_iff_getElem?.mp hjmem
    have hx : x = xs[j]?.getD 0 := by
      rw [List.getElem?_eq_getElem hjlt] at hxmem
      rw [List.getElem?_eq_getElem hjlt]
      simpa using hxmem.symm
    have hinj := (rowMajor_nonoverlap xs.length w j i a b hjlt hi haw hb).mp hcond.2
    obtain ⟨hji, hab⟩ := hinj
    subst j
    subst a
    simpa [hx] using hcond.1
  · intro h
    rw [List.any_eq_true]
    refine ⟨(xs[i]?.getD 0, i), ?_, ?_⟩
    · rw [List.mem_zipIdx_iff_getElem?]
      simp [hi]
    · rw [List.any_eq_true]
      refine ⟨b, by simp [lanes, hb], ?_⟩
      simp [h, layout]

private theorem atByte_little (n k j : Nat) (hj : j < k) :
    atByte (little n k) j = (n / 256 ^ j) % 256 := by
  unfold atByte little
  have hj' : j < ((List.range k).map (fun i => (n / 256 ^ i) % 256)).length := by
    simp [List.length_map, List.length_range, hj]
  rw [List.getElem?_eq_getElem hj']
  simp

private theorem little_testBit (v k j r : Nat) (hj : j < k) (hr : r < 8) :
    (atByte (little v k) j).testBit r = v.testBit (8 * j + r) := by
  rw [atByte_little v k j hj]
  have h256 : (256 : Nat) = 2 ^ 8 := by decide
  have hpow : 256 ^ j = 2 ^ (8 * j) := by rw [h256, Nat.pow_mul]
  rw [hpow, h256, Nat.testBit_mod_two_pow]
  simp only [hr, decide_true, Bool.true_and]
  rw [← Nat.testBit_add]
  congr 1
  omega

private theorem file_testBit (w : Nat) (xs : List Nat) (j r : Nat)
    (hj : j < byteCount (layout.capacity xs.length w)) (hr : r < 8) :
    (atByte (file layout w xs) j).testBit r = (image layout w xs).testBit (j * 8 + r) := by
  unfold file
  rw [little_testBit _ _ j r hj hr]
  congr 1
  omega

private theorem bit_bound (n w i b : Nat) (hi : i < n) (hb : b < w) :
    layout.bit n w i b < 8 * byteCount (layout.capacity n w) := by
  have hbit : i * w + b < n * w := by
    have hmul : (i + 1) * w ≤ n * w := Nat.mul_le_mul_right w (by omega)
    rw [Nat.succ_mul] at hmul
    omega
  change i * w + b < 8 * byteCount (n * w)
  unfold byteCount
  omega

/-- A serialized row-major plane bit is exactly the corresponding source bit. -/
theorem sound_PlanePredicate : RowMajor.PlanePredicate.Goal := by
  intro w xs b hb
  unfold planeRows
  apply List.filter_congr
  intro i hi
  have hi' : i < xs.length := by simpa using hi
  let p := layout.bit xs.length w i b
  change (atByte (file layout w xs) (p / 8)).testBit (p % 8) =
    (xs[i]?.getD 0).testBit b
  have hp := bit_bound xs.length w i b hi' hb
  have hj : p / 8 < byteCount (layout.capacity xs.length w) := by omega
  rw [file_testBit w xs (p / 8) (p % 8) hj (Nat.mod_lt p (by decide : 0 < 8))]
  have hdecomp : p / 8 * 8 + p % 8 = p := by
    have := Nat.div_add_mod p 8
    omega
  rw [hdecomp]
  exact image_bit_at w xs i b hi' hb

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_PlanePredicate
