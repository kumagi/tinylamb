import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_PlanePredicate

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor

-- One lane insertion step, written with the physical position `i*w+c`.
private def step (x i w : Nat) (a c : Nat) : Nat :=
  if x.testBit c then a ||| 2 ^ (i*w+c) else a

private theorem fold_step_far (acc x i w p : Nat) (ls : List Nat)
    (hfar : ∀ c ∈ ls, i*w+c ≠ p) :
    (ls.foldl (step x i w) acc).testBit p = acc.testBit p := by
  induction ls generalizing acc with
  | nil => simp
  | cons c cs ih =>
      rw [List.foldl_cons]
      have hcne : i*w+c ≠ p := hfar c (by simp)
      have hbit : (step x i w acc c).testBit p = acc.testBit p := by
        by_cases hxc : x.testBit c
        · simp [step, hxc, Nat.testBit_or, Nat.testBit_two_pow, hcne]
        · simp [step, hxc]
      rw [hbit]
      exact ih acc (by intro d hd; exact hfar d (by simp [hd]))

private theorem fold_step_self (acc x i w b : Nat) (ls : List Nat)
    (hmem : b ∈ ls) (hnd : ls.Nodup) :
    (ls.foldl (step x i w) acc).testBit (i*w+b) =
      (acc.testBit (i*w+b) || x.testBit b) := by
  induction ls generalizing acc with
  | nil => simp at hmem
  | cons c cs ih =>
      rw [List.foldl_cons]
      have hnd' : cs.Nodup := (List.nodup_cons.mp hnd).2
      have hcnot : c ∉ cs := (List.nodup_cons.mp hnd).1
      by_cases hcb : c = b
      · subst c
        have hbnot : b ∉ cs := hcnot
        have hfar : ∀ d ∈ cs, i*w+d ≠ i*w+b := by
          intro d hd heq
          have hdb : d = b := by omega
          exact hbnot (by simpa [hdb] using hd)
        have hbit : (step x i w acc b).testBit (i*w+b) =
            (acc.testBit (i*w+b) || x.testBit b) := by
          by_cases hxb : x.testBit b
          · simp [step, hxb, Nat.testBit_or, Nat.testBit_two_pow]
          · simp [step, hxb]
        rw [hbit]
        rw [fold_step_far (step x i w acc b) x i w (i*w+b) cs hfar]
      · have hbcs : b ∈ cs := by
          simp only [List.mem_cons] at hmem
          rcases hmem with hbc | hbcs
          · exact False.elim (hcb hbc.symm)
          · exact hbcs
        have hbit : (step x i w acc c).testBit (i*w+b) = acc.testBit (i*w+b) := by
          have hne : i*w+c ≠ i*w+b := by omega
          by_cases hxc : x.testBit c
          · simp [step, hxc, Nat.testBit_or, Nat.testBit_two_pow, hne]
          · simp [step, hxc]
        rw [hbit]
        exact ih acc hbcs hnd'

private def row (w i x acc : Nat) : Nat :=
  (List.range w).foldl (step x i w) acc

private theorem row_bit_self (acc x i w b : Nat) (hb : b < w) :
    (row w i x acc).testBit (i*w+b) = (acc.testBit (i*w+b) || x.testBit b) := by
  unfold row
  exact fold_step_self acc x i w b (List.range w) (List.mem_range.mpr hb) List.nodup_range

private theorem row_bit_far (acc x i w j b : Nat) (hb : b < w) (hji : j ≠ i) :
    (row w i x acc).testBit (j*w+b) = acc.testBit (j*w+b) := by
  unfold row
  apply fold_step_far
  intro c hc
  have hcw : c < w := List.mem_range.mp hc
  have hwpos : 0 < w := by omega
  intro heq
  have hcdiv : (i*w+c)/w = i := by
    rw [Nat.add_comm, Nat.add_mul_div_right c i hwpos, Nat.div_eq_of_lt hcw, Nat.zero_add]
  have hbdiv : (j*w+b)/w = j := by
    rw [Nat.add_comm, Nat.add_mul_div_right b j hwpos, Nat.div_eq_of_lt hb, Nat.zero_add]
  have hij : i = j := by
    rw [← hcdiv, ← hbdiv, heq]
  exact hji hij.symm

private def imageFrom (w off : Nat) (acc : Nat) (xs : List Nat) : Nat :=
  (xs.zipIdx off).foldl (fun a p => row w p.2 p.1 a) acc

private theorem image_eq_imageFrom (w : Nat) (xs : List Nat) :
    image layout w xs = imageFrom w 0 0 xs := by
  rfl

private theorem imageFrom_bit (w off i b acc : Nat) (xs : List Nat) (hb : b < w) :
    (imageFrom w off acc xs).testBit (i*w+b) =
      if off ≤ i ∧ i < off + xs.length then
        acc.testBit (i*w+b) || (xs[i-off]?.getD 0).testBit b
      else acc.testBit (i*w+b) := by
  induction xs generalizing off acc with
  | nil =>
      have hnot : ¬ (off ≤ i ∧ i < off + 0) := by omega
      simp [imageFrom, hnot]
  | cons x xs ih =>
      rw [imageFrom, List.zipIdx_cons, List.foldl_cons]
      change (imageFrom w (off+1) (row w off x acc) xs).testBit (i*w+b) = _
      rw [ih (off+1) (row w off x acc)]
      by_cases hio : i = off
      · subst i
        have hnot : ¬ (off+1 ≤ off ∧ off < off+1+xs.length) := by omega
        simp [hnot, row_bit_self acc x off w b hb]
      · by_cases hlt : i < off
        · have hnot1 : ¬ (off ≤ i ∧ i < off + (x::xs).length) := by
            simp only [List.length_cons]
            omega
          have hnot2 : ¬ (off+1 ≤ i ∧ i < off+1 + xs.length) := by omega
          simp [hnot1, hnot2, row_bit_far acc x off w i b hb hio]
        · have hgt : off < i := by omega
          have hcond1 : off ≤ i ∧ i < off + (x::xs).length := by
            simp only [List.length_cons]
            omega
          have hcond2 : off+1 ≤ i ∧ i < off+1 + xs.length := by omega
          have hidx : i - off = (i - (off+1)) + 1 := by omega
          simp [hcond1, hcond2, hidx, row_bit_far acc x off w i b hb hio]

private theorem little_byte_bit (v k j r : Nat) (hj : j < k) (hr : r < 8) :
    (((List.range k).map (fun j => (v / 256^j) % 256))[j]?.getD 0).testBit r =
      v.testBit (r + 8*j) := by
  rw [List.getElem?_eq_getElem (by simpa using hj)]
  simp only [Option.getD_some, List.getElem?_map, List.getElem?_range hj,
    Option.map_some]
  have h256 : 256 = 2 ^ 8 := by decide
  have hpow : 256 ^ j = 2 ^ (8*j) := by rw [h256, Nat.pow_mul]
  rw [hpow, Nat.testBit_mod_two_pow]
  simp [hr]

theorem sound_PlanePredicate : RowMajor.PlanePredicate.Goal := by
  intro w xs b hb
  unfold planeRows
  dsimp [layout]
  unfold file
  apply List.filter_congr
  intro i hi
  simp only [List.mem_range] at hi
  let v := image layout w xs
  let p := i*w+b
  let k := byteCount (xs.length*w)
  change (atByte (little v k) (p/8)).testBit (p%8) = (xs[i]?.getD 0).testBit b
  have hp_lt : p < xs.length*w := by
    have hi' : i + 1 ≤ xs.length := Nat.succ_le_of_lt hi
    have hmul : (i+1)*w ≤ xs.length*w := Nat.mul_le_mul_right w hi'
    have hp_le : p < (i+1)*w := by
      dsimp [p]
      rw [Nat.succ_mul]
      omega
    exact Nat.lt_of_lt_of_le hp_le hmul
  have hj : p/8 < k := by
    dsimp [k, byteCount]
    omega
  have hbyte : (atByte (little v k) (p/8)).testBit (p%8) =
      v.testBit ((p%8) + 8*(p/8)) := by
    unfold atByte little
    exact little_byte_bit v k (p/8) (p%8) hj (Nat.mod_lt p (by decide : 0 < 8))
  rw [hbyte]
  have hdecomp : (p%8) + 8*(p/8) = p := Nat.mod_add_div p 8
  rw [hdecomp]
  dsimp [p, v]
  rw [image_eq_imageFrom]
  have hcond : 0 ≤ i ∧ i < 0 + xs.length := by omega
  rw [imageFrom_bit w 0 i b 0 xs hb]
  simp [hcond]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_PlanePredicate
