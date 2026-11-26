import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_PlanePredicate

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow

/-- `align 8 w` rounds `w` up to a multiple of eight, hence is at least `w`. -/
private theorem align8_ge (w : Nat) : w ≤ align 8 w := by
  unfold align
  have hdiv := Nat.div_add_mod (w + 7) 8
  rw [Nat.mul_comm 8 ((w + 7) / 8)] at hdiv
  have hmod := Nat.mod_lt (w + 7) (by omega : 0 < 8)
  omega

-- Physical bit position of lane `c` in row `i`: `i * align 8 w + c`.
private def pos (i c w : Nat) : Nat := i * align 8 w + c

private theorem pos_ne_of_row_ne (i j c d w : Nat) (hc : c < w) (hd : d < w)
    (hij : i ≠ j) : pos i c w ≠ pos j d w := by
  intro h
  have hApos : 0 < align 8 w := by
    have hw : 0 < w := by omega
    exact Nat.lt_of_lt_of_le hw (align8_ge w)
  have hcA : c < align 8 w := Nat.lt_of_lt_of_le hc (align8_ge w)
  have hdA : d < align 8 w := Nat.lt_of_lt_of_le hd (align8_ge w)
  have hdiv := congrArg (fun x : Nat => x / align 8 w) h
  unfold pos at hdiv
  rw [Nat.add_comm, Nat.add_mul_div_right c i hApos, Nat.div_eq_of_lt hcA, Nat.zero_add,
    Nat.add_comm, Nat.add_mul_div_right d j hApos, Nat.div_eq_of_lt hdA, Nat.zero_add] at hdiv
  exact hij hdiv

-- One lane insertion step, written with the physical position `pos i c w`.
private def step (x i w : Nat) (a c : Nat) : Nat :=
  if x.testBit c then a ||| 2 ^ (pos i c w) else a

private theorem fold_step_far (acc x i w p : Nat) (ls : List Nat)
    (hfar : ∀ c ∈ ls, pos i c w ≠ p) :
    (ls.foldl (step x i w) acc).testBit p = acc.testBit p := by
  induction ls generalizing acc with
  | nil => simp
  | cons c cs ih =>
      rw [List.foldl_cons]
      have hcne : pos i c w ≠ p := hfar c (by simp)
      have hbit : (step x i w acc c).testBit p = acc.testBit p := by
        by_cases hxc : x.testBit c
        · simp [step, hxc, Nat.testBit_or, hcne]
        · simp [step, hxc]
      calc
        (List.foldl (step x i w) (step x i w acc c) cs).testBit p =
            (step x i w acc c).testBit p := ih (step x i w acc c)
              (by intro d hd; exact hfar d (by simp [hd]))
        _ = acc.testBit p := hbit

private theorem fold_step_self (acc x i w b : Nat) (ls : List Nat)
    (hall : ∀ c ∈ ls, c < w) (hmem : b ∈ ls) (hnd : ls.Nodup) :
    (ls.foldl (step x i w) acc).testBit (pos i b w) =
      (acc.testBit (pos i b w) || x.testBit b) := by
  induction ls generalizing acc with
  | nil => simp at hmem
  | cons c cs ih =>
      rw [List.foldl_cons]
      have hnd' : cs.Nodup := (List.nodup_cons.mp hnd).2
      have hcnot : c ∉ cs := (List.nodup_cons.mp hnd).1
      have hcw : c < w := hall c (by simp)
      have hall' : ∀ d ∈ cs, d < w := by intro d hd; exact hall d (by simp [hd])
      by_cases hcb : c = b
      · subst c
        have hbnot : b ∉ cs := hcnot
        have hbw : b < w := hall b (by simp)
        have hfar : ∀ d ∈ cs, pos i d w ≠ pos i b w := by
          intro d hd heq
          have hdw : d < w := hall' d hd
          have hdb : d = b := by
            unfold pos at heq
            exact Nat.add_left_cancel heq
          exact hbnot (by simpa [hdb] using hd)
        have hbit : (step x i w acc b).testBit (pos i b w) =
            (acc.testBit (pos i b w) || x.testBit b) := by
          by_cases hxb : x.testBit b
          · simp [step, hxb, Nat.testBit_or]
          · simp [step, hxb]
        calc
          (List.foldl (step x i w) (step x i w acc b) cs).testBit (pos i b w) =
              (step x i w acc b).testBit (pos i b w) :=
                fold_step_far (step x i w acc b) x i w (pos i b w) cs hfar
          _ = (acc.testBit (pos i b w) || x.testBit b) := hbit
      · have hbcs : b ∈ cs := by
          simp only [List.mem_cons] at hmem
          rcases hmem with hbc | hbcs
          · exact False.elim (hcb hbc.symm)
          · exact hbcs
        have hbit : (step x i w acc c).testBit (pos i b w) = acc.testBit (pos i b w) := by
          have hne : pos i c w ≠ pos i b w := by
            intro heq
            unfold pos at heq
            exact hcb (Nat.add_left_cancel heq)
          by_cases hxc : x.testBit c
          · simp [step, hxc, Nat.testBit_or, hne]
          · simp [step, hxc]
        calc
          (List.foldl (step x i w) (step x i w acc c) cs).testBit (pos i b w) =
              ((step x i w acc c).testBit (pos i b w) || x.testBit b) :=
                ih (step x i w acc c) hall' hbcs hnd'
          _ = (acc.testBit (pos i b w) || x.testBit b) := by rw [hbit]

private def row (w i x acc : Nat) : Nat :=
  (List.range w).foldl (step x i w) acc

private theorem row_bit_self (acc x i w b : Nat) (hb : b < w) :
    (row w i x acc).testBit (pos i b w) = (acc.testBit (pos i b w) || x.testBit b) := by
  unfold row
  exact fold_step_self acc x i w b (List.range w)
    (by intro c hc; exact List.mem_range.mp hc)
    (List.mem_range.mpr hb) List.nodup_range

private theorem row_bit_far (acc x i w j b : Nat) (hb : b < w) (hji : j ≠ i) :
    (row w i x acc).testBit (pos j b w) = acc.testBit (pos j b w) := by
  unfold row
  apply fold_step_far
  intro c hc
  have hcw : c < w := List.mem_range.mp hc
  exact pos_ne_of_row_ne i j c b w hcw hb (by intro h; exact hji h.symm)

private def imageFrom (w off : Nat) (acc : Nat) (xs : List Nat) : Nat :=
  (xs.zipIdx off).foldl (fun a p => row w p.2 p.1 a) acc

private theorem image_eq_imageFrom (w : Nat) (xs : List Nat) :
    image layout w xs = imageFrom w 0 0 xs := by
  rfl

private theorem imageFrom_bit (w off i b acc : Nat) (xs : List Nat) (hb : b < w) :
    (imageFrom w off acc xs).testBit (pos i b w) =
      if off ≤ i ∧ i < off + xs.length then
        acc.testBit (pos i b w) || (xs[i - off]?.getD 0).testBit b
      else acc.testBit (pos i b w) := by
  induction xs generalizing off acc with
  | nil =>
      simp [imageFrom]
  | cons x xs ih =>
      rw [imageFrom, List.zipIdx_cons, List.foldl_cons]
      change (imageFrom w (off + 1) (row w off x acc) xs).testBit (pos i b w) = _
      rw [ih (off + 1) (row w off x acc)]
      by_cases hio : i = off
      · subst i
        rw [if_neg (by omega : ¬ (off + 1 ≤ off ∧ off < off + 1 + xs.length))]
        rw [if_pos (by simp : off ≤ off ∧ off < off + (x :: xs).length)]
        simp only [Nat.sub_self, List.getElem?_cons_zero, Option.getD_some]
        rw [row_bit_self acc x off w b hb]
      · by_cases hlt : i < off
        · rw [if_neg (by omega : ¬ (off + 1 ≤ i ∧ i < off + 1 + xs.length))]
          rw [if_neg (by omega : ¬ (off ≤ i ∧ i < off + (x :: xs).length))]
          exact row_bit_far acc x off w i b hb hio
        · have hgt : off < i := by omega
          by_cases hbounded : i < off + 1 + xs.length
          · have hc1 : off + 1 ≤ i ∧ i < off + 1 + xs.length := by omega
            have hc2 : off ≤ i ∧ i < off + (x :: xs).length := by
              simp only [List.length_cons]
              omega
            rw [if_pos hc1, if_pos hc2]
            rw [row_bit_far acc x off w i b hb hio]
            have hidx : i - off = (i - (off + 1)) + 1 := by omega
            simp [hidx]
          · have hc1 : ¬ (off + 1 ≤ i ∧ i < off + 1 + xs.length) := by
              intro h
              exact hbounded h.2
            have hc2 : ¬ (off ≤ i ∧ i < off + (x :: xs).length) := by
              intro h
              simp only [List.length_cons] at h
              omega
            rw [if_neg hc1, if_neg hc2]
            exact row_bit_far acc x off w i b hb hio

private theorem little_byte_bit (v k j r : Nat) (hj : j < k) (hr : r < 8) :
    (((List.range k).map (fun j => (v / 256 ^ j) % 256))[j]?.getD 0).testBit r =
      v.testBit (r + 8 * j) := by
  simp only [List.getElem?_map, List.getElem?_range hj, Option.map_some, Option.getD_some]
  have h256 : 256 = 2 ^ 8 := by decide
  rw [h256, ← Nat.pow_mul, Nat.testBit_mod_two_pow, Nat.testBit_div_two_pow]
  simp [hr]

theorem sound_PlanePredicate : ByteAlignedRow.PlanePredicate.Goal := by
  intro w xs b hb
  unfold planeRows
  dsimp [layout]
  unfold file
  apply List.filter_congr
  intro i hi
  simp only [List.mem_range] at hi
  let v := image layout w xs
  let p := pos i b w
  let k := byteCount (xs.length * align 8 w)
  change (atByte (little v k) (p / 8)).testBit (p % 8) = (xs[i]?.getD 0).testBit b
  have hp_lt : p < xs.length * align 8 w := by
    have hi' : i + 1 ≤ xs.length := Nat.succ_le_of_lt hi
    have hmul : (i + 1) * align 8 w ≤ xs.length * align 8 w :=
      Nat.mul_le_mul_right (align 8 w) hi'
    have hp_le : p < (i + 1) * align 8 w := by
      dsimp [p, pos]
      rw [Nat.succ_mul]
      have hbA : b < align 8 w := Nat.lt_of_lt_of_le hb (align8_ge w)
      omega
    exact Nat.lt_of_lt_of_le hp_le hmul
  have hj : p / 8 < k := by
    dsimp [k, byteCount]
    omega
  have hbyte : (atByte (little v k) (p / 8)).testBit (p % 8) =
      v.testBit ((p % 8) + 8 * (p / 8)) := by
    unfold atByte little
    exact little_byte_bit v k (p / 8) (p % 8) hj (Nat.mod_lt p (by decide : 0 < 8))
  rw [hbyte]
  have hdecomp : (p % 8) + 8 * (p / 8) = p := Nat.mod_add_div p 8
  rw [hdecomp]
  dsimp [p, v]
  rw [image_eq_imageFrom]
  have hcond : 0 ≤ i ∧ i < 0 + xs.length := by omega
  rw [imageFrom_bit w 0 i b 0 xs hb]
  rw [if_pos hcond]
  simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_ByteAlignedRow_PlanePredicate
