import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentinePlane_PlanePredicate

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane

-- Physical position of lane `b` in row `i`: reverse the row order on odd planes.
private def off (b i n : Nat) : Nat := if b % 2 = 0 then i else n - 1 - i
private def pos (n _w i b : Nat) : Nat := b * n + off b i n

private theorem off_lt (b i n : Nat) (hi : i < n) : off b i n < n := by
  unfold off
  split
  · exact hi
  · omega

private theorem pos_div (n _w i b : Nat) (hi : i < n) : pos n _w i b / n = b := by
  have hn : 0 < n := by omega
  unfold pos
  rw [Nat.add_comm, Nat.add_mul_div_right (off b i n) b hn,
    Nat.div_eq_of_lt (off_lt b i n hi), Nat.zero_add]

private theorem pos_inj_lane (n w i c d : Nat) (hi : i < n) (_hc : c < w) (_hd : d < w)
    (h : pos n w i c = pos n w i d) : c = d := by
  have hdiv := congrArg (fun x : Nat => x / n) h
  rw [pos_div n w i c hi, pos_div n w i d hi] at hdiv
  exact hdiv

private theorem pos_ne_of_row_ne (n w j i c d : Nat) (hj : j < n) (hi : i < n)
    (_hc : c < w) (_hd : d < w) (hji : j ≠ i) : pos n w j c ≠ pos n w i d := by
  intro h
  have hdiv := congrArg (fun x : Nat => x / n) h
  rw [pos_div n w j c hj, pos_div n w i d hi] at hdiv
  subst d
  have hoff : off c j n = off c i n := Nat.add_left_cancel h
  unfold off at hoff
  by_cases hp : c % 2 = 0
  · simp [hp] at hoff
    exact hji hoff
  · simp [hp] at hoff
    omega

private def step (n w i x acc c : Nat) : Nat :=
  if x.testBit c then acc ||| 2 ^ (pos n w i c) else acc

private theorem fold_step_far (n w i acc x p : Nat) (ls : List Nat)
    (hfar : ∀ c ∈ ls, pos n w i c ≠ p) :
    (ls.foldl (step n w i x) acc).testBit p = acc.testBit p := by
  induction ls generalizing acc with
  | nil => simp
  | cons c cs ih =>
      rw [List.foldl_cons]
      have hcne : pos n w i c ≠ p := hfar c (by simp)
      have hbit : (step n w i x acc c).testBit p = acc.testBit p := by
        by_cases hxc : x.testBit c
        · simp [step, hxc, Nat.testBit_or, hcne]
        · simp [step, hxc]
      calc
        (List.foldl (step n w i x) (step n w i x acc c) cs).testBit p =
            (step n w i x acc c).testBit p := ih (step n w i x acc c)
              (by intro d hd; exact hfar d (by simp [hd]))
        _ = acc.testBit p := hbit

private theorem fold_step_self (n w i acc x b : Nat) (ls : List Nat)
    (hall : ∀ c ∈ ls, c < w) (hi : i < n) (hmem : b ∈ ls) (hnd : ls.Nodup) :
    (ls.foldl (step n w i x) acc).testBit (pos n w i b) =
      (acc.testBit (pos n w i b) || x.testBit b) := by
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
        have hfar : ∀ d ∈ cs, pos n w i d ≠ pos n w i b := by
          intro d hd heq
          have hdw : d < w := hall' d hd
          have hdb : d = b := pos_inj_lane n w i d b hi hdw hbw heq
          exact hbnot (by simpa [hdb] using hd)
        have hbit : (step n w i x acc b).testBit (pos n w i b) =
            (acc.testBit (pos n w i b) || x.testBit b) := by
          by_cases hxb : x.testBit b
          · simp [step, hxb, Nat.testBit_or]
          · simp [step, hxb]
        calc
          (List.foldl (step n w i x) (step n w i x acc b) cs).testBit (pos n w i b) =
              (step n w i x acc b).testBit (pos n w i b) :=
                fold_step_far n w i (step n w i x acc b) x (pos n w i b) cs hfar
          _ = (acc.testBit (pos n w i b) || x.testBit b) := hbit
      · have hbcs : b ∈ cs := by
          simp only [List.mem_cons] at hmem
          rcases hmem with hbc | hbcs
          · exact False.elim (hcb hbc.symm)
          · exact hbcs
        have hbit : (step n w i x acc c).testBit (pos n w i b) = acc.testBit (pos n w i b) := by
          have hne : pos n w i c ≠ pos n w i b := by
            intro heq
            exact hcb (pos_inj_lane n w i c b hi hcw (hall b hmem) heq)
          by_cases hxc : x.testBit c
          · simp [step, hxc, Nat.testBit_or, hne]
          · simp [step, hxc]
        calc
          (List.foldl (step n w i x) (step n w i x acc c) cs).testBit (pos n w i b) =
              ((step n w i x acc c).testBit (pos n w i b) || x.testBit b) :=
                ih (step n w i x acc c) hall' hbcs hnd'
          _ = (acc.testBit (pos n w i b) || x.testBit b) := by rw [hbit]

private def row (n w i x acc : Nat) : Nat := (List.range w).foldl (step n w i x) acc

private theorem row_bit_self (n w i acc x b : Nat) (hb : b < w) (hi : i < n) :
    (row n w i x acc).testBit (pos n w i b) = (acc.testBit (pos n w i b) || x.testBit b) := by
  unfold row
  exact fold_step_self n w i acc x b (List.range w)
    (by intro c hc; exact List.mem_range.mp hc) hi (List.mem_range.mpr hb) List.nodup_range

private theorem row_bit_far (n w i j acc x b : Nat) (hb : b < w) (hi : i < n) (hj : j < n)
    (hji : j ≠ i) :
    (row n w j x acc).testBit (pos n w i b) = acc.testBit (pos n w i b) := by
  unfold row
  apply fold_step_far
  intro c hc
  have hcw : c < w := List.mem_range.mp hc
  exact pos_ne_of_row_ne n w j i c b hj hi hcw hb hji

private def imageFrom (n w off : Nat) (acc : Nat) (xs : List Nat) : Nat :=
  (xs.zipIdx off).foldl (fun a p => row n w p.2 p.1 a) acc

private theorem image_eq_imageFrom (w : Nat) (xs : List Nat) :
    image layout w xs = imageFrom xs.length w 0 0 xs := by
  rfl

private theorem imageFrom_bit (n w off i b acc : Nat) (xs : List Nat)
    (hoff : off + xs.length ≤ n) (hi : i < n) (hb : b < w) :
    (imageFrom n w off acc xs).testBit (pos n w i b) =
      if off ≤ i ∧ i < off + xs.length then
        acc.testBit (pos n w i b) || (xs[i - off]?.getD 0).testBit b
      else acc.testBit (pos n w i b) := by
  induction xs generalizing off acc with
  | nil =>
      simp [imageFrom]
  | cons x xs ih =>
      rw [imageFrom, List.zipIdx_cons, List.foldl_cons]
      change (imageFrom n w (off + 1) (row n w off x acc) xs).testBit (pos n w i b) = _
      have hlen : (x :: xs).length = xs.length + 1 := by simp
      have hoff' : off + 1 + xs.length ≤ n := by omega
      rw [ih (off + 1) (row n w off x acc) hoff']
      have hofflt : off < n := by omega
      by_cases hio : i = off
      · subst i
        rw [ite_eq_right (by omega : ¬ (off + 1 ≤ off ∧ off < off + 1 + xs.length))]
        rw [ite_eq_left (by simp : off ≤ off ∧ off < off + (x :: xs).length)]
        simp only [Nat.sub_self, List.getElem?_cons_zero, Option.getD_some]
        rw [row_bit_self n w off acc x b hb hofflt]
      · by_cases hlt : i < off
        · rw [ite_eq_right (by omega : ¬ (off + 1 ≤ i ∧ i < off + 1 + xs.length))]
          rw [ite_eq_right (by omega : ¬ (off ≤ i ∧ i < off + (x :: xs).length))]
          exact row_bit_far n w i off acc x b hb hi hofflt (Ne.symm hio)
        · have hgt : off < i := by omega
          by_cases hbounded : i < off + 1 + xs.length
          · have hc1 : off + 1 ≤ i ∧ i < off + 1 + xs.length := by omega
            have hc2 : off ≤ i ∧ i < off + (x :: xs).length := by omega
            rw [ite_eq_left hc1, ite_eq_left hc2]
            rw [row_bit_far n w i off acc x b hb hi hofflt (Ne.symm hio)]
            have hidx : i - off = (i - (off + 1)) + 1 := by omega
            simp [hidx]
          · have hc1 : ¬ (off + 1 ≤ i ∧ i < off + 1 + xs.length) := by
              intro h
              exact hbounded h.2
            have hc2 : ¬ (off ≤ i ∧ i < off + (x :: xs).length) := by
              intro h
              omega
            rw [ite_eq_right hc1, ite_eq_right hc2]
            exact row_bit_far n w i off acc x b hb hi hofflt (Ne.symm hio)

private theorem little_byte_bit (v k j r : Nat) (hj : j < k) (hr : r < 8) :
    (((List.range k).map (fun j => (v / 256 ^ j) % 256))[j]?.getD 0).testBit r =
      v.testBit (r + 8 * j) := by
  simp only [List.getElem?_map, List.getElem?_range hj, Option.map_some, Option.getD_some]
  have h256 : 256 = 2 ^ 8 := by decide
  rw [h256, ← Nat.pow_mul, Nat.testBit_mod_two_pow, Nat.testBit_div_two_pow]
  simp [hr]

theorem sound_PlanePredicate : SerpentinePlane.PlanePredicate.Goal := by
  intro w xs b hb
  unfold planeRows
  dsimp [layout]
  unfold file
  apply List.filter_congr
  intro i hi
  simp only [List.mem_range] at hi
  let n := xs.length
  let v := image layout w xs
  let p := pos n w i b
  let k := byteCount (n * w)
  change (atByte (little v k) (p / 8)).testBit (p % 8) = (xs[i]?.getD 0).testBit b
  have hp_lt : p < n * w := by
    have hb' : b + 1 ≤ w := Nat.succ_le_of_lt hb
    have hmul : (b + 1) * n ≤ w * n := Nat.mul_le_mul_right n hb'
    have hp_le : p < (b + 1) * n := by
      dsimp [p, pos]
      rw [Nat.succ_mul]
      have ho := off_lt b i n hi
      omega
    rw [Nat.mul_comm n w]
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
  dsimp [p, v, n]
  rw [image_eq_imageFrom]
  rw [imageFrom_bit xs.length w 0 i b 0 xs (by omega) (by omega) hb]
  simp [hi]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentinePlane_PlanePredicate
