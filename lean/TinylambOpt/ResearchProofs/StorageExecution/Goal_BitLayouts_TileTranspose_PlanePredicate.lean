import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `TileTranspose.PlanePredicate` — BIT_TEST can produce matching row IDs from a single
  physical bit per row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_PlanePredicate

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose

-- Physical position of lane `b` of row `i`: tile-major, one byte per plane.
private def pos (w i b : Nat) : Nat := (i / 8) * 8 * w + b * 8 + i % 8

private theorem pos_mod8 (w i b : Nat) : pos w i b % 8 = i % 8 := by
  unfold pos
  have h1 : (i / 8) * 8 * w + b * 8 = 8 * ((i / 8) * w + b) := by
    simp [Nat.mul_add, Nat.mul_comm, Nat.mul_left_comm]
  rw [h1, Nat.add_comm, Nat.add_mul_mod_self_left,
    Nat.mod_eq_of_lt (Nat.mod_lt i (by decide))]

private theorem pos_div8 (w i b : Nat) : pos w i b / 8 = (i / 8) * w + b := by
  unfold pos
  have h1 : (i / 8) * 8 * w + b * 8 = 8 * ((i / 8) * w + b) := by
    simp [Nat.mul_add, Nat.mul_comm, Nat.mul_left_comm]
  rw [h1, Nat.mul_comm 8 ((i / 8) * w + b), Nat.add_comm,
    Nat.add_mul_div_right (i % 8) ((i / 8) * w + b) (by decide : 0 < 8),
    Nat.div_eq_of_lt (Nat.mod_lt i (by decide)), Nat.zero_add]

private theorem pos_inj (w j i c d : Nat) (hc : c < w) (hd : d < w)
    (h : pos w j c = pos w i d) : j = i ∧ c = d := by
  have hw : 0 < w := by omega
  have hmod := congrArg (fun x : Nat => x % 8) h
  rw [pos_mod8, pos_mod8] at hmod
  have hdiv := congrArg (fun x : Nat => x / 8) h
  rw [pos_div8, pos_div8] at hdiv
  have hcd : c = d := by
    have hm := congrArg (fun x : Nat => x % w) hdiv
    simpa [Nat.add_mod, Nat.mul_mod_left, Nat.mod_eq_of_lt hc, Nat.mod_eq_of_lt hd] using hm
  subst d
  have ht : j / 8 = i / 8 := by
    exact Nat.eq_of_mul_eq_mul_right hw (Nat.add_right_cancel hdiv)
  have hji : j = i := by
    have hj := Nat.div_add_mod j 8
    have hi := Nat.div_add_mod i 8
    omega
  exact ⟨hji, rfl⟩

private theorem pos_inj_lane (w i c d : Nat) (hc : c < w) (hd : d < w)
    (h : pos w i c = pos w i d) : c = d := (pos_inj w i i c d hc hd h).2

private theorem pos_ne_of_row_ne (w j i c d : Nat) (hc : c < w) (hd : d < w)
    (hji : j ≠ i) : pos w j c ≠ pos w i d := by
  intro h
  exact hji (pos_inj w j i c d hc hd h).1

private def step (w i x acc c : Nat) : Nat :=
  if x.testBit c then acc ||| 2 ^ (pos w i c) else acc

private theorem fold_step_far (w i acc x p : Nat) (ls : List Nat)
    (hfar : ∀ c ∈ ls, pos w i c ≠ p) :
    (ls.foldl (step w i x) acc).testBit p = acc.testBit p := by
  induction ls generalizing acc with
  | nil => simp
  | cons c cs ih =>
      rw [List.foldl_cons]
      have hcne : pos w i c ≠ p := hfar c (by simp)
      have hbit : (step w i x acc c).testBit p = acc.testBit p := by
        by_cases hxc : x.testBit c
        · simp [step, hxc, Nat.testBit_or, hcne]
        · simp [step, hxc]
      calc
        (List.foldl (step w i x) (step w i x acc c) cs).testBit p =
            (step w i x acc c).testBit p := ih (step w i x acc c)
              (by intro d hd; exact hfar d (by simp [hd]))
        _ = acc.testBit p := hbit

private theorem fold_step_self (w i acc x b : Nat) (ls : List Nat)
    (hall : ∀ c ∈ ls, c < w) (hmem : b ∈ ls) (hnd : ls.Nodup) :
    (ls.foldl (step w i x) acc).testBit (pos w i b) =
      (acc.testBit (pos w i b) || x.testBit b) := by
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
        have hfar : ∀ d ∈ cs, pos w i d ≠ pos w i b := by
          intro d hd heq
          have hdw : d < w := hall' d hd
          have hdb : d = b := pos_inj_lane w i d b hdw hbw heq
          exact hbnot (by simpa [hdb] using hd)
        have hbit : (step w i x acc b).testBit (pos w i b) =
            (acc.testBit (pos w i b) || x.testBit b) := by
          by_cases hxb : x.testBit b
          · simp [step, hxb, Nat.testBit_or]
          · simp [step, hxb]
        calc
          (List.foldl (step w i x) (step w i x acc b) cs).testBit (pos w i b) =
              (step w i x acc b).testBit (pos w i b) :=
            fold_step_far w i (step w i x acc b) x (pos w i b) cs hfar
          _ = (acc.testBit (pos w i b) || x.testBit b) := hbit
      · have hbcs : b ∈ cs := by
          simp only [List.mem_cons] at hmem
          rcases hmem with hbc | hbcs
          · exact False.elim (hcb hbc.symm)
          · exact hbcs
        have hbit : (step w i x acc c).testBit (pos w i b) = acc.testBit (pos w i b) := by
          have hne : pos w i c ≠ pos w i b := by
            intro heq
            exact hcb (pos_inj_lane w i c b hcw (hall b hmem) heq)
          by_cases hxc : x.testBit c
          · simp [step, hxc, Nat.testBit_or, hne]
          · simp [step, hxc]
        calc
          (List.foldl (step w i x) (step w i x acc c) cs).testBit (pos w i b) =
              ((step w i x acc c).testBit (pos w i b) || x.testBit b) :=
            ih (step w i x acc c) hall' hbcs hnd'
          _ = (acc.testBit (pos w i b) || x.testBit b) := by rw [hbit]

private def row (w i x acc : Nat) : Nat :=
  (List.range w).foldl (step w i x) acc

private theorem row_bit_self (w i acc x b : Nat) (hb : b < w) :
    (row w i x acc).testBit (pos w i b) = (acc.testBit (pos w i b) || x.testBit b) := by
  unfold row
  exact fold_step_self w i acc x b (List.range w)
    (by intro c hc; exact List.mem_range.mp hc) (List.mem_range.mpr hb) List.nodup_range

private theorem row_bit_far (w i j acc x b : Nat) (hb : b < w) (hji : j ≠ i) :
    (row w j x acc).testBit (pos w i b) = acc.testBit (pos w i b) := by
  unfold row
  apply fold_step_far
  intro c hc
  have hcw : c < w := List.mem_range.mp hc
  exact pos_ne_of_row_ne w j i c b hcw hb hji

private def imageFrom (w off : Nat) (acc : Nat) (xs : List Nat) : Nat :=
  (xs.zipIdx off).foldl (fun a p => row w p.2 p.1 a) acc

private theorem image_eq_imageFrom (w : Nat) (xs : List Nat) :
    image layout w xs = imageFrom w 0 0 xs := by
  rfl

private theorem imageFrom_bit (w off i b acc : Nat) (xs : List Nat) (hb : b < w) :
    (imageFrom w off acc xs).testBit (pos w i b) =
      if off ≤ i ∧ i < off + xs.length then
        acc.testBit (pos w i b) || (xs[i - off]?.getD 0).testBit b
      else acc.testBit (pos w i b) := by
  induction xs generalizing off acc with
  | nil =>
      have hnot : ¬ (off ≤ i ∧ i < off + 0) := by omega
      simp [imageFrom]
  | cons x xs ih =>
      rw [imageFrom, List.zipIdx_cons, List.foldl_cons]
      change (imageFrom w (off + 1) (row w off x acc) xs).testBit (pos w i b) = _
      rw [ih (off + 1) (row w off x acc)]
      by_cases hio : i = off
      · subst i
        have hnot : ¬ (off + 1 ≤ off ∧ off < off + 1 + xs.length) := by omega
        simp [hnot, row_bit_self w off acc x b hb]
      · have hoi : off ≠ i := fun h => hio h.symm
        by_cases hlt : i < off
        · have hnot1 : ¬ (off ≤ i ∧ i < off + (xs.length + 1)) := by omega
          have hnot2 : ¬ (off + 1 ≤ i ∧ i < off + 1 + xs.length) := by omega
          simp [hnot1, hnot2, row_bit_far w i off acc x b hb hoi]
        · have hgt : off < i := by omega
          by_cases hcond2 : off + 1 ≤ i ∧ i < off + 1 + xs.length
          · have hcond1 : off ≤ i ∧ i < off + (xs.length + 1) := by omega
            have hidx : i - off = (i - (off + 1)) + 1 := by omega
            simp [hcond1, hcond2, hidx, row_bit_far w i off acc x b hb hoi]
          · have hcond1 : ¬ (off ≤ i ∧ i < off + (xs.length + 1)) := by omega
            simp [hcond1, hcond2, row_bit_far w i off acc x b hb hoi]

private theorem little_byte_bit (v k j r : Nat) (hj : j < k) (hr : r < 8) :
    (((List.range k).map (fun j => (v / 256 ^ j) % 256))[j]?.getD 0).testBit r =
      v.testBit (r + 8 * j) := by
  simp only [List.getElem?_map, List.getElem?_range, Option.map_some,
    Option.getD_some, hj]
  have h256 : 256 = 2 ^ 8 := by decide
  rw [h256]
  rw [← Nat.pow_mul]
  rw [Nat.testBit_mod_two_pow]
  rw [Nat.testBit_div_two_pow]
  simp [hr]

theorem sound_PlanePredicate : TileTranspose.PlanePredicate.Goal := by
  intro w xs b hb
  unfold planeRows
  dsimp [layout]
  unfold file
  apply List.filter_congr
  intro i hi
  simp only [List.mem_range] at hi
  let n := xs.length
  let v := image layout w xs
  let p := pos w i b
  let k := byteCount (align 8 n * w)
  change (atByte (little v k) (p / 8)).testBit (p % 8) = (xs[i]?.getD 0).testBit b
  have hti8 : (i / 8) * 8 ≤ i := by
    have h := Nat.div_add_mod i 8
    omega
  have halign_ge : n ≤ align 8 n := by
    unfold align
    omega
  have halign_mod : (align 8 n) % 8 = 0 := by
    unfold align
    omega
  have ht8lt : (i / 8) * 8 < align 8 n := by omega
  have ht8next : (i / 8 + 1) * 8 ≤ align 8 n := by omega
  have hmul : ((i / 8 + 1) * 8) * w ≤ align 8 n * w :=
    Nat.mul_le_mul_right w ht8next
  have hp_le : p < ((i / 8 + 1) * 8) * w := by
    dsimp [p, pos]
    have hi8lt : i % 8 < 8 := Nat.mod_lt i (by decide)
    have hrew : ((i / 8 + 1) * 8) * w = (i / 8) * 8 * w + 8 * w := by
      simp [Nat.mul_add, Nat.mul_comm]
    rw [hrew]
    omega
  have hp_lt : p < align 8 n * w := Nat.lt_of_lt_of_le hp_le hmul
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
  rw [imageFrom_bit w 0 i b 0 xs hb]
  simp [hi]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_TileTranspose_PlanePredicate
