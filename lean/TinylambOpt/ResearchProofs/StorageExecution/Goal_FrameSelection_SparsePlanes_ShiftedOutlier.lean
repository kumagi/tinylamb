import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `SparsePlanes.ShiftedOutlier` — `∀ a b n, (write (List.replicate n
  0++[2^a+2^(a+b+1)])).length = ubytes (a+b+2)+(a+b+2)+2*ubytes n`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SparsePlanes_ShiftedOutlier

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SparsePlanes

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

/-- The bits of `2^a + 2^c` (with `a < c`) are exactly at positions `a` and `c`. -/
private theorem testBit_shifted (a c i : Nat) (hac : a < c) :
    (2 ^ a + 2 ^ c).testBit i = decide (i = a ∨ i = c) := by
  by_cases hic : i = c
  · subst i
    rw [Nat.add_comm (2 ^ a) (2 ^ c), Nat.testBit_two_pow_add_eq]
    have hne : a ≠ c := Nat.ne_of_lt hac
    simp [hne]
  · by_cases hia : i = a
    · subst i
      rw [Nat.add_comm (2 ^ a) (2 ^ c), Nat.testBit_two_pow_add_gt hac]
      simp
    · by_cases hci : i < c
      · rw [Nat.add_comm (2 ^ a) (2 ^ c), Nat.testBit_two_pow_add_gt hci]
        have hia' : a ≠ i := fun h => hia h.symm
        simp [hia, hia', hic]
      · have hci' : c + 1 ≤ i := by omega
        have hlt : 2 ^ c + 2 ^ a < 2 ^ i := by
          have hp : 2 ^ a < 2 ^ c := Nat.pow_lt_pow_of_lt (by decide : 1 < 2) hac
          have hnext : 2 ^ c + 2 ^ a < 2 ^ (c + 1) := by
            rw [Nat.pow_succ]
            omega
          exact Nat.lt_of_lt_of_le hnext (Nat.pow_le_pow_right (by decide : 1 ≤ 2) hci')
        rw [Nat.add_comm (2 ^ a) (2 ^ c), Nat.testBit_lt_two_pow hlt]
        simp [hic, hia]

/-- The highest set bit of `2^a + 2^c` is `c`, so its width is `c + 1`. -/
private theorem width_sum_two_pow (a c : Nat) (hac : a < c) :
    width (2 ^ a + 2 ^ c) = c + 1 := by
  let V := 2 ^ a + 2 ^ c
  change width V = c + 1
  have hne : V ≠ 0 := by
    dsimp [V]
    exact Nat.ne_of_gt (Nat.lt_of_lt_of_le (Nat.two_pow_pos c) (Nat.le_add_left (2 ^ c) (2 ^ a)))
  have hlt : V < 2 ^ (c + 1) := by
    dsimp [V]
    rw [Nat.pow_succ, Nat.mul_comm]
    simp only [Nat.two_mul]
    have hp : 2 ^ a < 2 ^ c := Nat.pow_lt_pow_of_lt (by decide : 1 < 2) hac
    omega
  have hge : 2 ^ c ≤ V := by
    dsimp [V]
    exact Nat.le_add_left (2 ^ c) (2 ^ a)
  have hle : width V ≤ c + 1 := by
    unfold width
    simp [hne]
    have hlog := (Nat.log2_lt hne).mpr hlt
    omega
  have hge' : c + 1 ≤ width V := by
    unfold width
    simp [hne]
    have hnlog : ¬ V.log2 < c := by
      intro hlog
      have hvlt : V < 2 ^ c := (Nat.log2_lt hne).mp hlog
      omega
    omega
  omega

/-- Split the plane indices into `[0..a)`, `a`, `(a..c)`, and `c`. -/
private theorem range_split (a b : Nat) :
    List.range (a + b + 2) =
      List.range a ++ [a] ++ List.map (fun x => a + 1 + x) (List.range b) ++ [a + b + 1] := by
  rw [show a + b + 2 = a + (b + 2) by omega, List.range_add]
  rw [show b + 2 = (b + 1) + 1 by omega, List.range_succ]
  rw [show b + 1 = 1 + b by omega, List.range_add]
  simp only [List.range_one, List.map_append, List.map_cons, List.map_nil]
  have hmap : List.map (fun x => a + x) (List.map (fun x => 1 + x) (List.range b)) =
      List.map (fun x => a + 1 + x) (List.range b) := by
    rw [List.map_map]
    apply List.map_congr_left
    intro x hx
    simp only [Function.comp_apply]
    omega
  rw [hmap]
  simp [Nat.add_assoc]
  omega

private theorem sum_ones (m : Nat) : (List.map (fun _ : Nat => 1) (List.range m)).sum = m := by
  induction m with
  | zero => rfl
  | succ m ih => simp [List.range_succ, ih]

private theorem sum_map_const_one (l : List Nat) : (l.map (fun _ => 1)).sum = l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih]; omega

theorem sound_ShiftedOutlier : SparsePlanes.ShiftedOutlier.Goal := by
  intro a b n
  let c := a + b + 1
  let V := 2 ^ a + 2 ^ c
  let xs := List.replicate n 0 ++ [V]
  have hac : a < c := by
    dsimp [c]
    omega
  have hc1 : c + 1 = a + b + 2 := by
    simp [c, Nat.add_assoc]
  have hpeak : peak xs = V := by
    have hfold : ∀ m, List.foldl max 0 (List.replicate m 0) = 0 := by
      intro m
      induction m with
      | zero => rfl
      | succ m ih => simp [List.replicate, ih]
    dsimp [xs, peak]
    induction n with
    | zero => simp
    | succ n ih => simp [List.replicate, List.max?, hfold]
  have hw : width (peak xs) = c + 1 := by
    rw [hpeak]
    dsimp [V]
    exact width_sum_two_pow a c hac
  have hmem : ∀ p ∈ (List.replicate n 0).zipIdx, p.1 = 0 := by
    intro p hp
    rcases p with ⟨v, i⟩
    have hz := List.mem_zipIdx hp
    simp at hz
    exact hz.2
  have hps : ∀ i, i < c + 1 →
      xs.zipIdx.filter (fun (v, _) => v.testBit i) =
        if i = a ∨ i = c then [(V, n)] else [] := by
    intro i hi
    have htb : V.testBit i = decide (i = a ∨ i = c) := by
      dsimp [V]
      exact testBit_shifted a c i hac
    have hrep : (List.replicate n 0).zipIdx.filter (fun (v, _) => v.testBit i) = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      rintro ⟨v, j⟩ hp
      have hpbase : (v, j) ∈ (List.replicate n 0).zipIdx := (List.mem_filter.mp hp).1
      have hv0 : v = 0 := hmem (v, j) hpbase
      have hbit := (List.mem_filter.mp hp).2
      rw [hv0] at hbit
      simp at hbit
    by_cases hset : i = a ∨ i = c
    · simp [xs, List.zipIdx_append, List.filter_append, hrep, htb, hset]
    · simp [xs, List.zipIdx_append, List.filter_append, hrep, htb, hset]
  have hsum :
      ((List.range (c + 1)).flatMap (fun i =>
        let ps := xs.zipIdx.filter (fun (v, _) => v.testBit i)
        uleb ps.length ++ ps.flatMap (fun (_, j) => uleb j))).length =
        (c + 1) + 2 * ubytes n := by
    rw [hc1, range_split]
    simp only [List.flatMap_append, List.flatMap_cons, List.flatMap_nil, List.length_append,
      List.length_nil]
    have hlo :
        ((List.range a).flatMap (fun i =>
          let ps := xs.zipIdx.filter (fun (v, _) => v.testBit i)
          uleb ps.length ++ ps.flatMap (fun (_, j) => uleb j))).length = a := by
      rw [List.length_flatMap]
      have hmap : (List.range a).map (fun i =>
          (let ps := xs.zipIdx.filter (fun (v, _) => v.testBit i)
           uleb ps.length ++ ps.flatMap (fun (_, j) => uleb j)).length) =
          (List.range a).map (fun _ => 1) := by
        apply List.map_congr_left
        intro i hia
        have hi' : i < c + 1 := by
          simp at hia
          omega
        have hia_ne : i ≠ a := by simp at hia; omega
        have hic_ne : i ≠ c := by simp at hia; omega
        have hempty := hps i hi'
        have hor : ¬ (i = a ∨ i = c) := by omega
        rw [ite_eq_right hor] at hempty
        simp [hempty, uleb_len_eq_ubytes, ubytes, width]
      rw [hmap, sum_ones]
    rw [hlo]
    have ha :
        (uleb (xs.zipIdx.filter (fun (v, _) => v.testBit a)).length).length +
          ((xs.zipIdx.filter (fun (v, _) => v.testBit a)).flatMap (fun (_, j) => uleb j)).length =
          1 + ubytes n := by
      have hpa := hps a (by dsimp [c]; omega)
      have hpa' : xs.zipIdx.filter (fun (v, _) => v.testBit a) = [(V, n)] := by
        simpa using hpa
      rw [hpa']
      simp [uleb_len_eq_ubytes, ubytes, width]
    rw [ha]
    have hmid :
        ((List.map (fun x => a + 1 + x) (List.range b)).flatMap (fun i =>
          let ps := xs.zipIdx.filter (fun (v, _) => v.testBit i)
          uleb ps.length ++ ps.flatMap (fun (_, j) => uleb j))).length = b := by
      rw [List.length_flatMap]
      have hmap : (List.map (fun x => a + 1 + x) (List.range b)).map (fun i =>
          (let ps := xs.zipIdx.filter (fun (v, _) => v.testBit i)
           uleb ps.length ++ ps.flatMap (fun (_, j) => uleb j)).length) =
          (List.map (fun x => a + 1 + x) (List.range b)).map (fun _ => 1) := by
        apply List.map_congr_left
        intro i hi
        rcases List.mem_map.mp hi with ⟨x, hx, rfl⟩
        have hx' : x < b := by simpa using hx
        have hi' : a + 1 + x < c + 1 := by dsimp [c]; omega
        have hia_ne : a + 1 + x ≠ a := by omega
        have hic_ne : a + 1 + x ≠ c := by dsimp [c]; omega
        have hempty := hps (a + 1 + x) hi'
        have hor : ¬ (a + 1 + x = a ∨ a + 1 + x = c) := by omega
        rw [ite_eq_right hor] at hempty
        simp [hempty, uleb_len_eq_ubytes, ubytes, width]
      rw [hmap, sum_map_const_one]
      simp
    rw [hmid]
    have hhc :
        (uleb (xs.zipIdx.filter (fun (v, _) => v.testBit (a + b + 1))).length).length +
          ((xs.zipIdx.filter (fun (v, _) => v.testBit (a + b + 1))).flatMap (fun (_, j) => uleb j)).length =
          1 + ubytes n := by
      have hpc := hps c (by omega)
      have hpc' : xs.zipIdx.filter (fun (v, _) => v.testBit (a + b + 1)) = [(V, n)] := by
        simpa [c] using hpc
      rw [hpc']
      simp [uleb_len_eq_ubytes, ubytes, width]
    rw [hhc]
    omega
  change (write xs).length = ubytes (a + b + 2) + (a + b + 2) + 2 * ubytes n
  simp only [SparsePlanes.write, hw, List.length_append, uleb_len_eq_ubytes]
  rw [hsum, hc1]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SparsePlanes_ShiftedOutlier
