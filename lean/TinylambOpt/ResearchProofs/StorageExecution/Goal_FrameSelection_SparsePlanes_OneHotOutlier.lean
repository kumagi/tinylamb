import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `SparsePlanes.OneHotOutlier` — `∀ n b, (write (List.replicate n 0 ++ [2^b])).length =
  ubytes (b+1)+(b+1)+ubytes n`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SparsePlanes_OneHotOutlier

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SparsePlanes

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem sum_const_range (b : Nat) : ((List.range b).map (fun _ => 1)).sum = b := by
  induction b with
  | zero => simp
  | succ b ih => simp [List.range_succ, ih]

theorem sound_OneHotOutlier : SparsePlanes.OneHotOutlier.Goal := by
  intro n b
  let xs := List.replicate n 0 ++ [2^b]
  change (write xs).length = ubytes (b+1)+(b+1)+ubytes n
  have hpeak : peak xs = 2^b := by
    dsimp [xs]
    unfold peak
    induction n with
    | zero => simp [List.max?]
    | succ n ih =>
        rw [show List.replicate (n+1) 0 ++ [2^b] = 0 :: (List.replicate n 0 ++ [2^b]) by simp [List.replicate]]
        rw [List.max?_cons]
        cases hm : (List.replicate n 0 ++ [2^b]).max? with
        | none =>
            simp [hm] at ih
            have hp := Nat.two_pow_pos b
            omega
        | some v => simpa [hm] using ih
  have hw' : width (2^b) = b+1 := by
    simp [width, Nat.log2_two_pow]
  have hzeros : ∀ (m k i : Nat), ((List.replicate m 0).zipIdx k).filter (fun (v,_) => v.testBit i) = [] := by
    intro m
    induction m with
    | zero => simp
    | succ m ih => intro k i; simp [List.replicate, ih]
  have hps : ∀ i, (xs.zipIdx.filter (fun (v,_) => v.testBit i)) = if i = b then [(2^b, n)] else [] := by
    intro i
    dsimp [xs]
    rw [List.zipIdx_append]
    simp only [hzeros, List.filter_append, List.length_replicate]
    by_cases h : i = b
    · subst h; simp
    · have hb : b ≠ i := by omega
      simp [h, hb]
  have hplane : ∀ i, (let ps := xs.zipIdx.filter (fun (v,_) => v.testBit i);
      (uleb ps.length ++ ps.flatMap (fun (_,j) => uleb j)).length) = if i = b then 1+ubytes n else 1 := by
    intro i
    rw [hps i]
    by_cases h : i = b <;> simp [h, uleb_len_eq_ubytes, ubytes, width]
  unfold SparsePlanes.write
  simp only [hpeak, hw', List.length_append, uleb_len_eq_ubytes]
  rw [List.length_flatMap]
  have hsum : ((List.range (b+1)).map (fun i =>
      let ps := xs.zipIdx.filter (fun (v,_) => v.testBit i);
      (uleb ps.length ++ ps.flatMap (fun (_,j) => uleb j)).length)).sum = (b+1)+ubytes n := by
    rw [List.range_succ, List.map_append, List.sum_append]
    have hbpart : ((List.range b).map (fun i =>
        let ps := xs.zipIdx.filter (fun (v,_) => v.testBit i);
        (uleb ps.length ++ ps.flatMap (fun (_,j) => uleb j)).length)).sum = b := by
      have hmap : (List.range b).map (fun i =>
          let ps := xs.zipIdx.filter (fun (v,_) => v.testBit i);
          (uleb ps.length ++ ps.flatMap (fun (_,j) => uleb j)).length) =
          (List.range b).map (fun _ => 1) := by
        apply List.map_congr_left
        intro i hi
        have hib : i ≠ b := by simp at hi; omega
        simpa [hib] using hplane i
      rw [hmap, sum_const_range]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
    have hlast : (let ps := xs.zipIdx.filter (fun (v,_) => v.testBit b);
        (uleb ps.length ++ ps.flatMap (fun (_,j) => uleb j)).length) = 1+ubytes n := by
      simpa using hplane b
    rw [hbpart, hlast]
    omega
  rw [hsum]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SparsePlanes_OneHotOutlier
