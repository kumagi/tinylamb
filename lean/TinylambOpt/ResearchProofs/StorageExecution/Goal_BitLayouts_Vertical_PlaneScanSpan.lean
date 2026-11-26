import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `Vertical.PlaneScanSpan` — `∀ n w b, n%8=0 → b<w → ((List.range n).map (fun i =>
  layout.bit n w i b/8)).eraseDups.length = n/8`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_PlaneScanSpan

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical

/-! For the vertical layout a bit cell is `b*n + i`. When `n` is a multiple of
eight, `b*n` is byte aligned, so `(b*n+i)/8 = b*(n/8) + i/8`: each byte of a
plane is hit by exactly eight consecutive rows, and the plane spans `n/8`
distinct bytes. -/

private theorem filter_replicate_ne (a m : Nat) :
    (List.replicate m a).filter (fun b => !b == a) = [] := by
  induction m with
  | zero => simp
  | succ m ih => simp [List.replicate_succ, ih]

private theorem erase_replicate (a m : Nat) :
    (List.replicate (m + 1) a).eraseDups = [a] := by
  rw [List.replicate_succ, List.eraseDups_cons, filter_replicate_ne]
  simp

private theorem mem_consec (a k x : Nat) :
    x ∈ (List.range (8 * k)).map (fun i => a + i / 8) ↔
      x ∈ (List.range k).map (fun j => a + j) := by
  simp only [List.mem_map, List.mem_range]
  constructor
  · rintro ⟨i, hi, rfl⟩
    refine ⟨i / 8, ?_, ?_⟩
    · omega
    · rfl
  · rintro ⟨j, hj, rfl⟩
    refine ⟨8 * j, ?_, ?_⟩
    · omega
    · omega

private theorem erase_consec (a k : Nat) :
    ((List.range (8 * k)).map (fun i => a + i / 8)).eraseDups =
      (List.range k).map (fun j => a + j) := by
  induction k with
  | zero => simp
  | succ k ih =>
      rw [show 8 * (k + 1) = 8 * k + 8 by omega]
      rw [List.range_add]
      simp only [List.map_append, List.map_map, Function.comp_def]
      rw [List.eraseDups_append]
      rw [ih]
      have hsecond :
          (List.range 8).map (fun r => a + (8 * k + r) / 8) =
            List.replicate 8 (a + k) := by
        apply List.ext_getElem
        · simp
        · intro i hi1 hi2
          rw [List.getElem_replicate, List.getElem_map, List.getElem_range]
          have hi : i < 8 := by simpa using hi1
          rw [Nat.mul_add_div (by decide : 0 < 8), Nat.div_eq_of_lt hi]
          omega
      rw [hsecond]
      have hnotmem : a + k ∉ (List.range k).map (fun j => a + j) := by
        simp [List.mem_map]
      have hnotmem0 : a + k ∉ (List.range (8 * k)).map (fun i => a + i / 8) := by
        rw [mem_consec]
        exact hnotmem
      have hrm :
          (List.replicate 8 (a + k)).removeAll
              ((List.range (8 * k)).map (fun i => a + i / 8)) =
            List.replicate 8 (a + k) := by
        simp [List.removeAll]
        intro x hx
        omega
      rw [hrm, erase_replicate]
      simp [List.range_succ]

theorem sound_PlaneScanSpan : Vertical.PlaneScanSpan.Goal := by
  intro n w b hn hw
  let k := n / 8
  have hn' : n = 8 * k := by
    have hdiv := Nat.div_add_mod n 8
    omega
  have hmap :
      (List.range n).map (fun i => layout.bit n w i b / 8) =
        (List.range (8 * k)).map (fun i => b * k + i / 8) := by
    rw [hn']
    apply List.map_congr_left
    intro i hi
    simp only [layout]
    have hmul : b * (8 * k) = 8 * (b * k) := by
      calc
        b * (8 * k) = b * (k * 8) := by rw [Nat.mul_comm 8 k]
        _ = (b * k) * 8 := by rw [Nat.mul_assoc]
        _ = 8 * (b * k) := by rw [Nat.mul_comm]
    rw [hmul, Nat.mul_add_div (by decide : 0 < 8)]
  rw [hmap, erase_consec]
  simp [k]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_Vertical_PlaneScanSpan
