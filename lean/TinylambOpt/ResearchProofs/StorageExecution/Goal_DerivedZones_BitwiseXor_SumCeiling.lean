import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
import TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_SoundInterval

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `BitwiseXor.SumCeiling` — A footer gives a deterministic interval for the SUM
  accumulator over non-NULL scores.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_SumCeiling

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BitwiseXor

-- 真偽確認: a,b,x,y を 0..7 で全網羅し、各スコアが XOR 由来の区間に入ること
-- (chk3) と、被覆ページの全行のスコア和が n*lo..n*hi に収まること (chk) を
-- #eval で確認済み。よって命題は真。

/-- A list of n values, each between lo and hi, has its sum between n*lo and
    n*hi. -/
private theorem sum_bounds {lo hi : Nat} {vs : List Nat}
    (hv : ∀ v ∈ vs, lo ≤ v ∧ v ≤ hi) :
    vs.length * lo ≤ vs.sum ∧ vs.sum ≤ vs.length * hi := by
  induction vs with
  | nil => simp
  | cons v vs ih =>
    have hhead := hv v (by simp)
    have htail := ih (fun x hx => hv x (by simp [hx]))
    simp only [List.length_cons, List.sum_cons, Nat.add_mul]
    omega

/-- Every non-NULL score of a covered page lies in the certified XOR interval;
    this is just the already proved soundInterval for the BitwiseXor kernel. -/
private theorem xor_score_bounds {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    inside (kernel.bounds t a b) v := by
  rcases List.mem_filterMap.mp hv with ⟨r, hrp, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  rw [← hscore]
  exact TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_SoundInterval.sound_SoundInterval t a b x y (hc r hrp x y hrxy).1 (hc r hrp x y hrxy).2

theorem sound_SumCeiling : BitwiseXor.SumCeiling.Goal := by
  intro t p a b hc
  change (scores kernel t p).length * (kernel.bounds t a b).lo ≤ (scores kernel t p).sum ∧
    (scores kernel t p).sum ≤ (scores kernel t p).length * (kernel.bounds t a b).hi
  exact sum_bounds (fun v hv => by
    simpa [inside] using xor_score_bounds (t := t) (p := p) (a := a) (b := b) hc hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BitwiseXor_SumCeiling
