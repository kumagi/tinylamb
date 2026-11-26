import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-!
# ChannelLocalDedupBeforeUnionDistinct

Removing duplicate complete rows within each channel preserves the distinct union and
its downstream total.

The executor can reduce input to the global distinct step by deduplicating each channel
before the union.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ChannelLocalDedupBeforeUnionDistinct
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem perm_sum {l₁ l₂ : List Int} (h : List.Perm l₁ l₂) : l₁.sum = l₂.sum := by
  exact h.foldr_eq' (by intros; simp [Int.add_left_comm]) 0

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.ChannelLocalDedupBeforeUnionDistinct.Goal := by
  intro channels k
  dsimp only
  let p : Nat × Int × Int → Bool := fun r => r.1 == k
  let A := ((channels.flatMap List.eraseDups).eraseDups.filter p)
  let B := (channels.flatten.eraseDups.filter p)
  have hnodA : A.Nodup := (KeyGrouping.nodup_eraseDups (channels.flatMap List.eraseDups)).filter p
  have hnodB : B.Nodup := (KeyGrouping.nodup_eraseDups channels.flatten).filter p
  have hmem : ∀ r, r ∈ A ↔ r ∈ B := by
    intro r
    simp [A, B, List.mem_eraseDups, List.mem_filter, p]
  have hperm : List.Perm A B := (List.perm_ext_iff_of_nodup hnodA hnodB).2 hmem
  have hsum : (A.map (fun r => r.2.1)).sum = (B.map (fun r => r.2.1)).sum :=
    perm_sum (hperm.map (fun r => r.2.1))
  simpa [A, B, total, p] using hsum
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ChannelLocalDedupBeforeUnionDistinct
