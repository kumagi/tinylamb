import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
import TinylambOpt.ResearchProofs.Compression.Wire

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_RelativeRestartVarintBudget

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory


/-!
Storing restart offsets relative to a page-local base cannot cost more bytes
than storing the absolute offsets: truncated subtraction never increases a
value, and the varint charge is monotone in the value.

The modeled base changes only the encoding of the directory, not the row
offsets observed by a seek.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_RelativeRestartVarintBudget

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchProofs.Compression.Wire

theorem ubytes_mono {n m : Nat} (h : n ≤ m) : ubytes n ≤ ubytes m := by
  have hmpos : 0 < ubytes m := by
    unfold ubytes
    omega
  have hlt : m < 128 ^ ubytes m := by
    apply (sound_WidthCliff m (ubytes m) hmpos).mp
    exact Nat.le_refl _
  exact (sound_WidthCliff n (ubytes m) hmpos).mpr (Nat.lt_of_le_of_lt h hlt)

theorem sound : RelativeRestartVarintBudget.Goal := by
  intro ds base
  induction ds with
  | nil => simp [streamBytes]
  | cons d ds ih =>
      simp [streamBytes] at ih ⊢
      exact Nat.add_le_add (ubytes_mono (Nat.sub_le d base)) ih

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.Goal_RelativeRestartVarintBudget
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.RestartDirectory.Goal_RelativeRestartVarintBudget
