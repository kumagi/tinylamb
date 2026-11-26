import TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

/-! A terminal failure reply is never upgraded by a later wakeup, even if the
    durability prefix grows in the meantime. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_TerminalReplyCannotBeUpgraded

open TinylambOpt.ResearchGoals.DatabaseCorrectness.DurableReplies

theorem sound : TerminalReplyCannotBeUpgraded.Goal := by
  intro a p t d future
  cases a <;> cases p <;> simp [launch, complete]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.DurableReplies.Goal_TerminalReplyCannotBeUpgraded
