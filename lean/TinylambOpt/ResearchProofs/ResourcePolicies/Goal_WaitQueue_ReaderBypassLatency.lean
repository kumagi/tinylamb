import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue
/-!
Registers the checked result for `WaitQueue.ReaderBypassLatency.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_ReaderBypassLatency
open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue
open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.ReaderBypassLatency

theorem sound : Goal := by
  intro active writer newcomer tail hactive hwriter hnewcomer hne1 hne2
  unfold waitTurns
  have hne1' : (active != writer) = true := by simp [bne_iff_ne, hne1]
  have hne2' : (newcomer != writer) = true := by simp [bne_iff_ne, hne2]
  have hwriter_ne : (writer != writer) = false := by simp
  have ha : active.writer = false := by simp at hactive; exact hactive
  have hw : writer.writer = true := hwriter
  have hn : newcomer.writer = false := by simp at hnewcomer; exact hnewcomer
  simp only [List.takeWhile, hne1', hne2', hwriter_ne, ite_true, ite_false,
    List.map_cons, List.map_nil, List.filter_cons, List.filter_nil,
    List.zip_cons_cons, List.zip_nil_left, List.countP_cons, List.countP_nil,
    ha, hw, hn, beq_false_of_ne (by simp [hne1] : active ≠ writer),
    beq_false_of_ne (by simp [hne2] : newcomer ≠ writer), beq_self_eq_true]
  decide
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_ReaderBypassLatency
