import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_SplitCode

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes

private theorem step_false (q : Nat) : model.step q false = some 1 := by
  simp [model]

private theorem step_true (q : Nat) :
    model.step q true = if q = 1 then none else some 0 := by
  by_cases hq : q = 1
  · subst q
    simp [model]
  · simp [model, hq]

private theorem count_one : ∀ n : Nat, count model n 1 = 1 := by
  intro n
  induction n with
  | zero => simp [count, model]
  | succ n ih => simp [count, step_false, step_true, ih]

private theorem lowCount_one (n q : Nat) : lowCount model n q = 1 := by
  simp [lowCount, step_false, count_one]

theorem sound_SplitCode : PrefixOnes.SplitCode.Goal := by
  intro q a b q' off hdesc
  induction a generalizing q q' off with
  | nil =>
      simp [descend] at hdesc
      rcases hdesc with ⟨rfl, rfl⟩
      simp
  | cons c cs ih =>
      have hlen : (c :: cs).length + b.length = (cs.length + b.length) + 1 := by
        simp [Nat.add_assoc, Nat.add_comm]
      rw [hlen] at hdesc
      cases hs : model.step q c with
      | none => simp [descend, hs] at hdesc
      | some q'' =>
          cases hd : descend model (cs.length + b.length) q'' cs with
          | none => simp [descend, hs, hd] at hdesc
          | some res =>
              rcases res with ⟨last, off'⟩
              have hpair : (last, off' + (if c then 1 else 0)) = (q', off) := by
                simpa [descend, hs, hd, lowCount_one] using hdesc
              have hq' : last = q' := congrArg Prod.fst hpair
              have hoff : off' + (if c then 1 else 0) = off := congrArg Prod.snd hpair
              subst q'
              subst off
              have ih' := ih q'' last off' hd
              have hrank : rank model q ((c :: cs) ++ b) =
                  (rank model q'' (cs ++ b)).map fun tail => tail + (if c then 1 else 0) := by
                change rank model q (c :: (cs ++ b)) = _
                rw [rank.eq_def]
                simp [hs, lowCount_one]
                cases rank model q'' (cs ++ b) <;> simp
              rw [hrank, ih']
              cases rank model last b with
              | none => simp
              | some r =>
                  simp
                  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_SplitCode
