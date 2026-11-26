import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_SplitCode

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits

-- The identity is, in fact, purely structural: `rank` and `descend` add the
-- same `lowCount` term at each selected bit, with the same remaining length.
private theorem splitCode_generic (m : Machine) :
    ∀ q a b q' off, descend m (a.length + b.length) q a = some (q', off) →
      rank m q (a ++ b) = (rank m q' b).map (off + ·) := by
  intro q a
  induction a generalizing q with
  | nil =>
      intro b q' off h
      simp [descend] at h
      rcases h with ⟨rfl, rfl⟩
      simp
  | cons c a ih =>
      intro b q' off h
      have hlen : (a.length + 1) + b.length = a.length + b.length + 1 := by omega
      simp only [List.length_cons] at h
      rw [hlen] at h
      cases hs : m.step q c with
      | none =>
          simp [descend, hs] at h
      | some qc =>
          cases hd : descend m (a.length + b.length) qc a with
          | none =>
              simp [descend, hs, hd] at h
          | some p =>
              have hpair :
                  (p.1, p.2 + (if c then lowCount m (a.length + b.length) q else 0)) =
                    (q', off) := by
                simpa [descend, hs, hd] using h
              have hq' : q' = p.1 := (congrArg Prod.fst hpair).symm
              have hoff : off = p.2 + (if c then lowCount m (a.length + b.length) q else 0) :=
                (congrArg Prod.snd hpair).symm
              have hd' := ih qc b p.1 p.2 hd
              subst q'
              subst off
              cases hr : rank m p.1 b with
              | none => simp [hr, rank, hs, hd']
              | some r =>
                  simp [hr, rank, hs, hd', Nat.add_assoc, Nat.add_comm]

theorem sound_SplitCode : IndependentHits.SplitCode.Goal := by
  exact splitCode_generic model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_SplitCode
