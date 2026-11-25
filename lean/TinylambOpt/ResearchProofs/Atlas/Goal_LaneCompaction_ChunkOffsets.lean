import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LaneCompaction_ChunkOffsets

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction

/-- `zipIdx` started at an offset is the unoffset one shifted by that offset,
    once the false lanes have been filtered out. -/
theorem slots_offset (l : List Bool) (o : Nat) :
    ((l.zipIdx o).filter Prod.fst).map Prod.snd =
      (((l.zipIdx 0).filter Prod.fst).map Prod.snd).map (fun i => o + i) := by
  induction l generalizing o with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      cases x
      · simp only [List.filter_cons, Bool.false_eq_true, ↓reduceIte, Nat.zero_add]
        rw [ih (o + 1), ih 1]
        simp only [List.map_map]
        apply List.map_congr_left
        intro p hp
        change o + 1 + p.snd = o + (1 + p.snd)
        omega
      · simp only [List.filter_cons, ↓reduceIte, List.map_cons, Nat.zero_add, Nat.add_zero]
        congr 1
        rw [ih (o + 1), ih 1]
        simp only [List.map_map]
        apply List.map_congr_left
        intro p hp
        change o + 1 + p.snd = o + (1 + p.snd)
        omega

theorem sound_LaneCompaction_ChunkOffsets :
    TinylambOpt.ResearchGoals.Atlas.ExecutorResources.LaneCompaction.ChunkOffsets.Goal := by
  intro a b
  simp only [slots]
  rw [List.zipIdx_append]
  simp only [List.filter_append, List.map_append, Nat.zero_add]
  rw [slots_offset b a.length]

end TinylambOpt.ResearchProofs.Atlas.Goal_LaneCompaction_ChunkOffsets
