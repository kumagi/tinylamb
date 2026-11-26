import TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
import TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharOrderLemmas

/-!
An inclusive compound-prefix range over full-arity binary keys agrees with the
physical encoded range test. Each field encoding is an order embedding and is
self-delimiting, so an encoded prefix comparison decides the decoded tuple
prefix comparison; appending the 0xFF fence for a proper prefix bounds exactly
the keys extending that prefix. SQL may therefore use the physical range scan
without losing rows that belong to an inclusive bound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_InclusiveCompoundPrefixScan

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
open TinylambOpt.ResearchProofs.DatabaseCorrectness.VarcharOrderLemmas

theorem sound : InclusiveCompoundPrefixScan.Goal := by
  intro arity lo hi k hk hlo hhi
  unfold mayRead logical
  simp only [decide_eq_true_eq]
  constructor
  · intro h
    constructor
    · by_cases hlo0 : lo = []
      · simp [hlo0]
      · have hle : image lo ≤ image k := by
          rcases h.1 with h0 | hle
          · exact False.elim (hlo0 h0)
          · exact hle
        exact Or.inr ((image_le_prefix lo k (by omega)).mp hle)
    · by_cases hhi0 : hi = []
      · simp [hhi0]
      · have hle : image k ≤ upper arity hi := by
          rcases h.2 with h0 | hle
          · exact False.elim (hhi0 h0)
          · exact hle
        by_cases hleneq : hi.length = arity
        · have hlenk : hi.length = k.length := by omega
          have hle' : image k ≤ image hi := by
            simpa [upper, hhi0, hleneq] using hle
          have hres := (image_le_prefix k hi (by omega)).mp hle'
          have htakehi : hi.take k.length = hi := take_of_length_le hi k.length (by omega)
          have hres' : k ≤ hi := by simpa [htakehi] using hres
          exact Or.inr (by simpa [take_of_length_le k hi.length (by omega)] using hres')
        · have hlene : hi.length < arity := by omega
          have hle' : image k ≤ image hi ++ [255] := by
            simpa [upper, hhi0, hlene] using hle
          exact Or.inr ((image_le_ff hi k (by omega)).mp hle')
  · intro h
    constructor
    · by_cases hlo0 : lo = []
      · simp [hlo0, image]
      · have hle0 : lo ≤ k.take lo.length := by
          rcases h.1 with h0 | hle
          · exact False.elim (hlo0 h0)
          · exact hle
        exact Or.inr ((image_le_prefix lo k (by omega)).mpr hle0)
    · by_cases hhi0 : hi = []
      · simp [hhi0]
      · have hle0 : k.take hi.length ≤ hi := by
          rcases h.2 with h0 | hle
          · exact False.elim (hhi0 h0)
          · exact hle
        by_cases hleneq : hi.length = arity
        · have hlenk : hi.length = k.length := by omega
          have hres0 : k ≤ hi := by simpa [hlenk] using hle0
          have htakehi : hi.take k.length = hi := take_of_length_le hi k.length (by omega)
          have hle' : image k ≤ image hi := (image_le_prefix k hi (by omega)).mpr (by simpa [htakehi] using hres0)
          exact Or.inr (by simpa [upper, hhi0, hleneq] using hle')
        · have hlene : hi.length < arity := by omega
          have hle' : image k ≤ image hi ++ [255] := (image_le_ff hi k (by omega)).mpr hle0
          exact Or.inr (by simpa [upper, hhi0, hlene] using hle')

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SnapshotIndex_InclusiveCompoundPrefixScan
