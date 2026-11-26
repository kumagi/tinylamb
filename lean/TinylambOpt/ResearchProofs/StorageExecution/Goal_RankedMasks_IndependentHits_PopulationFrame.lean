import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_PopulationFrame

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.IndependentHits
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples

/-! There is no `IndependentHits.PopulationFrame.Goal`: inside `IndependentHits`
    the ten declarations are Ordinal, Decoder, RejectSlack, PrefixRange,
    SplitCode, CheapConcatenation, WireLookup, SpaceTest, Cofactor and
    FibonacciCount. `PopulationFrame.Goal` exists only for `FixedPopcount`
    (where `count model n k = choose n k`). Hence this file cannot register a
    proof/refutation of a declared ResearchGoal; it records the outcome for the
    literal body with `IndependentHits.model` substituted, in case that was the
    intended question. That statement is false already for `xs = [true]`. -/

private abbrev analogueBody (xs : List Bool) : Prop :=
  let n := xs.length
  let k := xs.count true
  let r := (rank model k xs).getD 0
  let bits := width (choose n k - 1)
  let packet := [k] ++ little r (byteCount bits)
  unrank model n (packet.head?.getD 0) (fromLittle (packet.drop 1)) = some xs ∧
    packet.length = 1 + byteCount (codeBits model n k)

/-! Truth check on all bitmaps of length at most 3. -/
private def check : Bool :=
  (List.range 4).all fun n =>
  (List.range (2 ^ n)).all fun i =>
    decide (analogueBody ((List.range n).map fun j => (i / 2 ^ j) % 2 == 1))

#eval check
#eval decide (analogueBody [true])

theorem disproof_PopulationFrame_analogue :
    ¬ (∀ xs, xs.length < 256 → analogueBody xs) := by
  intro h
  have hc := h [true] (by decide)
  exact (by native_decide : ¬ analogueBody [true]) hc

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_IndependentHits_PopulationFrame
