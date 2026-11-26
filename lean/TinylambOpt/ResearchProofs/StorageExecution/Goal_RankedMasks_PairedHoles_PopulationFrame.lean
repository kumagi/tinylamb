import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PairedHoles_PopulationFrame

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PairedHoles
open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples

/-! There is no `PairedHoles.PopulationFrame.Goal`: inside `PairedHoles` the ten
    declarations are Ordinal, Decoder, RejectSlack, PrefixRange, SplitCode,
    EraseZeroPair, WireLookup, SpaceTest, Cofactor and EvenTail.
    `PopulationFrame.Goal` is declared only for `FixedPopcount`, where the
    state is the remaining population and `count model n k = choose n k`.
    Hence this file cannot register a proof/refutation of a declared
    ResearchGoal; it records the outcome for the literal body with
    `PairedHoles.model` substituted, in case that was the intended question.
    That statement is false already for `xs = [false]`: from start state 0 the
    single false bit leaves state 1, which is not accepting, so the rank is
    absent and the substituted frame decodes the rank-zero word `[true]`. -/

private abbrev analogueBody (xs : List Bool) : Prop :=
  let n := xs.length
  let k := xs.count true
  let r := (rank model k xs).getD 0
  let bits := width (choose n k - 1)
  let packet := [k] ++ little r (byteCount bits)
  unrank model n (packet.head?.getD 0) (fromLittle (packet.drop 1)) = some xs ∧
    packet.length = 1 + byteCount (codeBits model n k)

/-! Truth check on all bitmaps of length at most 5. -/
private def check : Bool :=
  (List.range 6).all fun n =>
  (List.range (2 ^ n)).all fun i =>
    decide (analogueBody ((List.range n).map fun j => (i / 2 ^ j) % 2 == 1))

#eval check
#eval decide (analogueBody [])
#eval decide (analogueBody [false])
#eval decide (analogueBody [true])

theorem disproof_PopulationFrame_analogue :
    ¬ (∀ xs, xs.length < 256 → analogueBody xs) := by
  intro h
  have hc := h [false] (by decide)
  exact (by native_decide : ¬ analogueBody [false]) hc

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PairedHoles_PopulationFrame
