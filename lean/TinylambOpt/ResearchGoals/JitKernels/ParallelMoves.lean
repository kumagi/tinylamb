import Std

/-! OSR, trace stitching and PHI lowering transfer simultaneously live values between registers. A copy reads its source immediately; the reference transfer reads from the old register file. -/
namespace TinylambOpt.ResearchGoals.JitKernels.ParallelMoves

abbrev Move := Nat × Nat
abbrev File := Nat → Nat
def put (s : File) (d v : Nat) : File := fun r => if r == d then v else s r
def serial (s : File) (ms : List Move) := ms.foldl (fun t (d,r) => put t d (t r)) s
def parallelCopy (s : File) (ms : List Move) := ms.foldl (fun t (d,r) => put t d (s r)) s
def freeSrc (doneMs : List Move) (next : Move) := !(doneMs.map Prod.fst).contains next.2

/-- Lower parallel moves in program order when each source is untouched by all preceding destinations. -/
def NoEarlierClobberSchedule.Goal : Prop := ∀ (ms : List Move) s, (∀ i m, ms[i]? = some m → freeSrc (ms.take i) m = true) → serial s ms = parallelCopy s ms

/-- Distinct destinations suffice for using a straight sequence of machine moves at a join. -/
def DestinationUniquenessCopySchedule.Goal : Prop := ∀ (ms : List Move) s, (ms.map Prod.fst).Nodup → serial s ms = parallelCopy s ms

/-- Break a PHI swap cycle with one temporary whose value is dead on exit. -/
def TwoCycleScratchRotation.Goal : Prop := ∀ s a b tmp, tmp ≠ a → tmp ≠ b → ∀ r, r ≠ tmp →
  serial s [(tmp,a),(a,b),(b,tmp)] r = parallelCopy s [(a,b),(b,a)] r

/-- Delete serial moves to registers dead at the exit even if they supplied another move in the same bundle. -/
def DeadOutputMovePruning.Goal : Prop := ∀ s ms (live : List Nat) r, r ∈ live → serial s (ms.filter (fun m => live.contains m.1)) r = serial s ms r

/-- In a parallel bundle, discard all but the last move to each destination before generating code. -/
def SourceSnapshotDeadStore.Goal : Prop := ∀ s ms, parallelCopy s ((ms.reverse.eraseDupsBy (fun a b => a.1 == b.1)).reverse) = parallelCopy s ms

/-- Commute two serial move bundles when neither writes a source or destination used by the other. -/
def IndependentCopyBlocks.Goal : Prop := ∀ s (a b : List Move), (∀ x ∈ a, ∀ y ∈ b, x.1 ≠ y.1 ∧ x.1 ≠ y.2 ∧ y.1 ≠ x.2) →
  serial s (a++b) = serial s (b++a)

/-- Register renaming that is injective on destinations alone suffices for all observed transferred outputs. -/
def RegisterRenamingWithLiveAlias.Goal : Prop := ∀ s (ms : List Move) (f : Nat → Nat), (ms.map (fun m => f m.1)).Nodup →
  (∀ x ∈ ms, s (f x.2) = s x.2) → ∀ m ∈ ms, serial s (ms.map (fun x => (f x.1,f x.2))) (f m.1) = serial s ms m.1

/-- A scratch register dead on the hot path can also be omitted from a side-exit reconstruction after a swap. -/
def ScratchOnHiddenDeoptEdge.Goal : Prop := ∀ s a b tmp, a ≠ b → tmp ≠ a → tmp ≠ b →
  serial s [(tmp,a),(a,b),(b,tmp)] tmp = parallelCopy s [(a,b),(b,a)] tmp

end TinylambOpt.ResearchGoals.JitKernels.ParallelMoves
