import Std

/-! Checked INT64 reductions can validate whole chunks from prefix extrema. SQL-observable overflow depends on the evaluation order, including temporary excursions that cancel. -/
namespace TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

def path (xs : List Int) := xs.scanl (·+·) 0
def low (xs : List Int) := (path xs).min?.getD 0
def high (xs : List Int) := (path xs).max?.getD 0
def safe (lo hi s : Int) (xs : List Int) := (path xs).all (fun v => lo ≤ s+v && s+v ≤ hi)
def faultAt (lo hi s : Int) (xs : List Int) := (path xs).findIdx? (fun v => s+v < lo || hi < s+v)

/-- One interval comparison at entry licenses all unchecked additions in an ordered chunk. -/
def PrefixExcursionSingleGuard.Goal : Prop := ∀ lo hi s xs, safe lo hi s xs = true ↔ lo ≤ s+low xs ∧ s+high xs ≤ hi

/-- The initial value and net change suffice if the chunk's signed sum is nonnegative. -/
def NetChangeVectorGuard.Goal : Prop := ∀ lo hi s xs, 0 ≤ xs.sum → lo ≤ s → s+xs.sum ≤ hi → safe lo hi s xs

/-- Fuse two compiled segments using only their totals and prefix extrema. -/
def ExcursionSummaryConcatenation.Goal : Prop := ∀ a b, (low (a++b),high (a++b)) = (min (low a) (a.sum+low b), max (high a) (a.sum+high b))

/-- Reuse an unchecked entry specialized at a smaller accumulator when only its upper margin shrinks. -/
def CounterOffsetGuardReuse.Goal : Prop := ∀ lo hi a b xs, a ≤ b → safe lo hi a xs → b+high xs ≤ hi → safe lo hi b xs

/-- A safe left-to-right SUM certifies every reassociated contiguous subtotal computed from zero. -/
def HorizontalTreeSafety.Goal : Prop := ∀ lo hi s a b c, lo ≤ 0 → 0 ≤ hi → safe lo hi s (a++b++c) → safe lo hi 0 b

/-- A selection mask that deletes terms preserves a previously certified prefix range. -/
def PredicateErasureMaintainsRange.Goal : Prop := ∀ lo hi s xs (keep : Int → Bool), safe lo hi s xs → safe lo hi s (xs.filter keep)

/-- Guard many identical vector iterations from one period and the sign of its drift. -/
def ExcursionPeriodUnroll.Goal : Prop := ∀ xs n, high ((List.replicate (n+1) xs).flatten) = high xs + max 0 ((n:Int)*xs.sum)

/-- Resume the scalar error locator after a certified prefix and recover the same failing addition. -/
def FailIndexAfterCleanPrefix.Goal : Prop := ∀ lo hi s a b, safe lo hi s a → faultAt lo hi s (a++b) = (faultAt lo hi (s+a.sum) b).map (a.length+·)

/-- Specialize reverse scans from the forward extrema and total without a second pass. -/
def ReverseChunkRange.Goal : Prop := ∀ xs, low xs.reverse = xs.sum - high xs ∧ high xs.reverse = xs.sum - low xs

/-- Binary-search for the first overflow using whether the midpoint accumulator alone is representable. -/
def EndpointBisectionOfOverflow.Goal : Prop := ∀ (hi s : Int) (xs : List Int) i j, i ≤ j → j ≤ xs.length → hi < s+(xs.take i).sum → hi < s+(xs.take j).sum

end TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards
