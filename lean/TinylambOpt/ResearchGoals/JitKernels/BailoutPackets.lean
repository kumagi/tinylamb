import Std

/-! Strip-mined compiled kernels validate packets then commit outputs or replay faults. Observable results include the input ordinal and error code, even if selection and physical lane order disagree. -/
namespace TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

abbrev Cell := Except Nat (Option Int)
def bad : Cell → Bool | .error _ => true | _ => false
def first (xs : List Cell) := xs.zipIdx.findSome? (fun (c,i) => match c with
  | .error e => some (i,e) | _ => none)
def prefixOut (xs : List Cell) := (xs.takeWhile (fun c => !(bad c))).filterMap (fun c => c.toOption.join)
def gather (xs : List Cell) (sel : List Nat) := sel.filterMap (fun i => xs[i]?)

/-- Translate a fault reported by a compacted packet back to the actual row selected for that lane. -/
def CompactedBailoutOrdinal.Goal : Prop := ∀ xs sel j e, (∀ i ∈ sel, i < xs.length) → first (gather xs sel) = some (j,e) →
  ∃ i, sel[j]? = some i ∧ xs[i]? = some (.error e)

/-- Report the smallest physical input ordinal among faults even when a selection vector repeats and reorders inputs. -/
def LowestSourceOrdinalIsFirstFault.Goal : Prop := ∀ xs sel j e, first (gather xs sel) = some (j,e) →
  ∀ (k i z : Nat), sel[k]? = some i → xs[i]? = some (.error z) → sel[j]?.getD i ≤ i

/-- Reexecute just the first failing packet after skipping successful packets while retaining exact preceding output. -/
def WordOfFaultsReplayPrefix.Goal : Prop := ∀ a b, first a = none → prefixOut (a++b) = prefixOut a ++ prefixOut b

/-- Two error-producing sites evaluated in separate vector passes may report the first failing site before the first failing row. -/
def LaneMajorSitePriority.Goal : Prop := ∀ (a b : List Cell), a.length = b.length →
  let lanes := a.zipWith (fun x y => if bad x then x else y) b
  (first a).orElse (fun _ => first b) = first lanes

/-- An error ballot can be intersected with demanded ordinals before taking the cold path. -/
def ErrorBitsSelectedOnly.Goal : Prop := ∀ xs sel, (∀ i ∈ sel, i < xs.length) →
  (first (gather xs sel)).isSome = sel.any (fun i => xs[i]?.any bad)

/-- Infer the replay cursor from successfully materialized output count when only a compact output buffer survived. -/
def SuccessfulNullLaneProgress.Goal : Prop := ∀ xs i e, first xs = some (i,e) → (prefixOut xs).length = i

/-- Starting scalar replay on the reported failing row observes the original site immediately, even after NULL-producing rows. -/
def ColdReplayAtFirstFault.Goal : Prop := ∀ xs i e, first xs = some (i,e) → first (xs.drop i) = some (0,e)

/-- When a fault exists, commit exactly the materializable values in the preceding physical prefix. -/
def ScatterEarlierSuccessesOnly.Goal : Prop := ∀ xs i e, first xs = some (i,e) → prefixOut xs = ((xs.take i).filterMap (fun c => c.toOption.join))

/-- Resume a packet at the next vector-aligned offset after a fault, assuming the rest of that packet can be reconstructed from its fault bit. -/
def MisalignedPacketResume.Goal : Prop := ∀ xs i e w, 0 < w → first xs = some (i,e) →
  prefixOut (xs.drop (i+1)) = prefixOut (xs.drop ((i/w+1)*w))

end TinylambOpt.ResearchGoals.JitKernels.BailoutPackets
