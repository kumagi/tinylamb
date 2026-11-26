import Std

/-! Emit aggregate or predicate kernels directly on the engine’s 64-row null words. Set bits mean SQL NULL. Physical padding bits after the logical row count are unconstrained. -/
namespace TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

abbrev W := BitVec 64
def window (n : Nat) : W := BitVec.ofNat 64 (2^(min 64 n)-1)
def ords (w : W) := (List.range 64).filter w.getLsbD
def valid (n : Nat) (nulls : W) := window n &&& ~~~nulls
def gather (w : W) (sel : List Nat) := sel.map w.getLsbD
def lanes (words : List W) (n : Nat) :=
  (List.range n).map (fun i => (words[i/64]?.getD 0).getLsbD (i%64))

/-- A word-level non-null version guard may ignore high padding bits in its last input word. -/
def NoNullsFromTailWord.Goal : Prop := ∀ n (m : W), n ≤ 64 → ((valid n m = window n) ↔ ∀ i < n, m.getLsbD i = false)

/-- Compute COUNT(col) as 64 minus the null popcount even on a short final word when padding bits are clear. -/
def PaddingBitComplementCount.Goal : Prop := ∀ n (m : W), n ≤ 64 → m &&& ~~~(window n) = 0 → (ords (valid n m)).length = 64-(ords m).length

/-- Derive validity of COALESCE from input masks alone and materialize only the lanes where one value exists. -/
def CoalesceNullIntersection.Goal : Prop := ∀ a b n, valid n (a &&& b) = valid n a ||| valid n b

/-- Generate the SQL CASE null mask with a bit select rather than per-lane tests; NULL predicate lanes use the else branch. -/
def BranchNullMaskMux.Goal : Prop := ∀ (truth predNull thenNull elseNull : W) i, i < 64 →
  let takeThen := truth &&& ~~~predNull
  ((takeThen &&& thenNull) ||| (~~~takeThen &&& elseNull)).getLsbD i =
    (if truth.getLsbD i && !(predNull.getLsbD i) then thenNull.getLsbD i else elseNull.getLsbD i)

/-- If each strict operand has at least one NULL, bypass the entire packet as an all-NULL result. -/
def StrictArithmeticAllNull.Goal : Prop := ∀ a b n, a &&& window n ≠ 0 → b &&& window n ≠ 0 → valid n (a ||| b) = 0

/-- Use low-six-bit lane indices with the first null word for an entire gathered packet when the indices span less than 64. -/
def GatherWordIndexNarrowing.Goal : Prop := ∀ (words : List W) start, ((List.range 32).map (fun j => (lanes words (start+32))[start+j]?)) =
  ((List.range 32).map (fun j => some ((words[start/64]?.getD 0).getLsbD ((start+j)%64))))

/-- Allow the unchecked unmasked value path if the actual selection word and null word are disjoint. -/
def SparseNullGuardOnDemand.Goal : Prop := ∀ (selection nulls : W), (selection &&& nulls = 0 ↔ ∀ i ∈ ords selection, nulls.getLsbD i = false)

/-- Reuse an unpacked validity vector from the previous column when the columns have equal null counts. -/
def SharedUnpackCacheCount.Goal : Prop := ∀ a b sel, (ords a).length = (ords b).length → gather a sel = gather b sel

/-- Pass a suffix of the original bitmap to a nested kernel without repacking when its slice starts at a word boundary. -/
def WordAlignedSliceReuse.Goal : Prop := ∀ ws base n, (lanes ws (64*base+n)).drop (64*base) = lanes (ws.drop base) n

/-- Comparing two nullable columns for IS NOT DISTINCT FROM requires equal validity and either NULL or equal payload. -/
def NullableEqualityMaskIntersection.Goal : Prop := ∀ (a b equalPayload : W) i, i < 64 →
  ((a &&& b) ||| (~~~(a ||| b) &&& equalPayload)).getLsbD i =
    (if a.getLsbD i then b.getLsbD i else !(b.getLsbD i) && equalPayload.getLsbD i)

end TinylambOpt.ResearchGoals.JitKernels.NullWordFusion
