import Std

/-! One VARCHAR column of a DataChunk and its optional row positions. Logical
rows can be flat or constant, nullable and empty strings are distinct. Slicing a
column must keep its collation, even when the selected rows are all NULL. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

abbrev Cell := Option String
inductive Layout where | flat (xs : List Cell) | constant (n : Nat) (x : Cell)
  deriving DecidableEq, Repr
def rows : Layout → List Cell
  | .flat xs => xs | .constant n x => List.replicate n x
def portion (c : Layout) (i n : Nat) : Layout := match c with
  | .flat xs => .flat ((xs.drop i).take n)
  | .constant size x => .constant (min n (size-i)) x
structure Chunk where
  collation : Nat
  column : Layout
  positions : Option (List Nat)
  deriving DecidableEq, Repr
def slice (c : Chunk) (start size : Nat) : Chunk :=
  {c with column := portion c.column start size,
          positions := c.positions.map (fun ids => (ids.drop start).take size)}
def unionSlice (chunks : List Layout) (off count : Nat) : List Cell :=
  match chunks with
  | [] => []
  | c::cs =>
    let part := rows (portion c off count)
    part ++ unionSlice cs (off-(rows c).length) (count-part.length)

/-- A compact constant column may be sliced before expansion, including a request wholly beyond its end. -/
def ConstantOrFlatSliceDecodesExactly.Goal : Prop := ∀ c i n,
  rows (portion c i n) = ((rows c).drop i).take n

/-- Nested views preserve positions, logical count and collation when the second view is clipped by the first. -/
def NestedChunkViewIdentity.Goal : Prop := ∀ c i n j k,
  slice (slice c i n) j k = slice c (i+j) (min k (n-j))

/-- OFFSET/LIMIT spanning several UNION ALL chunks agrees with the scalar bag in stream order. -/
def UnionAllCrossChunkLimitOffset.Goal : Prop := ∀ chunks off count,
  unionSlice chunks off count = ((chunks.flatMap rows).drop off).take count

/-- Data and physical row positions remain paired after projecting an arbitrary slice. -/
def SpillPositionsStayWithSlicedRows.Goal : Prop := ∀ c ids off n, c.positions = some ids →
  let sub := slice c off n
  (rows sub.column).zip (sub.positions.getD []) = (((rows c.column).zip ids).drop off).take n

/-- A NULL-only slice cannot infer collation from payload bytes after crossing a spill or projection boundary. -/
def NullSliceKeepsComparisonDomain.Goal : Prop := ∀ c off n, ((rows c.column).drop off |>.take n).all Option.isNone →
  let sub := slice c off n
  sub.collation = c.collation ∧ (rows sub.column).all Option.isNone

/-- A constant empty string must not be serialized as NULL when a limit lands exactly at the next chunk boundary. -/
def EmptyStringNullSliceBoundary.Goal : Prop := ∀ n m, 0 < n → 0 < m →
  unionSlice [.constant n (some ""),.constant m none] (n-1) 2 = [some "",none]

end TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices
