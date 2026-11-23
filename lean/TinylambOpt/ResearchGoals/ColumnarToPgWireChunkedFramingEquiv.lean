import Std

namespace TinylambOpt.ResearchGoals.ColumnarToPgWireChunkedFramingEquiv

inductive ColVec where
  | intCol : List (Option Int) → ColVec
  | strCol : List (Option (List Nat)) → ColVec
  deriving DecidableEq

inductive Cell where
  | intVal : Option Int → Cell
  | strVal : Option (List Nat) → Cell
  deriving DecidableEq

def colLength : ColVec → Nat
  | .intCol xs => xs.length
  | .strCol xs => xs.length

def cellAt (rowIdx : Nat) : ColVec → Cell
  | .intCol xs => .intVal (xs.getD rowIdx none)
  | .strCol xs => .strVal (xs.getD rowIdx none)

def transposeCols (cols : List ColVec) (n : Nat) : List (List Cell) :=
  (List.range n).map fun r => cols.map (cellAt r)

def be16 (n : Nat) : List Nat :=
  [ (n / 256) % 256
  , n % 256
  ]

def be32 (n : Nat) : List Nat :=
  [ (n / 16777216) % 256
  , (n / 65536) % 256
  , (n / 256) % 256
  , n % 256
  ]

def nullFieldMarker : List Nat := [255, 255, 255, 255] -- BE32(-1)

def natToAsciiDigitsGo (n fuel : Nat) (acc : List Nat) : List Nat :=
  match fuel with
  | 0 => acc
  | fuel' + 1 =>
      let d := 48 + (n % 10)
      let q := n / 10
      if q == 0 then d :: acc
      else natToAsciiDigitsGo q fuel' (d :: acc)

def natToAscii (n : Nat) : List Nat :=
  natToAsciiDigitsGo n (n + 1) []

def intToAsciiBytes (x : Int) : List Nat :=
  if x < 0 then 45 :: natToAscii (-x).toNat
  else natToAscii x.toNat

def encodeCellBytes : Cell → List Nat
  | .intVal none => nullFieldMarker
  | .intVal (some x) =>
      let payload := intToAsciiBytes x
      be32 payload.length ++ payload
  | .strVal none => nullFieldMarker
  | .strVal (some bytes) =>
      be32 bytes.length ++ bytes

/-- Reference `DataRow` PG wire encoding via row-by-row `Row` materialization:
    `'D' (0x44 = 68)` + BE32 `(4 + 2 + payload.length)` + BE16 `cols.length` +
    encoded fields. -/
def refEncodeDataRow (row : List Cell) : List Nat :=
  let fields := (row.map encodeCellBytes).flatten
  let frameLen := 4 + 2 + fields.length
  68 :: (be32 frameLen ++ be16 row.length ++ fields)

def refRowEncode (rows : List (List Cell)) : List Nat :=
  (rows.map refEncodeDataRow).flatten

/-- Computes decimal digit count without allocating any string buffer. -/
def natDecimalLenGo (n fuel : Nat) : Nat :=
  match fuel with
  | 0 => 0
  | fuel' + 1 =>
      if n < 10 then 1 else 1 + natDecimalLenGo (n / 10) fuel'

def natDecimalLen (n : Nat) : Nat :=
  natDecimalLenGo n (n + 1)

def intDecimalLen (x : Int) : Nat :=
  if x < 0 then 1 + natDecimalLen (-x).toNat
  else natDecimalLen x.toNat

def optColFieldWireLen (col : ColVec) (rowIdx : Nat) : Nat :=
  match col with
  | .intCol xs =>
      match xs.getD rowIdx none with
      | none => 4
      | some x => 4 + intDecimalLen x
  | .strCol xs =>
      match xs.getD rowIdx none with
      | none => 4
      | some bytes => 4 + bytes.length

/-- Computes the exact PG `DataRow` header frame length directly from decimal
    digit counts and string byte lengths before formatting cell payloads. -/
def optRowFrameLen (cols : List ColVec) (rowIdx : Nat) : Nat :=
  let fieldsLen :=
    (cols.map (fun col => optColFieldWireLen col rowIdx)).foldl (· + ·) 0
  4 + 2 + fieldsLen

def colVecCellBytes (col : ColVec) (rowIdx : Nat) : List Nat :=
  match col with
  | .intCol xs =>
      match xs.getD rowIdx none with
      | none => nullFieldMarker
      | some x =>
          let p := intToAsciiBytes x
          be32 p.length ++ p
  | .strCol xs =>
      match xs.getD rowIdx none with
      | none => nullFieldMarker
      | some bytes => be32 bytes.length ++ bytes

def optEncodeRowDirect (cols : List ColVec) (rowIdx : Nat) : List Nat :=
  let frameLen := optRowFrameLen cols rowIdx
  let fields := (cols.map (fun col => colVecCellBytes col rowIdx)).flatten
  68 :: (be32 frameLen ++ be16 cols.length ++ fields)

/-- Splits an accumulated wire buffer into full `chunkBytes` frames plus trailing
    partial bytes. -/
def drainFullChunksGo (xs : List Nat) (chunkBytes fuel : Nat) (acc : List (List Nat)) :
    List (List Nat) × List Nat :=
  match fuel with
  | 0 => (acc.reverse, xs)
  | fuel' + 1 =>
      if xs.length < chunkBytes then (acc.reverse, xs)
      else drainFullChunksGo (xs.drop chunkBytes) chunkBytes fuel' (xs.take chunkBytes :: acc)

/-- Row-at-a-time streaming chunk accumulator: encodes each row directly via
    `optRowFrameLen`, appends into the active buffer, and flushes completed
    `chunkBytes` chunks on the fly (modeling `PullSelectWireChunk`). -/
def optChunkedEncode (cols : List ColVec) (n chunkBytes : Nat) :
    List (List Nat) :=
  let (fullChunks, rem) :=
    (List.range n).foldl
      (fun (st : List (List Nat) × List Nat) (rowIdx : Nat) =>
        let (doneChunks, curBuf) := st
        let combined := curBuf ++ optEncodeRowDirect cols rowIdx
        let (flushed, nextBuf) := drainFullChunksGo combined chunkBytes combined.length []
        (doneChunks ++ flushed, nextBuf))
      ([], [])
  if rem.isEmpty then fullChunks else fullChunks ++ [rem]

/-- Direct columnar-to-PostgreSQL `DataRow` chunked wire serialization (H2O H2,
    H4 / BigBench B1 / LDBC L2 / STATS-CEB C2): computing `be32` frame headers
    directly via `intDecimalLen` / `bytes.length` without intermediate `Row`
    materialization and streaming `chunkBytes`-sized frames yields the exact same
    concatenated byte stream as `refRowEncode (transposeCols cols n)`. -/
def Goal : Prop :=
  ∀ (cols : List ColVec) (n chunkBytes : Nat),
    0 < chunkBytes →
    (∀ c : ColVec, c ∈ cols → colLength c = n) →
    (optChunkedEncode cols n chunkBytes).flatten = refRowEncode (transposeCols cols n)

example :
    let cols : List ColVec :=
      [ .intCol [some (-42), some 0, none]
      , .strCol [some [], none, some [104, 105]] -- "", NULL, "hi"
      ]
    (optChunkedEncode cols 3 1).flatten = refRowEncode (transposeCols cols 3) ∧
    (optChunkedEncode cols 3 16).flatten = refRowEncode (transposeCols cols 3) ∧
    (optChunkedEncode cols 3 256).flatten = refRowEncode (transposeCols cols 3) ∧
    (optChunkedEncode cols 3 7).length > 1 := by
  decide

example :
    intDecimalLen (-1000) = 5 ∧
    intDecimalLen 0 = 1 ∧
    optChunkedEncode [.intCol [], .strCol []] 0 64 = [] ∧
    refRowEncode (transposeCols [.intCol [], .strCol []] 0) = [] := by
  decide

end TinylambOpt.ResearchGoals.ColumnarToPgWireChunkedFramingEquiv
