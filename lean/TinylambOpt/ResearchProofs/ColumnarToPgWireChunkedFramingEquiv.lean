import TinylambOpt.ResearchGoals.ColumnarToPgWireChunkedFramingEquiv

/-!
Proved

- `Goal` — Direct columnar-to-PostgreSQL `DataRow` chunked wire serialization (H2O H2,
  H4 / DictionaryJoinsAndLateLabels B1 / LDBC L2 / STATS-CEB C2): computing `be32`
  frame headers directly via `intDecimalLen` / `bytes.length` without intermediate
  `Row` materialization and streaming `chunkBytes`-sized frames yields the exact same
  concatenated byte stream as `refRowEncode (transposeCols cols n)`.

The reference model is defined in
`TinylambOpt.ResearchGoals.ColumnarToPgWireChunkedFramingEquiv`.
-/

namespace TinylambOpt.ResearchProofs.ColumnarToPgWireChunkedFramingEquiv

open TinylambOpt.ResearchGoals.ColumnarToPgWireChunkedFramingEquiv

-- The allocation-free digit counter computes exactly the length of the
-- ASCII decimal digits produced by `natToAsciiDigitsGo`.
private theorem natDigitsLenGo (n fuel : Nat) (acc : List Nat) :
    (natToAsciiDigitsGo n fuel acc).length = acc.length + natDecimalLenGo n fuel := by
  induction fuel generalizing n acc with
  | zero => simp [natToAsciiDigitsGo, natDecimalLenGo]
  | succ fuel ih =>
      unfold natToAsciiDigitsGo natDecimalLenGo
      by_cases h : n < 10
      · have hq : n / 10 = 0 := by omega
        simp [h, hq]
      · have hq : n / 10 ≠ 0 := by omega
        simp [h, hq, ih, Nat.add_assoc, Nat.add_comm]

private theorem natAsciiLen (n : Nat) : (natToAscii n).length = natDecimalLen n := by
  simpa [natToAscii, natDecimalLen] using natDigitsLenGo n (n + 1) []

private theorem intAsciiLen (x : Int) : (intToAsciiBytes x).length = intDecimalLen x := by
  by_cases h : x < 0
  · have hlen : (natToAscii (-x).toNat).length = natDecimalLen (-x).toNat := by
      simpa [natToAscii, natDecimalLen] using natDigitsLenGo (-x).toNat ((-x).toNat + 1) []
    simp [intToAsciiBytes, intDecimalLen, h, Nat.add_comm, hlen]
  · have hlen : (natToAscii x.toNat).length = natDecimalLen x.toNat := by
      simpa [natToAscii, natDecimalLen] using natDigitsLenGo x.toNat (x.toNat + 1) []
    simp [intToAsciiBytes, intDecimalLen, h, hlen]

-- Field bytes and field wire lengths agree between the row-materialized and
-- the columnar-direct encoders.
private theorem cellBytesEq (c : ColVec) (r : Nat) :
    encodeCellBytes (cellAt r c) = colVecCellBytes c r := by
  cases c with
  | intCol xs =>
      cases h : xs[r]?.getD none <;> simp [cellAt, encodeCellBytes, colVecCellBytes, h]
  | strCol xs =>
      cases h : xs[r]?.getD none <;> simp [cellAt, encodeCellBytes, colVecCellBytes, h]

private theorem fieldLenEq (c : ColVec) (r : Nat) :
    (encodeCellBytes (cellAt r c)).length = optColFieldWireLen c r := by
  cases c with
  | intCol xs =>
      cases h : xs[r]?.getD none with
      | none => simp [cellAt, encodeCellBytes, optColFieldWireLen, h, nullFieldMarker]
      | some x => simp [cellAt, encodeCellBytes, optColFieldWireLen, h, be32, intAsciiLen] <;> omega
  | strCol xs =>
      cases h : xs[r]?.getD none with
      | none => simp [cellAt, encodeCellBytes, optColFieldWireLen, h, nullFieldMarker]
      | some b => simp [cellAt, encodeCellBytes, optColFieldWireLen, h, be32] <;> omega

private theorem fieldsLenFoldl (cols : List ColVec) (r : Nat) :
    (cols.map (fun col => optColFieldWireLen col r)).foldl (· + ·) 0 =
      ((cols.map (fun col => encodeCellBytes (cellAt r col))).flatten).length := by
  have hgen : ∀ (acc : Nat) (cs : List ColVec),
      (cs.map (fun col => optColFieldWireLen col r)).foldl (· + ·) acc =
        acc + ((cs.map (fun col => encodeCellBytes (cellAt r col))).flatten).length := by
    intro acc cs
    induction cs generalizing acc with
    | nil => simp
    | cons c cs ih => simp [ih, fieldLenEq c r, Nat.add_assoc]
  simpa using hgen 0 cols

-- One directly encoded row is byte-for-byte the reference `DataRow` frame.
private theorem rowEq (cols : List ColVec) (r : Nat) :
    optEncodeRowDirect cols r = refEncodeDataRow (cols.map (cellAt r)) := by
  have hfields : (cols.map (fun col => colVecCellBytes col r)).flatten =
      ((cols.map (cellAt r)).map encodeCellBytes).flatten := by
    induction cols with
    | nil => rfl
    | cons c cs ih => simp [ih, cellBytesEq c r]
  have hlen := fieldsLenFoldl cols r
  have hmap : (cols.map (cellAt r)).map encodeCellBytes =
      (cols.map (fun col => encodeCellBytes (cellAt r col))) := by
    simp [List.map_map, Function.comp_def]
  have hlen' : (cols.map (fun col => optColFieldWireLen col r)).foldl (· + ·) 0 =
      (((cols.map (cellAt r)).map encodeCellBytes).flatten).length := by
    rw [hlen, ← hmap]
  unfold optEncodeRowDirect refEncodeDataRow optRowFrameLen
  rw [hfields, hlen']
  simp

-- Draining full chunks partitions the input byte stream; no bytes are lost.
private theorem drainConserves (xs : List Nat) (chunkBytes fuel : Nat)
    (acc : List (List Nat)) :
    (let r := drainFullChunksGo xs chunkBytes fuel acc
     acc.reverse.flatten ++ xs = r.1.flatten ++ r.2) := by
  induction fuel generalizing xs acc with
  | zero => simp [drainFullChunksGo]
  | succ fuel ih =>
      unfold drainFullChunksGo
      by_cases h : xs.length < chunkBytes
      · simp [h]
      · simp [h]
        simpa [List.take_append_drop, List.flatten_append] using
          ih (xs.drop chunkBytes) (xs.take chunkBytes :: acc)

-- Streaming invariant: emitted chunks plus the active buffer always equal
-- the concatenation of all rows encoded so far, independently of chunking.
private theorem foldConserves (cols : List ColVec) (chunkBytes : Nat) :
    ∀ (idxs : List Nat) (done : List (List Nat)) (cur : List Nat),
      (let st := idxs.foldl
          (fun (st : List (List Nat) × List Nat) (rowIdx : Nat) =>
            let (doneChunks, curBuf) := st
            let combined := curBuf ++ optEncodeRowDirect cols rowIdx
            let (flushed, nextBuf) := drainFullChunksGo combined chunkBytes combined.length []
            (doneChunks ++ flushed, nextBuf))
          (done, cur)
       st.1.flatten ++ st.2 =
         done.flatten ++ cur ++ (idxs.map (fun r => optEncodeRowDirect cols r)).flatten) := by
  intro idxs
  induction idxs with
  | nil => intro done cur; simp
  | cons r rs ih =>
      intro done cur
      simp only [List.foldl_cons]
      let combined := cur ++ optEncodeRowDirect cols r
      let flushed := (drainFullChunksGo combined chunkBytes combined.length []).1
      let nextBuf := (drainFullChunksGo combined chunkBytes combined.length []).2
      have hc := drainConserves combined chunkBytes combined.length []
      dsimp at hc
      have hi := ih (done ++ flushed) nextBuf
      dsimp at hi
      rw [hi]
      simp [List.flatten_append]
      have hc' : flushed.flatten ++ nextBuf = combined := by
        simpa [flushed, nextBuf] using hc.symm
      rw [← List.append_assoc, hc']
      simp [combined, List.append_assoc]

private theorem finalEquiv (cols : List ColVec) (n chunkBytes : Nat) :
    (optChunkedEncode cols n chunkBytes).flatten = refRowEncode (transposeCols cols n) := by
  unfold optChunkedEncode
  dsimp
  cases hst : (List.range n).foldl
          (fun (st : List (List Nat) × List Nat) (rowIdx : Nat) =>
            let (doneChunks, curBuf) := st
            let combined := curBuf ++ optEncodeRowDirect cols rowIdx
            let (flushed, nextBuf) := drainFullChunksGo combined chunkBytes combined.length []
            (doneChunks ++ flushed, nextBuf))
          ([], []) with
  | mk fullChunks rem =>
      have hcons := foldConserves cols chunkBytes (List.range n) [] []
      dsimp at hcons
      rw [hst] at hcons
      dsimp at hcons
      have hout : (if rem.isEmpty then fullChunks else fullChunks ++ [rem]).flatten =
          fullChunks.flatten ++ rem := by
        cases rem with
        | nil => simp
        | cons x xs => simp
      rw [hout, hcons]
      simp [refRowEncode, transposeCols, rowEq, List.map_map, Function.comp_def]

theorem sound : TinylambOpt.ResearchGoals.ColumnarToPgWireChunkedFramingEquiv.Goal := by
  intro cols n chunkBytes _hchunk _hrect
  exact finalEquiv cols n chunkBytes

end TinylambOpt.ResearchProofs.ColumnarToPgWireChunkedFramingEquiv

-- C++ references to this model or its proof.
-- query/sql_engine.hpp
-- server/postgres_protocol.cpp
-- server/postgres_protocol.hpp
-- server/postgres_server.cpp
