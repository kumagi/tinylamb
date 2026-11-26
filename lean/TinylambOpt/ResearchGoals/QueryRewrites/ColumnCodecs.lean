import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.Compression.Wire

/-!
These goals specify lossless encoding and indexed reads for columns used by
SQL scans. They state the conditions under which a reader can reconstruct a
value or byte range from compact storage.
-/


namespace TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs

open TinylambOpt.ResearchGoals.Compression.Wire

-- Benchmark matchup: Storage.TpchDuckDB
/--
Decoding signed commit and receipt date deltas relative to the ship date recovers both
dates and leaves the trailing bytes intact. A lineitem scan can store and decode those
dates relative to one base.
-/
def SignedDateDeltaRoundtrip.Goal : Prop := ∀ (ship commit receipt : Int) (tail : Bytes),
  let wire := uleb (zig (commit-ship)) ++ uleb (zig (receipt-ship)) ++ tail
  (do let (a,rest) ← readU wire
      let (b,suffix) ← readU rest
      pure (ship+unzig a,ship+unzig b,suffix)) = some (commit,receipt,tail)

-- Benchmark matchup: Storage.TpchClickHouse
/--
Looking up the first cumulative run end greater than the row position returns the same
order key as an expanded column. A scan can locate a row's key without expanding all
repeated keys.
-/
def RunEndOrderKeyLookup.Goal : Prop := ∀ (runs : List (Nat × Nat)) (i : Nat),
  let ends := (runs.map Prod.snd).scanl (·+·) 0 |>.drop 1
  ((ends.zip (runs.map Prod.fst)).find? (fun r => i < r.1)).map Prod.snd =
    (runs.flatMap (fun (key,n) => List.replicate n key))[i]?

-- Benchmark matchup: Storage.TpcdsDuckDB
/--
A predicted product-warehouse grid plus position-indexed exceptions reconstructs the
original key sequence when the warehouse count is positive. The inventory scan can
derive regular keys and look up only exceptions.
-/
def GridKeysWithSparseExceptions.Goal : Prop := ∀ (rows : List (Nat × Nat)) start warehouses, 0 < warehouses →
  let predicted := fun i => ((start+i)/warehouses,(start+i)%warehouses)
  let exceptions := rows.zipIdx.filter (fun (v,i) => v != predicted i)
  (List.range rows.length).map (fun i =>
    ((exceptions.find? (fun e => e.2 == i)).map Prod.fst).getD (predicted i)) = rows

-- Benchmark matchup: Storage.TpcdsClickHouse
/--
A validity bit and its prefix rank select the correct packed residual for every non-NULL
row, provided all residuals fit the bit width. A column reader can recover individual
nullable keys without decoding the preceding keys.
-/
def NullableRankIndexedResiduals.Goal : Prop := ∀ (rows : List (Option Nat)) (base i w : Nat),
  (∀ v ∈ rows.filterMap id, base ≤ v ∧ v-base < 2^w) →
  let mask := rows.map Option.isSome
  let stream := pack w ((rows.filterMap id).map (·-base))
  (if mask[i]? == some true then some (base + takeAt stream w ((mask.take i).count true)) else none) =
    rows[i]?.join

-- Benchmark matchup: Storage.ClickDuckDB
/--
A stride anchor and the lengths since that anchor identify the exact byte range for an
in-bounds URL. A column reader can fetch a single URL using sparse offsets instead of
storing every absolute offset.
-/
def SparseOffsetsWithLocalLengthLookup.Goal : Prop := ∀ (rows : List Bytes) stride i, 0 < stride → i < rows.length →
  let lengths := rows.map List.length
  let anchor := (i/stride)*stride
  let offset := (lengths.take anchor).sum + ((lengths.drop anchor).take (i-anchor)).sum
  (rows.flatten.drop offset).take (lengths[i]?.getD 0) = rows[i]?.getD []

-- Benchmark matchup: Storage.ClickClickHouse
/--
For equal-length timestamp columns with residuals that fit the chosen width, the per-row
median and the selected signed residual reconstruct the timestamp at any valid index. A
column reader can recover one timestamp without expanding all three columns.
-/
def MedianClockResidualRoundtrip.Goal : Prop := ∀ (a b c : List Int) (w i : Nat),
  a.length = b.length → b.length = c.length → i < a.length →
  let anchors := (a.zip (b.zip c)).map (fun (x,y,z) => ([x,y,z].mergeSort)[1]?.getD 0)
  ∀ col ∈ [a,b,c],
    let residual := (col.zip anchors).map (fun (v,k) => zig (v-k))
    (∀ d ∈ residual, d < 2^w) →
    (anchors[i]?.getD 0) + unzig (takeAt (pack w residual) w i) = col[i]?.getD 0

def byteLane (page : Bytes) (width lane : Nat) :=
  (page.zipIdx.filter (fun p => p.2 % width == lane)).map Prod.fst

-- Benchmark matchup: Storage.AnalyticsDuckDB
/--
Transposing bytes into position-modulo-width lanes preserves each byte at its computed
lane offset. A storage reader can address the original byte after reorganizing a
fixed-width frame for compression.
-/
def ByteLaneTransposeRoundtrip.Goal : Prop := ∀ (page : Bytes) w i, 0 < w → i < page.length →
  let lanes := (List.range w).map (byteLane page w)
  let beginLane := ((lanes.take (i%w)).map List.length).sum
  lanes.flatten[beginLane + i/w]? = page[i]?

-- Benchmark matchup: Storage.AnalyticsClickHouse
/--
Given lossless frame codecs, reading the indexed intersections with a logical byte range
returns the same bytes as slicing the concatenated frames. A scan can decode only frames
that overlap the requested range.
-/
def IndexedFrameRangeRead.Goal : Prop := ∀ (frames : List Bytes) (enc dec : Bytes → Bytes) start len,
  (∀ b ∈ frames, dec (enc b) = b) →
  let starts := (frames.map List.length).scanl (·+·) 0
  let index := starts.zip (frames.map (fun b => (b.length,enc b)))
  let readSpan := fun (entry : Nat × Nat × Bytes) =>
    let (pos,sz,body) := entry
    if pos + sz ≤ start || start + len ≤ pos then []
    else ((dec body).drop (start-pos)).take (start+len-max pos start)
  index.flatMap readSpan = (frames.flatten.drop start).take len

-- Benchmark matchup: Storage.OfflineClickClickHouse
/--
A shared URL prefix and the stored original-URL suffix reconstruct the original URL and
leave the following bytes intact. A scan can store the common prefix once and recover
either URL when needed.
-/
def SharedUrlPrefixRoundtrip.Goal : Prop := ∀ (url original : Bytes) (tail : Bytes),
  let n := ((url.zip original).takeWhile (fun (a,b) => a == b)).length
  let suffix := original.drop n
  let frame := uleb n ++ uleb suffix.length ++ suffix ++ tail
  (do let (prefixSize,rest) ← readU frame
      let (suffixSize,bytes) ← readU rest
      pure (url.take prefixSize ++ bytes.take suffixSize, bytes.drop suffixSize)) = some (original,tail)

end TinylambOpt.ResearchGoals.QueryRewrites.ColumnCodecs
