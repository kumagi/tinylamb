import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-!
These goals model exact aggregate results from row ID ranges, file weights, and paired
extrema.

They establish when SQL execution can aggregate file metadata or share comparisons
without scanning each value separately for each result.
-/

namespace TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema

-- Benchmark matchup: Cloud.Public108DuckDB
/--
Intersecting league, position, and year row-ID sets gives exactly the rows passing all
three filters. The aggregate can read only those row IDs.
-/
def IntersectedRowMasksDetermineGroupExistence.Goal : Prop := ∀ (rows league pos yr : List Nat),
  let active := members (members league pos) yr
  let gathered := rows.filter active.contains
  gathered = rows.filter (fun i => league.contains i && pos.contains i && yr.contains i) ∧
    (gathered.isEmpty ↔ ¬ ∃ i ∈ rows, i ∈ league ∧ i ∈ pos ∧ i ∈ yr)

-- Benchmark matchup: Cloud.Public108ClickHouse
/--
For pages fully selected by the query, stored extrema can be combined with extrema
computed by scanning mixed pages. This reduces the number of metric values decoded.
-/
def QualifiedPageExtremaReuse.Goal : Prop := ∀ (pages : List (List Nat)) (passes : Nat → Bool)
    (metric : Fin 4 → Nat → Option Int) (col : Fin 4),
  let extrema := fun (vs : List Int) => (vs.min?,vs.max?)
  let summarize := fun (p : List Nat) =>
    if p.all passes then extrema (p.filterMap (metric col))
    else extrema ((p.filter passes).filterMap (metric col))
  let summaries := pages.map summarize
  ((summaries.filterMap Prod.fst).min?,(summaries.filterMap Prod.snd).max?) =
    extrema ((pages.flatten.filter passes).filterMap (metric col))

-- Benchmark matchup: Cloud.IceBaseDuckDB
/--
Per-file counts and row-ID sums weighted by manifest multiplicity reproduce the modeled
joined totals. The executor can avoid expanding each matching manifest row.
-/
def ManifestWeightedFileTotals.Goal : Prop := ∀ (files : List Nat) (rows : List (Nat × Int)),
  let joinRows := rows.flatMap fun (f,r) => List.replicate (files.count f) r
  let perFile := fun f => (rows.filter (fun r => r.1 == f)).map Prod.snd
  (joinRows.length,nsum (joinRows.map some)) =
    ((files.map (fun f => (perFile f).length)).sum,nsum (files.map (fun f => nsum ((perFile f).map some))))

/- One comparison orders each pair. Its smaller member enters only the MIN
   stream and its larger member enters only the MAX stream. -/
def extremes : List Int → List Int × List Int
  | [] => ([],[])
  | [x] => ([x],[x])
  | a::b::rest =>
    let (lows,highs) := extremes rest
    if a ≤ b then (a::lows,b::highs) else (b::lows,a::highs)

-- Benchmark query: Cloud.Public108
/--
Comparing non-NULL values in pairs and taking minima of the lower values and maxima of
the higher values preserves both extrema and satisfies the modeled comparison bound.

A scan computing MIN and MAX together can share the comparison within each pair and
handle an unmatched last value.
-/
def PairedExtremaReduction.Goal : Prop := ∀ (rows : List (Fin 4 → Option Int)) (col : Fin 4),
  let vs := rows.filterMap (· col)
  let (lo,hi) := extremes vs
  (lo.min?,hi.max?) = (vs.min?,vs.max?) ∧
    vs.length/2 + (lo.length-1) + (hi.length-1) ≤ (3*vs.length+1)/2-2

-- Benchmark query: Cloud.IceBase
/--
For contiguous row ID runs, the row count and sum equal the closed-form progression
totals weighted by file occurrences in the manifest.

An executor with certified run bounds can compute COUNT and exact SUM by range. It needs
the count to recover SQL SUM's NULL result on empty input.
-/
def ContiguousFileRowIdTotals.Goal : Prop := ∀ (runs : List (Nat × Nat × Nat)) (manifest : List Nat),
  let rows := runs.flatMap fun (f,base,n) =>
    (List.range n).flatMap fun i => List.replicate (manifest.count f) (base+i)
  (rows.length,rows.sum) =
    ((runs.map (fun (f,_,n) => manifest.count f*n)).sum,
     (runs.map (fun (f,base,n) => manifest.count f*(n*(2*base+n-1)/2))).sum)

end TinylambOpt.ResearchGoals.QueryRewrites.FileRangesAndExtrema

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
