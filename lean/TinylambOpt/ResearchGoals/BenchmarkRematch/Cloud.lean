import TinylambOpt.ResearchGoals.BenchmarkRematch.Common

namespace TinylambOpt.ResearchGoals.BenchmarkRematch.Cloud

/-- MLB Q108 は league・pos・yr の RID 集合の共通部分だけから単一の GROUP BY 状態を開く。計測は集約に渡す RID 数。 -/
def Public108DuckDB.Goal : Prop := ∀ (rows league pos yr : List Nat),
  let active := members (members league pos) yr
  let gathered := rows.filter active.contains
  gathered = rows.filter (fun i => league.contains i && pos.contains i && yr.contains i) ∧
    (gathered.isEmpty ↔ ¬ ∃ i ∈ rows, i ∈ league ∧ i ∈ pos ∧ i ∈ yr)

/-- MLB Q108 は全行が条件を満たすページで4列の MIN/MAX 要約を使い、混在ページだけを走査する。計測は実デコードした指標値数。 -/
def Public108ClickHouse.Goal : Prop := ∀ (pages : List (List Nat)) (passes : Nat → Bool)
    (metric : Fin 4 → Nat → Option Int) (col : Fin 4),
  let extrema := fun (vs : List Int) => (vs.min?,vs.max?)
  let summarize := fun (p : List Nat) =>
    if p.all passes then extrema (p.filterMap (metric col))
    else extrema ((p.filter passes).filterMap (metric col))
  let summaries := pages.map summarize
  ((summaries.filterMap Prod.fst).min?,(summaries.filterMap Prod.snd).max?) =
    extrema ((pages.flatten.filter passes).filterMap (metric col))

/-- Iceberg SQL base はファイルごとの行数と rid 合計を manifest の一致数で合成する。計測は実体化したファイル結合行数。 -/
def IceBaseDuckDB.Goal : Prop := ∀ (files : List Nat) (rows : List (Nat × Int)),
  let joinRows := rows.flatMap fun (f,r) => List.replicate (files.count f) r
  let perFile := fun f => (rows.filter (fun r => r.1 == f)).map Prod.snd
  (joinRows.length,nsum (joinRows.map some)) =
    ((files.map (fun f => (perFile f).length)).sum,nsum (files.map (fun f => nsum ((perFile f).map some))))

end TinylambOpt.ResearchGoals.BenchmarkRematch.Cloud
