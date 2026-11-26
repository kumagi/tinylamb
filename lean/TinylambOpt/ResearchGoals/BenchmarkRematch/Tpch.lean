import TinylambOpt.ResearchGoals.BenchmarkRematch.Common

namespace TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch

/-- Q1 は日付を満たす行の割引後価格を一度計算して2つの SUM に配る。計測は割引積の評価回数。 -/
def Q01DuckDB.Goal : Prop := ∀ (rows : List (Int × Int × Int)),
  let discounted := rows.map fun (p,d,t) => (p*(100-d),100+t)
  (total discounted Prod.fst, total discounted (fun (v,t) => v*t)) =
  (total rows (fun (p,d,_) => p*(100-d)), total rows (fun (p,d,t) => p*(100-d)*(100+t)))

/-- Q3 は注文単位の売上を確定したワーカーで上位10件だけを merge に渡す。計測は最終ソート入力数。 -/
def Q03DuckDB.Goal : Prop := ∀ (shards : List (List Nat)),
  best 10 (shards.flatMap (best 10)) = best 10 shards.flatten

/-- Q7 は国を2値コードに落として逆向きの2組だけを通す。計測は nation 間の文字列比較回数。 -/
def Q07DuckDB.Goal : Prop := ∀ (rows : List (Nat × Nat)) a b, a ≠ b →
  rows.filter (fun (s,c) => [a,b].contains s && [a,b].contains c && s != c) =
  rows.filter (fun (s,c) => (s == a && c == b) || (s == b && c == a))

/-- Q8 は年ごとに PERU／その他の2状態を作り、分母をその和から得る。計測は volume の再読回数。 -/
def Q08DuckDB.Goal : Prop := ∀ (rows : List (Bool × Int)),
  total rows Prod.snd = total (rows.filter Prod.fst) Prod.snd + total (rows.filter (!·.1)) Prod.snd

/-- Q15 は supplier 別集約の局所最大と同値の全候補を残す。計測は最大値確定後の詳細行プローブ数。 -/
def Q15DuckDB.Goal : Prop := ∀ (shards : List (List (Nat × Int))),
  let maxima := fun (rs : List (Nat × Int)) => (rs.map Prod.snd).max?
  let candidates := shards.flatMap fun rs => rs.filter (fun r => some r.2 == maxima rs)
  candidates.filter (fun r => some r.2 == maxima candidates) =
    shards.flatten.filter (fun r => some r.2 == maxima shards.flatten)

end TinylambOpt.ResearchGoals.BenchmarkRematch.Tpch
