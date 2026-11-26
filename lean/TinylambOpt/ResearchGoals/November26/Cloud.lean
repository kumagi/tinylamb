import TinylambOpt.ResearchGoals.November26.Common

namespace TinylambOpt.ResearchGoals.November26.Cloud

/- One comparison orders each pair. Its smaller member enters only the MIN
   stream and its larger member enters only the MAX stream. -/
def extremes : List Int → List Int × List Int
  | [] => ([],[])
  | [x] => ([x],[x])
  | a::b::rest =>
    let (lows,highs) := extremes rest
    if a ≤ b then (a::lows,b::highs) else (b::lows,a::highs)

/-- MLB Q108 は4指標それぞれの MIN/MAX を対ごとの1比較から更新する。観測量は両端を求めるための比較命令数。 -/
def Public108.Goal : Prop := ∀ (rows : List (Fin 4 → Option Int)) (col : Fin 4),
  let vs := rows.filterMap (· col)
  let (lo,hi) := extremes vs
  (lo.min?,hi.max?) = (vs.min?,vs.max?) ∧
    vs.length/2 + (lo.length-1) + (hi.length-1) ≤ (3*vs.length+1)/2-2

/-- Iceberg SQL base は各ファイルの連続 RID 区間を等差数列として加算する。観測量は読み出した RID 数と例外区間数。 -/
def IceBase.Goal : Prop := ∀ (runs : List (Nat × Nat × Nat)) (manifest : List Nat),
  let rows := runs.flatMap fun (f,base,n) =>
    (List.range n).flatMap fun i => List.replicate (manifest.count f) (base+i)
  (rows.length,rows.sum) =
    ((runs.map (fun (f,_,n) => manifest.count f*n)).sum,
     (runs.map (fun (f,base,n) => manifest.count f*(n*(2*base+n-1)/2))).sum)

end TinylambOpt.ResearchGoals.November26.Cloud
