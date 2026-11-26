import TinylambOpt.ResearchGoals.November26.Common

namespace TinylambOpt.ResearchGoals.November26.Tpch
open BenchmarkRematch

/-- Q1 は日付上限までの完全ブロックで事前集約を使い、境界だけ走査する。観測量は数量を復号する行数。 -/
def Q01.Goal : Prop := ∀ (pages : List (List (Int × Option Int))) cutoff,
  let scan := fun (p : List (Int × Option Int)) => nsum ((p.filter (fun r => r.1 ≤ cutoff)).map Prod.snd)
  let state := fun (p : List (Int × Option Int)) =>
    if (p.map Prod.fst).max?.any (· ≤ cutoff) then nsum (p.map Prod.snd) else scan p
  nsum (pages.map state) = scan pages.flatten

/-- Q3 は売上の非負上界と確定済み10件から、集計不要な注文を判定する。観測量は省略した出荷明細。 -/
def Q03.Goal : Prop := ∀ (known : List Int) (pending : List Int) upper,
  pending.sum ≤ upper → 10 ≤ countWhere known (upper < ·) →
  (known.mergeSort.reverse.take 10) = ((pending.sum :: known).mergeSort.reverse.take 10)

/-- Q6 は日付・数量を通った行を割引率別に畳み、積を割引率ごとに一度取る。観測量は乗算数。 -/
def Q06.Goal : Prop := ∀ (rows : List (Int × Int)) lo hi,
  let eligible := rows.filter (fun (d,_) => lo ≤ d && d ≤ hi)
  total eligible (fun (d,p) => d*p) =
    total (eligible.map Prod.fst).eraseDups (fun d => d*(forKey eligible d).sum)

/-- Q7 は取引先の国を注文キーに伝播し、逆向きの相手国だけで明細を展開する。観測量は展開後の国ペア件数。 -/
def Q07.Goal : Prop := ∀ (orders : List (Nat × Bool)) (lines : List (Nat × Bool)) (supplier : Bool),
  let matchingOrders := (orders.filter (fun r => r.2 != supplier)).map Prod.fst
  let matchingLines := (lines.filter (fun r => r.2 == supplier)).map Prod.fst
  (matchingLines.map matchingOrders.count).sum =
    (lines.flatMap (fun (k,s) => (forKey orders k).filter (fun c => s == supplier && s != c))).length

/-- Q8 は年・仕入先別に volume を先に足し、仕入先ごとの国を後引きする。観測量は国の照会回数。 -/
def Q08.Goal : Prop := ∀ (sales : List (Nat × Int)) (peru : Nat → Bool),
  total (sales.filter (fun r => peru r.1)) Prod.snd =
    total ((sales.map Prod.fst).eraseDups.filter peru) (fun supplier => (forKey sales supplier).sum)

/-- Q10 は顧客の上位20件を決めた後に幅広いコメントと住所を引く。観測量は復元した顧客文字列バイト。 -/
def Q10.Goal : Prop := ∀ (ids : List Nat) (rank : Nat → Nat) (attrs : Nat → String),
  (((ids.map (fun i => (i,attrs i))).mergeSort (fun a b => decide (rank a.1 ≤ rank b.1))).take 20) =
    ((sortedBy ids rank).take 20).map (fun i => (i,attrs i))

/-- Q19 は品目側のブランド枝に数量区間を付け、共通の航空便条件を一度だけ通す。観測量は明細あたりの条件評価数。 -/
def Q19.Goal : Prop := ∀ (rows : List (Bool × Nat × Nat)),
  let bands := [(5,15),(15,25),(29,39)]
  let accepts := fun (r : Bool × Nat × Nat) => r.1 && (bands[r.2.1]?).any (fun (l,h) => l ≤ r.2.2 && r.2.2 ≤ h)
  rows.filter accepts = rows.filter (fun (air,tag,q) => air &&
    ((tag == 0 && 5 ≤ q && q ≤ 15) || (tag == 1 && 15 ≤ q && q ≤ 25) || (tag == 2 && 29 ≤ q && q ≤ 39)))

/-- Q20 は候補 partsupp のキー集合だけで数量を集約し、該当なしの SUM は NULL のままにする。観測量は年度集約のキー数。 -/
def Q20.Goal : Prop := ∀ (pairs : List (Nat × Nat)) (lines : List ((Nat × Nat) × Option Int)) k,
  k ∈ pairs → nsum (forKey (lines.filter (fun r => pairs.eraseDups.contains r.1)) k) = nsum (forKey lines k)

end TinylambOpt.ResearchGoals.November26.Tpch
