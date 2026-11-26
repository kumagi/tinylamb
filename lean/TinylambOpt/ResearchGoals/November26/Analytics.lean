import TinylambOpt.ResearchGoals.November26.Common

namespace TinylambOpt.ResearchGoals.November26.Analytics
open BenchmarkRematch

/-- J1 は会社国・会社種別を通る movie_companies を映画別の重みとし、タイトルから件数と年和を引く。観測量はタイトル側との結合行数。 -/
def J1.Goal : Prop := ∀ (movieYears : List (Nat × Int)) (companies : List Nat),
  let expanded := companies.flatMap (forKey movieYears)
  (expanded.length,expanded.sum) =
    ((movieYears.map (fun r => companies.count r.1)).sum,total movieYears (fun (k,y) => y*(companies.count k:Int)))

/-- J2 は評価条件を満たす映画だけを drama の索引で探し、両側の重複度を年別件数に反映する。観測量は映画別に生成した組数。 -/
def J2.Goal : Prop := ∀ (titles drama ratings : List Nat),
  (titles.flatMap (fun k => (drama.filter (· == k)).flatMap (fun _ => ratings.filter (· == k)))).length =
    ((titles.filter ratings.contains).map (fun k => drama.count k * ratings.count k)).sum

/-- J3 は映画ごとに不適格を0、制作年を1..41の1バイトで引き、年の合計と COUNT を同時に復元する。観測量は映画プローブで読んだバイト。 -/
def J3.Goal : Prop := ∀ (movieIDs : List Nat) (year : Nat → Option Nat),
  (∀ k y, year k = some y → 1970 ≤ y ∧ y ≤ 2010) →
  let encode := fun k => ((year k).map (· - 1969)).getD 0
  let codes := movieIDs.map encode
  let hits := codes.length - codes.count 0
  (∀ c ∈ codes, c ≤ 41) ∧ (hits,codes.sum+1969*hits) =
    ((movieIDs.filterMap year).length,(movieIDs.filterMap year).sum)

/-- J5 は aka-1 の前方一致を同じ照合順の上限開区間に変換してからタイトルを引く。観測量は比較した別名文字列バイト。 -/
def J5.Goal : Prop := ∀ (aliases : List (String × Nat)),
  aliases.filter (fun r => r.1.startsWith "aka-1") =
    aliases.filter (fun r => "aka-1" ≤ r.1 && r.1 < "aka-2")

/-- C1 は posts の score と view_count の区画統計で不適格ページを省き、適格 post_id の順を保つ。観測量はコメント・投票照会前に飛ばしたページ数。 -/
def C1.Goal : Prop := ∀ (pages : List (List (Int × Int × Nat))),
  let qualify := fun (p : Int × Int × Nat) => decide (5 < p.1 ∧ p.2.1 < 10000)
  let possible := fun (p : List (Int × Int × Nat)) =>
    (p.map Prod.fst).max?.any (5 < ·) && (p.map (fun r => r.2.1)).min?.any (· < 10000)
  (pages.filter possible).flatten.filter qualify = pages.flatten.filter qualify

/-- C2 は適格投稿を owner ごとのキー集合にまとめ、コメント件数を投稿の重複ぶんだけ加算する。観測量は owner 集約前の行数。 -/
def C2.Goal : Prop := ∀ (posts : List (Nat × Nat)) (passedComments : List Nat) owner,
  let owned := forKey posts owner
  (owned.flatMap (fun p => passedComments.filter (· == p))).length =
    ((frequency owned).map (fun (p,n) => n*passedComments.count p)).sum

/-- H2 は完成したグループを件数バケットに散布し、件数の降順で同数グループを列挙する。観測量は比較ソートの比較回数。 -/
def H2.Goal : Prop := ∀ (groups : List (Nat × Nat)) maxCount,
  (∀ r ∈ groups, r.2 ≤ maxCount) →
  (List.range (maxCount+1)).reverse.flatMap (fun n => groups.filter (fun r => r.2 == n)) =
    groups.mergeSort (fun a b => decide (b.2 ≤ a.2))

/-- H4 は選択マスクの連続した通過区間だけを AVG に渡し、浮動小数の入力順も保つ。観測量はマスク判定の回数。 -/
def H4.Goal : Prop := ∀ (runs : List (Nat × List (Option Int))),
  (runs.filter (fun r => r.1 < 4)).flatMap (fun r => r.2.filterMap id) =
    (runs.flatMap (fun (id3,vs) => vs.map (fun v => (id3,v)))).filterMap
      (fun (k,v) => if k < 4 then v else none)

/-- H5 は結合キーごとの積を最大ペア数つきのタイルに分けて偏りを分散する。観測量は最大タイル行数と SUM の部分状態数。 -/
def H5.Goal : Prop := ∀ (xs ys : List Int) cap, 0 < cap →
  let pairs := xs.flatMap (fun x => ys.map (fun y => (x,y)))
  let tiles := (List.range ((pairs.length+cap-1)/cap)).map (fun t =>
    (List.range (min cap (pairs.length-t*cap))).filterMap (fun j => do
      let p := t*cap+j
      pure (← xs[p/ys.length]?, ← ys[p%ys.length]?)))
  tiles.flatten = pairs ∧ (∀ t ∈ tiles, t.length ≤ cap)

/-- L2 は年別メッセージに存在する forum だけの会員で年齢結合を行う。観測量は person 側に渡す会員数。 -/
def L2.Goal : Prop := ∀ (members : List (Nat × Nat)) (forums : List Nat) (ageMatches : Nat → Nat),
  let weight := fun (r : Nat × Nat) => forums.count r.1 * ageMatches r.2
  ((members.filter (fun r => forums.eraseDups.contains r.1)).map weight).sum = (members.map weight).sum

/-- L3 は正の score の返信を parent_id ごとの COUNT・SUM に圧縮し、親の年へ伝播する。観測量は親メッセージのプローブ数。 -/
def L3.Goal : Prop := ∀ (children : List (Option Nat × Int)) (parents : List Nat),
  let replies := fun p => (forKey children (some p)).filter (0 < ·)
  let joined := parents.flatMap replies
  (joined.length,joined.sum) =
    (((frequency parents).map (fun (p,n) => n*(replies p).length)).sum,
     total (frequency parents) (fun (p,n) => (n:Int)*(replies p).sum))

/-- L4 は親 ID の有効ビットと2012年の行マスクを交差させて score だけを順番に集める。観測量は復号した parent_id バイト。 -/
def L4.Goal : Prop := ∀ (rs : List (Nat × Option Nat × Option Int)),
  let indices := (rs.zipIdx.filter (fun (r,_) => r.1 == 2012 && r.2.1.isSome)).map Prod.snd
  valuesAt (rs.map (fun r => r.2.2)) indices =
    (rs.filter (fun (y,parent,_) => y == 2012 && parent != none)).map (fun r => r.2.2)

/-- L5 は同じ場所に住む適格人物の辺をまとめてから国を解決する。観測量は place へのプローブ数。 -/
def L5.Goal : Prop := ∀ (edgeAges : List (Nat × Option Int)) (country : Nat → Nat) k,
  let places := ((edgeAges.map Prod.fst).eraseDups).filter (fun p => country p == k)
  let ages := (edgeAges.filter (fun r => country r.1 == k)).map Prod.snd
  (ages.length,nsum ages,(ages.filterMap id).length) =
    ((places.map (fun p => (forKey edgeAges p).length)).sum,nsum (places.map (fun p => nsum (forKey edgeAges p))),
     (places.map (fun p => ((forKey edgeAges p).filterMap id).length)).sum)

/-- B3 は商品・店舗の物理 ID で一次集約し、同じ category・region へ二次集約する。観測量は次元ラベルの照会回数。 -/
def B3.Goal : Prop := ∀ (sales : List ((Nat × Nat) × Option Int)) (labels : Nat × Nat → Nat × Nat) k,
  let rawKeys := (sales.map Prod.fst).eraseDups.filter (fun p => labels p == k)
  nsum (rawKeys.map (fun p => nsum (forKey sales p))) =
    nsum ((sales.filter (fun r => labels r.1 == k)).map Prod.snd)

/-- B4 は検索語の辞書を走査し、data が部分一致した RID と米国 RID を重複なく合成する。観測量は LIKE の照合数。 -/
def B4.Goal : Prop := ∀ (rows : List (Bool × String)),
  let dict := (rows.map Prod.snd).eraseDups.filter (·.contains "data")
  (rows.zipIdx.filter (fun (r,_) => r.1 || dict.contains r.2)).map Prod.snd =
    (rows.zipIdx.filter (fun (r,_) => r.1 || r.2.contains "data")).map Prod.snd

/-- B5 はソート済み価格索引の境界から対象商品を作り、重複キーの結合件数も保つ。観測量は読んだ価格索引の範囲。 -/
def B5.Goal : Prop := ∀ (items : List (Int × Nat)) k, (items.map Prod.fst).Pairwise (· ≤ ·) →
  ((items.dropWhile (fun r => r.1 ≤ 100)).map Prod.snd).count k =
    countWhere items (fun r => 100 < r.1 && r.2 == k)

end TinylambOpt.ResearchGoals.November26.Analytics
