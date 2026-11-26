import TinylambOpt.ResearchGoals.BenchmarkRematch.Common

namespace TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

/-- Q1 は TN 店舗だけで顧客返品額をまとめて店舗平均を作る。計測は相関集約に渡す行数。 -/
def Q01DuckDB.Goal : Prop := ∀ (returns : List (Nat × Int)) (tn : Nat → Bool) store, tn store = true →
  ((returns.filter (fun r => tn r.1)).filter (fun r => r.1 == store)) =
    returns.filter (fun r => r.1 == store)

/-- Q2 は販売2経路の曜日別 nullable SUM を部分集約して結合する。計測は UNION 後に渡す行数。 -/
def Q02DuckDB.Goal : Prop := ∀ (a b : List (Option Int)), nsum [nsum a,nsum b] = nsum (a++b)

/-- Q2 は53週ずれた自己結合に配列参照を使い、日付側の重複度は保持する。計測は比較した週ペア数。 -/
def Q02ClickHouse.Goal : Prop := ∀ (a b : List Nat),
  (a.flatMap (fun w => b.filter (· == w+53))).length = (a.map (fun w => b.count (w+53))).sum

/-- Q4 は1顧客に必要な販売経路×2年の6スロットを用意する。計測は year_total の自己プローブ数。 -/
def Q04DuckDB.Goal : Prop := ∀ (xs : List ((Nat × Nat) × Int)), ∀ c y : Nat, c < 3 → y < 2 →
  xs.filter (fun r => r.1 == (c,y)) =
    xs.filter (fun r => r.1.2 < 2 && 2*r.1.1 + r.1.2 == 2*c+y)

/-- Q10 は店舗あり、かつ web／catalog のどちらかありという顧客集合を先に求める。計測は EXISTS プローブ数。 -/
def Q10DuckDB.Goal : Prop := ∀ (customers s w c : List Nat),
  customers.filter (fun k => s.contains k && (w.contains k || c.contains k)) =
    customers.filter ((members s (w++c)).contains ·)

/-- Q11 は重い顧客属性を年別比較の後に付ける。一意な顧客 ID に従属する属性に限る。計測は集約キー幅。 -/
def Q11DuckDB.Goal : Prop := ∀ (sales : List (Nat × Option Int)) (attr : Nat → String),
  let ids := (sales.map Prod.fst).eraseDups
  let wide := sales.map (fun (k,v) => ((k,attr k),v))
  let wideKeys := (wide.map Prod.fst).eraseDups
  ids.map (fun k => ((k,attr k), nsum ((sales.filter (fun r => r.1 == k)).map Prod.snd))) =
    wideKeys.map (fun key => (key, nsum ((wide.filter (fun r => r.1 == key)).map Prod.snd)))

/-- Q14 はブランド・クラス・カテゴリの組の全経路共通部分を一度作る。計測は item 側の会員照会数。 -/
def Q14DuckDB.Goal : Prop := ∀ (a b c : List (Nat × Nat × Nat)) k,
  (members (members a b) c).contains k = (a.contains k && b.contains k && c.contains k)

/-- Q15 は住所側の ZIP／州通過を先に印づけ、未通過行だけ売価を比較する。計測は売価のしきい値比較数。 -/
def Q15DuckDB.Goal : Prop := ∀ (rows : List (Bool × Int)),
  (rows.filter (fun (address,price) => address || price > 500)).Perm
    ((rows.filter Prod.fst) ++ rows.filter (fun (a,p) => !a && p > 500))

/-- Q16 は同一注文に別の非 NULL 倉庫があるかを両端で判定する。計測は catalog_sales の再走査行。 -/
def Q16DuckDB.Goal : Prop := ∀ (warehouses : List (Option Nat)) w,
  warehouses.any (fun v => v.any (· != w)) =
    (((warehouses.filterMap id).min?).any (· < w) || ((warehouses.filterMap id).max?).any (w < ·))

/-- Q17 は結合で複製される数量の2次モーメントを多重度で更新する。計測は STDDEV 状態の更新回数。 -/
def Q17DuckDB.Goal : Prop := ∀ (xs : List Int) n,
  total (xs.flatMap (List.replicate n)) (fun x => x*x) = (n:Int)*total xs (fun x => x*x)

/-- Q21 は在庫日の境界で分けた2本のストリームから before/after を加算する。計測は CASE 分岐数。 -/
def Q21DuckDB.Goal : Prop := ∀ (rows : List (Int × Int)) cut,
  (total rows (fun (d,q) => if d < cut then q else 0),total rows (fun (d,q) => if d < cut then 0 else q)) =
    (total (rows.filter (fun r => r.1 < cut)) Prod.snd,total (rows.filter (fun r => cut ≤ r.1)) Prod.snd)

/-- Q25 は完全な結合キー別に利益・損失を畳み、他側の fanout を一度掛ける。計測は三重結合の出力件数。 -/
def Q25DuckDB.Goal : Prop := ∀ (sales returns catalog : List Int),
  total (sales.flatMap (fun _ => returns.flatMap (fun v => catalog.map (fun _ => v)))) id =
    (sales.length:Int) * returns.sum * catalog.length

/-- Q29 は ticket 側の一致が存在する item/customer の catalog 数量だけを読む。計測は数量ペイロード読取件数。 -/
def Q29DuckDB.Goal : Prop := ∀ (catalog : List (Nat × Int)) (tickets : Nat → Nat),
  total (catalog.filter (fun r => tickets r.1 != 0)) (fun (k,q) => (tickets k:Int)*q) =
    total catalog (fun (k,q) => (tickets k:Int)*q)

/-- Q30 は候補の返品州を一度だけ抽出し、その州の全 CTE 行から平均を求める。計測は構築した州別状態数。 -/
def Q30DuckDB.Goal : Prop := ∀ (states : List Nat) (returns : List (Nat × Int)) k, k ∈ states →
  ((returns.filter (fun r => states.eraseDups.contains r.1)).filter (fun r => r.1 == k)) =
    returns.filter (fun r => r.1 == k)

/-- Q35 はグループキーに含まれる扶養者数の MIN/MAX を代表キーに置き換える。計測は集約状態バイト。 -/
def Q35DuckDB.Goal : Prop := ∀ (v : Option Int) n, 0 < n →
  let vals := (List.replicate n v).filterMap id
  (vals.min?,vals.max?) = (v,v)

/-- Q38 は3チャネルを氏名・日付の同じキー空間で交差させる。計測は二重 DISTINCT 入力数。 -/
def Q38DuckDB.Goal : Prop := ∀ (a b c : List (Option String × Option String × Nat)),
  (members (members a b) c).length = countWhere a.eraseDups (fun k => b.contains k && c.contains k)

/-- Q38 は日付で互いに素な区画を作り、区画ごとの共通件数だけを集める。計測は大域ハッシュ表バイト。 -/
def Q38ClickHouse.Goal : Prop := ∀ (s c w : List (Nat × Nat)) n, 0 < n →
  let byDay := fun (rs : List (Nat × Nat)) (p : Nat) => rs.filter (fun k => k.2 % n == p)
  let intersectCount := fun a b d => (members (members a b) d).length
  ((List.range n).map (fun p => intersectCount (byDay s p) (byDay c p) (byDay w p))).sum =
    intersectCount s c w

/-- Q39 は1月と2月だけで在庫の統計を作り自己結合する。計測は統計状態の個数。 -/
def Q39DuckDB.Goal : Prop := ∀ (xs : List (Nat × Int)) m, m = 1 ∨ m = 2 →
  ((xs.filter (fun r => r.1 == 1 || r.1 == 2)).filter (fun r => r.1 == m)) =
    xs.filter (fun r => r.1 == m)

/-- Q40 は返品のない売上も1回残し、返品件数と返金合計から差引額を作る。計測は外部結合中間件数。 -/
def Q40DuckDB.Goal : Prop := ∀ (price : Int) (refunds : List (Option Int)),
  total (if refunds.isEmpty then [none] else refunds) (fun r => price-r.getD 0) =
    (max 1 refunds.length:Int)*price - (refunds.filterMap id).sum

/-- Q49 は2つの RANK<=10 の和集合だけを詳細化する。同順位を全件残す。計測は順位付け後の転送量。 -/
def Q49DuckDB.Goal : Prop := ∀ (rows : List (Nat × Nat)),
  let first := rows.map Prod.fst
  let second := rows.map Prod.snd
  let withinTen := fun (keys : List Nat) (x : Nat) => (keys.mergeSort[9]?).all (x ≤ ·)
  rows.filter (fun (x,y) => withinTen first x || withinTen second y) =
    rows.filter (fun (x,y) => countWhere first (· < x) < 10 || countWhere second (· < y) < 10)

/-- Q57 は隣接 rank の探索を整列済み3行窓に置く。連番で重複のないパーティションが条件。計測は自己結合プローブ数。 -/
def Q57DuckDB.Goal : Prop := ∀ (xs : List Int) (i : Nat),
  let lookup := fun (k : Nat) => (xs.zipIdx.find? (fun r => r.2 == k)).map Prod.fst
  (lookup i, lookup (i+1), lookup (i+2)) = (xs[i]?, xs[i+1]?, xs[i+2]?)

/-- Q58 は該当週の日付を一度解決して各チャネルの事実表に配る。計測は date_dim の検索回数。 -/
def Q58DuckDB.Goal : Prop := ∀ (days : List (Nat × Nat)) week date,
  ((days.filter (fun r => r.2 == week)).map Prod.fst).contains date =
    days.any (fun (d,w) => d == date && w == week)

/-- Q59 は52週差で相手がある店舗・週だけを比較に渡す。日付側の重複は保持する。計測は join への入力。 -/
def Q59DuckDB.Goal : Prop := ∀ (left right : List (Nat × Nat)),
  let hits := fun (r : Nat × Nat) => right.count (r.1,r.2+52)
  (left.map hits).sum = ((left.filter (fun k => hits k != 0)).map hits).sum

/-- Q62 は発送遅延を1つのバケット番号に変換する。計測は5本の CASE の比較回数。 -/
def Q62DuckDB.Goal : Prop := ∀ d : Int,
  let b := bucket [30,60,90,120] d
  (b == 0, b == 1, b == 2, b == 3, b == 4) =
    (decide (d ≤ 30), decide (30 < d ∧ d ≤ 60), decide (60 < d ∧ d ≤ 90),
     decide (90 < d ∧ d ≤ 120), decide (120 < d))

/-- Q63 は manager 内の月別合計に対する平均との差を、合計と有効月数で評価する。計測は窓行の再読回数。 -/
def Q63DuckDB.Goal : Prop := ∀ (blocks : List (List (Option Int))),
  ((blocks.flatten.filterMap id).sum,(blocks.flatten.filterMap id).length) =
    ((blocks.map (fun b => (b.filterMap id).sum)).sum,(blocks.map (fun b => (b.filterMap id).length)).sum)

/-- Q66 はチャネル別月次売上を12スロットで統合する。計測は数値状態の merge 回数。 -/
def Q66DuckDB.Goal : Prop := ∀ (pages : List (List (Nat × Int))) month,
  total pages.flatten (fun (m,v) => if m == month then v else 0) =
    total pages (fun p => total (p.filter (fun r => r.1 == month)) Prod.snd)

/-- Q72 は商品・週の在庫数量を整列し、販売数量未満の件数を境界位置から得る。計測は在庫比較数。 -/
def Q72DuckDB.Goal : Prop := ∀ (stock : List Int) sold,
  (stock.mergeSort.takeWhile (· < sold)).length = countWhere stock (· < sold)

/-- Q74 は両経路の2年間が揃った顧客だけで比率を計算する。計測は年次集約後の除算数。 -/
def Q74DuckDB.Goal : Prop := ∀ (s1 s2 w1 w2 : List Nat) k,
  (members (members s1 s2) (members w1 w2)).contains k =
    (s1.contains k && s2.contains k && w1.contains k && w2.contains k)

/-- Q75 は各チャネルで返品による重複を反映して数量差を集める。計測は年月別統合への入力件数。 -/
def Q75DuckDB.Goal : Prop := ∀ (q : Int) (rs : List Int),
  (if rs.isEmpty then q else total rs (fun v => q-v)) = (max 1 rs.length:Int)*q-rs.sum

/-- Q79 は city の30文字切出しを (city, ticket) 集約後に行う。計測は文字列切出し回数。 -/
def Q79DuckDB.Goal : Prop := ∀ (cities : List String) name,
  ((frequency cities).filter (fun p => (p.1.take 30).toString == name) |>.map Prod.snd).sum =
    countWhere cities (fun s => (s.take 30).toString == name)

/-- Q81 は顧客IDで100位の境界を求め、同値を全件残してから住所順を解決する。計測は住所列復元バイト。 -/
def Q81DuckDB.Goal : Prop := ∀ (ids : List Nat) (addressKey : Nat → Nat) (customerKey : Nat → Nat),
  let prefixKeys := (ids.map customerKey).mergeSort
  let candidates := ids.filter (fun i => (prefixKeys[99]?).all (customerKey i ≤ ·))
  let cmp := fun (a b : Nat) => decide (customerKey a < customerKey b ∨
    (customerKey a = customerKey b ∧ addressKey a ≤ addressKey b))
  (candidates.mergeSort cmp).take 100 = (ids.mergeSort cmp).take 100

/-- Q83 は3つの日付が属する週の和集合で返品をフィルタする。同じ週が重なっても重複させない。計測は日付プローブ。 -/
def Q83DuckDB.Goal : Prop := ∀ (weeks : List Nat) (returns : List (Nat × Int)),
  returns.filter (fun r => weeks.eraseDups.contains r.1) = returns.filter (fun r => weeks.contains r.1)

/-- Q85 は3つの nullable AVG が分母を混同しない部分状態を使う。計測は結合行の再走査数。 -/
def Q85DuckDB.Goal : Prop := ∀ (rs : List (Option Int × Option Int × Option Int)) n,
  let cols : List ((Option Int × Option Int × Option Int) → Option Int) :=
    [Prod.fst, (fun r => r.2.1), (fun r => r.2.2)]
  cols.map (fun f => let vs := (rs.flatMap (List.replicate n)).filterMap f
                     (vs.sum, vs.length)) =
    cols.map (fun f => let vs := rs.filterMap f
                       ((n:Int)*vs.sum, n*vs.length))

/-- Q86 は subtotal NULL と実データ NULL を別の grouping-tag で順位付けする。計測は順位区画のソート入力。 -/
def Q86DuckDB.Goal : Prop := ∀ (xs : List ((Nat × Option Nat) × Int)) tag parent sales,
  let partition := (xs.filter (fun r => r.1 == (tag,parent))).map Prod.snd
  1 + (partition.mergeSort.reverse.takeWhile (sales < ·)).length =
    1 + countWhere xs (fun r => r.1 == (tag,parent) && sales < r.2)

/-- Q87 は2経路の氏名・日付の和集合を除外表にする。計測は EXCEPT 中間表の要素数。 -/
def Q87DuckDB.Goal : Prop := ∀ (s c w : List (Option String × Option String × Nat)),
  (s.eraseDups.filter (!c.contains ·)).filter (!w.contains ·) = s.eraseDups.filter (!(c++w).contains ·)

/-- Q90 は午前と午後のフィルタ済み入力を同一走査で数える。計測は web_sales の読取バイト。 -/
def Q90DuckDB.Goal : Prop := ∀ (hours : List Nat), let selected := hours.filter ([8,9,19,20].contains ·)
  (countWhere selected (· ≤ 9),countWhere selected (19 ≤ ·)) =
    (countWhere hours ([8,9].contains ·),countWhere hours ([19,20].contains ·))

/-- Q99 は遅延区分の局所ヒストグラムを warehouse・配送・call center ごとに足す。計測は大域状態の更新数。 -/
def Q99DuckDB.Goal : Prop := ∀ (parts : List (List (Nat × Option Int))) groupKey,
  let classify := fun (p : List (Nat × Option Int)) =>
    ((p.filter (fun r => r.1 == groupKey)).filterMap Prod.snd).map (bucket [30,60,90,120])
  let bins := List.range 5
  bins.map (fun c => (parts.map (fun p => (classify p).count c)).sum) =
    bins.map (classify parts.flatten).count

end TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds
