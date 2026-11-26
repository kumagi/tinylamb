import TinylambOpt.ResearchGoals.November26.Common

namespace TinylambOpt.ResearchGoals.November26.Tpcds
open BenchmarkRematch

/-- Q2 は sold_date_sk 別の nullable SUM を先に作って曜日と週を引く。観測量は date_dim へのプローブ数。 -/
def Q02.Goal : Prop := ∀ (sales : List (Nat × Option Int)) (weekDay : Nat → Nat × Nat) cell,
  nsum ((sales.filter (fun r => weekDay r.1 == cell)).map Prod.snd) =
    nsum ((((sales.map Prod.fst).eraseDups).filter (fun d => weekDay d == cell)).map (fun d => nsum (forKey sales d)))

/-- Q4 は前年の分母が正の顧客だけに2年目のチャネル集約を作る。観測量は2年目の顧客状態数。 -/
def Q04.Goal : Prop := ∀ (prev : Nat → List Int) (sales : List (Nat × Option Int)) k,
  let active := fun i => decide (0 < (prev i).sum)
  active k = true → forKey (sales.filter (fun r => active r.1)) k = forKey sales k

/-- Q11 は正分母の比率順序を整数の交差積で比較する。観測量は高精度除算へのフォールバック数。 -/
def Q11.Goal : Prop := ∀ (a b c d scale : Int), 0 < b → 0 < d → 0 < scale →
  b ∣ a*scale → d ∣ c*scale → (a*scale/b > c*scale/d ↔ a*d > c*b)

/-- Q14 は ROLLUP の上位階層に、HAVING を通った明細グループの nullable 状態だけを渡す。観測量は上位階層への投入行数。 -/
def Q14.Goal : Prop := ∀ (cells : List (List (Option Int))) threshold,
  let kept := cells.filter (fun g => (nsum g).any (threshold < ·))
  nsum (kept.map nsum) = nsum kept.flatten

/-- Q15 は ZIP の先頭5文字の判定を住所辞書に移し、同じ住所から繰り返す切出しを省く。観測量は ZIP を切り出した回数。 -/
def Q15.Goal : Prop := ∀ (zips allowed : List String) (codes : List Nat),
  let good := (zips.zipIdx.filter (fun (z,_) => allowed.contains (z.take 5).toString)).map Prod.snd
  codes.filter good.contains = codes.filter (fun i => zips[i]?.any (fun z => allowed.contains (z.take 5).toString))

/-- Q16 は候補注文に属する返品だけで除外表を作り、NULL 注文番号の SQL 等値を保つ。観測量は除外表のキー数。 -/
def Q16.Goal : Prop := ∀ (orders returns : List (Option Nat)),
  let anti := fun (ks : List (Option Nat)) => orders.filter (fun o => !(ks.any (hit o)))
  anti (returns.filter (fun r => orders.any (hit r))) = anti returns

/-- Q17 は数量をブロック内の基準値からの差に置き換え、分散の分子を小さい数で累積する。観測量はワイド整数へ昇格した状態数。 -/
def Q17.Goal : Prop := ∀ (quantities : List Int) origin,
  let numerator := fun (xs : List Int) => (xs.length:Int)*total xs (fun x => x*x) - xs.sum*xs.sum
  numerator (quantities.map (· - origin)) = numerator quantities

/-- Q21 は期間内の価格該当商品だけを対象とする在庫ブロックを商品ID範囲から選ぶ。観測量は復号した在庫ブロック数。 -/
def Q21.Goal : Prop := ∀ (blocks : List (List Nat)) (cheap : List Nat),
  let possible := fun (p : List Nat) => cheap.any (fun k => p.min?.any (· ≤ k) && p.max?.any (k ≤ ·))
  ((blocks.filter possible).flatten.filter cheap.contains) = blocks.flatten.filter cheap.contains

/-- Q25 は販売側の (customer,item,ticket) を返品側に伝播してから catalog と照合する。観測量は catalog へ渡す返品キー数。 -/
def Q25.Goal : Prop := ∀ (sales returns : List ((Nat × Nat) × Nat)) (catalog : Nat × Nat → Nat),
  let joinCount := fun (rs : List ((Nat × Nat) × Nat)) => (rs.map (fun k => sales.count k * catalog k.1)).sum
  joinCount (returns.filter sales.eraseDups.contains) = joinCount returns

/-- Q29 は ticket で一致した後の (customer,item) ごとに catalog 数量の非 NULL 和をキャッシュする。観測量は catalog 数量の再走査数。 -/
def Q29.Goal : Prop := ∀ (matchedTickets : List (Nat × Nat)) (catalog : (Nat × Nat) → List (Option Int)),
  nsum (matchedTickets.flatMap catalog) =
    nsum ((frequency matchedTickets).map (fun (k,n) => (nsum (catalog k)).map (fun v => v*(n:Int))))

/-- Q30 は州内平均の1.2倍との比較を州合計・非 NULL 人数で行う。観測量は顧客単位の除算数。 -/
def Q30.Goal : Prop := ∀ (stateTotals : List (Option Int)) (v : Int),
  let xs := stateTotals.filterMap id
  let n : Int := xs.length
  0 < n → 5*n ∣ 6*xs.sum → (v > 6*xs.sum/(5*n) ↔ 5*n*v > 6*xs.sum)

/-- Q35 は店舗購入者のうち必要な顧客だけで web・catalog の EXISTS を構築する。観測量は両経路の印の個数。 -/
def Q35.Goal : Prop := ∀ (customers store web catalog : List Nat),
  let demand := members customers store
  let extra := (web++catalog).filter demand.contains
  customers.filter (fun k => store.contains k && extra.contains k) =
    customers.filter (fun k => store.contains k && (web.contains k || catalog.contains k))

/-- Q38 は顧客IDを同姓同名の共通コードへ正規化してから日付との INTERSECT を取る。観測量は氏名比較バイト。 -/
def Q38.Goal : Prop := ∀ (s c w : List (String × Nat)) (code : String → Nat),
  (∀ x y, code x = code y → x = y) →
  let encode := fun (p : String × Nat) => (code p.1,p.2)
  (members (members (s.map encode) (c.map encode)) (w.map encode)).length = (members (members s c) w).length

/-- Q39 はグループ数量の最小・最大が同じなら変動係数の正のしきい値を通らず、二乗和更新を省ける。観測量は分散計算を省いたグループ数。 -/
def Q39.Goal : Prop := ∀ (xs : List Int) v, xs.min? = some v → xs.max? = some v →
  (xs.length:Int)*total xs (fun x => x*x) - xs.sum*xs.sum = 0

/-- Q40 は限定期間の販売キーだけで返品のハッシュ表を作り、返品なしの外部行を保つ。観測量は返品ビルド側の行数。 -/
def Q40.Goal : Prop := ∀ (sold : List (Nat × Nat)) (returns : List ((Nat × Nat) × Int)),
  let outerRows := fun (rs : List ((Nat × Nat) × Int)) => sold.flatMap (fun k =>
    let vs := forKey rs k
    if vs.isEmpty then [none] else vs.map some)
  outerRows (returns.filter (fun r => sold.contains r.1)) = outerRows returns

/-- Q41 は相関 COUNT(*) > 0 を適格メーカーの存在表で解決する。観測量は内側 item の行検査数。 -/
def Q41.Goal : Prop := ∀ (outerKeys : List (Option String)) (items : List (Option String × Bool)),
  let names := (items.filter Prod.snd).filterMap Prod.fst
  outerKeys.filter (fun k => k.any names.eraseDups.contains) =
    outerKeys.filter (fun k => 0 < countWhere items (fun r => hit k r.1 && r.2))

/-- Q45 は指定された物理キーに紐づく商品IDから全履歴の物理キーへ逆展開し、半結合を一度にする。観測量は売上側の商品ID文字列照合数。 -/
def Q45.Goal : Prop := ∀ (versions : List (Nat × Option String)) (seed : List Nat)
    (facts : List (Nat × Bool)),
  (versions.map Prod.fst).Nodup →
  let names := (versions.filter (fun r => seed.contains r.1)).filterMap Prod.snd
  let expanded := (versions.filter (fun r => r.2.any names.contains)).map Prod.fst
  let joinItem := fun k => versions.filter (fun r => r.1 == k)
  (facts.flatMap (fun (k,z) => (joinItem k).filter (fun r => z || r.2.any names.contains))) =
    ((facts.filter (fun (k,z) => z || expanded.contains k)).flatMap (fun r => joinItem r.1))

/-- Q49 は返品額の下限が NULL 延長行を必ず落とすので外部結合を内部結合に変える。観測量は生成した NULL 延長行。 -/
def Q49.Goal : Prop := ∀ (sold : List Nat) (refunds : Nat → List (Option Int)),
  let qualified := fun (x : Option Int) => x.any (10000 < ·)
  (sold.flatMap (fun k => if (refunds k).isEmpty then [none] else refunds k)).filter qualified =
    sold.flatMap (fun k => (refunds k).filter qualified)

/-- Q57 は対象年の窓平均の計算と前後年から読む隣接月を分離する。観測量は平均計算に含めるパーティション数。 -/
def Q57.Goal : Prop := ∀ (months : List ((Int × Nat) × Option Int)) (y : Int),
  let extend := months.filter (fun ((yr,m),_) => yr == y || (yr == y-1 && m == 12) || (yr == y+1 && m == 1))
  let yearValues := fun (rs : List ((Int × Nat) × Option Int)) =>
    (rs.filter (fun r => r.1.1 == y)).filterMap Prod.snd
  yearValues extend = yearValues months

/-- Q58 は3経路の売上が互いに許容範囲内かを最小値と最大値で判定する。観測量は売上比の比較回数。 -/
def Q58.Goal : Prop := ∀ (a b c : Nat),
  let xs := [a,b,c]
  (∀ x ∈ xs, ∀ y ∈ xs, 9*y ≤ 10*x ∧ 10*x ≤ 11*y) ↔
    (9*max a (max b c) ≤ 10*min a (min b c) ∧ 10*max a (max b c) ≤ 11*min a (min b c))

/-- Q59 は52週差の週ペアに対応する日付行の積を重みとし、同じ結果行の繰返しを遅延する。観測量は整列前の展開行数。 -/
def Q59.Goal : Prop := ∀ (weeks firstYear nextYear : List Nat) w,
  let joined := weeks.flatMap (fun k => (firstYear.filter (· == k)).flatMap
    (fun _ => (nextYear.filter (· == k+52)).map (fun _ => k)))
  joined.count w = weeks.count w * firstYear.count w * nextYear.count (w+52)

/-- Q62 は倉庫名の先頭20文字が同じ倉庫の集約状態を統合する。観測量は部分文字列の評価回数。 -/
def Q62.Goal : Prop := ∀ (rs : List (String × Nat)) name bin,
  let keys := (rs.map Prod.fst).eraseDups.filter (fun s => (s.take 20).toString == name)
  (keys.map (fun k => (forKey rs k).count bin)).sum = countWhere rs (fun r => (r.1.take 20).toString == name && r.2 == bin)

/-- Q63 はカテゴリ・クラス・ブランドの枝ごとの適格商品を集合和し、重複しても売上を一度だけ加える。観測量は fact 側の文字列述語数。 -/
def Q63.Goal : Prop := ∀ (facts a b : List Nat) (price : Nat → Int),
  total (facts.filter (a++b).eraseDups.contains) price =
    total (facts.filter (fun k => a.contains k || b.contains k)) price

/-- Q66 は倉庫の面積など一定の属性を月別加算の後で補う。観測量は月別グループキー幅。 -/
def Q66.Goal : Prop := ∀ (facts : List ((Nat × Nat) × Int)) (attrs : Nat → String) w m,
  let wide := facts.map (fun ((id,month),v) => ((id,attrs id,month),v))
  forKey wide (w,attrs w,m) = forKey facts (w,m)

/-- Q72 は不要な返品列を復元せず、一致する返品数とプロモーション有無で3つの件数を合成する。観測量は外部結合の生成行。 -/
def Q72.Goal : Prop := ∀ (promoCopies returnCopies : Nat),
  let promo := if promoCopies == 0 then [false] else List.replicate promoCopies true
  let expanded := promo.flatMap (List.replicate (max 1 returnCopies))
  (expanded.count false,expanded.count true,expanded.length) =
    (if promoCopies == 0 then max 1 returnCopies else 0, promoCopies*max 1 returnCopies,
     max 1 promoCopies * max 1 returnCopies)

/-- Q74 は前年の store/web の組に一致する顧客から順に評価し、100件が確定したら残りの比率計算を打ち切る。観測量は評価した顧客数。 -/
def Q74.Goal : Prop := ∀ (ordered : List Nat) (pass : Nat → Bool) cut,
  100 ≤ countWhere (ordered.take cut) pass →
    ((ordered.take cut).filter pass).take 100 = (ordered.filter pass).take 100

/-- Q75 は UNION DISTINCT の完全行をチャネル内でも圧縮してから年別数量を集約する。観測量は大域 DISTINCT に送る行数。 -/
def Q75.Goal : Prop := ∀ (channels : List (List (Nat × Int × Int))) k,
  let qty := fun (rs : List (Nat × Int × Int)) => total (rs.eraseDups.filter (fun r => r.1 == k)) (fun r => r.2.1)
  qty (channels.flatMap List.eraseDups) = qty channels.flatten

/-- Q79 は同じ ticket・顧客・市の中で住所が一意なときだけ、集約キーから住所を省く。観測量は狭いキーを使えたグループ数。 -/
def Q79.Goal : Prop := ∀ (rows : List (Nat × Option Int)) addr,
  (∀ r ∈ rows, r.1 = addr) →
  let wide := (rows.map Prod.fst).eraseDups.map (fun a => nsum (forKey rows a))
  wide = (if rows.isEmpty then [] else [nsum (rows.map Prod.snd)])

/-- Q80 はチャネル小計から総計を作り、空入力・全 NULL の SUM も再現する。観測量は総計への状態更新数。 -/
def Q80.Goal : Prop := ∀ (channels : List (List (List (Option Int)))),
  nsum (channels.map (fun ids => nsum (ids.map nsum))) = nsum channels.flatten.flatten

/-- Q81 は返品州内の顧客別合計を一度だけ集約し、現住所 GA の候補には州の合計と人数を引く。観測量は州平均の反復走査数。 -/
def Q81.Goal : Prop := ∀ (totals : List (Nat × Nat × Option Int)) (georgia : Nat → Bool),
  let vals := fun state => (totals.filter (fun r => r.1 == state)).filterMap (fun r => r.2.2)
  let states := ((totals.filter (fun r => georgia r.2.1)).map Prod.fst).eraseDups
  let cache := states.map (fun k => (k,((vals k).sum,(vals k).length)))
  ∀ r ∈ totals, georgia r.2.1 = true →
    forKey cache r.1 = [((vals r.1).sum,(vals r.1).length)]

/-- Q83 は item_id 順で3チャネルすべてにあるキーを100件見つけてから数量を読む。一意な非 NULL 集約キーが条件。観測量は復元した数量行数。 -/
def Q83.Goal : Prop := ∀ (sr cr wr : List Nat) stop (qty : Nat → Int × Int × Int),
  let common := members (members sr cr) wr
  let prefixOf := fun (keys : List Nat) => keys.filter (· ≤ stop)
  100 ≤ (prefixOf common).length →
  let winners := (members (members (prefixOf sr) (prefixOf cr)) (prefixOf wr)).mergeSort.take 100
  winners.map (fun k => (k,qty k)) =
    (common.mergeSort.map (fun k => (k,qty k))).take 100

/-- Q85 は2つの人口統計キーを同じ非 NULL 属性コードに正規化し、文字列の二重比較を省く。観測量は属性文字列の比較バイト。 -/
def Q85.Goal : Prop := ∀ (a b : Option (String × String)) (code : String × String → Nat),
  (∀ x y, code x = code y → x = y) → hit (a.map code) (b.map code) = hit a b

/-- Q86 は順位の比較を売上の相異値とその頻度に集約して同額を一括処理する。観測量は順位比較数。 -/
def Q86.Goal : Prop := ∀ (partition : List Int) value,
  1 + ((frequency partition).filter (fun r => value < r.1) |>.map Prod.snd).sum =
    1 + countWhere partition (value < ·)

/-- Q87 は店頭側に存在する氏名・日付の候補だけを保持し、他チャネルの行で印を消す。観測量は除外側の最大状態数。 -/
def Q87.Goal : Prop := ∀ (store catalog web : List Nat),
  let eraseSeen := fun (keys : List Nat) (k : Nat) => keys.filter (· != k)
  ((catalog++web).foldl eraseSeen store.eraseDups).length =
    countWhere store.eraseDups (fun k => !(catalog.contains k || web.contains k))

/-- Q89 は月間売上の区間全体が平均の許容帯に入ったブランド区画をまとめて棄却する。観測量は偏差を計算した月数。 -/
def Q89.Goal : Prop := ∀ (months : List Int) (n sum lo hi : Int),
  0 < n → 0 < sum → (∀ x ∈ months, lo ≤ x ∧ x ≤ hi) →
  9*sum ≤ 10*n*lo → 10*n*hi ≤ 11*sum →
    months.filter (fun x => 10*(n*x-sum).natAbs > sum.toNat) = []

/-- Q90 は同じ時刻・世帯・ページの fact を頻度表にし、両方の時間帯の次元照会を再利用する。観測量は次元表へのプローブ数。 -/
def Q90.Goal : Prop := ∀ (keys : List (Nat × Nat × Nat)) (passes : Nat × Nat × Nat → Nat) (hour : Nat → Nat),
  let band := fun (low : Nat) (k : Nat × Nat × Nat) =>
    if low ≤ hour k.1 ∧ hour k.1 ≤ low+1 then passes k else 0
  let hist := frequency keys
  [8,19].map (fun low => (keys.map (band low)).sum) =
    [8,19].map (fun low => (hist.map (fun (k,n) => n*band low k)).sum)

/-- Q99 は同じ販売日と出荷日の run を重み付き遅延バケットに変換する。観測量は日付差の評価回数。 -/
def Q99.Goal : Prop := ∀ (runs : List ((Int × Int) × Nat)) bin,
  let delay := fun (p : Int × Int) => bucket [30,60,90,120] (p.2-p.1)
  ((runs.filter (fun r => delay r.1 == bin)).map Prod.snd).sum =
    countWhere (runs.flatMap (fun (p,n) => List.replicate n p)) (fun p => delay p == bin)

end TinylambOpt.ResearchGoals.November26.Tpcds
