import TinylambOpt.ResearchGoals.BenchmarkRematch.Common

namespace TinylambOpt.ResearchGoals.BenchmarkRematch.Click

/-- Q12 は端末・機種の組ごとに UserID を辞書化し、ブロック内で重複を落とす。計測は大域 DISTINCT への挿入数。 -/
def Q12DuckDB.Goal : Prop := ∀ (parts : List (List Nat)),
  (parts.flatMap List.eraseDups).eraseDups = parts.flatten.eraseDups

/-- Q14 は UserID で分割して同じ検索語に対する局所 DISTINCT 件数を加算する。計測は統合時の ID 転送量。 -/
def Q14ClickHouse.Goal : Prop := ∀ (users : List Nat) n, 0 < n →
  ((List.range n).map (fun i => (users.filter (fun u => u%n == i)).eraseDups.length)).sum = users.eraseDups.length

/-- Q18 は最初の10グループを固定し、残りの走査ではその組だけを加算する。計測はグループ表の最大要素数。 -/
def Q18ClickHouse.Goal : Prop := ∀ (xs : List (Nat × Nat)),
  let keep := xs.eraseDups.take 10
  keep.map (fun k => (xs.filter keep.contains).count k) = keep.map xs.count

/-- Q20 はブロックの UserID 最小・最大で点検索対象を絞る。計測はデコードしたブロック数。 -/
def Q20DuckDB.Goal : Prop := ∀ (blocks : List (List Int)) k,
  (blocks.filter (fun b => (b.min?).any (· ≤ k) && (b.max?).any (k ≤ ·))).flatten.count k = blocks.flatten.count k

/-- Q20 は圧縮辞書における対象 ID の出現数から定数行を生成する。計測は元行ペイロードの読取数。 -/
def Q20ClickHouse.Goal : Prop := ∀ (xs : List Int) k,
  List.replicate (xs.count k) k = xs.filter (· == k)

/-- Q21 は URL 辞書の語ごとに部分一致し、頻度で COUNT を得る。計測は文字列照合回数。 -/
def Q21DuckDB.Goal : Prop := ∀ (urls : List String),
  ((frequency urls).filter (fun r => r.1.contains "google") |>.map Prod.snd).sum =
    countWhere urls (·.contains "google")

/-- Q21 は必須部分文字列 goo のある区画だけを検査する。計測は再照合した URL バイト数。 -/
def Q21ClickHouse.Goal : Prop := ∀ (blocks : List (List String)),
  countWhere ((blocks.filter (fun b => b.any (·.contains "goo"))).flatten) (·.contains "google") =
    countWhere blocks.flatten (·.contains "google")

/-- Q22 は確定した局所群の MIN(URL) を統合する。計測は検索語ごとの文字列コピー数。 -/
def Q22DuckDB.Goal : Prop := ∀ (shards : List (List String)),
  (shards.filterMap List.min?).min? = shards.flatten.min?

/-- Q22 は同じ URL の gram 適合を一度だけ判定し、検索語別の頻度を残す。計測は LIKE の評価数。 -/
def Q22ClickHouse.Goal : Prop := ∀ (rows : List (String × String)) phrase, phrase ≠ "" →
  ((frequency rows).filter (fun ((u,p),_) => u.contains "google" && p == phrase) |>.map Prod.snd).sum =
    countWhere rows (fun (u,p) => p == phrase && u.contains "google")

/-- Q23 は Title 条件と非空検索語を先に通し、残りの URL にだけ否定 LIKE を適用する。計測は URL 照合バイト。 -/
def Q23DuckDB.Goal : Prop := ∀ (xs : List (String × String × String)),
  ((xs.filter (fun (p,t,_) => p != "" && t.contains "Google")).filter (fun (_,_,u) => !u.contains ".google.")) =
  xs.filter (fun (p,t,u) => t.contains "Google" && !u.contains ".google." && p != "")

/-- Q24 は完全な順位キーで10件の RID を決めてから全列を gather する。計測は幅広い行の復元件数。 -/
def Q24DuckDB.Goal : Prop := ∀ (ids : List Nat) (key : Nat → Nat) (payload : Nat → String),
  let cmp := fun (a b : Nat) => decide (key a ≤ key b)
  ((ids.mergeSort cmp).take 10).map payload =
    (((ids.map (fun i => (i,payload i))).mergeSort (fun a b => cmp a.1 b.1)).take 10).map Prod.snd

/-- Q28 は同じ CounterID の URL 長を終点差と非空件数に集約する。計測は本文のデコード量。 -/
def Q28ClickHouse.Goal : Prop := ∀ (lengths : List Nat),
  ((lengths.filter (· != 0)).sum, (lengths.filter (· != 0)).length) =
    (lengths.sum, lengths.length - lengths.count 0)

/-- Q31 は代表 SearchEngineID を先に分配して ClientIP だけを内部キーにする。計測はハッシュキー比較バイト。 -/
def Q31ClickHouse.Goal : Prop := ∀ (pairs : List (Nat × Nat)) e ip,
  ((pairs.filter (fun p => p.1 == e)).map Prod.snd).count ip = pairs.count (e,ip)

/-- Q32 は WatchID が一意な区画の単一行集約を直接作る。計測は一意性を確認した区画のハッシュ挿入数。 -/
def Q32DuckDB.Goal : Prop := ∀ (xs : List (Nat × Nat)), (xs.map Prod.fst).Nodup →
  ∀ k ∈ xs, xs.count k = 1

/-- Q32 は非空検索語の通過 RID だけから WatchID と IP を gather する。計測は2列の gather 行数。 -/
def Q32ClickHouse.Goal : Prop := ∀ (ids : List Nat) (keep : Nat → Bool) (pair : Nat → Nat × Nat),
  (ids.filter keep).map pair = (ids.map (fun i => (keep i,pair i))).filterMap (fun (b,k) => if b then some k else none)

/-- Q37 は URL ごとの部分件数に未読件数を足しても10位に届かない群を捨てる。計測は最後まで保持した群数。 -/
def Q37DuckDB.Goal : Prop := ∀ (seen : List Nat) (remaining : List (List Nat)) (keys : List Nat) i threshold,
  keys.Nodup →
  let upper := seen.count i + (remaining.map (fun page => if page.contains i then page.length else 0)).sum
  10 ≤ countWhere keys (fun k => threshold ≤ seen.count k) → upper < threshold →
  let allRows := seen ++ remaining.flatten
  10 ≤ countWhere keys (fun k => allRows.count i < allRows.count k)

/-- Q37 はフラグと日付の通過 RID だけで局所 URL コードを数え、同名の辞書項目を統合する。計測は解決する文字列の個数。 -/
def Q37ClickHouse.Goal : Prop := ∀ (pages : List (List (Bool × Nat) × (Nat → String))) key,
  let pageCount := fun (p : List (Bool × Nat) × (Nat → String)) =>
    let hist := frequency ((p.1.filter Prod.fst).map Prod.snd)
    ((hist.filter (fun r => p.2 r.1 == key)).map Prod.snd).sum
  (pages.map pageCount).sum = countWhere
    (pages.flatMap (fun (rs,dict) => (rs.filter Prod.fst).map (fun r => dict r.2))) (· == key)

/-- Q38 は異なる部分辞書に同名 Title があれば加算してから順位を決める。計測は大域集約への入力行。 -/
def Q38DuckDB.Goal : Prop := ∀ (pages : List (List String)) title,
  (pages.map (fun p => p.count title)).sum = pages.flatten.count title

/-- Q38 は非空 Title と固定フラグの通過ビットを同時に数える。計測は圧縮ブロックの走査回数。 -/
def Q38ClickHouse.Goal : Prop := ∀ (flags : List Bool) (titles : List String),
  countWhere (flags.zip titles) (fun (b,t) => b && t != "") =
    ((flags.zip titles).filterMap (fun (b,t) => if b then some t else none) |>.filter (· != "")).length

/-- Q39 はグループ件数確定後に各区画で1010位までを残す。計測は OFFSET 部分を含む merge 入力。 -/
def Q39DuckDB.Goal : Prop := ∀ (shards : List (List Nat)), ranges (shards.flatMap (best 1010)) 1000 10 =
  ranges shards.flatten 1000 10

/-- Q40 は検索・広告 ID がともにゼロの枝だけで参照元を取得する。計測は Referer の gather 件数。 -/
def Q40DuckDB.Goal : Prop := ∀ (rows : List (Nat × Nat × Nat)) (referrer : Nat → String),
  rows.map (fun (s,a,i) => if s == 0 && a == 0 then referrer i else "") =
    (rows.map (fun (s,a,i) => (s,a,referrer i))).map (fun (s,a,r) => if s == 0 && a == 0 then r else "")

/-- Q42 は URLHash 通過後の distinct 解像度数で OFFSET の空結果を確定する。計測は回避した順位ソート件数。 -/
def Q42DuckDB.Goal : Prop := ∀ (dims : List (Nat × Nat)) (rank : Nat × Nat → Nat),
  dims.eraseDups.length ≤ 10000 → ranges (dims.eraseDups.map rank) 10000 10 = []

/-- Q43 は実際に存在する分だけを整列し、返却する10分の頻度だけを求める。計測は集約カウンタ数。 -/
def Q43DuckDB.Goal : Prop := ∀ (minutes : List Nat),
  let page := ranges minutes.eraseDups 1000 10
  page.map (fun m => (minutes.filter page.contains).count m) = page.map minutes.count

end TinylambOpt.ResearchGoals.BenchmarkRematch.Click
