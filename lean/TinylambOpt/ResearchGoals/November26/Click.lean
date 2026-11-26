import TinylambOpt.ResearchGoals.November26.Common

namespace TinylambOpt.ResearchGoals.November26.Click
open BenchmarkRematch

/-- Q12 は端末グループ内で UserID を固定幅の区画に分け、使用ビット数を足す。観測量は DISTINCT 状態のバイト数。 -/
def Q12.Goal : Prop := ∀ (users : List Nat) w, 0 < w →
  let pages := (users.map (· / w)).eraseDups
  (pages.map (fun p => ((List.range w).filter (fun bit => users.contains (p*w+bit))).length)).sum =
    users.eraseDups.length

/-- Q14 は頻出候補の DISTINCT 下限と残りの行数上限で検索語を剪定する。観測量は維持した UserID 集合数。 -/
def Q14.Goal : Prop := ∀ (seen rest : List Nat) (otherFinal : List Nat),
  10 ≤ countWhere otherFinal (seen.eraseDups.length + rest.length < ·) →
    10 ≤ countWhere otherFinal ((seen++rest).eraseDups.length < ·)

/-- Q16 は上位頻度の候補を標本から得て、未収録キーの上界が10位より小さいときだけ候補内で確定する。観測量は全件集計へのフォールバック率。 -/
def Q16.Goal : Prop := ∀ (users candidates : List Nat) threshold,
  let keys := users.eraseDups
  let kept := keys.filter candidates.contains
  10 ≤ countWhere kept (fun k => threshold ≤ users.count k) →
  (∀ k ∈ keys, ¬ k ∈ candidates → users.count k < threshold) →
  let rank := fun (a b : Nat) => decide (users.count a > users.count b ∨
    (users.count a = users.count b ∧ a ≤ b))
  (kept.mergeSort rank).take 10 = (keys.mergeSort rank).take 10

/-- Q18 は辞書コードの連続 run を1回の加算で (UserID, SearchPhrase) ごとに数える。観測量はハッシュ表の更新回数。 -/
def Q18.Goal : Prop := ∀ (runs : List ((Nat × Nat) × Nat)) k,
  (forKey runs k).sum = (runs.flatMap (fun (key,n) => List.replicate n key)).count k

/-- Q20 は昇順 UserID 区画で等値範囲の両端から該当件数を得る。観測量は比較した ID 数。 -/
def Q20.Goal : Prop := ∀ (ids : List Int) target, ids.Pairwise (· ≤ ·) →
  (ids.takeWhile (· ≤ target)).length - (ids.takeWhile (· < target)).length = ids.count target

/-- Q21 は URL をフレーズに分割したまま google を照合し、境界に5文字の履歴を渡す。観測量は連結した URL のバイト数。 -/
def Q21.Goal : Prop := ∀ (a b : String),
  (a++b).contains "google" = (a.contains "google" || b.contains "google" ||
    ((a.takeEnd 5).toString ++ (b.take 5).toString).contains "google")

/-- Q22 は件数で10位候補を絞ってから MIN(URL) を計算する。同点候補は全て残す。観測量は最小文字列の更新数。 -/
def Q22.Goal : Prop := ∀ (matched : List (Nat × String)) k cutoff,
  let phrases := matched.map Prod.fst
  cutoff ≤ phrases.count k →
  (forKey (matched.filter (fun r => cutoff ≤ phrases.count r.1)) k).min? = (forKey matched k).min?

/-- Q23 は Title と URL の適格辞書コード同士を、出現した組に限って一度検査する。観測量は照合した文字列ペア数。 -/
def Q23.Goal : Prop := ∀ (pairs : List (Nat × Nat)) (title url : Nat → String),
  let accepts := fun (k : Nat × Nat) => (title k.1).contains "Google" && !(url k.2).contains ".google."
  let allowed := pairs.eraseDups.filter accepts
  pairs.filter allowed.contains = pairs.filter accepts

/-- Q24 は EventTime 順序の下限が現在の10位より後のページで URL 自体を読まない。観測量はスキップした LIKE 入力バイト。 -/
def Q24.Goal : Prop := ∀ (previous page : List Nat) (passes : Nat → Bool) lower cutoff,
  10 ≤ countWhere previous (· ≤ cutoff) → cutoff < lower → (∀ k ∈ page, lower ≤ k) →
  best 10 (previous ++ page.filter passes) = best 10 previous

/-- Q27 は時刻順の走査を10行目と同時刻の末尾まで進め、検索語で境界内を整列する。観測量は整列した検索語数。 -/
def Q27.Goal : Prop := ∀ (rs : List (Nat × Nat)) t, (rs.map Prod.fst).Pairwise (· ≤ ·) →
  10 ≤ countWhere rs (fun r => r.1 ≤ t) →
  let cmp := fun (a b : Nat × Nat) => decide (a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2))
  ((rs.takeWhile (fun r => r.1 ≤ t)).mergeSort cmp).take 10 = (rs.mergeSort cmp).take 10

/-- Q28 は CounterID の全行頻度が10万以下なら URL が非空でも HAVING を通らない。観測量は調べた URL 長の件数。 -/
def Q28.Goal : Prop := ∀ (rows : List (Nat × Nat)) k,
  let lengths := forKey rows k
  100000 < (lengths.filter (· > 0)).length →
    100000 < (rows.map Prod.fst).count k

/-- Q31 は低位 IP の配列を上位プレフィックスごとに持ち、元のグループを復元する。観測量は複合キー表の割当量。 -/
def Q31.Goal : Prop := ∀ (rows : List ((Nat × Nat) × Int)) engine ip,
  let split := rows.map (fun ((e,a),v) => ((e,a/256,a%256),v))
  forKey split (engine,ip/256,ip%256) = forKey rows (engine,ip)

/-- Q32 は WatchID ごとの IP を単一スロットに置き、異なる IP のある ID だけ補助表に昇格させる。観測量は補助表へ昇格した WatchID 数。 -/
def Q32.Goal : Prop := ∀ (ips : List Nat) (first query : Nat),
  let exceptions := ips.filter (· != first)
  (if query == first then ips.length-exceptions.length else exceptions.count query) = ips.count query

/-- Q36 は ClientIP だけでグループ化し、派生した3つの差をグループ確定後に作る。観測量はハッシュ・比較したキーバイト。 -/
def Q36.Goal : Prop := ∀ (ips : List Int),
  let wide := fun (i : Int) => (i,i-1,i-2,i-3)
  frequency (ips.map wide) = (frequency ips).map (fun (i,n) => (wide i,n))

/-- Q37 は CounterID ごとに日付順の URL 出現リストを持ち、7月の件数を2つの境界で取る。観測量は走査した投稿リスト要素数。 -/
def Q37.Goal : Prop := ∀ (days : List Nat) lo hi, days.Pairwise (· ≤ ·) → lo ≤ hi →
  (days.takeWhile (· ≤ hi)).length - (days.takeWhile (· < lo)).length =
    countWhere days (fun d => lo ≤ d && d ≤ hi)

/-- Q38 はフィルタ済み辞書の最大頻度で Title の候補をページから選び、全体上界で漏れを判定する。観測量は大域表に登録する Title 数。 -/
def Q38.Goal : Prop := ∀ (pages : List (List String)) (candidates : List String) cap t,
  (∀ p ∈ pages, ∀ title ∈ p, ¬ title ∈ candidates → p.count title ≤ cap) →
  cap*pages.length < pages.flatten.count t → t ∈ candidates

/-- Q39 は件数ごとのグループ数から OFFSET が入る順位帯を求め、同数の URL 内だけで残りの OFFSET を処理する。観測量は順位付けする URL 数。 -/
def Q39.Goal : Prop := ∀ (groups : List (Nat × Nat)) cutoff,
  let higher := countWhere groups (fun r => cutoff < r.2)
  let tied := groups.filter (fun r => r.2 == cutoff)
  let ranked := groups.mergeSort (fun a b => decide (b.2 ≤ a.2))
  higher ≤ 1000 → 1010 ≤ higher+tied.length →
    ((ranked.drop 1000).take 10) = ((tied.drop (1000-higher)).take 10)

/-- Q40 は参照元を使う枝と定数の枝を別々に集約し、完全キーの分離を保証する。観測量は参照元列つきのハッシュ挿入数。 -/
def Q40.Goal : Prop := ∀ (rows : List (Nat × Nat × String)) s a src,
  let key := fun (r : Nat × Nat × String) => (r.1,r.2.1,if r.1 == 0 && r.2.1 == 0 then r.2.2 else "")
  (rows.map key).count (s,a,src) =
    if s == 0 && a == 0 then rows.count (0,0,src)
    else if src == "" then (rows.map (fun r => (r.1,r.2.1))).count (s,a) else 0

/-- Q42 は解像度列を読む前に URLHash 等の通過行数だけで OFFSET を判定する。観測量は省略した解像度ペアの復号行数。 -/
def Q42.Goal : Prop := ∀ (hits : List Nat) (resolution : Nat → Nat × Nat),
  hits.length ≤ 10000 → ((hits.map resolution).eraseDups.drop 10000).isEmpty = true

/-- Q43 はフィルタ通過後、時刻区画の最小・最大が同じ分なら切捨てを1回で済ませる。観測量は行単位の時刻切捨て回数。 -/
def Q43.Goal : Prop := ∀ (seconds : List Int) lo hi,
  (∀ s ∈ seconds, lo ≤ s ∧ s ≤ hi) → lo/60 = hi/60 →
  seconds.map (· / 60) = List.replicate seconds.length (lo/60)

end TinylambOpt.ResearchGoals.November26.Click
