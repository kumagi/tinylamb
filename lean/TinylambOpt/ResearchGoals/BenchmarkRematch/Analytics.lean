import TinylambOpt.ResearchGoals.BenchmarkRematch.Common

namespace TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

/-- J3 は role を通った cast の person_id 範囲だけに女性名フィルタを作る。計測は名前フィルタの占有バイト。 -/
def J3DuckDB.Goal : Prop := ∀ (cast names : List Nat) lo hi,
  (∀ k ∈ cast, lo ≤ k ∧ k ≤ hi) →
  let bits := (List.range (hi+1-lo)).map (fun i => names.contains (lo+i))
  cast.filter (fun k => bits[k-lo]? == some true) = cast.filter names.contains

/-- J3 は通過した出演を映画ごとに数えてから制作年を掛ける。計測は title へのプローブ回数。 -/
def J3ClickHouse.Goal : Prop := ∀ (cast : List Nat) (years : Nat → List Int),
  let expanded := cast.flatMap years
  (expanded.length,expanded.sum) =
    (((frequency cast).map (fun (k,n) => n*(years k).length)).sum,
     total (frequency cast) (fun (k,n) => (n:Int)*(years k).sum))

/-- C1 は post ごとの票数でコメントの COUNT と nullable SUM を重み付けする。計測は生成したコメント×票のペア数。 -/
def C1DuckDB.Goal : Prop := ∀ (scores : List (Option Int)) votes,
  let expanded := scores.flatMap (List.replicate votes)
  (expanded.length,nsum expanded) = (scores.length*votes,
    if votes == 0 then none else (nsum scores).map (fun x => (votes:Int)*x))

/-- C1 は適格 post にコメントが存在する場合だけ投票リストを読む。計測は読んだ投票ペイロード行数。 -/
def C1ClickHouse.Goal : Prop := ∀ (posts comments votes : List Nat),
  let joins := fun (ps : List Nat) => (ps.map fun k => comments.count k * votes.count k).sum
  joins (posts.filter comments.contains) = joins posts

/-- C2 は各投稿の commenter 別頻度に reputation 条件を通った users 行の多重度を掛ける。計測は users への反復プローブ数。 -/
def C2DuckDB.Goal : Prop := ∀ (commenters eligibleUsers : List Nat),
  ((frequency commenters).map (fun (k,n) => n * eligibleUsers.count k)).sum =
    (commenters.flatMap (fun k => eligibleUsers.filter (· == k))).length

/-- H2 は範囲内の2列キーを衝突なく1次元化し、密な COUNT 配列を使う。計測は複合キーのハッシュ回数。 -/
def H2DuckDB.Goal : Prop := ∀ (rows : List (Nat × Nat)) width a b,
  b < width → (∀ r ∈ rows, r.2 < width) →
  (rows.map (fun (x,y) => x*width+y)).count (a*width+b) = rows.count (a,b)

/-- H4 は id3 のページ下限で対象外ページを飛ばし、AVG の非 NULL 値を元の順序で渡す。計測は v1 のデコードページ数。 -/
def H4DuckDB.Goal : Prop := ∀ (blocks : List (List (Nat × Option Int))),
  let scan := fun (rs : List (Nat × Option Int)) => (rs.filter (fun r => r.1 < 4)).filterMap Prod.snd
  (blocks.filter (fun b => (b.map Prod.fst).min?.any (· < 4))).flatMap scan = scan blocks.flatten

/-- H5 は id1・id4 ごとに両側を部分集約し、2本の SUM を相手側の件数で復元する。計測は結合ペア生成数。 -/
def H5DuckDB.Goal : Prop := ∀ (x y : List (Option Int)),
  let pairs := x.flatMap fun a => y.map (fun b => (a,b))
  (nsum (pairs.map Prod.fst),nsum (pairs.map Prod.snd)) =
    (if y.isEmpty then none else (nsum x).map (fun v => v*(y.length:Int)),
     if x.isEmpty then none else (nsum y).map (fun v => v*(x.length:Int)))

/-- L2 は適格な会員・人物の結合件数を forum 単位で求め、年別メッセージに掛ける。計測は forum 展開後の行数。 -/
def L2DuckDB.Goal : Prop := ∀ (members people : List Nat) (scores : List Int),
  let n := (members.map people.count).sum
  let expanded := members.flatMap fun k => (people.filter (· == k)).flatMap (fun _ => scores)
  (expanded.length,expanded.sum) = (n*scores.length,(n:Int)*scores.sum)

/-- L4 は2012年の返信メッセージを forum ごとに畳んでから forum と結合する。計測は forum 結合の入力数。 -/
def L4DuckDB.Goal : Prop := ∀ (scores : List (Option Int)) (forumCopies : Nat),
  let joined := (List.replicate forumCopies scores).flatten
  (joined.length, (joined.filterMap id).sum, (joined.filterMap id).length) =
    (forumCopies*scores.length,(forumCopies:Int)*(scores.filterMap id).sum,forumCopies*(scores.filterMap id).length)

/-- L5 は country 内の各人の出次数で COUNT と年齢の AVG 状態を更新する。計測は person_knows から復元した行数。 -/
def L5DuckDB.Goal : Prop := ∀ (people : List (Nat × Option Int)) (edges : List Nat),
  let ages := people.flatMap (fun (k,a) => List.replicate (edges.count k) a)
  (ages.length,(ages.filterMap id).sum,(ages.filterMap id).length) =
    ((people.map (fun r => edges.count r.1)).sum,
     total people (fun (k,a) => (edges.count k:Int)*a.getD 0),
     ((people.filter (fun r => r.2.isSome)).map (fun r => edges.count r.1)).sum)

/-- L5 は人物側から対象の country と年に合う始点集合を作り、辺の走査で棄却する。計測は国別集約に届く辺数。 -/
def L5ClickHouse.Goal : Prop := ∀ (edges peopleInCountry : List Nat),
  let admitted := edges.filter peopleInCountry.eraseDups.contains
  admitted.flatMap (fun k => peopleInCountry.filter (· == k)) =
    edges.flatMap (fun k => peopleInCountry.filter (· == k))

/-- B3 は region・category・対象6年の複合キーを整数化する。計測はグループキー比較バイト。 -/
def B3DuckDB.Goal : Prop := ∀ (r c y r' c' y' categories : Nat),
  c < categories → c' < categories → y < 6 → y' < 6 →
  ((r*categories+c)*6+y = (r'*categories+c')*6+y' ↔ (r,c,y) = (r',c',y'))

/-- B3 は適格 customer の印を先に読み、不一致の販売では店舗・商品を引かない。計測は2つの次元表へのプローブ数。 -/
def B3ClickHouse.Goal : Prop := ∀ (sales : List Nat) (dimRows : Nat → List Int) (eligible : Nat → Nat),
  let project := fun k => (dimRows k).flatMap (List.replicate (eligible k))
  (sales.filter (fun k => eligible k > 0)).flatMap project = sales.flatMap project

/-- B4 は米国行を先に加算し、残りだけ部分文字列検索する。計測は LIKE に渡した文字列バイト。 -/
def B4DuckDB.Goal : Prop := ∀ (rs : List (Bool × Bool × Option Int)),
  let amounts := fun (xs : List (Bool × Bool × Option Int)) => xs.map (fun r => r.2.2)
  let us := rs.filter Prod.fst
  let textHits := rs.filter (fun r => !r.1 && r.2.1)
  let hits := rs.filter (fun r => r.1 || r.2.1)
  (hits.length,(nsum (amounts hits)).getD 0) =
    (us.length+textHits.length,(nsum (amounts us)).getD 0 + (nsum (amounts textHits)).getD 0)

/-- B5 は売上ブロックごとの相異なる商品だけで価格条件を引く。一意な商品表で選択マスクを再利用する。計測は価格照会数。 -/
def B5DuckDB.Goal : Prop := ∀ (sold : List Nat) (priced : Nat → Bool),
  let mask := sold.eraseDups.filter priced
  sold.filter mask.contains = sold.filter priced

/-- B5 は年内の商品別売上を部分集約し、高価格の商品行数を掛けて nullable SUM を合成する。計測は価格結合を渡る行数。 -/
def B5ClickHouse.Goal : Prop := ∀ (sales : List (Nat × Option Int)) (items : List Nat),
  let keys := (sales.map Prod.fst).eraseDups
  let groupSum := fun k => nsum ((sales.filter (fun r => r.1 == k)).map Prod.snd)
  nsum (sales.flatMap (fun (k,v) => List.replicate (items.count k) v)) =
    nsum ((keys.filter items.contains).map (fun k => (groupSum k).map (fun v => v*items.count k)))

end TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
