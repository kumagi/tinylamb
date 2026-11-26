import Std

/-!
An ordered access path needs to preserve the rows and boundaries selected by the SQL
range predicate.
-/

namespace TinylambOpt.ResearchGoals.Atlas.RangePlans

/-! A probe supplies two parameters. Build rows have two integer coordinates
and a signed measure, sortable score, or multiplicity, according to the query.
Boxes are half open. A signed stencil can describe a union with holes. -/
structure Point where
  x : Int
  y : Int
  deriving BEq, Repr
abbrev Rows := List (Point × Int)
structure Box where
  x0 : Int
  x1 : Int
  y0 : Int
  y1 : Int
  coeff : Int := 1
def box (a b c d : Int) (w : Int := 1) : Box := ⟨a,b,c,d,w⟩
structure Shape where
  qualifies : Point → Point → Bool
  stencil : Int → Point → List Box
def bounded (b : Int) (r : Point) := -b ≤ r.x ∧ r.x ≤ b ∧ -b ≤ r.y ∧ r.y ≤ b
def scan (s : Shape) (p : Point) (rs : Rows) :=
  ((rs.filter (fun r => s.qualifies p r.1)).map Prod.snd).sum
def prefixMass (rs : Rows) (x y : Int) :=
  ((rs.filter (fun r => r.1.x < x && r.1.y < y)).map Prod.snd).sum
def corners (q : Box) (pref : Int → Int → Int) :=
  if q.x0 < q.x1 && q.y0 < q.y1 then
    q.coeff * (pref q.x1 q.y1 - pref q.x0 q.y1 - pref q.x1 q.y0 + pref q.x0 q.y0)
  else 0
def indexed (s : Shape) (b : Int) (p : Point) (pref : Int → Int → Int) :=
  ((s.stencil b p).map (corners · pref)).sum

structure Page where
  lower : Point
  upper : Point -- inclusive
  rows : Rows
def valid (g : Page) : Prop := ∀ r ∈ g.rows,
  g.lower.x ≤ r.1.x ∧ r.1.x ≤ g.upper.x ∧ g.lower.y ≤ r.1.y ∧ r.1.y ≤ g.upper.y
def uniform (g : Page) (q : Box) : Option Int :=
  if q.x1 ≤ q.x0 || q.y1 ≤ q.y0 || g.upper.x < q.x0 || q.x1 ≤ g.lower.x ||
     g.upper.y < q.y0 || q.y1 ≤ g.lower.y then some 0
  else if q.x0 ≤ g.lower.x && g.upper.x < q.x1 && q.y0 ≤ g.lower.y && g.upper.y < q.y1 then
    some q.coeff else none
def pageScan (s : Shape) (b : Int) (p : Point) (g : Page) :=
  match (s.stencil b p).mapM (uniform g) with
  | some ws => ws.sum * (g.rows.map Prod.snd).sum
  | none => scan s p g.rows

/-- Transpose a batch of probe rectangles into signed start/end events. The
prefix sum of event weights gives the number/weight of probes covering a row. -/
def events (q : Box) (w : Int) : Rows :=
  if q.x0 < q.x1 && q.y0 < q.y1 then
    let z := w*q.coeff
    [(⟨q.x0,q.y0⟩,z),(⟨q.x1,q.y0⟩,-z),(⟨q.x0,q.y1⟩,-z),(⟨q.x1,q.y1⟩,z)]
  else []
def compile (s : Shape) (b : Int) (ps : Rows) :=
  ps.flatMap (fun (p,w) => (s.stencil b p).flatMap (events · w))
def atPoint (es : Rows) (p : Point) := prefixMass es (p.x+1) (p.y+1)
def covered (s : Shape) (ps : List Point) (p : Point) := ps.any (fun q => s.qualifies q p)
def beforeScore (s : Shape) (b : Int) (p : Point) (rs : Rows) (score : Int) :=
  indexed s b p (prefixMass ((rs.filter (·.2 ≤ score)).map (fun r => (r.1,(1:Int)))))
def seek (rank : Int → Int) (k : Nat) : Nat → Int → Int → Int
  | 0,lo,_ => lo
  | fuel+1,lo,hi =>
    if hi ≤ lo then lo else
    let mid := (lo+hi)/2
    if (k:Int) < rank mid then seek rank k fuel lo mid else seek rank k fuel (mid+1) hi

namespace ContainsTick

/-- 有効期間が照会時刻を含む。 -/
def shape : Shape where
  qualifies p v := v.x ≤ p.x && p.x < v.y
  stencil b p :=
    [box (-b) (p.x+1) (p.x+1) (b+1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 有効区間の開始累積量から終了累積量を引き、時点ごとの生存量を得る。 -/
def EndpointSweep.Goal : Prop :=
  ∀ p rs, (∀ r ∈ rs, r.1.x ≤ r.1.y) → scan shape p rs =
    ((rs.filter (fun r => r.1.x ≤ p.x)).map Prod.snd).sum - ((rs.filter (fun r => r.1.y ≤ p.x)).map Prod.snd).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end ContainsTick

namespace StrictOverlap

/-- 販売期間と販促期間が正の長さで交差する。 -/
def shape : Shape where
  qualifies p v := max p.x v.x < min p.y v.y
  stencil b p :=
    if p.x < p.y then [box (-b) (p.y) (p.x+1) (b+1)] else []

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1 ∧ r.1.x < r.1.y) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- 強仮説。build の空期間を許したままヘッダだけでページの交差判定を済ませられるか。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 空期間を除いた二表の重なりを、片端ずつの一次元索引の差だけで判定する。 -/
def StartMinusEnd.Goal : Prop :=
  ∀ p rs, p.x < p.y → (∀ r ∈ rs, r.1.x < r.1.y) → scan shape p rs =
    ((rs.filter (fun r => r.1.x < p.y)).map Prod.snd).sum - ((rs.filter (fun r => r.1.y ≤ p.x)).map Prod.snd).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1 ∧ r.1.x < r.1.y) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ r.1.x < r.1.y ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end StrictOverlap

namespace NestedVersion

/-- 照会期間に丸ごと収まる版を列挙する。 -/
def shape : Shape where
  qualifies p v := p.x ≤ v.x && v.y ≤ p.y
  stencil b p :=
    [box (p.x) (b+1) (-b) (p.y+1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 照会区間の包含順と非負性を使い、外側の集計が閾値未満なら内側の再評価を省く。 -/
def OuterCertificate.Goal : Prop :=
  ∀ p q rs, p.x ≤ q.x → q.y ≤ p.y → (∀ r ∈ rs, 0 ≤ r.2) → scan shape q rs ≤ scan shape p rs

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end NestedVersion

namespace StaggeredHandoff

/-- 開始が重なり、終了は後ろにずれる受け渡し。 -/
def shape : Shape where
  qualifies p v := p.x < v.x && v.x < p.y && p.y < v.y
  stencil b p :=
    [box (p.x+1) (p.y) (p.y+1) (b+1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- プローブ群を境界イベントへ転置し、重複する両側の行の重みを一走査で掛ける。 -/
def ProbeTransposition.Goal : Prop :=
  ∀ b (ps rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    (ps.map (fun (p,w) => w * scan shape p rs)).sum =
      (rs.map (fun (r,w) => w * atPoint (compile shape b ps) r)).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end StaggeredHandoff

namespace EndpointContact

/-- 二期間の開始または終了が相手の端点に一致する。 -/
def shape : Shape where
  qualifies p v := v.x == p.y || v.y == p.x
  stencil b p :=
    [box (p.y) (p.y+1) (-b) (b+1),
      box (-b) (b+1) (p.x) (p.x+1),
      box (p.y) (p.y+1) (p.x) (p.x+1) (-1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 開始・終了の二つの単一キー表と端点の組の一致件数で重複を補正する。 -/
def EndpointHash.Goal : Prop :=
  ∀ p rs, scan shape p rs =
    ((rs.filter (fun r => r.1.x == p.y)).map Prod.snd).sum + ((rs.filter (fun r => r.1.y == p.x)).map Prod.snd).sum -
    ((rs.filter (fun r => r.1 == ⟨p.y,p.x⟩)).map Prod.snd).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end EndpointContact

namespace PriceCorridor

/-- 基準価格と在庫水準の非対称な許容帯。 -/
def shape : Shape where
  qualifies p v := (v.x-p.x).natAbs ≤ 3 && p.y-2 ≤ v.y && v.y ≤ p.y+5
  stencil _ p :=
    [box (p.x-3) (p.x+4) (p.y-2) (p.y+6)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 照会価格を一目盛ずらしたとき、入る価格帯と出る価格帯の二列だけで集計を修正する。 -/
def SlidingStrip.Goal : Prop :=
  ∀ p rs, let stripe := fun (x : Int) => ((rs.filter (fun r => r.1.x == x && p.y-2 ≤ r.1.y && r.1.y ≤ p.y+5)).map Prod.snd).sum
    scan shape ⟨p.x+1,p.y⟩ rs - scan shape p rs = stripe (p.x+4) - stripe (p.x-3)

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end PriceCorridor

namespace ParetoFront

/-- 両指標で劣らず少なくとも一方で優れる。 -/
def shape : Shape where
  qualifies p v := p.x ≤ v.x && p.y ≤ v.y && (p.x < v.x || p.y < v.y)
  stencil b p :=
    [box (p.x) (b+1) (p.y) (b+1),
      box (p.x) (p.x+1) (p.y) (p.y+1) (-1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- ある行を優越する候補の存在判定を、入力全体の非優越行だけに限定する。 -/
def SkylineWitness.Goal : Prop :=
  ∀ p (points : List Point),
    points.any (shape.qualifies p) = (points.filter (fun r => !points.any (shape.qualifies r))).any (shape.qualifies p)

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end ParetoFront

namespace ReversedRank

/-- 価格順位と売上順位が逆転する組。 -/
def shape : Shape where
  qualifies p v := (v.x < p.x && p.y < v.y) || (p.x < v.x && v.y < p.y)
  stencil b p :=
    [box (-b) (p.x) (p.y+1) (b+1),
      box (p.x+1) (b+1) (-b) (p.y)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 第一座標をソートすると、不一致順位の自己結合を第二座標の反転数として数えられる。 -/
def InversionPairs.Goal : Prop :=
  ∀ (ps : List Point), ps.Pairwise (fun a b => a.x < b.x) →
    (ps.map (fun p => ps.countP (shape.qualifies p))).sum =
      2*(ps.zipIdx.map (fun (p,i) => (ps.drop (i+1)).countP (fun q => q.y < p.y))).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end ReversedRank

namespace OutsideBand

/-- 二つの許容帯の少なくとも片方を超える。 -/
def shape : Shape where
  qualifies p v := (v.x-p.x).natAbs > 2 || (v.y-p.y).natAbs > 4
  stencil b p :=
    [box (-b) (b+1) (-b) (b+1),
      box (p.x-2) (p.x+3) (p.y-4) (p.y+5) (-1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 外側の広い領域を走査せず、全体 SUM から内側の狭い候補だけ差し引く。 -/
def ComplementSum.Goal : Prop :=
  ∀ p rs, scan shape p rs = (rs.map Prod.snd).sum -
    ((rs.filter (fun r => (r.1.x-p.x).natAbs ≤ 2 && (r.1.y-p.y).natAbs ≤ 4)).map Prod.snd).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end OutsideBand

namespace RingNeighborhood

/-- 近傍のうち中心の密集域を除く。 -/
def shape : Shape where
  qualifies p v := let d := max (v.x-p.x).natAbs (v.y-p.y).natAbs; 1 < d && d ≤ 5
  stencil _ p :=
    [box (p.x-5) (p.x+6) (p.y-5) (p.y+6),
      box (p.x-1) (p.x+2) (p.y-1) (p.y+2) (-1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 中心からの最大座標距離を集約したヒストグラムの四バケットだけでリングの総和を読む。 -/
def PerimeterHistogram.Goal : Prop :=
  ∀ p rs, scan shape p rs = ([2,3,4,5].map (fun d =>
    ((rs.filter (fun r => max (r.1.x-p.x).natAbs (r.1.y-p.y).natAbs == d)).map Prod.snd).sum)).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end RingNeighborhood

namespace ExactlyOneKey

/-- 二つの自然キーの片方だけが一致する。 -/
def shape : Shape where
  qualifies p v := (v.x == p.x) != (v.y == p.y)
  stencil b p :=
    [box (p.x) (p.x+1) (-b) (b+1),
      box (-b) (b+1) (p.y) (p.y+1),
      box (p.x) (p.x+1) (p.y) (p.y+1) (-2)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 一致行の XOR を二つの周辺ハッシュ表と複合一致表の二倍補正で得る。 -/
def SubtractiveGrouping.Goal : Prop :=
  ∀ p rs, scan shape p rs =
    ((rs.filter (fun r => r.1.x == p.x)).map Prod.snd).sum + ((rs.filter (fun r => r.1.y == p.y)).map Prod.snd).sum -
    2*((rs.filter (fun r => r.1 == p)).map Prod.snd).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end ExactlyOneKey

namespace PriorTradingDay

/-- 同日なら低い通番、過去日なら直前の三日を照合する。 -/
def shape : Shape where
  qualifies p v := (p.x-3 ≤ v.x && v.x < p.x) || (v.x == p.x && v.y < p.y)
  stencil b p :=
    [box (p.x-3) (p.x) (-b) (b+1),
      box (p.x) (p.x+1) (-b) (p.y)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 全過去日のうち三日だけは日次集計を使い、当日の未確定部分だけ通番索引を引く。 -/
def DailyState.Goal : Prop :=
  ∀ p rs, scan shape p rs = ([(1:Int),2,3].map (fun d => ((rs.filter (fun r => r.1.x == p.x-d)).map Prod.snd).sum)).sum +
    ((rs.filter (fun r => r.1.x == p.x && r.1.y < p.y)).map Prod.snd).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end PriorTradingDay

namespace LexSuccessor

/-- 複合キーの辞書順で後続する候補を一括集計する。 -/
def shape : Shape where
  qualifies p v := p.x < v.x || (p.x == v.x && p.y < v.y)
  stencil b p :=
    [box (p.x+1) (b+1) (-b) (b+1),
      box (p.x) (p.x+1) (p.y+1) (b+1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 複合キー昇順に整列したストリームからカーソル以下の先頭だけを落として SUM する。 -/
def ReverseCursor.Goal : Prop :=
  ∀ p rs, rs.Pairwise (fun a b => a.1.x < b.1.x ∨ (a.1.x = b.1.x ∧ a.1.y ≤ b.1.y)) →
    scan shape p rs = ((rs.dropWhile (fun r => !shape.qualifies p r.1)).map Prod.snd).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end LexSuccessor

namespace EitherEndpointInside

/-- 左右端の片方でも注文期間内にあるものを採用する。 -/
def shape : Shape where
  qualifies p v := (p.x ≤ v.x && v.x < p.y) || (p.x ≤ v.y && v.y < p.y)
  stencil b p :=
    [box (p.x) (p.y) (-b) (b+1),
      box (-b) (b+1) (p.x) (p.y),
      box (p.x) (p.y) (p.x) (p.y) (-1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 期間を二つのスライスへ分割して合算するとき、両スライスに端点がある行だけを一度引く。 -/
def SplitBoundaryDebt.Goal : Prop :=
  ∀ p m rs, p.x ≤ m → m ≤ p.y → scan shape p rs = scan shape ⟨p.x,m⟩ rs + scan shape ⟨m,p.y⟩ rs -
    ((rs.filter (fun r => shape.qualifies ⟨p.x,m⟩ r.1 && shape.qualifies ⟨m,p.y⟩ r.1)).map Prod.snd).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end EitherEndpointInside

namespace CrossChannel

/-- 異なるチャネルで期間キーだけを照合する。 -/
def shape : Shape where
  qualifies p v := v.x != p.x && (v.y-p.y).natAbs ≤ 2
  stencil b p :=
    [box (-b) (b+1) (p.y-2) (p.y+3),
      box (p.x) (p.x+1) (p.y-2) (p.y+3) (-1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- プローブ群を境界イベントへ転置し、重複する両側の行の重みを一走査で掛ける。 -/
def ProbeTransposition.Goal : Prop :=
  ∀ b (ps rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    (ps.map (fun (p,w) => w * scan shape p rs)).sum =
      (rs.map (fun (r,w) => w * atPoint (compile shape b ps) r)).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end CrossChannel

namespace VarianceBudget

/-- 二段階の数量・価格偏差予算を共有する。 -/
def shape : Shape where
  qualifies p v := let x := (v.x-p.x).natAbs; let y := (v.y-p.y).natAbs; (x ≤ 1 && y ≤ 4) || (x ≤ 4 && y ≤ 1)
  stencil _ p :=
    [box (p.x-1) (p.x+2) (p.y-4) (p.y+5),
      box (p.x-4) (p.x+5) (p.y-1) (p.y+2),
      box (p.x-1) (p.x+2) (p.y-1) (p.y+2) (-1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- プローブ群を境界イベントへ転置し、重複する両側の行の重みを一走査で掛ける。 -/
def ProbeTransposition.Goal : Prop :=
  ∀ b (ps rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    (ps.map (fun (p,w) => w * scan shape p rs)).sum =
      (rs.map (fun (r,w) => w * atPoint (compile shape b ps) r)).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end VarianceBudget

namespace AlignedWeek

/-- 負の通算日を含む同じ週かつ同じ価格階級に属す。 -/
def shape : Shape where
  qualifies p v := v.x/7 == p.x/7 && v.y/10 == p.y/10
  stencil _ p :=
    [box (7*(p.x/7)) (7*(p.x/7)+7) (10*(p.y/10)) (10*(p.y/10)+10)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 週と価格階級を取り込み時に群キーへ符号化し、範囲結合を完全等値の辞書参照へ変える。 -/
def BucketEquality.Goal : Prop :=
  ∀ p (rs : Rows), let bucket := fun q : Point => (q.x/7,q.y/10)
    let states := rs.map (fun r => (bucket r.1,r.2))
    scan shape p rs = ((states.filter (fun e => e.1 == bucket p)).map Prod.snd).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end AlignedWeek

namespace GapSession

/-- セッション終了後の次の開始と、有効な利用者範囲を照合する。 -/
def shape : Shape where
  qualifies p v := p.y ≤ v.x && v.x ≤ p.y+7 && p.x-1 ≤ v.y && v.y ≤ p.x+1
  stencil _ p :=
    [box (p.y) (p.y+8) (p.x-1) (p.x+2)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- プローブ群を境界イベントへ転置し、重複する両側の行の重みを一走査で掛ける。 -/
def ProbeTransposition.Goal : Prop :=
  ∀ b (ps rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    (ps.map (fun (p,w) => w * scan shape p rs)).sum =
      (rs.map (fun (r,w) => w * atPoint (compile shape b ps) r)).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end GapSession

namespace OverdueOrLost

/-- 期限と確定額を用いる例外条件を一つの集計にまとめる。 -/
def shape : Shape where
  qualifies p v := (v.x < p.x && v.y > p.y) || (v.x == p.x && v.y == 0)
  stencil b p :=
    [box (-b) (p.x) (p.y+1) (b+1),
      box (p.x) (p.x+1) (0) (1)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- プローブ群を境界イベントへ転置し、重複する両側の行の重みを一走査で掛ける。 -/
def ProbeTransposition.Goal : Prop :=
  ∀ b (ps rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    (ps.map (fun (p,w) => w * scan shape p rs)).sum =
      (rs.map (fun (r,w) => w * atPoint (compile shape b ps) r)).sum

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end OverdueOrLost

namespace AdjacentTiles

/-- 同じ区画を除いた上下左右四つの隣接区画に絞る。 -/
def shape : Shape where
  qualifies p v := (v.x-p.x).natAbs + (v.y-p.y).natAbs == 1
  stencil _ p :=
    [box (p.x-1) (p.x) (p.y) (p.y+1),
      box (p.x+1) (p.x+2) (p.y) (p.y+1),
      box (p.x) (p.x+1) (p.y-1) (p.y),
      box (p.x) (p.x+1) (p.y+1) (p.y+2)]

/-- 符号付き矩形の四隅の累積値で、全一致行の和を読む。 -/
def PrefixIndex.Goal : Prop :=
  ∀ b p rs, (∀ r ∈ rs, bounded b r.1) →
    indexed shape b p (prefixMass rs) = scan shape p rs

/-- ページ全体の採否と穴の重なりを求め、一定の場合はヘッダの SUM だけを読む。 -/
def PageVerdict.Goal : Prop :=
  ∀ b p g, valid g → (∀ r ∈ g.rows, bounded b r.1) →
    pageScan shape b p g = scan shape p g.rows

/-- 隣接区画の結合を四つの等値プローブに変え、同一区画の大量の重複を丸ごと飛ばす。 -/
def SparseNeighbors.Goal : Prop :=
  ∀ p rs, let freq := fun q => ((rs.filter (fun r => r.1 == q)).map Prod.snd).sum
    scan shape p rs = freq ⟨p.x-1,p.y⟩ + freq ⟨p.x+1,p.y⟩ + freq ⟨p.x,p.y-1⟩ + freq ⟨p.x,p.y+1⟩

/-- 複数プローブの和で被覆数ゼロを検出し、重複プローブや負の穴があっても anti-join の行順を保つ。 -/
def UnionAnti.Goal : Prop :=
  ∀ b (ps : List Point) (rs : Rows), (∀ r ∈ rs, bounded b r.1) →
    let es := compile shape b (ps.map (fun p => (p,1)))
    rs.filter (fun r => !covered shape ps r.1) = rs.filter (fun r => atPoint es r.1 == 0)

/-- スコア索引上の一致件数を二分探索し、同点を含む第 k 値を全件ソートせずに得る。 -/
def RankSeek.Goal : Prop :=
  ∀ b p (rs : Rows) (lo hi : Int) (k : Nat),
    (∀ r ∈ rs, bounded b r.1 ∧ lo ≤ r.2 ∧ r.2 ≤ hi) →
    let scores := ((rs.filter (fun r => shape.qualifies p r.1)).map Prod.snd).mergeSort
    k < scores.length → scores[k]? = some
      (seek (beforeScore shape b p rs) k ((hi-lo).toNat.log2+2) lo hi)

end AdjacentTiles


-- The build row [1,1) is empty; both bounding comparisons still succeed.
#guard let p : Point := ⟨0,2⟩
       let g : Page := ⟨⟨1,1⟩,⟨1,1⟩,[(⟨1,1⟩,7)]⟩
       pageScan StrictOverlap.shape 2 p g == 7 && scan StrictOverlap.shape p g.rows == 0

end TinylambOpt.ResearchGoals.Atlas.RangePlans
