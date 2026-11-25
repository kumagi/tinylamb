import TinylambOpt.ResearchGoals.Compression.Wire

namespace TinylambOpt.ResearchGoals.Compression.Partition
open Wire

/-! The encoder chooses real contiguous page boundaries. Cost is a fully
framed byte count supplied by the candidate writer; the partition header can
add a page-count or width-dependent directory separately. -/
abbrev Cost := List Nat → Nat
def splits : List Nat → List (List (List Nat))
  | [] => [[]]
  | x::xs => (splits xs).flatMap (fun pages => match pages with
    | [] => [[[x]]]
    | first::rest => [(x::first)::rest,[x]::first::rest])
def bytes (cost : Cost) (ps : List (List Nat)) := (ps.map cost).sum
def best (cost : Cost) (xs : List Nat) := ((splits xs).map (bytes cost)).min?.getD 0
def dp (cost : Cost) : Nat → List Nat → Nat
  | 0,xs => if xs.isEmpty then 0 else cost xs
  | fuel+1,xs => if xs.isEmpty then 0 else
    ((List.range xs.length).map (fun i => cost (xs.take (i+1))+dp cost fuel (xs.drop (i+1)))).min?.getD 0
def valid (ps : List (List Nat)) (xs : List Nat) := ps.flatten = xs ∧ ∀ p ∈ ps, p ≠ []
def capped (n : Nat) (ps : List (List Nat)) := ps.all (fun p => p.length ≤ n)
def boundedBest (cost : Cost) (n : Nat) (xs : List Nat) :=
  (((splits xs).filter (capped n)).map (bytes cost)).min?
def tagged (modes : List Cost) (p : List Nat) :=
  ((modes.map (fun f => 1+f p)).min?).getD (1+p.length*8)
def commonWidth (ps : List (List Nat)) := width (peak (ps.map List.length))
def indexed (cost : Cost) (ps : List (List Nat)) :=
  bytes cost ps + byteCount (ps.length*commonWidth ps) + ubytes ps.length
def estimate (cost : Cost) (xs : List Nat) := (splits xs).mergeSort (fun a b => bytes cost a ≤ bytes cost b) |>.head?.getD []

/-- 許される全ての切断列を対象に DP が最小の実バイトを返す。 -/
def ShortestPartition.Goal : Prop := ∀ cost xs, dp cost xs.length xs = best cost xs

/-- 証明対象にした候補境界が特定の分割パターンに偏っていないことを特徴づける。 -/
def CompleteBoundaries.Goal : Prop := ∀ ps xs, ps ∈ splits xs ↔ valid ps xs

/-- 既存の固定ページ分割も候補に含めれば、ディレクトリ・ヘッダ込みの費用で局所選択を保証できる。 -/
def LegacyCeiling.Goal : Prop := ∀ cost xs ps, valid ps xs → best cost xs ≤ bytes cost ps

/-- 参照しやすい最大展開行数を課しても、制限を緩めたとき最適サイズは悪化しない。 -/
def SeekBudget.Goal : Prop := ∀ cost a b xs n, a ≤ b → boundedBest cost a xs = some n →
  ∃ m ≤ n, boundedBest cost b xs = some m

/-- 同じページ形式の強制を解除し、タグ込みでページごとに最短形式へ切り替えた場合の保証。 -/
def MixedModes.Goal : Prop := ∀ modes (f : Cost) xs, f ∈ modes →
  best (tagged modes) xs ≤ best (fun p => 1+f p) xs

/-- 連続二表の間に切断点を置くことが可能なら、両表を独立に最適化した合計を上界として使える。 -/
def AppendEnvelope.Goal : Prop := ∀ cost a b, best cost (a++b) ≤ best cost a + best cost b

/-- ページごとの固定費が上がったとき、採用されるページ数が増えないことを使いヘッダ価格を探索する。 -/
def HeaderSensitivity.Goal : Prop := ∀ (cost : Cost) xs low high, low < high →
  (estimate (fun p => high+cost p) xs).length ≤ (estimate (fun p => low+cost p) xs).length

/-- 旧プランのページ当たり誤差上限から、見積りに基づく選択の実バイト regret を制限する。 -/
def EstimationRegret.Goal : Prop := ∀ (actual approx : Cost) (xs : List Nat) e,
  (∀ ps ∈ splits xs, ∀ p ∈ ps, ((actual p:Int)-approx p).natAbs ≤ e) →
  let selected := estimate approx xs
  let optimum := estimate actual xs
  bytes actual selected ≤ bytes actual optimum + e*(selected.length+optimum.length)

/-- 分割数が一定なら固定ヘッダを目的関数から外しても候補同士の順序が保存される。 -/
def FixedCount.Goal : Prop := ∀ (cost : Cost) a b h, a.length = b.length →
  (bytes (fun p => h+cost p) a ≤ bytes (fun p => h+cost p) b ↔ bytes cost a ≤ bytes cost b)

/-- グローバルな長さ幅をディレクトリに使うとき、最大ページの閾値候補を列挙して厳密最適値を得る。 -/
def DirectoryWidthSearch.Goal : Prop := ∀ cost xs,
  ((splits xs).map (indexed cost)).min? =
    ((List.range (width xs.length+1)).flatMap (fun w =>
      ((splits xs).filter (fun ps => commonWidth ps ≤ w)).map (fun ps =>
        bytes cost ps + byteCount (ps.length*w) + ubytes ps.length))).min?

/-- 強仮説。隣接二ブロックの結合が都度割高なら、三ブロック以上の同時結合も割高か。 -/
def PairwiseLocal.Goal : Prop := ∀ (cost : Cost) (a b c : List Nat),
  cost a+cost b ≤ cost (a++b) → cost b+cost c ≤ cost (b++c) →
  cost a+cost b+cost c ≤ cost (a++b++c)

/-- 行上限を守る二つのデータ断片を独立に最適化すれば、連結した問題の実現可能解になる。 -/
def CappedAppend.Goal : Prop := ∀ cost a b cap ca cb,
  boundedBest cost cap a = some ca → boundedBest cost cap b = some cb →
  ∃ merged ≤ ca+cb, boundedBest cost cap (a++b) = some merged

/-- raw の既定モードをタグ付きで用意すると、新規形式を揃えても最悪増分を一タグに留められる。 -/
def RawFallback.Goal : Prop := ∀ modes xs, xs ≠ [] →
  best (tagged ((fun p => p.length*8)::modes)) xs ≤ 1+xs.length*8

/-- 全候補のフレームが一つの単位に整列すると、段末に再度丸める必要がなくなる。 -/
def AlignedComposition.Goal : Prop := ∀ (cost : Cost) ps unit, 0 < unit →
  (∀ p ∈ ps, unit ∣ cost p) → align unit (bytes cost ps) = bytes cost ps

/-- ページが粗くなりすぎると復号量が二次的に増える。均等確率の点参照を行数二乗和で課金する。 -/
def lookupWork (ps : List (List Nat)) := (ps.map (fun p => p.length^2)).sum
def PointReadPenalty.Goal : Prop := ∀ pre a b post,
  lookupWork (pre++[a++b]++post) - lookupWork (pre++[a,b]++post) = 2*a.length*b.length

/-- バイト数とランダム参照の二目的を重み付きで選ぶ際、支配される境界集合を除ける。 -/
def DominatedPlan.Goal : Prop := ∀ cost a b readPrice, bytes cost a ≤ bytes cost b → lookupWork a ≤ lookupWork b →
  bytes cost a + readPrice*lookupWork a ≤ bytes cost b + readPrice*lookupWork b

#guard let c : Cost := fun p => if p.length == 3 then 1 else 2*p.length
       c [0]+c [1] == c [0,1] && c [1]+c [2] == c [1,2] && c [0]+c [1]+c [2] > c [0,1,2]
end TinylambOpt.ResearchGoals.Compression.Partition
