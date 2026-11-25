import Std

namespace TinylambOpt.ResearchGoals.Frontiers.TextIndex

/-! Literal patterns over binary-collated symbols. The empty suffix is a distinct
index entry. Documents are physical rows, so identical documents have different IDs. -/
abbrev Word := List Nat
def sa (s : Word) := (List.range (s.length+1)).mergeSort (fun i j => s.drop i ≤ s.drop j)
def hits (s p : Word) := (sa s).filter (fun i => p.isPrefixOf (s.drop i))
def lo (s p : Word) := (sa s).countP (fun i => s.drop i < p)
def preceding (s : Word) (i : Nat) := if i == 0 then none else s[i-1]?
def occ (s : Word) (a stop : Nat) := ((sa s).take stop).countP (fun i => preceding s i == some a)
def first (s : Word) (a : Nat) := 1 + s.countP (· < a)

/-- 一致する suffix を区間の始点・件数だけで表し、LIKE の一致位置を後から列挙する。 -/
def Contiguous.Goal : Prop := ∀ s p, ((sa s).drop (lo s p)).take (hits s p).length = hits s p

/-- FM 索引でパターンの左端を一文字伸ばす。二回の rank と累積頻度から新しい半開区間を得る。 -/
def BackwardSearch.Goal : Prop := ∀ s p a,
  let l := lo s p
  let h := l + (hits s p).length
  (lo s (a::p), (hits s (a::p)).length) = (first s a + occ s a l, occ s a h - occ s a l)

/-- 一致区間内の前置文字だけをプローブし、再ソートせず左拡張の全住所を得る。 -/
def LocateExtension.Goal : Prop := ∀ s p a, hits s (a::p) =
  ((hits s p).filter (fun i => preceding s i == some a)).map (·-1)

def lf (s : Word) (r : Nat) :=
  match preceding s ((sa s)[r]?.getD 0) with
  | none => 0
  | some a => first s a + occ s a r
def rewind (s : Word) : Nat → Nat → Nat
  | 0,r => r
  | n+1,r => rewind s n (lf s r)

/-- stride の倍数だけ住所を保存した suffix 配列から、LF を高々 stride−1 回辿って元位置を復元する。 -/
def SampledLocate.Goal : Prop := ∀ s r stride, r < (sa s).length → 0 < stride →
  let pos := (sa s)[r]?.getD 0
  let steps := pos % stride
  (sa s)[rewind s steps r]? = some (pos-steps)

def owners (docs : List Word) (p : Word) := docs.zipIdx.flatMap (fun (s,i) => (hits s p).map (fun _ => i))

/-- 同一文書内の複数一致をまとめ、元の行を読まず一致文書ごとの売上を一度だけ合計する。 -/
def DocumentSum.Goal : Prop := ∀ docs p (weight : Nat → Int),
  (((owners docs p).eraseDups).map weight).sum =
    ((docs.zipIdx.filter (fun (s,_) => (List.range (s.length+1)).any
      (fun i => p.isPrefixOf (s.drop i)))).map (fun e => weight e.2)).sum

def previous (ids : List Nat) (doc pos : Nat) :=
  ((((ids.take pos).zipIdx).filter (fun e => e.1 == doc)).getLast?).map Prod.snd

/-- 任意の suffix 区間で直前の同一文書が左端より手前の印だけを数え、行 DISTINCT を返す。 -/
def DistinctMarks.Goal : Prop := ∀ (ids : List Nat) l w,
  let slice := (ids.drop l).take w
  slice.eraseDups.length = (slice.zipIdx.countP (fun (doc,j) =>
    (previous ids doc (l+j)).all (· < l)))

/-- 強仮説。複数行を区切りなしに連結して一つの suffix 索引を作り、EXISTS を任意の非空語で共有できるか。 -/
def Unseparated.Goal : Prop := ∀ (docs : List Word) p, p ≠ [] →
  (!(hits docs.flatten p).isEmpty) = docs.any (fun s => !(hits s p).isEmpty)

def markerEncode (s : Word) := s.map Nat.succ ++ [0]

/-- 各行末の予約記号を一つだけ使い、全行を共有する索引から非空リテラルの総出現数を得る。 -/
def SeparatedCount.Goal : Prop := ∀ (docs : List Word) p, p ≠ [] →
  (hits (docs.flatMap markerEncode) (p.map Nat.succ)).length =
    (docs.map (fun s => (hits s p).length)).sum

/-- 部分語ごとの出現位置をオフセットで突き合わせ、連続フレーズの一致を元文字列の再照合なしで得る。 -/
def PositionalIntersect.Goal : Prop := ∀ s left right,
  hits s (left ++ right) = (hits s left).filter (fun i => (hits s right).contains (i+left.length))

/-- 二つの部分語を別々の辞書で照合し、一方の出現頻度だけでもフレーズ頻度の打切り上限を得る。 -/
def PhraseBound.Goal : Prop := ∀ s left right,
  (hits s (left ++ right)).length ≤ min (hits s left).length (hits s right).length

#guard hits [1,2] [1,2] == [0] && (owners [[1],[2]] [1,2]).isEmpty
#guard hits [1,1] [] == [2,1,0] && rewind [1,1] 2 0 == 2

end TinylambOpt.ResearchGoals.Frontiers.TextIndex
