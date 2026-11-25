import Std

namespace TinylambOpt.ResearchGoals.Atlas.TextExecution
abbrev Word := List Nat
def positions (s p : Word) :=
  (List.range (s.length+1)).filter (fun i => p.isPrefixOf (s.drop i))
def occurs (s p : Word) := !(positions s p).isEmpty

namespace Grammar
inductive Text | literal (s : Word) | concat (l r : Text) | repeat (n : Nat) (t : Text)
def expand : Text → Word
  | .literal s => s
  | .concat a b => expand a ++ expand b
  | .repeat n s => (List.replicate n (expand s)).flatten
def seamWord (a b p : Word) := (a.drop (a.length-(p.length-1))) ++ b.take (p.length-1)
def crossing (a b p : Word) :=
  let lim := min a.length (p.length-1)
  (positions (seamWord a b p) p).countP (fun i => i < lim && lim < i+p.length)

/-- LZ や辞書の二文節にまたがる出現を両端の短い断片だけで数える。 -/
def BoundaryHits.Goal : Prop := ∀ a b p, p ≠ [] → (positions (a++b) p).length =
  (positions a p).length + (positions b p).length + crossing a b p

/-- パターン長以上の語を反復する文法節の出現数を、反復内と隣接境界の二項で求める。 -/
def LongPower.Goal : Prop := ∀ s p n, p ≠ [] → p.length ≤ s.length →
  (positions (expand (.repeat n (.literal s))) p).length =
    n*(positions s p).length + (n-1)*crossing s s p

/-- 短語の反復でも、一周期分の開始位相から末尾まで残る有効位置数を重み付けする。 -/
def ShortPower.Goal : Prop := ∀ s p n, s ≠ [] → p ≠ [] →
  let period := s.length
  let probe := expand (.repeat (p.length/period+2) (.literal s))
  (positions (expand (.repeat n (.literal s))) p).length =
    ((List.range period).map (fun i => if p.isPrefixOf (probe.drop i) && i+p.length ≤ n*period
      then (n*period-i-p.length)/period+1 else 0)).sum

/-- 文法節の部分文字列だけを再帰的に開き、巨大な未使用の左・右部分木を飛ばす。 -/
def length : Text → Nat
  | .literal s => s.length | .concat a b => length a+length b | .repeat n a => n*length a
def atChar : Text → Nat → Option Nat
  | .literal s,i => s[i]?
  | .concat a b,i => if i < length a then atChar a i else atChar b (i-length a)
  | .repeat n a,i => if i < n*length a then atChar a (i%length a) else none
def SliceDecode.Goal : Prop := ∀ g l w, ((expand g).drop l).take w =
  (List.range w).filterMap (fun j => atChar g (l+j))

/-- 強仮説。語の長さと内部出現数だけで、後続の任意の辞書節と結合した結果も共有できるか。 -/
def InteriorCache.Goal : Prop := ∀ a b p, a.length = b.length → (positions a p).length = (positions b p).length →
  ∀ tail, (positions (a++tail) p).length = (positions (b++tail) p).length
end Grammar

namespace EditFilter
def grams (q : Nat) (s : Word) := (List.range (s.length+1-q)).map (fun i => (s.drop i).take q)
def shared (a b : List Word) := (a.eraseDups.map (fun g => min (a.count g) (b.count g))).sum
inductive Edits : Nat → Word → Word → Prop
  | refl (s) : Edits 0 s s
  | drop {k a} (l r : Word) (x : Nat) : Edits k a (l++x::r) → Edits (k+1) a (l++r)
  | add {k a} (l r : Word) (x : Nat) : Edits k a (l++r) → Edits (k+1) a (l++x::r)
  | subst {k a} (l r : Word) (x y : Nat) : Edits k a (l++x::r) → Edits (k+1) a (l++y::r)

/-- 編集距離条件の候補を多重 q-gram の最小共通件数で棄却する。 -/
def GramLowerBound.Goal : Prop := ∀ s t k q, 0 < q → Edits k s t →
  max (grams q s).length (grams q t).length ≤ shared (grams q s) (grams q t)+k*q

/-- パターンを k+1 個の非空部分語に切り、一つも現れない文書を捨てる。 -/
def PigeonholeAnchor.Goal : Prop := ∀ (ps : List Word) t k, k < ps.length →
  (∀ p ∈ ps, p ≠ []) → Edits k ps.flatten t → ∃ p ∈ ps, occurs t p = true

/-- 文書先頭の編集によるずれも k 以内なので、候補 anchor の検索範囲をその幅に制限する。 -/
def PositionalAnchor.Goal : Prop := ∀ (ps : List Word) t k, k < ps.length → (∀ p ∈ ps, p ≠ []) →
  Edits k ps.flatten t → ∃ i < ps.length, ∃ j ∈ positions t (ps[i]?.getD []),
    ((j:Int) - ((ps.take i).map List.length).sum).natAbs ≤ k

/-- 任意の文字集合への射影後の文字数差を、編集距離の下界として索引に使う。 -/
def ClassLength.Goal : Prop := ∀ k s t (keep : Nat → Bool), Edits k s t →
  ((s.countP keep:Int)-t.countP keep).natAbs ≤ k

/-- 離れた語彙では頻度差の L1 ノルムが大きい。転置索引の頻度から距離予算超過を判定する。 -/
def HistogramFence.Goal : Prop := ∀ k s t, Edits k s t →
  ((s++t).eraseDups.map (fun a => ((s.count a:Int)-t.count a).natAbs)).sum ≤ 2*k
end EditFilter

namespace Wavelet
def lower (bit : Nat) (vs : Word) := vs.filter (fun x => !x.testBit bit)
def upper (bit : Nat) (vs : Word) := vs.filter (fun x => x.testBit bit)
def remap (bit l w : Nat) (xs : Word) (high : Bool) :=
  let takeBranch := fun x : Nat => x.testBit bit == high
  (((xs.take l).countP takeBranch),(((xs.drop l).take w).countP takeBranch))
def kth : Nat → Word → Nat → Nat
  | 0,_,_ => 0
  | d+1,xs,k =>
    let zs := lower d xs
    if k < zs.length then kth d zs k else 2^d+kth d (upper d xs) (k-zs.length)

/-- ドキュメント ID の suffix 区間を下位・上位の安定分割後の二つの区間に写す。 -/
def RangeMap.Goal : Prop := ∀ bit l w xs high,
  let branch := fun x : Nat => x.testBit bit == high
  let (start,len) := remap bit l w xs high
  (((xs.drop l).take w).filter branch) = (((xs.filter branch).drop start).take len)

/-- 波形木のレベルごとのゼロ件数から、第 k 辞書 ID を整列せず選ぶ。 -/
def Quantile.Goal : Prop := ∀ d xs k, (∀ x ∈ xs, x < 2^d) → k < xs.length →
  xs.mergeSort[k]? = some (kth d xs k)

/-- 二つの文字列条件の文書一致ペアを、同じビット枝に入った組だけに分けて計算する。 -/
def pairs (a b : Word) := (a.map b.count).sum
def SameBranch.Goal : Prop := ∀ a b bit, pairs a b =
  pairs (lower bit a) (lower bit b) + pairs (upper bit a) (upper bit b)

/-- 完全に覆われた ID 接頭辞ではビット木を降りず、部分一致する枝だけを開く。 -/
def BetweenCodes.Goal : Prop := ∀ (xs : Word) level lo hi, lo ≤ hi →
  xs.countP (fun x => lo*2^level ≤ x && x < hi*2^level) =
    xs.countP (fun x => lo ≤ x/2^level && x/2^level < hi)

/-- 辞書 ID が二の冪を跨いで増えたとき、既存の下位木を一つの枝として再利用する。 -/
def DictionaryGrowth.Goal : Prop := ∀ d old more k, (∀ x ∈ old, x < 2^d) →
  (∀ y ∈ more, 2^d ≤ y ∧ y < 2^(d+1)) → k < old.length+more.length →
  kth (d+1) (old++more) k = if k < old.length then kth d old k else 2^d+kth d more (k-old.length)
end Wavelet

namespace Lcp
def common : Word → Word → Nat
  | a::xs,b::ys => if a == b then 1+common xs ys else 0
  | _,_ => 0
def lcpEdges (ws : List Word) := (ws.zip (ws.drop 1)).map (fun (a,b) => common a b)

/-- 辞書の任意二語の共通接頭辞長を、間の隣接 LCP の区間最小値で得る。 -/
def RangeMinimum.Goal : Prop := ∀ (ws : List Word) i j, ws.Pairwise (· ≤ ·) → i < j → j < ws.length →
  ((lcpEdges ws).drop i |>.take (j-i)).min? = some (common (ws[i]?.getD []) (ws[j]?.getD []))

/-- フロントコーディングで実際に保存する記号数をリンクごとの LCP で見積もる。 -/
def frontBytes (xs : List Word) := ((xs.zip ([]::xs)).map (fun (w,prev) => (w.drop (common prev w)).length)).sum
def DictionarySize.Goal : Prop := ∀ ws, frontBytes ws + (lcpEdges ws).sum = (ws.map List.length).sum

/-- 辞書両端の接頭辞が一致するページは途中の全語でも LIKE 前方一致を採用する。 -/
def PagePrefix.Goal : Prop := ∀ (p a b w : Word), a ≤ w → w ≤ b → p.isPrefixOf a → p.isPrefixOf b → p.isPrefixOf w

/-- 共通接頭辞の境界より短い二分探索キーでは、既知の文字比較から途中語の順序を推定する。 -/
def CompareSkip.Goal : Prop := ∀ a b needle, common a b > common a needle →
  compare a needle = compare b needle

/-- 辞書を別々に作って連結するとき、最初の語にだけ追加の前方圧縮が効く。 -/
def ShardBridge.Goal : Prop := ∀ a b,
  frontBytes (a++b) + common (a.getLast?.getD []) (b.head?.getD []) = frontBytes a + frontBytes b
end Lcp

#guard let p : Word := [1,2]
       (positions [1] p).length == (positions [0] p).length &&
         (positions [1,2] p).length == 1 && (positions [0,2] p).length == 0

end TinylambOpt.ResearchGoals.Atlas.TextExecution
