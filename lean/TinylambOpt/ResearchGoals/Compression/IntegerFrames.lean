import TinylambOpt.ResearchGoals.Compression.Wire

namespace TinylambOpt.ResearchGoals.Compression.IntegerFrames
open Wire

/-! A patched frame stores the low w bits of every offset and (ordinal, high
part) only for nonzero high parts. Row count, base, width and number of patches
are persisted. The scheme here uses absolute ULEB ordinals, then compares gaps. -/
def lows (w : Nat) (xs : List Nat) := xs.map (· % 2^w)
def patches (w : Nat) (xs : List Nat) :=
  (xs.zipIdx.filter (fun (v,_) => 2^w ≤ v)).map (fun (v,i) => (i,v/2^w))
def high (ps : List (Nat × Nat)) (i : Nat) := ((ps.find? (fun e => e.1 == i)).map Prod.snd).getD 0
def patched (w : Nat) (xs : List Nat) (i : Nat) :=
  takeAt (pack w (lows w xs)) w i + 2^w*high (patches w xs) i
def patchStream (ps : List (Nat × Nat)) := ps.flatMap (fun (i,h) => uleb i ++ uleb h)
def body (base : Int) (w : Nat) (xs : List Nat) :=
  let ps := patches w xs
  [1] ++ uleb xs.length ++ uleb (zig base) ++ uleb w ++ uleb ps.length ++ pack w (lows w xs) ++ patchStream ps
def charge (base : Int) (w : Nat) (xs : List Nat) :=
  let ps := patches w xs
  1+ubytes xs.length+sbytes base+ubytes w+ubytes ps.length+byteCount (w*xs.length) +
    (ps.map (fun (i,h) => ubytes i+ubytes h)).sum
def gaps : Nat → List Nat → List Nat
  | _,[] => []
  | p,i::is => (i-p) :: gaps (i+1) is
def undoGaps : Nat → List Nat → List Nat
  | _,[] => []
  | p,g::gs => (p+g) :: undoGaps (p+g+1) gs
def tlWidth (span : Nat) := if span == 0 then 0 else if span < 256 then 1 else
  if span < 65536 then 2 else if span < 2^32 then 4 else 8

/-- 例外を上位ビットだけとして復元し、ゼロ幅と重複値も正しい ordinal へ戻す。 -/
def PatchedInverse.Goal : Prop := ∀ w xs, (List.range xs.length).map (patched w xs) = xs

/-- 例外の数・位置・上位・基準値まで含めたシリアライズ長を事前に正確に見積もる。 -/
def ExactQuote.Goal : Prop := ∀ base w xs, (body base w xs).length = charge base w xs

/-- ベース幅を 1 増やしたときに消える例外を一つの値域ヒストグラムから数える。 -/
def ExceptionDrop.Goal : Prop := ∀ w xs, (patches w xs).length - (patches (w+1) xs).length =
  xs.countP (fun v => 2^w ≤ v && v < 2^(w+1))

/-- 最大 bit width までの探索で、全ての幅における最小フォーマット長を達成できる。 -/
def FiniteSearch.Goal : Prop := ∀ base xs w,
  let widths := List.range (width (peak xs)+1)
  (widths.map (fun k => charge base k xs)).min?.getD 0 ≤ charge base w xs

/-- 例外位置をスキップ長に変換し、隣り合う例外をゼロ差分で表す。 -/
def GapOrdinals.Goal : Prop := ∀ w xs, let ids := (patches w xs).map Prod.fst
  undoGaps 0 (gaps 0 ids) = ids

/-- 昇順例外列の gap を使うと、絶対位置 ULEB より長くならない。 -/
def GapDominance.Goal : Prop := ∀ w xs, let ids := (patches w xs).map Prod.fst
  streamBytes (gaps 0 ids) ≤ streamBytes ids

/-- 単一パッチの上位値幅がページ内で揃う場合、それらもビットパックして上位別 ULEB を省ける。 -/
def HighStreamBudget.Goal : Prop := ∀ w xs,
  let hi := (patches w xs).map Prod.snd
  (pack (width (peak hi)) hi).length + ubytes (width (peak hi)) < streamBytes hi ↔
    byteCount (width (peak hi)*hi.length)+ubytes (width (peak hi)) < streamBytes hi

/-- 外れ値がなければ TLCS の 1/2/4/8 バイト幅の間をビット詰めしてペイロードを減らせる。 -/
def ForEnvelope.Goal : Prop := ∀ xs, peak xs < 2^64 → packedSize xs ≤ tlWidth (peak xs)*xs.length

/-- 一定差の倍数でしか変化しない列を GCD で割り、同じ基準値から原値を復元する。 -/
def divisor (xs : List Nat) := xs.foldl Nat.gcd 0
def StrideFactor.Goal : Prop := ∀ xs, let d := divisor xs
  (xs.map (fun x => (x/d)*d)) = xs

/-- 倍率を一度書く価値があるかを、最大値と件数の情報だけで厳密に判断する。 -/
def GcdBudget.Goal : Prop := ∀ xs, let d := divisor xs
  (ubytes d + packedSize (xs.map (·/d)) < packedSize xs ↔
    ubytes d + byteCount (xs.length*width (peak xs/d)) < packedSize xs)

/-- ページの最小値を引く前後で符号付き端点差を拡張精度で保つ。 -/
def SignedFor.Goal : Prop := ∀ xs : List Int, let lo := xs.min?.getD 0
  (xs.map (fun x => lo+((x-lo).toNat:Int))) = xs

/-- Rice の二進余りと単項商を分離し、ストリームの占有 bit 数を計算する。 -/
def rice (k v : Nat) := List.replicate (v/2^k) true ++ [false] ++
  (List.range k).map (fun i => v.testBit i)
def RiceCharge.Goal : Prop := ∀ k (xs : List Nat), (xs.flatMap (rice k)).length =
  xs.length*(k+1)+(xs.map (·/2^k)).sum

/-- Rice パラメータを一段広くしたときに商が節約する bit と全行の余り増分を比較する。 -/
def RiceStep.Goal : Prop := ∀ k (xs : List Nat),
  ((xs.flatMap (rice (k+1))).length < (xs.flatMap (rice k)).length ↔
   xs.length < (xs.map (fun x => x/2^k-x/2^(k+1))).sum)

/-- 商の節約量が初めて一行一 bit 以下になった幅で止めてよいかを問う。 -/
def RiceUnimodal.Goal : Prop := ∀ k (xs : List Nat), (xs.flatMap (rice k)).length ≤ (xs.flatMap (rice (k+1))).length →
  ∀ j ≥ k, (xs.flatMap (rice k)).length ≤ (xs.flatMap (rice j)).length

/-- 強仮説。最大値まで広げて例外の件数を最小にする選択が、ページ全体のバイト数も最小にするか。 -/
def FewestExceptions.Goal : Prop := ∀ base xs w, charge base (width (peak xs)) xs ≤ charge base w xs

/-- 値の最大公約数が一でも、分割点を選び直すとローカル倍率を使える。各倍率を記録する前の payload 上限を与える。 -/
def TwoStrideSegments.Goal : Prop := ∀ (a b : List Nat) da db, 0 < da → 0 < db →
  (∀ x ∈ a, da ∣ x) → (∀ x ∈ b, db ∣ x) →
  packedSize (a.map (·/divisor a)) + packedSize (b.map (·/divisor b)) ≤
    packedSize (a.map (·/da)) + packedSize (b.map (·/db))

#guard let xs := 65535 :: List.replicate 127 0
       charge 0 0 xs < charge 0 (width (peak xs)) xs
end TinylambOpt.ResearchGoals.Compression.IntegerFrames
