import TinylambOpt.ResearchGoals.Compression.Wire

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.
-/

namespace TinylambOpt.ResearchGoals.Compression.BitPatterns
open Wire

/-! Values are unsigned binary64 bit images. Bitwise reconstruction preserves
all quiet/signaling NaN payloads, infinities and the two zero encodings. -/
def delta : Nat → List Nat → List Nat
  | _,[] => []
  | prev,x::xs => (prev ^^^ x) :: delta x xs
def undo : Nat → List Nat → List Nat
  | _,[] => []
  | prev,d::ds => let x := prev ^^^ d; x :: undo x ds
def tz (x : Nat) := (List.range 64).findIdx (fun i => x.testBit i)
def activeBits (x : Nat) := width x - tz x
def significant (x : Nat) := x/2^(tz x)
def fitsWindow (lo bits x : Nat) := x % 2^lo == 0 && x/2^lo < 2^bits
def plane (xs : List Nat) (p : Nat) := xs.map (fun x => x.testBit p)
def rebuild (n : Nat) (test : Nat → Nat → Bool) :=
  (List.range 64).foldl (fun acc p => if test p n then acc+2^p else acc) 0

/-- 生の binary64 のビット列を XOR 差分で格納し、NaN payload と負のゼロも保存する。 -/
def XorChain.Goal : Prop := ∀ prev xs, undo prev (delta prev xs) = xs

/-- 先頭・末尾のゼロを除いた XOR 差分の有効窓と下位位置だけで原語を復元する。 -/
def SignificantWindow.Goal : Prop := ∀ x, x < 2^64 →
  (significant x % 2^activeBits x)*2^tz x = x

/-- 前行と同じ指数字段の符号ビット列では、その部分の XOR を送らずに済む。 -/
def CommonHighBits.Goal : Prop := ∀ a b k, a/2^k = b/2^k → (a ^^^ b) < 2^k

/-- どの値が以前の非零窓を再利用できるかは、窓外ビットのゼロ検査で判定する。 -/
def PreviousWindow.Goal : Prop := ∀ lo w x,
  fitsWindow lo w x = true ↔ ((x/2^lo)%2^w)*2^lo = x

/-- より狭い窓を開くコストを、その窓を繰り返し使う行数で回収できる条件。 -/
def AmortizedWindow.Goal : Prop := ∀ (n oldWidth newWidth header : Nat), newWidth ≤ oldWidth →
  (header+n*newWidth < n*oldWidth ↔ header < n*(oldWidth-newWidth))

/-- IEEE 生ビット辞書で反復する NaN を物理的に等しいコードとして扱い、再生時は payload が一致する。 -/
def RawDictionary.Goal : Prop := ∀ xs : List Nat, let d := xs.eraseDups
  (xs.map (fun x => d[d.idxOf x]?.getD 0)) = xs

/-- 強仮説。符号付きゼロを同じ辞書 ID にしても raw double の全ビットを保存できるか。 -/
def ZeroAlias.Goal : Prop := ∀ xs : List Nat,
  (xs.map (fun x => if x == 2^63 then 0 else x)) = xs

/-- 列内で一定の bit plane を一つの既定マスクに移し、変動部分だけから全レーンを復元する。 -/
def ConstantPlanes.Goal : Prop := ∀ xs i, (∀ x ∈ xs, x < 2^64) → i < xs.length →
  let ps := fun p => plane xs p
  rebuild i (fun p row => if (ps p).all id then true else (ps p)[row]?.getD false) = xs[i]?.getD 0

/-- ビットプレーンごとに少数側の位置だけを保存して再構成する。 -/
def SparsePlanes.Goal : Prop := ∀ xs i, (∀ x ∈ xs, x < 2^64) → i < xs.length →
  let bit := fun p => let bs := plane xs p; decide (bs.count true ≤ bs.count false)
  rebuild i (fun p row => let mark := bit p
    xor (!mark) (((plane xs p).zipIdx.filter (fun (v,_) => v == mark)).map Prod.snd |>.contains row)) =
    xs[i]?.getD 0

/-- バイトシャッフルにより等しい指数部を連続させても、元レーンの 8 バイトを直接逆変換できる。 -/
def bytePlanes (xs : List Nat) := (List.range 8).flatMap (fun j => xs.map (fun x => (x/256^j)%256))
def ByteShuffle.Goal : Prop := ∀ xs i, i < xs.length → (∀ x ∈ xs, x < 2^64) →
  fromLittle ((List.range 8).map (fun j => (bytePlanes xs)[j*xs.length+i]?.getD 0)) = xs[i]?.getD 0

/-- 共通辞書の XOR 基準値を固定したまま切替えるとき、差分ストリーム先頭だけを書き直す。 -/
def Reanchor.Goal : Prop := ∀ a b xs,
  (delta a xs).drop 1 = (delta b xs).drop 1

/-- bit image の下位 k ビットがすべて同じ場合、低位を一回保存し上位コードと結合する。 -/
def SharedMantissaTail.Goal : Prop := ∀ k (xs : List Nat) rem, (∀ x ∈ xs, x%2^k = rem) →
  xs.map (fun x => (x/2^k)*2^k+rem) = xs

/-- 候補となる可逆変換で戻らないビットだけを疎な例外表で上書きする。丸め変換でも SQL 値を保つ。 -/
def PatchedFloatConversion.Goal : Prop := ∀ (xs : List Nat) (encode decode : Nat → Nat),
  let es := xs.zipIdx.filter (fun (x,_) => decode (encode x) != x)
  xs.zipIdx.map (fun (x,i) => (es.find? (fun e => e.2 == i)).map Prod.fst |>.getD (decode (encode x))) = xs

/-- 32-bit の候補と sparse な 64-bit 例外を使うとき、例外の件数と ordinal のヘッダ込みで勝つ条件。 -/
def NarrowWithExceptions.Goal : Prop := ∀ (n h : Nat) (ordinals : List Nat),
  (4*n+h+streamBytes ordinals + 8*ordinals.length < 8*n ↔
    h+streamBytes ordinals + 8*ordinals.length < 4*n)

#guard ([0,2^63] : List Nat).map (fun x => if x == 2^63 then 0 else x) != [0,2^63]
end TinylambOpt.ResearchGoals.Compression.BitPatterns
