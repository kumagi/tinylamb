import Std

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.
-/

namespace TinylambOpt.ResearchGoals.Compression.Wire

/-! Natural-number wire models. A Byte is a natural in [0,256) on canonical
streams. Integers are exact; width 0 has a single representable value, zero. -/
abbrev Bytes := List Nat
def width (n : Nat) := if n == 0 then 0 else n.log2+1
def byteCount (bits : Nat) := (bits+7)/8
def align (unit n : Nat) := ((n+unit-1)/unit)*unit
def peak (xs : List Nat) := xs.max?.getD 0
def packedSize (xs : List Nat) := byteCount (xs.length * width (peak xs))
def zig : Int → Nat | .ofNat n => 2*n | .negSucc n => 2*n+1
def unzig (n : Nat) : Int := if n%2 == 0 then (n/2:Nat) else -((n/2:Nat):Int)-1

def uleb (n : Nat) : Bytes :=
  if n < 128 then [n] else (128+n%128) :: uleb (n/128)
termination_by n
decreasing_by exact Nat.div_lt_self (by omega) (by decide)
def readU : Bytes → Option (Nat × Bytes)
  | [] => none
  | b::rest => if b < 128 then some (b,rest) else if b < 256 then
      (readU rest).map (fun (v,tail) => (b%128+128*v,tail)) else none
def ubytes (n : Nat) := max 1 ((width n+6)/7)
def sbytes (n : Int) := ubytes (zig n)
def streamBytes (xs : List Nat) := (xs.map ubytes).sum

def word (w : Nat) : List Nat → Nat
  | [] => 0
  | x::xs => x%2^w + 2^w * word w xs
def little (n k : Nat) : Bytes := (List.range k).map (fun i => (n/256^i)%256)
def fromLittle : Bytes → Nat | [] => 0 | b::bs => b + 256*fromLittle bs
def pack (w : Nat) (xs : List Nat) := little (word w xs) (byteCount (xs.length*w))
def get (bs : Bytes) (w i : Nat) := (fromLittle bs / 2^(i*w))%2^w
def takeAt (bs : Bytes) (w i : Nat) :=
  let start := i*w/8
  let shift := i*w%8
  (fromLittle ((bs.drop start).take (byteCount (shift+w))) / 2^shift)%2^w

/-- 任意の後続フレームを保持したまま整数を復元し、長さ表を省く。 -/
def DelimitedInteger.Goal : Prop := ∀ n tail, readU (uleb n ++ tail) = some (n,tail)

/-- 可変長整数の符号長を入力値から事前に正確に課金する。 -/
def VarintCharge.Goal : Prop := ∀ n, (uleb n).length = ubytes n

/-- 値域全体を k バイト以下で運べるかを一つの上端比較で判定する。 -/
def WidthCliff.Goal : Prop := ∀ n k, 0 < k → (ubytes n ≤ k ↔ n < 128^k)

/-- 狭い符号付き残差を交互配置しても負の最小値を含めて元へ戻る。 -/
def SignedInverse.Goal : Prop := ∀ x, unzig (zig x) = x

/-- signed 可変長の境界を整数区間へ逆写像し、ページの min/max だけで判定する。 -/
def SignedCliff.Goal : Prop := ∀ (x : Int) (k : Nat), 0 < k →
  (sbytes x ≤ k ↔ -((128^k/2:Nat):Int) ≤ x ∧ x < (128^k/2:Nat))

/-- 64 ビット絶対値を符号化する際の最悪長を求め、raw fallback 用の予約域を制限する。 -/
def MaxWordBytes.Goal : Prop := ∀ n, n < 2^64 → ubytes n ≤ 10

/-- 正準な先頭整数が一致する二つのストリームは整数境界も一致する。 -/
def PrefixFree.Goal : Prop := ∀ a b tail, (uleb a).isPrefixOf (uleb b) →
  readU (uleb b ++ tail) = some (a,tail)

/-- ビット詰めが各値と物理順を保つ条件。定数ブロックは幅ゼロも扱う。 -/
def PackedInverse.Goal : Prop := ∀ w xs, (∀ x ∈ xs, x < 2^w) →
  (List.range xs.length).map (get (pack w xs) w) = xs

/-- クロスバイトのレーンを含め、必要最小のバイト窓だけで点参照する。 -/
def LocalRead.Goal : Prop := ∀ bs w i, (∀ b ∈ bs, b < 256) → takeAt bs w i = get bs w i

/-- 長さと bit width だけでパディング込みの実バイト数を算出する。 -/
def PackedCharge.Goal : Prop := ∀ w xs, (pack w xs).length = byteCount (xs.length*w)

/-- 途中バイトで接する二つの整数列を、値へ復号せずビットシフトで接続する。 -/
def BitSplice.Goal : Prop := ∀ w a b, pack w (a++b) = little
  (fromLittle (pack w a) + 2^(w*a.length)*fromLittle (pack w b)) (byteCount ((a.length+b.length)*w))

/-- 各ブロックの末尾ビットを共有すると、消える端数バイトの上限はブロック境界数になる。 -/
def PaddingDebt.Goal : Prop := ∀ (sizes : List Nat),
  (sizes.map byteCount).sum - byteCount sizes.sum ≤ sizes.length-1

/-- 固定幅基数はその列の最大値で選べ、これより短い均一幅ではその列を復元できない。 -/
def MinimumWidth.Goal : Prop := ∀ xs w, (∀ x ∈ xs, x < 2^w) ↔ width (peak xs) ≤ w

/-- raw の整数幅を最小ビット幅へ縮める。ヘッダの増分を払っても小さくなる境界をバイト単位で求める。 -/
def BitPackBreakEven.Goal : Prop := ∀ rows bits oldBytes overhead, overhead < rows*oldBytes →
  (overhead + byteCount (rows*bits) < rows*oldBytes ↔ rows*bits ≤ 8*(rows*oldBytes-overhead-1))

/-- 幅を 1 ビット縮める効果がバイト丸め後にも必ず出る最小十分行数。 -/
def OneBitSaving.Goal : Prop := ∀ n w, 8 ≤ n → 0 < w →
  byteCount (n*(w-1)) + n/8 ≤ byteCount (n*w)

/-- 同じ幅の低位ストリームを二つに分けた場合に余分に払うのは高々一バイト。 -/
def SplitPenalty.Goal : Prop := ∀ a b, byteCount (a+b) ≤ byteCount a+byteCount b ∧
  byteCount a+byteCount b ≤ byteCount (a+b)+1

/-- ストリームごとの 8 バイト整列を一括整列にすると何バイトまで節約できるか。 -/
def AlignmentDebt.Goal : Prop := ∀ unit (sizes : List Nat), 0 < unit →
  (sizes.map (align unit)).sum - align unit sizes.sum ≤ (sizes.length-1)*unit

/-- 符号幅の見積もりに平均値だけを用いる強仮説。外れ値が立つページで検証する。 -/
def MeanWidth.Goal : Prop := ∀ xs : List Nat, xs ≠ [] →
  pack (width (xs.sum/xs.length)) xs = pack (width (peak xs)) xs

#guard readU (uleb 16384 ++ [42]) == some (16384,[42])
#guard pack 1 [0,0,0,7] != pack 3 [0,0,0,7]
end TinylambOpt.ResearchGoals.Compression.Wire
