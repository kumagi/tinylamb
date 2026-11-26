import TinylambOpt.ResearchGoals.Compression.IntegerFrames
import TinylambOpt.ResearchGoals.ExecutionStrategies.EncodedBlocks

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.
-/

namespace TinylambOpt.ResearchGoals.Compression.Nullable
open Wire
open IntegerFrames (gaps undoGaps)
open TinylambOpt.ResearchGoals.EncodedBlocks (dense)

abbrev Col := List (Option Int)
def flags (xs : Col) := xs.map Option.isSome
def ids (fs : List Bool) (chosen : Bool) :=
  (fs.zipIdx.filter (fun (flag,_) => flag == chosen)).map Prod.snd
def marks (fs : List Bool) := pack 1 (fs.map Bool.toNat)
def minority (fs : List Bool) := decide (fs.count true ≤ fs.count false)
def sparse (fs : List Bool) := gaps 0 (ids fs (minority fs))
def expandMask (n : Nat) (bit : Bool) (gs : List Nat) := (List.range n).map
  (fun i => if (undoGaps 0 gs).contains i then bit else !bit)
def flips (fs : List Bool) := (fs.zipIdx.filter
  (fun (f,i) => 0 < i && fs[i-1]? != some f)).map Prod.snd
def fill (fs : List Bool) (vals : List Int) := fs.zipIdx.map (fun (v,i) =>
  if v then vals[(fs.take i).count true]? else none)
def ordinalWidth (n : Nat) := width (n-1)
def distinct (xs : Col) := (dense xs).eraseDups
def codes (xs : Col) :=
  let d := distinct xs
  xs.map (fun v => (v.map (fun x => d.idxOf x + 1)).getD 0)
def striped (ds : List Int) (bs : Bytes) (w n : Nat) := (List.range n).map (fun i =>
  let c := takeAt bs w i
  if c == 0 then none else ds[c-1]?)

/-- 有効値の密列だけを保存し、NULL を含む行順をマスクの rank で復元する。 -/
def DenseRoundTrip.Goal : Prop := ∀ xs, fill (flags xs) (dense xs) = xs

/-- byte-per-row の有効フラグを 1-bit 化し、物理位置から直接読める。 -/
def ValidityBits.Goal : Prop := ∀ fs i, i < fs.length → takeAt (marks fs) 1 i = (fs[i]?.getD false).toNat

/-- NULL と非 NULL の少ない方を gap のみで表し、極端に偏ったページの bitmap を省く。 -/
def MinorityRoundTrip.Goal : Prop := ∀ fs, expandMask fs.length (minority fs) (sparse fs) = fs

/-- 少数側のインデックス数は行数の半分以下で、例外表の最悪容量を抑えられる。 -/
def ExceptionCardinality.Goal : Prop := ∀ fs, (sparse fs).length = min (fs.count true) (fs.count false)

/-- polarity と件数をヘッダに置いたときの byte 境界を含め、疎表と密 bitmap の損益を求める。 -/
def SparseWins.Goal : Prop := ∀ fs, let gs := sparse fs
  (1+(uleb gs.length ++ gs.flatMap uleb).length < (marks fs).length ↔
    1+ubytes gs.length+streamBytes gs < byteCount fs.length)

/-- 最初のフラグと反転位置だけを保持し、任意位置の可視性を prefix parity で読む。 -/
def ToggleMask.Goal : Prop := ∀ fs i, i < fs.length →
  fs[i]? = some (xor (fs.head?.getD false) (((flips fs).countP (· ≤ i))%2 == 1))

/-- 繰り返し現れる NULL パターンの位相を保持して別列のマスクを参照する。 -/
def SharedMask.Goal : Prop := ∀ (a b : Col), flags a = flags b → fill (flags a) (dense b) = b

/-- マスクが少しだけ違う列同士なら XOR 例外だけ保存して元のマスクを取り戻す。 -/
def MaskXor.Goal : Prop := ∀ (a b : List Bool), a.length = b.length →
  a.zipWith xor (expandMask a.length true (gaps 0 (ids (a.zipWith xor b) true))) = b

/-- フラグが行単位でクラスタ化されていれば、複数ページの反転表を境界一つの補正で結合する。 -/
def ToggleBoundary.Goal : Prop := ∀ a b, (flips (a++b)).length =
  (flips a).length + (flips b).length +
    if a ≠ [] && b ≠ [] && a.getLast? != b.head? then 1 else 0

/-- NULL 専用コードと局所辞書を一つの bit stream にまとめ、別の validity を省く。 -/
def SentinelRoundTrip.Goal : Prop := ∀ xs, let d := distinct xs
  let w := width d.length
  striped d (pack w (codes xs)) w xs.length = xs

/-- 辞書の要素数が二の冪のときだけ、NULL 専用コードの追加が bit width を増やす。 -/
def SentinelCliff.Goal : Prop := ∀ d, 0 < d →
  (width (d-1) < width d ↔ ∃ k : Nat, d = 2^k)

/-- 有効値だけの ID 列を使うか NULL sentinel と全行 ID を使うかを、辞書の共通費を消して比較する。 -/
def DenseVsSentinel.Goal : Prop := ∀ xs, let d := distinct xs
  let vs := dense xs
  let w := ordinalWidth d.length
  ((marks (flags xs)).length + (pack w (vs.map d.idxOf)).length < (pack (width d.length) (codes xs)).length ↔
   byteCount xs.length + byteCount (vs.length*w) < byteCount (xs.length*width d.length))

/-- 全て NULL のブロックでは件数を除くレーンデータを持たず、空の辞書から復元する。 -/
def AllNullZeroWidth.Goal : Prop := ∀ n, striped [] [] 0 n = List.replicate n none

/-- 有効値が全て等しいブロックでは、定数一つとマスクから 8-bit 辞書コード列を省く。 -/
def NullableConstant.Goal : Prop := ∀ xs v, (∀ x ∈ dense xs, x = v) →
  (flags xs).map (fun good => if good then some v else none) = xs

/-- 254 件を超える辞書で全行を幅広化せず、一部の例外行だけ元値を付ける。 -/
def hotCodes (hot : List Int) (xs : Col) := xs.map (fun v => match v with
  | none => 0 | some x => if hot.contains x then 2+hot.idxOf x else 1)
def escapes (hot : List Int) (xs : Col) := (dense xs).filter (fun x => !hot.contains x)
def HotEscape.Goal : Prop := ∀ hot xs, let tags := hotCodes hot xs
  tags.zipIdx.map (fun (t,i) => if t == 0 then none else if t == 1 then
    (escapes hot xs)[(tags.take i).count 1]? else hot[t-2]?) = xs

/-- cold key を一つ hot に昇格させたときに省ける生値の格納数を、そのキーの行数から正確に得る。 -/
def PromoteKey.Goal : Prop := ∀ hot xs x, x ∉ hot →
  (escapes hot xs).length - (escapes (x::hot) xs).length = xs.count (some x)

/-- 対応表を付けて局所辞書をグローバル辞書に移す際、欠損コード 0 を予約したまま復号する。 -/
def TranslateLocal.Goal : Prop := ∀ xs (global : List Int),
  (∀ x ∈ dense xs, x ∈ global) →
  let translated := (distinct xs).map global.idxOf
  (codes xs).map (fun c => if c == 0 then none else translated[c-1]?.bind (fun j => global[j]?)) = xs

/-- 強仮説。辞書から重複を落とした後の異なり数だけで、どの配置でも RLE のラン数を見積もれるか。 -/
def RunsFromDistinct.Goal : Prop := ∀ (fs gs : List Bool), fs.Perm gs → (flips fs).length = (flips gs).length

#guard (flips [true,true,false,false]).length == 1 && (flips [true,false,true,false]).length == 3
end TinylambOpt.ResearchGoals.Compression.Nullable
