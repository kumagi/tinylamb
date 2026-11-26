import TinylambOpt.ResearchGoals.Compression.Text

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.
-/

namespace TinylambOpt.ResearchGoals.Compression.TemporalText
open Wire
open Text (Words)

/-! ClickBench's TLCS fixtures physically store the ASCII spelling
YYYY-MM-DD HH:MM:SS+00. Encode its fourteen digits bijectively, including
leading zeros. Calendar validity, time zones and SQL timestamp coercions
are deliberately unnecessary. Nonconforming blocks keep their raw format. -/
def layout : List (Option Nat) :=
  [none,none,none,none,some 45,none,none,some 45,none,none,some 32,
   none,none,some 58,none,none,some 58,none,none,some 43,some 48,some 48]
def digits : List (Option Nat) → Bytes → Option (List Nat)
  | [],[] => some []
  | none::fmt,b::bs => if 48 ≤ b && b ≤ 57 then (digits fmt bs).map ((b-48)::·) else none
  | some t::fmt,b::bs => if t == b then digits fmt bs else none
  | _,_ => none
def value (ds : List Nat) := ds.foldl (fun n d => 10*n+d) 0
def code (s : Bytes) := (digits layout s).map value
def paint : List (Option Nat) → List Nat → Bytes
  | [],_ => []
  | some t::fmt,ds => t :: paint fmt ds
  | none::fmt,d::ds => (48+d) :: paint fmt ds
  | none::_,[] => []
def spell (v : Nat) := paint layout ((List.range 14).reverse.map (fun i => v/10^i%10))
def stream (ws : Words) := (ws.mapM code).map (pack 47)
def extract (bs : Bytes) (i : Nat) := spell (takeAt bs 47 i)
def exceptions (ws : Words) := ws.zipIdx.filter (fun (w,_) => (code w).isNone)
def masked (ws : Words) := pack 47 (ws.map (fun w => (code w).getD 0))
def exceptionBody (ws : Words) := (exceptions ws).flatMap (fun (w,i) => uleb i ++ uleb w.length ++ w)

/-- 認識した綴りは桁・空白・UTC サフィックスを含めて原バイトへ戻る。 -/
def LexicalInverse.Goal : Prop := ∀ w v, code w = some v → spell v = w

/-- 先頭ゼロを含む全 14 桁のコード空間を使用でき、47 bit のうちの未使用域を特定する。 -/
def CodeDomain.Goal : Prop := ∀ v, (∃ w, code w = some v) ↔ v < 10^14

/-- 有効な二語のバイナリ照合順はパック前の整数比較で引き受けられる。 -/
def LexicalOrder.Goal : Prop := ∀ a b x y, code a = some x → code b = some y →
  (a < b ↔ x < y)

/-- 認識ブロックの任意行を隣接バイトだけから取り出し、文字列として返す。 -/
def PackedSeek.Goal : Prop := ∀ ws bs i, stream ws = some bs → i < ws.length →
  some (extract bs i) = ws[i]?

/-- 既存の 4-byte 終点と 22-byte 本体を、行ヘッダを増やさず固定幅へ変える場合の正確な節約量。 -/
def FixedWidthCharge.Goal : Prop := ∀ ws bs, stream ws = some bs →
  Text.raw ws = 26*ws.length ∧ bs.length = byteCount (47*ws.length)

/-- 現行ブロック境界が 8 行単位なら、47-bit ページ間で詰め物を運ぶ必要がない。 -/
def AlignedAppend.Goal : Prop := ∀ a b first second, a.length%8 = 0 →
  stream a = some first → stream b = some second → stream (a++b) = some (first++second)

/-- ミリ秒や別タイムゾーンなど書式が違う行だけを保存すれば混合ブロックでも可逆。 -/
def MixedSpelling.Goal : Prop := ∀ ws i, i < ws.length →
  let ex := (exceptions ws).find? (fun e => e.2 == i)
  (ex.map Prod.fst).getD (extract (masked ws) i) = ws[i]?.getD []

/-- 外れ行の位置と長さも課金して、例外を含むブロックで採用すべき条件を決める。 -/
def ExceptionCharge.Goal : Prop := ∀ ws h,
  (h+(masked ws).length+(exceptionBody ws).length < Text.raw ws ↔
   h + byteCount (47*ws.length) +
     ((exceptions ws).map (fun (w,i) => ubytes i+ubytes w.length+w.length)).sum < Text.raw ws)

/-- 日付部分が同一のブロックでは 6 桁の時分秒のみ 20 bit で保存し、共有日付と合成する。 -/
def SharedDay.Goal : Prop := ∀ (ws : Words) vs day, ws.mapM code = some vs →
  (∀ x ∈ vs, x/10^6 = day) →
  let bs := pack 20 (vs.map (· % 10^6))
  (List.range vs.length).map (fun i => spell (day*10^6 + takeAt bs 20 i)) = ws

/-- 同じ日付を格納する 3 列から日付を 1 回だけ読み、列ごとの時刻部分を展開する。 -/
def CrossColumnDate.Goal : Prop := ∀ (a b : Words) xs ys, a.mapM code = some xs → b.mapM code = some ys →
  xs.map (· / 10^6) = ys.map (· / 10^6) →
  xs.zipWith (fun x y => spell ((x/10^6)*10^6+y%10^6)) ys = b

/-- 文字列範囲フィルタの両端を整数に移しても、重複と入力順を含めて該当行が一致する。 -/
def RangeScan.Goal : Prop := ∀ lo hi l h (ws : Words) (vs : List Nat), code lo = some l → code hi = some h →
  ws.mapM code = some vs →
  ws.filter (fun w => lo ≤ w && w < hi) = (vs.filter (fun v => l ≤ v && v < h)).map spell

/-- 強仮説。UTC サフィックスを省いて保存しても異なる表記の原バイトを復元できるか。 -/
def DiscardSuffix.Goal : Prop := ∀ x (off : Bytes), x < 10^14 → off.length = 3 →
  (spell x).take 19 ++ off = spell x

#guard code (spell 20130714203847) == some 20130714203847
#guard code ((spell 1).take 19 ++ [45,48,48]) == none
end TinylambOpt.ResearchGoals.Compression.TemporalText
