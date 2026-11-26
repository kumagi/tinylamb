import TinylambOpt.ResearchGoals.Compression.Wire

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.
-/

namespace TinylambOpt.ResearchGoals.Compression.Predictors
open Wire

/-! History is newest first. A reference column is already persisted and is
addressed at the same physical ordinal. Param values, predictor tags, and
restart seeds require space; extra below charges any new external metadata. -/
abbrev Predict := Nat → List Int → (Nat → Int) → Int
def lag (h : List Int) (i : Nat) := h[i]?.getD 0
def encode (p : Predict) (side : Nat → Int) : Nat → List Int → List Int → List Nat
  | _,_,[] => []
  | i,h,x::xs => zig (x-p i h side) :: encode p side (i+1) (x::h) xs
def restore (p : Predict) (side : Nat → Int) : Nat → List Int → List Nat → List Int
  | _,_,[] => []
  | i,h,e::es =>
    let x := p i h side + unzig e
    x :: restore p side (i+1) (x::h) es
def frame (es : List Nat) : Bytes :=
  let w := width (peak es)
  [0] ++ uleb es.length ++ uleb w ++ pack w es
def quote (n w : Nat) := 1 + ubytes n + ubytes w + byteCount (n*w)
def certificate (p : Predict) (side : Nat → Int) (i : Nat) (h xs : List Int) (w : Nat) :=
  ∀ j < xs.length, zig ((xs[j]?.getD 0)-p (i+j) ((xs.take j).reverse++h) side) < 2^w

namespace Previous

/-- 差分値。 -/
def predict : Predict := fun _ h _ => lag h 0

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ side i h xs, restore predict side i h (encode predict side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ side i h xs w extra oldBytes, certificate predict side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode predict side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ side i h xs, encode predict side i (h.take (1)) xs = encode predict side i h xs

/-- 同値ランの一行目以降の残差はゼロにできる。 -/
def FlatRun.Goal : Prop :=
  ∀ v n side i, encode predict side i [v] (List.replicate n v) = List.replicate n 0

end Previous

namespace Linear

/-- 二階差分。 -/
def predict : Predict := fun _ h _ => 2*lag h 0-lag h 1

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ side i h xs, restore predict side i h (encode predict side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ side i h xs w extra oldBytes, certificate predict side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode predict side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ side i h xs, encode predict side i (h.take (2)) xs = encode predict side i h xs

/-- 等間隔イベントの全時刻を二つのアンカーだけから再生する。 -/
def ConstantStep.Goal : Prop :=
  ∀ (a d : Int) n side i, restore predict side i [a-d,a-2*d] (List.replicate n 0) =
    (List.range n).map (fun (j : Nat) => a+d*j)

end Linear

namespace Quadratic

/-- 三階差分。 -/
def predict : Predict := fun _ h _ => 3*lag h 0-3*lag h 1+lag h 2

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ side i h xs, restore predict side i h (encode predict side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ side i h xs w extra oldBytes, certificate predict side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode predict side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ side i h xs, encode predict side i (h.take (3)) xs = encode predict side i h xs

/-- 加速度一定の系列は三初期値以降の payload が不要となる。 -/
def PolynomialRun.Goal : Prop :=
  ∀ (a b c : Int) n side i, let q := fun (j : Int) => a*j^2+b*j+c
    restore predict side i [q (-1),q (-2),q (-3)] (List.replicate n 0) =
      (List.range n).map (fun (j : Nat) => q j)

end Quadratic

namespace Seasonal

/-- 固定周期の季節差分。 -/
def predict (period : Nat) : Predict := fun _ h _ => lag h period

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ period side i h xs, restore (predict period) side i h (encode (predict period) side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ period side i h xs w extra oldBytes, certificate (predict period) side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode (predict period) side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ period side i h xs, encode (predict period) side i (h.take (period+1)) xs = encode (predict period) side i h xs

/-- 同じ辞書ページの再登場を、前の周期の各位置から予測する。 -/
def RepeatCycle.Goal : Prop :=
  ∀ (block : List Int) n side i, block ≠ [] →
    encode (predict (block.length-1)) side i block.reverse (List.replicate n block |>.flatten) =
    List.replicate (n*block.length) 0

end Seasonal

namespace TrendSeason

/-- トレンド付き季節差分。 -/
def predict (period : Nat) : Predict := fun _ h _ => lag h 0 + lag h period - lag h (period+1)

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ period side i h xs, restore (predict period) side i h (encode (predict period) side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ period side i h xs w extra oldBytes, certificate (predict period) side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode (predict period) side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ period side i h xs, encode (predict period) side i (h.take (period+2)) xs = encode (predict period) side i h xs

/-- 周期ごとに一定量ドリフトする系列の平行移動で符号列が変わらない。 -/
def TranslatedCycle.Goal : Prop :=
  ∀ period side i h xs delta, period+2 ≤ h.length →
    encode (predict period) side i (h.map (·+delta)) (xs.map (·+delta)) = encode (predict period) side i h xs

end TrendSeason

namespace Median

/-- 外れ値に強い三点中央値。 -/
def predict : Predict := fun _ h _ => max (min (lag h 0) (lag h 1)) (min (max (lag h 0) (lag h 1)) (lag h 2))

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ side i h xs, restore predict side i h (encode predict side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ side i h xs w extra oldBytes, certificate predict side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode predict side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ side i h xs, encode predict side i (h.take (3)) xs = encode predict side i h xs

/-- 直近三点のうち二点が定常値へ戻れば次の定常行の残差もゼロになる。 -/
def OutlierRecovery.Goal : Prop :=
  ∀ a b side i, predict i [a,b,a] side = a ∧ predict i [b,a,a] side = a

end Median

namespace ClampLinear

/-- 直近レンジでクランプする外挿。 -/
def predict : Predict := fun _ h _ => max (min (lag h 1) (lag h 2)) (min (max (lag h 1) (lag h 2)) (2*lag h 0-lag h 1))

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ side i h xs, restore predict side i h (encode predict side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ side i h xs w extra oldBytes, certificate predict side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode predict side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ side i h xs, encode predict side i (h.take (3)) xs = encode predict side i h xs

/-- 履歴と新しい値が同一区間なら外れた線形外挿にもかかわらず必要な残差幅を抑える。 -/
def BoundedResidual.Goal : Prop :=
  ∀ h side i x lo hi, lo ≤ x → x ≤ hi → lo ≤ lag h 1 → lag h 1 ≤ hi → lo ≤ lag h 2 → lag h 2 ≤ hi →
    (x-predict i h side).natAbs ≤ (hi-lo).toNat

end ClampLinear

namespace Mirror

/-- 交互符号の値。 -/
def predict : Predict := fun _ h _ => -lag h 0

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ side i h xs, restore predict side i h (encode predict side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ side i h xs w extra oldBytes, certificate predict side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode predict side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ side i h xs, encode predict side i (h.take (1)) xs = encode predict side i h xs

/-- 符号が交互に変わる精算額を最初の値だけで表す。 -/
def AlternatingRun.Goal : Prop :=
  ∀ v n side i, restore predict side i [-v] (List.replicate n 0) =
    (List.range n).map (fun j => (-1:Int)^j*v)

end Mirror

namespace Ordinal

/-- 物理 ordinal に比例する列。 -/
def predict (base slope : Int) : Predict := fun i _ _ => base + slope*(i:Int)

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ base slope side i h xs, restore (predict base slope) side i h (encode (predict base slope) side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ base slope side i h xs w extra oldBytes, certificate (predict base slope) side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode (predict base slope) side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ base slope side i h xs, encode (predict base slope) side i (h.take (0)) xs = encode (predict base slope) side i h xs

/-- 独立ページをどこで分割しても先頭のための例外値を払わずに済む。 -/
def AbsoluteSeek.Goal : Prop :=
  ∀ base slope side i n, encode (predict base slope) side i []
    ((List.range n).map (fun (j : Nat) => base+slope*(i+j:Nat))) = List.replicate n 0

end Ordinal

namespace Bucket

/-- 段階的な epoch 値。 -/
def predict (base step : Int) (period : Nat) : Predict := fun i _ _ => base + step*(i/(period+1):Nat)

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ base step period side i h xs, restore (predict base step period) side i h (encode (predict base step period) side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ base step period side i h xs w extra oldBytes, certificate (predict base step period) side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode (predict base step period) side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ base step period side i h xs, encode (predict base step period) side i (h.take (0)) xs = encode (predict base step period) side i h xs

/-- ページが epoch 境界をまたいでも残差を保存せずランを取り出す。 -/
def RunCrossing.Goal : Prop :=
  ∀ base step period side i n, restore (predict base step period) side i [] (List.replicate n 0) =
    (List.range n).map (fun j => base+step*((i+j)/(period+1):Nat))

end Bucket

namespace ModuloCounter

/-- 周期的に wrap する通番。 -/
def predict (step : Int) (period : Nat) : Predict := fun _ h _ => (lag h 0 + step) % (period+1:Nat)

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ step period side i h xs, restore (predict step period) side i h (encode (predict step period) side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ step period side i h xs w extra oldBytes, certificate (predict step period) side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode (predict step period) side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ step period side i h xs, encode (predict step period) side i (h.take (1)) xs = encode (predict step period) side i h xs

/-- wrap 直後の大きな負差分を保持せず、通算値の剰余から時系列を再生する。 -/
def WrappedProgression.Goal : Prop :=
  ∀ step period a side i n, restore (predict step period) side i [a-step] (List.replicate n 0) =
    (List.range n).map (fun (j : Nat) => (a+step*j)%(period+1:Nat))

end ModuloCounter

namespace SideAffine

/-- 共有キーからのアフィン予測。 -/
def predict (base slope : Int) : Predict := fun i _ side => base+slope*side i

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ base slope side i h xs, restore (predict base slope) side i h (encode (predict base slope) side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ base slope side i h xs w extra oldBytes, certificate (predict base slope) side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode (predict base slope) side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ base slope side i h xs, encode (predict base slope) side i (h.take (0)) xs = encode (predict base slope) side i h xs

/-- 単位変換された同じ列を参照し、別列の値ストリームを省く。 -/
def ProportionalColumns.Goal : Prop :=
  ∀ base slope side i n, encode (predict base slope) side i []
    ((List.range n).map (fun j => base+slope*side (i+j))) = List.replicate n 0

end SideAffine

namespace SideDelta

/-- 二列に共通する時間差。 -/
def predict : Predict := fun i h side => lag h 0+side i-side (i-1)

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ side i h xs, restore predict side i h (encode predict side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ side i h xs w extra oldBytes, certificate predict side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode predict side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ side i h xs, encode predict side i (h.take (1)) xs = encode predict side i h xs

/-- 固定オフセットのクライアント時計は最初のオフセットを種にして値域に関係なく表せる。 -/
def ClockOffset.Goal : Prop :=
  ∀ offset side i n, 0 < i → encode predict side i [side (i-1)+offset]
    ((List.range n).map (fun j => side (i+j)+offset)) = List.replicate n 0

end SideDelta

namespace SideQuotient

/-- 共有時刻の粒度変換。 -/
def predict (period : Nat) : Predict := fun i _ side => side i / (period+1:Nat)

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ period side i h xs, restore (predict period) side i h (encode (predict period) side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ period side i h xs w extra oldBytes, certificate (predict period) side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode (predict period) side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ period side i h xs, encode (predict period) side i (h.take (0)) xs = encode (predict period) side i h xs

/-- 負の時刻を含む日付の丸め結果を元列から再生する。 -/
def DateProjection.Goal : Prop :=
  ∀ period side i n, restore (predict period) side i [] (List.replicate n 0) =
    (List.range n).map (fun j => side (i+j)/(period+1:Nat))

end SideQuotient

namespace TwoPhase

/-- 奇数・偶数で異なる定額ドリフト。 -/
def predict (evenStep oddStep : Int) : Predict := fun i h _ => lag h 1 + if i%2 == 0 then evenStep else oddStep

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ evenStep oddStep side i h xs, restore (predict evenStep oddStep) side i h (encode (predict evenStep oddStep) side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ evenStep oddStep side i h xs w extra oldBytes, certificate (predict evenStep oddStep) side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode (predict evenStep oddStep) side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ evenStep oddStep side i h xs, encode (predict evenStep oddStep) side i (h.take (2)) xs = encode (predict evenStep oddStep) side i h xs

/-- 二つの計器の交互サンプルを、それぞれの一階差分として復元する。 -/
def TwoLanes.Goal : Prop :=
  ∀ a b d e side n, restore (predict d e) side 0 [b-e,a-d] (List.replicate n 0) =
    (List.range n).map (fun j => if j%2 == 0 then a+d*(j/2:Nat) else b+e*(j/2:Nat))

end TwoPhase

namespace SideResidue

/-- 参照列の値域に対応する周期成分。 -/
def predict (base : Int) (period : Nat) : Predict := fun i _ side => base+side i%(period+1:Nat)

/-- 予測を外した行も残差を使って順序どおり復元する。 -/
def ResidualRoundTrip.Goal : Prop :=
  ∀ base period side i h xs, restore (predict base period) side i h (encode (predict base period) side i h xs) = xs

/-- 残差幅の証明書とヘッダ・追加情報の実バイトを使い、旧ストリームを下回る条件を決める。 -/
def ByteBudget.Goal : Prop :=
  ∀ base period side i h xs w extra oldBytes, certificate (predict base period) side i h xs w →
    extra + quote xs.length w < oldBytes → extra + (frame (encode (predict base period) side i h xs)).length < oldBytes

/-- 参照履歴を必要な長さだけ保存しても途中のページから同じ符号列で再開できる。 -/
def RestartSeed.Goal : Prop :=
  ∀ base period side i h xs, encode (predict base period) side i (h.take (0)) xs = encode (predict base period) side i h xs

/-- 参照列を周期の整数倍だけ動かしても共有残差を再利用できる。 -/
def PeriodShift.Goal : Prop :=
  ∀ base period side i h xs (k : Int), encode (predict base period) (fun j => side j + k*(period+1:Nat)) i h xs =
    encode (predict base period) side i h xs

end SideResidue

end TinylambOpt.ResearchGoals.Compression.Predictors
