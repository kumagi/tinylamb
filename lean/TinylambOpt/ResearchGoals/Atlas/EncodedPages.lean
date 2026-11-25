import TinylambOpt.ResearchGoals.ExecutionStrategies.PairAggregates

namespace TinylambOpt.ResearchGoals.Atlas.EncodedPages

/-! A page maps physical ordinals to optional integer values. Missing ordinals
are SQL NULL for these aggregates. A lane stores two arithmetic progressions,
one for ordinals and one for values, without expanding repeated rows. -/
structure Config where
  n : Nat
  split : Nat
  parameter : Nat
  base : Int
  step : Int
  bias : Int
  dict : List Int
def width (c : Config) := c.parameter+1
structure Lane where
  start : Nat
  stride : Nat
  n : Nat
  base : Int
  step : Int
def lane (limit start stride : Nat) (base step : Int) : Lane :=
  ⟨start,stride,(limit-start+stride-1)/stride,base,step⟩
def span (c : Config) (l h : Nat) (z d : Int) := lane (min c.n h) l 1 z d
def striped (c : Config) (period : Nat) (make : Nat → Int × Int) :=
  (List.range (min period c.n)).map (fun r =>
    let (z,d) := make r
    lane c.n r period z d)
structure Layout where
  read : Config → Nat → Option Int
  lanes : Config → List Lane
def rows (layout : Layout) (c : Config) :=
  (List.range c.n).filterMap (fun i => (layout.read c i).map (i,·))
def tri (n : Nat) : Int := TinylambOpt.ResearchGoals.PairAggregates.choose2 n
def squares (n : Nat) : Int := (n*(n-1)*(2*n-1)/6:Nat)
def partialSum (a : Lane) (n : Nat) := (n:Int)*a.base + tri n*a.step
def beforePos (a : Lane) (p : Nat) := min a.n ((p-a.start+a.stride-1)/a.stride)
def beforeVal (a : Lane) (cut : Int) : Nat :=
  if a.step == 0 then if a.base < cut then a.n else 0
  else if 0 < a.step then min a.n ((cut-a.base+a.step-1)/a.step).toNat
  else a.n - min a.n (((a.base-cut)/(-a.step))+1).toNat
def band (a : Lane) (lo hi : Int) := beforeVal a hi - beforeVal a lo

structure Progression where
  lo : Int
  step : Nat
  n : Nat
def positions (a : Lane) : Progression := ⟨a.start,a.stride,a.n⟩
def values (a : Lane) : Progression :=
  ⟨if a.step < 0 then a.base + ((a.n-1:Nat):Int)*a.step else a.base, a.step.natAbs, a.n⟩
def lastVal (a : Progression) := a.lo + ((a.n-1:Nat):Int)*a.step
def contains (a : Progression) (v : Int) :=
  0 < a.n && a.lo ≤ v && v ≤ lastVal a && (a.step == 0 || (v-a.lo)%(a.step:Int) == 0)

/-- Extended Euclid supplies the inverse after dividing out the common factor. -/
def bezout (a b : Nat) : Int × Int :=
  if _h : b = 0 then (1,0) else
    let (x,y) := bezout b (a%b)
    (y,x-(a/b:Int)*y)
termination_by b
decreasing_by exact Nat.mod_lt _ (Nat.pos_of_ne_zero _h)
def intersection (a b : Progression) : Progression := Id.run do
  if a.n == 0 || b.n == 0 then return ⟨0,1,0⟩
  if a.step == 0 then return ⟨a.lo,1,if contains b a.lo then 1 else 0⟩
  if b.step == 0 then return ⟨b.lo,1,if contains a b.lo then 1 else 0⟩
  let g := a.step.gcd b.step
  let gap := b.lo-a.lo
  if gap%(g:Int) != 0 then return ⟨0,1,0⟩
  let period := (a.step/g)*b.step
  let unit := (bezout (a.step/g) (b.step/g)).1
  let root := a.lo+(a.step:Int)*((gap/(g:Int)*unit)%(b.step/g:Nat))
  let low := max a.lo b.lo
  let first := low+(root-low)%(period:Int)
  let high := min (lastVal a) (lastVal b)
  return ⟨first,period,if first ≤ high then ((high-first)/(period:Int)).toNat+1 else 0⟩
def joinCount (a b : Lane) :=
  (intersection (values a) (values b)).n *
    (if a.step == 0 then a.n else 1) * (if b.step == 0 then b.n else 1)
def firstAt (a : Lane) (r : Progression) :=
  a.base + ((r.lo-(a.start:Int))/(a.stride:Int))*a.step
def slopeAt (a : Lane) (r : Progression) := ((r.step/a.stride:Nat):Int)*a.step
def deletedSum (a : Lane) (mask : Progression) :=
  let overlap := intersection (positions a) mask
  (overlap.n:Int)*firstAt a overlap + tri overlap.n*slopeAt a overlap
def aligned (a b : Lane) :=
  let r := intersection (positions a) (positions b)
  let x := firstAt a r
  let y := firstAt b r
  let dx := slopeAt a r
  let dy := slopeAt b r
  (r.n:Int)*x*y + tri r.n*(x*dy+y*dx) + squares r.n*dx*dy

def distinctValues (layout : Layout) (c : Config) := ((rows layout c).map Prod.snd).eraseDups
def occurrence (layout : Layout) (c : Config) (v : Int) := (rows layout c).countP (fun r => r.2 == v)

namespace LinearOrdinal

/-- 行番号を frame-of-reference の差分で復元する列。 -/
def layout : Layout where
  read c i := some (c.base+c.step*i)
  lanes c := [lane c.n 0 1 c.base c.step]

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 単調で符号付きの等差列では割り切れる値だけを ordinal に逆写像して点照会する。 -/
def InverseLookup.Goal : Prop :=
  ∀ c v, c.step ≠ 0 →
    ((rows layout c).filter (fun r => r.2 == v)).map Prod.fst =
      let i := (v-c.base)/c.step
      if (v-c.base)%c.step == 0 && 0 ≤ i && i < c.n then [i.toNat] else []

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end LinearOrdinal

namespace SaturatingCounter

/-- 上限に達すると値が一定になる通算カウンタ。 -/
def layout : Layout where
  read c i := if i < c.split then some (c.base+c.step*i) else some (c.base+c.step*c.split)
  lanes c := [span c 0 c.split c.base c.step, span c c.split c.n (c.base+c.step*c.split) 0]

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 飽和点に等しい値の件数をフッタから求め、その部分の GROUP BY を省く。 -/
def PlateauFrequency.Goal : Prop :=
  ∀ c, c.step ≠ 0 → occurrence layout c (c.base+c.step*c.split) = c.n-c.split

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end SaturatingCounter

namespace SegmentedSlope

/-- 途中で増分が変わる訂正済み価格列。 -/
def layout : Layout where
  read c i := if i < c.split then some (c.base+c.step*i) else some (c.base+c.step*c.split+c.bias*(i-c.split:Nat))
  lanes c := [span c 0 c.split c.base c.step, span c c.split c.n (c.base+c.step*c.split) c.bias]

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 区間の傾きが途中で変わっても最大値の候補を四つの物理境界に絞る。 -/
def TurnExtrema.Goal : Prop :=
  ∀ c, ((rows layout c).map Prod.snd).max? =
    (([0,c.split-1,c.split,c.n-1].filter (· < c.n)).filterMap (layout.read c)).max?

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end SegmentedSlope

namespace MirroredTail

/-- 境界以降は逆順で格納された通番。 -/
def layout : Layout where
  read c i := if i < c.split then some (c.base+c.step*i) else some (c.base+c.step*((c.n-1-i:Nat):Int))
  lanes c := [span c 0 c.split c.base c.step, span c c.split c.n (c.base+c.step*((c.n-1-c.split:Nat):Int)) (-c.step)]

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 反転区間と前方区間の値集合は同じ先頭を持つため、DISTINCT 件数を長い方の長さで得る。 -/
def OverlapDistinct.Goal : Prop :=
  ∀ c, c.step ≠ 0 → (distinctValues layout c).length = max (min c.n c.split) (c.n-c.split)

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end MirroredTail

namespace BucketRuns

/-- 同一バケットの行には同じ epoch を返す。 -/
def layout : Layout where
  read c i := some (c.base+c.step*(i/width c:Nat))
  lanes c := ((List.range ((c.n+width c-1)/width c)).map (fun j =>
    span c (j*width c) ((j+1)*width c) (c.base+c.step*j) 0))

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 最終の不完全バケットを考慮し、辞書値ごとの GROUP BY 件数を割算なしで生成する。 -/
def PartialBucket.Goal : Prop :=
  ∀ c (j : Nat), c.step ≠ 0 → occurrence layout c (c.base+c.step*j) = min (width c) (c.n-j*width c)

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end BucketRuns

namespace WrappingSequence

/-- 周期境界で値が巻き戻るローカル ID。 -/
def layout : Layout where
  read c i := some (c.base+c.step*(i%width c:Nat))
  lanes c := striped c (width c) (fun r => (c.base+c.step*r,0))

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 循環する ID の各頻度を完全周期数と残りの先頭部分から得る。 -/
def QuotientHistogram.Goal : Prop :=
  ∀ c (r : Nat), c.step ≠ 0 → r < width c → occurrence layout c (c.base+c.step*r) =
    c.n/width c + if r < c.n%width c then 1 else 0

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end WrappingSequence

namespace DictionaryCycle

/-- 可変長辞書が繰り返されるカテゴリ列。空辞書は全 NULL。 -/
def layout : Layout where
  read c i := c.dict[i%c.dict.length]?
  lanes c := (List.range (min c.dict.length c.n)).map (fun r =>
    lane c.n r c.dict.length (c.dict[r]?.getD 0) 0)

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 辞書自体が重複を含んでも、完全周期と末尾の辞書 prefix の頻度だけで GROUP BY する。 -/
def DuplicateCodes.Goal : Prop :=
  ∀ c v, c.dict ≠ [] → occurrence layout c v =
    (c.n/c.dict.length)*c.dict.count v + (c.dict.take (c.n%c.dict.length)).count v

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end DictionaryCycle

namespace DictionaryDrift

/-- epoch ごとに辞書全体へオフセットを加える価格コード。 -/
def layout : Layout where
  read c i := c.dict[i%c.dict.length]?.map (fun v => v+c.step*(i/c.dict.length:Nat))
  lanes c := (List.range (min c.dict.length c.n)).map (fun r =>
    lane c.n r c.dict.length (c.dict[r]?.getD 0) c.step)

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 値の半開区間で COUNT を計算し、降順・定数レーンも展開せず扱う。 -/
def PredicateRank.Goal : Prop :=
  ∀ c lo hi, ((rows layout c).countP (fun r => lo ≤ r.2 && r.2 < hi)) =
    ((layout.lanes c).map (fun a => band a lo hi)).sum

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end DictionaryDrift

namespace AlternatingSign

/-- 交互に符号を変える精算金額。 -/
def layout : Layout where
  read c i := some ((if i%2 == 0 then 1 else (-1:Int))*(c.base+c.step*i))
  lanes c := [lane c.n 0 2 c.base (2*c.step), lane c.n 1 2 (-(c.base+c.step)) (-2*c.step)]

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 交互符号の連続勘定を二行ずつ相殺し、余った末尾だけを加える。 -/
def CancelPairs.Goal : Prop :=
  ∀ c, ((rows layout c).map Prod.snd).sum = -c.step*((c.n/2:Nat):Int) +
    if c.n%2 == 1 then c.base+c.step*(c.n-1:Nat) else 0

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end AlternatingSign

namespace TriangleWave

/-- 上昇と下降が周期的に切り替わるセンサー列。 -/
def layout : Layout where
  read c i := some (let w := width c; let r := i%(2*w); c.base+c.step*(min r (2*w-r):Nat))
  lanes c := striped c (2*width c) (fun r => (c.base+c.step*(min r (2*width c-r):Nat),0))

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 三角波で最大振幅を取る行だけを位相の合同式で直接列挙する。 -/
def PeakPositions.Goal : Prop :=
  ∀ c, c.step ≠ 0 → (((rows layout c).filter (fun r => r.2 == c.base+c.step*width c)).map Prod.fst) =
    (List.range c.n).filter (fun i => i%(2*width c) == width c)

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end TriangleWave

namespace LaneMajor

/-- 同じ物理ストライプに同じチャネル基点を用いる列。 -/
def layout : Layout where
  read c i := some (c.base+c.step*(i/width c:Nat)+c.bias*(i%width c:Nat))
  lanes c := striped c (width c) (fun r => (c.base+c.bias*r,c.step))

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- チャネル基点が epoch の値域より広く離れるなら行番号ごとの値が一意になり、DISTINCT を省ける。 -/
def FrequencySeparation.Goal : Prop :=
  ∀ c, c.step ≠ 0 → c.step.natAbs*(c.n/width c) < c.bias.natAbs →
    (distinctValues layout c).length = c.n

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end LaneMajor

namespace Boustrophedon

/-- 二次元タイルを一行おきに反転する蛇行順の列。 -/
def layout : Layout where
  read c i := some (let w := width c; let k := i/w; let r := i%w; c.base+c.step*(k*w + (if k%2 == 0 then r else w-1-r):Nat))
  lanes c := striped c (2*width c) (fun r => let w := width c; (c.base+c.step*(if r < w then r else 2*w-1-(r-w):Nat),c.step*(2*w)))

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 完全な蛇行タイルでは行順に依存しない集約に連続 ordinal の経路を使う。 -/
def FullTiles.Goal : Prop :=
  ∀ c, c.n%width c = 0 → ((rows layout c).map Prod.snd).Perm ((rows LinearOrdinal.layout c).map Prod.snd)

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end Boustrophedon

namespace RaggedStripe

/-- 各ストライプの最終スロットだけが欠損する。 -/
def layout : Layout where
  read c i := if i%width c == width c-1 then none else some (c.base+c.step*i)
  lanes c := (List.range (min c.n (width c-1))).map (fun r =>
    lane c.n r (width c) (c.base+c.step*r) (c.step*width c))

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- ストライプ末尾にだけ出る NULL を読み飛ばし、COUNT 有効行数をヘッダから返す。 -/
def NullCount.Goal : Prop :=
  ∀ c, (rows layout c).length = c.n - c.n/width c

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end RaggedStripe

namespace PeriodicPatch

/-- 周期先頭だけ別の訂正値を持つ連続カウンタ。 -/
def layout : Layout where
  read c i := some (c.base+c.step*i+if i%width c == 0 then c.bias else 0)
  lanes c := striped c (width c) (fun r => (c.base+c.step*r+(if r == 0 then c.bias else 0),c.step*width c))

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 訂正の入る物理行数だけを定額で重み付けし、基底列の SUM に加える。 -/
def PatchBudget.Goal : Prop :=
  ∀ c, ((rows layout c).map Prod.snd).sum = ((rows LinearOrdinal.layout c).map Prod.snd).sum +
    c.bias*((c.n+width c-1)/width c:Nat)

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end PeriodicPatch

namespace SparseSample

/-- 指定された剰余類だけに実値が入る列。 -/
def layout : Layout where
  read c i := if i%width c == c.split%width c then some (c.base+c.step*(i/width c:Nat)) else none
  lanes c := [lane c.n (c.split%width c) (width c) c.base c.step]

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 値の半開区間で COUNT を計算し、降順・定数レーンも展開せず扱う。 -/
def PredicateRank.Goal : Prop :=
  ∀ c lo hi, ((rows layout c).countP (fun r => lo ≤ r.2 && r.2 < hi)) =
    ((layout.lanes c).map (fun a => band a lo hi)).sum

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end SparseSample

namespace DeletedSpan

/-- 連続した物理削除区間を飛ばす元 ordinal 列。 -/
def layout : Layout where
  read c i := if c.split ≤ i && i < c.split+width c then none else some (c.base+c.step*i)
  lanes c := [span c 0 c.split c.base c.step,
    span c (c.split+width c) c.n (c.base+c.step*(c.split+width c)) c.step]

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 削除区間以降の生存行番号に穴の長さだけを足して物理 ordinal に戻す。 -/
def SeekSurvivor.Goal : Prop :=
  ∀ c (k : Nat), k < (rows layout c).length → ((rows layout c)[k]?).map Prod.fst =
    some (if k < c.split then k else k+width c)

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end DeletedSpan

namespace RotatedPage

/-- リングバッファの物理先頭が論理順の途中にある。 -/
def layout : Layout where
  read c i := some (c.base+c.step*((i+c.split%c.n)%c.n:Nat))
  lanes c := let s := c.split%c.n
    [span c 0 (c.n-s) (c.base+c.step*s) c.step, span c (c.n-s) c.n c.base c.step]

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- リングの先頭位置に依存しない袋集約を、回転前のページから返す。 -/
def PermutationAggregate.Goal : Prop :=
  ∀ c, ((rows layout c).map Prod.snd).Perm ((rows LinearOrdinal.layout c).map Prod.snd)

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end RotatedPage

namespace GrayTile

/-- 下位レーン番号を Gray コードにしたタイル。 -/
def layout : Layout where
  read c i := some (let k := i/width c; let r := i%width c; c.base+c.step*(k*width c + (r ^^^ (r/2)):Nat))
  lanes c := striped c (width c) (fun r => (c.base+c.step*(r ^^^ (r/2):Nat),c.step*width c))

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 幅が二の冪で完全なタイルでは Gray 符号による再配置をソートや集約の前に除く。 -/
def PowerOfTwo.Goal : Prop :=
  ∀ c (d : Nat), width c = 2^d → c.n%width c = 0 →
    ((rows layout c).map Prod.snd).Perm ((rows LinearOrdinal.layout c).map Prod.snd)

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end GrayTile

namespace RestartedDelta

/-- 区間切替時に独立な基点へリスタートする差分列。 -/
def layout : Layout where
  read c i := if i < c.split then some (c.base+c.step*i) else some (c.bias+c.step*(i-c.split:Nat))
  lanes c := [span c 0 c.split c.base c.step, span c c.split c.n c.bias c.step]

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 値の半開区間で COUNT を計算し、降順・定数レーンも展開せず扱う。 -/
def PredicateRank.Goal : Prop :=
  ∀ c lo hi, ((rows layout c).countP (fun r => lo ≤ r.2 && r.2 < hi)) =
    ((layout.lanes c).map (fun a => band a lo hi)).sum

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end RestartedDelta

namespace PagedTombstones

/-- 偶数番の全タイルだけが残る時系列。 -/
def layout : Layout where
  read c i := if (i/width c)%2 == 0 then some (c.base+c.step*i) else none
  lanes c := (List.range (min c.n (width c))).map (fun r =>
    lane c.n r (2*width c) (c.base+c.step*r) (c.step*(2*width c)))

/-- 任意の物理区間の有効件数と SUM をレーンの両端から読む。 -/
def SliceState.Goal : Prop :=
  ∀ c lo hi, lo ≤ hi →
    let selected := (rows layout c).filter (fun r => lo ≤ r.1 && r.1 < hi)
    let ls := layout.lanes c
    (selected.length,(selected.map Prod.snd).sum) =
      ((ls.map (fun a => beforePos a hi - beforePos a lo)).sum,
       (ls.map (fun a => partialSum a (beforePos a hi)-partialSum a (beforePos a lo))).sum)

/-- 削除済みタイルが交互に現れるファイルの生存行数を完全周期と残部から得る。 -/
def SkipEpochs.Goal : Prop :=
  ∀ c, (rows layout c).length = (c.n/(2*width c))*width c + min (width c) (c.n%(2*width c))

/-- 異なるレーン間の等値結合件数を合同式で求め、定数の多重度を復元する。 -/
def ResidueJoin.Goal : Prop :=
  ∀ c (b : Lane),
    ((rows layout c).map (fun r => (List.range b.n).countP
      (fun (i : Nat) => r.2 == b.base+b.step*(i:Int)))).sum = ((layout.lanes c).map (joinCount · b)).sum

/-- 周期的な位置削除に当たる値の総和だけを合同式から引く。 -/
def DeleteProgression.Goal : Prop :=
  ∀ c mask, (((rows layout c).filter (fun r => !contains mask r.1)).map Prod.snd).sum =
    ((layout.lanes c).map (fun a => partialSum a a.n - deletedSum a mask)).sum

/-- 遅延読取りの二列を ordinal で突き合わせ、積和を零・一・二次モーメントに畳む。 -/
def AlignedProduct.Goal : Prop :=
  ∀ c (b : Lane), 0 < b.stride →
    ((rows layout c).map (fun r => ((List.range b.n).map (fun i =>
      if r.1 == b.start+i*b.stride then r.2*(b.base+b.step*(i:Int)) else 0)).sum)).sum =
    ((layout.lanes c).map (aligned · b)).sum

end PagedTombstones


#guard let a : Lane := ⟨1,3,5,8,-2⟩
       let b : Lane := ⟨4,6,2,6,0⟩
       joinCount a b == 2 && aligned a b == 48 && deletedSum a (positions b) == 8
#guard (intersection ⟨-9,4,5⟩ ⟨-7,6,4⟩).n == 1

end TinylambOpt.ResearchGoals.Atlas.EncodedPages
