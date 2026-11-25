import Std

namespace TinylambOpt.ResearchGoals.Atlas.ExecutorResources
namespace Pipeline
/-! Two serial stages on distinct resources, unlimited queue between them.
A job must finish the first stage before it starts the second. -/
abbrev Job := Nat × Nat
def step (clocks : Nat × Nat) (job : Job) :=
  let release := clocks.1+job.1
  (release,max release clocks.2+job.2)
def finish (xs : List Job) := (xs.foldl step (0,0)).2
def johnson (xs : List Job) :=
  let (front,back) := xs.partition (fun j => j.1 ≤ j.2)
  front.mergeSort (fun a b => a.1 ≤ b.1) ++ back.mergeSort (fun a b => a.2 ≥ b.2)

/-- 読取りと評価を独立資源で重ね、Johnson 順で全バッチの完了時刻を最小化する。 -/
def TwoStageOrder.Goal : Prop := ∀ xs ys, xs.Perm ys → finish (johnson xs) ≤ finish ys

/-- 二段パイプラインの完了時刻を、切断位置ごとの読取り prefix と評価 suffix の最大値へ変換する。 -/
def CriticalCut.Goal : Prop := ∀ xs, finish xs =
  (xs.zipIdx.map (fun (_,i) => ((xs.take (i+1)).map Prod.fst).sum + ((xs.drop i).map Prod.snd).sum)).max?.getD 0

/-- morsel をまたぐパイプラインの状態を三つの数で合成し再スケジュールを省く。 -/
def BatchSummary.Goal : Prop := ∀ a b, finish (a++b) =
  max (finish a + (b.map Prod.snd).sum) ((a.map Prod.fst).sum + finish b)

/-- 同じ負荷のバッチを多数流す定常運転の時間を、遅い段の時間から閉形式で得る。 -/
def SteadyState.Goal : Prop := ∀ n a b, 0 < n →
  finish (List.replicate n (a,b)) = a+b+(n-1)*max a b

/-- 一つのジョブを同じ合計費用の二つに分割すると、読み終わった一部を先に評価できる。 -/
def SplitOverlap.Goal : Prop := ∀ l r a b c d,
  finish (l++[(a,b),(c,d)]++r) ≤ finish (l++[(a+c,b+d)]++r)
end Pipeline

namespace Exchanges
abbrev Bucket := Nat × Nat -- destination × bytes
def volume (xs : List Bucket) (d : Nat) := ((xs.filter (·.1 == d)).map Prod.snd).sum
def packets (cap n : Nat) := (n+cap-1)/cap
def traffic (cap : Nat) (xs : List Bucket) :=
  ((xs.map Prod.fst).eraseDups.map (fun d => packets cap (volume xs d))).sum

/-- 行ごとの送信を宛先別に合体すると、最大 payload が一定の全送信方法の中でパケット数が最小になる。 -/
def PacketLowerBound.Goal : Prop := ∀ cap xs wire, 0 < cap →
  (∀ d, volume wire d = volume xs d) → (∀ p ∈ wire, p.2 ≤ cap) → traffic cap xs ≤ wire.length

/-- 二回のフラッシュをまとめて節約できる端数パケットを共通宛先数で抑える。 -/
def FlushDebt.Goal : Prop := ∀ cap a b, 0 < cap →
  traffic cap a + traffic cap b - traffic cap (a++b) ≤
    (a.map Prod.fst).eraseDups.countP (fun d => (b.map Prod.fst).contains d)

/-- 大きいフレームを小さいフレームの整数倍にすると送信件数の上下界を再計算せずに得る。 -/
def FrameScale.Goal : Prop := ∀ cap xs n, 0 < cap → 0 < n →
  traffic (cap*n) xs ≤ traffic cap xs ∧ traffic cap xs ≤ n*traffic (cap*n) xs

/-- 物理宛先を統合する配置で宛先別の余剰パディングを減らせる。 -/
def ReceiverCoalesce.Goal : Prop := ∀ cap xs (route : Nat → Nat), 0 < cap →
  traffic cap (xs.map (fun (d,w) => (route d,w))) ≤ traffic cap xs

/-- 強仮説。複数宛先でも総バイト数だけから必要なフレーム数を厳密に得られるか。 -/
def GlobalPacking.Goal : Prop := ∀ cap xs, 0 < cap → traffic cap xs = packets cap (xs.map Prod.snd).sum
end Exchanges

namespace Gather
def pages (cap : Nat) (ids : List Nat) := (ids.map (·/cap)).eraseDups
def extents (cap : Nat) (ids : List Nat) :=
  let sorted := (pages cap ids).mergeSort
  sorted.zipIdx.filter (fun (v,i) => i == 0 || (sorted[i-1]?.getD v)+1 != v) |>.length
def readCost (setup page : Nat) (xs : List (Nat × Nat)) := (xs.map (fun (a,b) => setup+page*(b-a))).sum
def runCost (xs : List Nat) (setup perPage : Nat) :=
  match xs with
  | [] => 0
  | x::rest => setup+perPage+((x::rest).zip rest |>.map
    (fun (a,b) => perPage+min setup (perPage*(b-a-1)))).sum
def covers (xs : List (Nat × Nat)) (p : Nat) := xs.any (fun (a,b) => a ≤ p && p < b)

/-- 飛び飛びのページを読む際は隣接ギャップごとに seek と余分な読取りを比較すると最適になる。 -/
def OptimalRead.Goal : Prop := ∀ ids cap setup price ext, 0 < cap →
  (∀ e ∈ ext, e.1 < e.2) → (∀ p ∈ pages cap ids, covers ext p = true) →
  runCost (pages cap ids |>.mergeSort) setup price ≤ readCost setup price ext

/-- 全ページを詰めて読む方法とページごとに seek する方法の両方で最適費用を上から押さえる。 -/
def CostEnvelope.Goal : Prop := ∀ ids cap setup price, 0 < cap →
  let ps := (pages cap ids).mergeSort
  runCost ps setup price ≤ min (ps.length*(setup+price))
    (setup + price*(ps.getLast?.getD 0 + 1 - ps.head?.getD 0))

/-- 連続する二つの物理ページへ遅延投影する容量を倍にすると distinct 読取りページ数は増えない。 -/
def CoarserPages.Goal : Prop := ∀ ids cap n, 0 < cap → 0 < n → (pages (cap*n) ids).length ≤ (pages cap ids).length

/-- distinct ページ数と隣接ペア数の差から連続 I/O の個数を得る。 -/
def ExtentCount.Goal : Prop := ∀ cap ids, 0 < cap →
  let ps := pages cap ids
  extents cap ids + ps.countP (fun p => ps.contains (p+1)) = ps.length

/-- 遅延 gather の順序を最適化しても、要求列への scatter で重複と範囲外 ordinal を復元する。 -/
def ScatterBack.Goal : Prop := ∀ (payload : List Int) (ids : List Nat),
  let addresses := ids.mergeSort.eraseDups
  let cache := addresses.map (fun i => payload[i]?)
  ids.map (fun i => (cache[addresses.idxOf i]?).join) = ids.map (fun i => payload[i]?)
end Gather

namespace LaneCompaction
abbrev Flags := List Bool
def slots (mask : Flags) := (mask.zipIdx.filter Prod.fst).map Prod.snd
def pack {α : Type} (mask : Flags) (vals : List α) := (mask.zip vals).filterMap (fun (ok,v) => if ok then some v else none)
def recover {α : Type} (mask : Flags) (dense : List α) :=
  mask.zipIdx.map (fun (ok,i) => if ok then dense[(mask.take i).count true]? else none)

/-- 二度の SIMD 圧縮を一つの物理レーンマスクに戻して gather を一回にする。 -/
def MaskCompose.Goal : Prop := ∀ (v : List Int) m second, m.length = v.length →
  second.length = m.count true →
  pack second (pack m v) = pack ((recover m second).map (·.getD false)) v

/-- 途中でバッチを分けた場合にも prefix popcount のオフセットだけで元 ordinal を保存する。 -/
def ChunkOffsets.Goal : Prop := ∀ a b, slots (a++b) = slots a ++ (slots b).map (a.length+·)

/-- 短い末尾を含む SIMD バッチの選択的デコードを、rank からの逆写像で元レーンへ戻す。 -/
def ExpandBack.Goal : Prop := ∀ m (v : List Int), m.length = v.length →
  recover m (pack m v) = (m.zip v).map (fun (ok,x) => if ok then some x else none)

/-- 選択済みレーンの整数 payload から、余分な eager decode が読むワード数を厳密に求める。 -/
def DecodeSavings.Goal : Prop := ∀ m (widths : List Nat), m.length = widths.length →
  widths.sum - (pack m widths).sum = (pack (m.map (!·)) widths).sum

/-- 上位 k 個の有効レーンの物理位置を得た時点で以降のデコードを切る。 -/
def EarlyDemand.Goal : Prop := ∀ m (xs : List Int) k, m.length = xs.length →
  let stop := ((slots m)[k]?).getD m.length
  (pack m xs).take k = pack (m.take stop) (xs.take stop)
end LaneCompaction

#guard Exchanges.traffic 4 [(0,1),(1,1)] == 2 && Exchanges.packets 4 2 == 1

end TinylambOpt.ResearchGoals.Atlas.ExecutorResources
