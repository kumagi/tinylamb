import TinylambOpt.ResearchGoals.BenchmarkRematch.Common
import TinylambOpt.ResearchGoals.Compression.Wire

namespace TinylambOpt.ResearchGoals.BenchmarkRematch.Storage
open TinylambOpt.ResearchGoals.Compression.Wire

/-- TPC-H は lineitem の shipdate を基準に commitdate・receiptdate の符号付き差を連結する。計測は例外と日付ヘッダ込みの総バイト。 -/
def TpchDuckDB.Goal : Prop := ∀ (ship commit receipt : Int) (tail : Bytes),
  let wire := uleb (zig (commit-ship)) ++ uleb (zig (receipt-ship)) ++ tail
  (do let (a,rest) ← readU wire
      let (b,suffix) ← readU rest
      pure (ship+unzig a,ship+unzig b,suffix)) = some (commit,receipt,tail)

/-- TPC-H は lineitem の同一注文の run を終了位置で索引し、orderkey の反復を除く。計測は run 長と探索索引を含む列バイト。 -/
def TpchClickHouse.Goal : Prop := ∀ (runs : List (Nat × Nat)) (i : Nat),
  let ends := (runs.map Prod.snd).scanl (·+·) 0 |>.drop 1
  ((ends.zip (runs.map Prod.fst)).find? (fun r => i < r.1)).map Prod.snd =
    (runs.flatMap (fun (key,n) => List.replicate n key))[i]?

/-- TPC-DS は inventory の規則的な商品×倉庫の並びを暗黙化し、飛びと順序の乱れを例外表にする。計測は例外 RID とブロック起点を含むキー列バイト。 -/
def TpcdsDuckDB.Goal : Prop := ∀ (rows : List (Nat × Nat)) start warehouses, 0 < warehouses →
  let predicted := fun i => ((start+i)/warehouses,(start+i)%warehouses)
  let exceptions := rows.zipIdx.filter (fun (v,i) => v != predicted i)
  (List.range rows.length).map (fun i =>
    ((exceptions.find? (fun e => e.2 == i)).map Prod.fst).getD (predicted i)) = rows

/-- TPC-DS は売上・返品の nullable キーを有効ビットと密な残差に分離し rank で点参照する。計測は rank 索引・基準値込みのキー列バイト。 -/
def TpcdsClickHouse.Goal : Prop := ∀ (rows : List (Option Nat)) (base i w : Nat),
  (∀ v ∈ rows.filterMap id, base ≤ v ∧ v-base < 2^w) →
  let mask := rows.map Option.isSome
  let stream := pack w ((rows.filterMap id).map (·-base))
  (if mask[i]? == some true then some (base + takeAt stream w ((mask.take i).count true)) else none) =
    rows[i]?.join

/-- ClickBench は URL の終点を間引き、区画起点と格納長から本文だけを点参照する。計測は長さ列・起点列と圧縮本文の総バイト。 -/
def ClickDuckDB.Goal : Prop := ∀ (rows : List Bytes) stride i, 0 < stride → i < rows.length →
  let lengths := rows.map List.length
  let anchor := (i/stride)*stride
  let offset := (lengths.take anchor).sum + ((lengths.drop anchor).take (i-anchor)).sum
  (rows.flatten.drop offset).take (lengths[i]?.getD 0) = rows[i]?.getD []

/-- ClickBench は3時計を時点ごとの中央値と符号付き残差で表し、正規化した時刻をランダム復号する。計測は共通時計・残差幅・書式例外込みのバイト。 -/
def ClickClickHouse.Goal : Prop := ∀ (a b c : List Int) (w i : Nat),
  a.length = b.length → b.length = c.length → i < a.length →
  let anchors := (a.zip (b.zip c)).map (fun (x,y,z) => ([x,y,z].mergeSort)[1]?.getD 0)
  ∀ col ∈ [a,b,c],
    let residual := (col.zip anchors).map (fun (v,k) => zig (v-k))
    (∀ d ∈ residual, d < 2^w) →
    (anchors[i]?.getD 0) + unzig (takeAt (pack w residual) w i) = col[i]?.getD 0

def byteLane (page : Bytes) (width lane : Nat) :=
  (page.zipIdx.filter (fun p => p.2 % width == lane)).map Prod.fst

/-- 派生分析5種はフレーム内のバイトを位置の剰余で転置してから圧縮する。計測は幅タグと WAL 込みの全バイト。 -/
def AnalyticsDuckDB.Goal : Prop := ∀ (page : Bytes) w i, 0 < w → i < page.length →
  let lanes := (List.range w).map (byteLane page w)
  let beginLane := ((lanes.take (i%w)).map List.length).sum
  lanes.flatten[beginLane + i/w]? = page[i]?

/-- 派生分析5種は blocks を独立圧縮し、論理オフセットを索引で引いて必要な範囲だけ再構築する。計測は索引・カタログ・WAL 込みの全バイト。 -/
def AnalyticsClickHouse.Goal : Prop := ∀ (frames : List Bytes) (enc dec : Bytes → Bytes) start len,
  (∀ b ∈ frames, dec (enc b) = b) →
  let starts := (frames.map List.length).scanl (·+·) 0
  let index := starts.zip (frames.map (fun b => (b.length,enc b)))
  let readSpan := fun (entry : Nat × Nat × Bytes) =>
    let (pos,sz,body) := entry
    if pos + sz ≤ start || start + len ≤ pos then []
    else ((dec body).drop (start-pos)).take (start+len-max pos start)
  index.flatMap readSpan = (frames.flatten.drop start).take len

/-- ClickBench の1 MiB 試作は URL と OriginalURL の共通接頭辞を残差化する。計測は置換フレーム・索引をすべて再圧縮した純減バイト。 -/
def OfflineClickClickHouse.Goal : Prop := ∀ (url original : Bytes) (tail : Bytes),
  let n := ((url.zip original).takeWhile (fun (a,b) => a == b)).length
  let suffix := original.drop n
  let frame := uleb n ++ uleb suffix.length ++ suffix ++ tail
  (do let (prefixSize,rest) ← readU frame
      let (suffixSize,bytes) ← readU rest
      pure (url.take prefixSize ++ bytes.take suffixSize, bytes.drop suffixSize)) = some (original,tail)

end TinylambOpt.ResearchGoals.BenchmarkRematch.Storage
