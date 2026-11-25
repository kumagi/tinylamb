import TinylambOpt.ResearchGoals.Compression.Text

namespace TinylambOpt.ResearchGoals.Compression.PhraseTables
open Wire
open Text (Words)
-- One byte per dictionary symbol. Code 255 quotes the following literal byte.
def expand (d : Words) : Bytes → Option Bytes
  | [] => some []
  | 255::x::rest => (expand d rest).map (x::·)
  | c::rest => do
    if c == 255 then none else
      let w ← d[c]?
      let tail ← expand d rest
      return w++tail
def choices (d : Words) (s : Bytes) :=
  (d.take 255).zipIdx.filter (fun (w,_) => !w.isEmpty && w.isPrefixOf s)
def literals (s : Bytes) := s.flatMap (fun x => [255,x])
def shortest (cands : List Bytes) := (cands.mergeSort (fun a b => a.length ≤ b.length)).head?.getD []
def parse (d : Words) : Nat → Bytes → Bytes
  | 0,s => literals s
  | _+1,[] => []
  | fuel+1,x::xs =>
    let s := x::xs
    let raw := [255,x]++parse d fuel xs
    shortest (raw :: (choices d s).map (fun (w,i) => i :: parse d fuel (s.drop w.length)))
def encode (d : Words) (s : Bytes) := parse d s.length s
def greedy (d : Words) : Nat → Bytes → Bytes
  | 0,s => literals s
  | _+1,[] => []
  | fuel+1,x::xs =>
    let s := x::xs
    match ((choices d s).mergeSort (fun a b => a.1.length ≥ b.1.length)).head? with
    | some (w,i) => i :: greedy d fuel (s.drop w.length)
    | none => [255,x]++greedy d fuel xs
def tableBytes (d : Words) := ubytes d.length + (Text.lengths d).length
def rowFrame (d : Words) (s : Bytes) := let stream := encode d s; uleb stream.length ++ stream
def charge (d ws : Words) := tableBytes d + (ws.map (fun s => (rowFrame d s).length)).sum

/-- 最短トークン列を選ぶ際に文字列の原バイトを失わない。escape 自体も literal として再生する。 -/
def OptimalInverse.Goal : Prop := ∀ d s, (∀ b ∈ s, b < 256) → expand d (encode d s) = some s

/-- 1-byte symbol と escaped literal で表せる全符号列のうち、動的計画が最短長を達成する。 -/
def OptimalParse.Goal : Prop := ∀ d s stream, d.length ≤ 255 → (∀ b ∈ stream, b < 256) →
  expand d stream = some s → (encode d s).length ≤ stream.length

/-- 追加の辞書語を採用しても、以前の ID を保持する限りデータ部の長さは増えない。 -/
def TableExtension.Goal : Prop := ∀ d new s, d.length + new.length ≤ 255 →
  (encode (d++new) s).length ≤ (encode d s).length

/-- 新しい語が既存語と同じ ID コストで一度も使われなければ辞書から除去して全体の費用を減らせる。 -/
def UnusedSuffix.Goal : Prop := ∀ d w ws, d.length < 255 →
  (∀ s ∈ ws, ∀ i < s.length, !(w.isPrefixOf (s.drop i)) = true) →
  charge d ws ≤ charge (d++[w]) ws

/-- 重複する辞書語を除いて件数タグと辞書本体を減らせる。 -/
def AliasPrune.Goal : Prop := ∀ d ws, d.length ≤ 255 → charge d.eraseDups ws ≤ charge d ws

/-- 辞書語順の入替えで全コードの幅が一定なら、圧縮総量は変わらず探索順だけを調整できる。 -/
def ReorderTable.Goal : Prop := ∀ d e ws, d.Perm e → d.length ≤ 255 → charge d ws = charge e ws

/-- 同じ URL 列の各ブロックに同一辞書を繰り返す場合、辞書を一度だけ書いたときの削減量を数える。 -/
def ExternalTable.Goal : Prop := ∀ d (blocks : List Words), blocks ≠ [] →
  (blocks.map (charge d)).sum = charge d blocks.flatten + (blocks.length-1)*tableBytes d

/-- フレーズ辞書を共有する二ページで行ペイロード長とタグを保ったまま部分デコードを独立させる。 -/
def RowBoundary.Goal : Prop := ∀ d w tail,
  readU (rowFrame d w ++ tail) = some ((encode d w).length,encode d w ++ tail)

/-- DP のリテラル経路により、最悪時のデータ部を 2 倍以下に制限する。 -/
def LiteralCeiling.Goal : Prop := ∀ d s, (encode d s).length ≤ 2*s.length

/-- 辞書の最長語から、どんなパースでも必要な最小コード数を見積もる。 -/
def TokenFloor.Goal : Prop := ∀ d s,
  s.length ≤ (encode d s).length * max 1 (peak ((d.take 255).map List.length))

/-- separator を保存した部分文字列では先に二つに分けても最適連結パースの上界が得られる。 -/
def JoinFragments.Goal : Prop := ∀ d a b, (encode d (a++b)).length ≤ (encode d a).length+(encode d b).length

/-- byte n-gram 辞書を使うとき、語と辞書を両方反転しても最短符号長を保てる。 -/
def ReverseParsing.Goal : Prop := ∀ d s, (encode (d.map List.reverse) s.reverse).length = (encode d s).length

/-- 強仮説。最長一致を貪欲に採用しても、後続の長語を使う最短パースを失わないか。 -/
def LongestIsOptimal.Goal : Prop := ∀ d s, (greedy d s.length s).length = (encode d s).length

/-- 辞書語を一回だけ使う行でも、ヘッダの費用まで足すと必ずしも利益はない。その境界を全行の節約量で判定する。 -/
def AmortizeSymbol.Goal : Prop := ∀ d w ws, d.length < 255 →
  (charge (d++[w]) ws < charge d ws ↔
    tableBytes (d++[w])-tableBytes d <
      (ws.map (fun s => (rowFrame d s).length-(rowFrame (d++[w]) s).length)).sum)

/-- 混合ブロックの辞書を先頭サンプルだけで選ぶ強仮説。後続の分布変化で逆転するか。 -/
def TrainOnPrefix.Goal : Prop := ∀ a b train tail, charge a train ≤ charge b train →
  charge a (train++tail) ≤ charge b (train++tail)

#guard let d := [[1,2],[1],[2,3,4]]
       (greedy d 4 [1,2,3,4]).length == 5 && (encode d [1,2,3,4]).length == 2
#guard let a : Words := []
       let b : Words := [[1,2,3]]
       charge a [] < charge b [] && charge b [[1,2,3],[1,2,3]] < charge a [[1,2,3],[1,2,3]]
end TinylambOpt.ResearchGoals.Compression.PhraseTables
