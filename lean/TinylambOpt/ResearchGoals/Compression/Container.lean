import TinylambOpt.ResearchGoals.Compression.Wire

namespace TinylambOpt.ResearchGoals.Compression.Container
open Wire

/-! Mirrors seekable_space.py. The 32-byte header and 17-byte entries are
charged separately from the independently encoded chunks. Compression and
decompression are external functions subject to a checked round-trip contract. -/
structure Codec where
  compress : Bytes → Bytes
  decompress : Bytes → Option Bytes
structure Block where
  coded : Bool
  body : Bytes
def choose (c : Codec) (s : Bytes) : Block :=
  let out := c.compress s
  if out.length < s.length then ⟨true,out⟩ else ⟨false,s⟩
def unpack (c : Codec) (b : Block) := if b.coded then c.decompress b.body else some b.body
def chunks (k : Nat) (s : Bytes) :=
  (List.range ((s.length+k-1)/k)).map (fun i => (s.drop (i*k)).take k)
def blocks (c : Codec) (k : Nat) (s : Bytes) := (chunks k s).map (choose c)
def payload (bs : List Block) := (bs.map (fun b => b.body.length)).sum
def onDisk (bs : List Block) := 32 + payload bs + 17*bs.length
def offset (bs : List Block) (i : Nat) := 32 + payload (bs.take i)
def rangeBlocks (k n pos len : Nat) :=
  let stop := min n (pos+len)
  if pos < stop then (List.range ((stop-1)/k - pos/k + 1)).map (pos/k+·) else []
def viaDirectory (c : Codec) (k : Nat) (s : Bytes) (pos len : Nat) : Option Bytes := do
  let dirs := blocks c k s
  let parts ← (rangeBlocks k s.length pos len).mapM (fun i => dirs[i]?.bind (unpack c))
  return (parts.flatten.drop (pos%k)).take len

/-- 混合 raw/圧縮ページの可逆性は各圧縮ページで検査できる。 -/
def VerifiedContainer.Goal : Prop := ∀ c k s, 0 < k →
  (∀ x ∈ chunks k s, c.decompress (c.compress x) = some x) →
  ((blocks c k s).mapM (unpack c)).map List.flatten = some s

/-- 先頭・末尾にまたがる任意の部分読取りが元の TLCS バイトを返す。CRC 成功だけに依存しない。 -/
def RandomRead.Goal : Prop := ∀ c k s pos len, 0 < k →
  (∀ x ∈ chunks k s, c.decompress (c.compress x) = some x) →
  viaDirectory c k s pos len = some ((s.drop pos).take len)

/-- raw fallback でファイル全体の最悪増分をヘッダとディレクトリだけに制限する。 -/
def RawEnvelope.Goal : Prop := ∀ c k s, 0 < k → onDisk (blocks c k s) ≤
  s.length + 32 + 17*((s.length+k-1)/k)

/-- 競合ファイルを下回るために必要な実圧縮バイト節約を、ヘッダ、全 entry、他の永続ファイル込みで判定する。 -/
def BeatTarget.Goal : Prop := ∀ c k s extra target, 0 < k →
  let gains := ((chunks k s).map (fun b => b.length-(c.compress b).length)).sum
  (onDisk (blocks c k s)+extra < target ↔
    s.length+32+17*(chunks k s).length+extra < target+gains)

/-- 各チャンク最短の候補は、同じ entry 幅で全チャンクに一つの codec を強制する案より大きくならない。 -/
def PerChunkChoice.Goal : Prop := ∀ (cs : List Codec) (sizes : List Bytes) (c : Codec), c ∈ cs →
  let best := fun (b : Bytes) => (b.length::cs.map (fun f => (f.compress b).length)).min?.getD b.length
  (sizes.map best).sum ≤ (sizes.map (fun b => (choose c b).body.length)).sum

/-- 単一 ordinal で開く圧縮フレームが一つに決まり、範囲外はフレームを要求しない。 -/
def PointFanout.Goal : Prop := ∀ k n i, 0 < k →
  rangeBlocks k n i 1 = if i < n then [i/k] else []

/-- 範囲読取りが触るフレーム数の最悪上界から、許せる seek 粒度を決める。 -/
def ReadAmplification.Goal : Prop := ∀ k n pos len, 0 < k →
  (rangeBlocks k n pos len).length*k ≤ len + 2*(k-1)

/-- 同じチャンクに当たる複数列の gather は、展開を一回に束ねても要求範囲をすべて覆う。 -/
def BatchedReads.Goal : Prop := ∀ k n (req : List (Nat × Nat)) i, 0 < k →
  (i ∈ (req.flatMap (fun (p,l) => rangeBlocks k n p l)).eraseDups ↔
    ∃ p ∈ req, ∃ x < n, p.1 ≤ x ∧ x < p.1+p.2 ∧ x/k = i)

/-- 変更されたバイトのあるチャンクだけを再圧縮し、残りの内容アドレスを再利用する。 -/
def ReuseClean.Goal : Prop := ∀ c k (s t : Bytes) i, 0 < k → s.length = t.length →
  (∀ j < k, s[i*k+j]? = t[i*k+j]?) →
  ((blocks c k s)[i]?.map Block.body) = ((blocks c k t)[i]?.map Block.body)

/-- ディレクトリから payload を切り出すと宣言長とオフセットが一致する。 -/
def BodyLocator.Goal : Prop := ∀ (bs : List Block) i, i < bs.length →
  (((bs.flatMap Block.body).drop (offset bs i-32)).take ((bs[i]?.map (·.body.length)).getD 0)) =
    ((bs[i]?.map Block.body).getD [])

/-- 変更フレームより後ろのオフセットだけを、長さの増減分で一括修正する。 -/
def Relocation.Goal : Prop := ∀ (bs : List Block) i j b, i < j → j ≤ bs.length →
  (offset (bs.set i b) j:Int) - offset bs j =
    (b.body.length:Int) - ((bs[i]?.map (·.body.length)).getD 0)

/-- 昇順オフセットの各差を 32 ビット長に置き換え、定期アンカーから絶対位置を復元する。 -/
def AnchoredDirectory.Goal : Prop := ∀ (bs : List Block) a i, a ≤ i → i ≤ bs.length →
  offset bs i = offset bs a + payload ((bs.drop a).take (i-a))

/-- 可変長のフレーム長表と 64 ビットの疎なアンカーの合計が、密な offset 表を下回る条件。 -/
def IndexBreakEven.Goal : Prop := ∀ (bs : List Block) k, 0 < k →
  let lengths := bs.map (fun b => b.body.length)
  let anchors := (bs.length+k-1)/k
  (8*anchors + streamBytes lengths + 5*bs.length < 17*bs.length ↔
   8*anchors + streamBytes lengths < 12*bs.length)

/-- 連続読取りで二つの要求の境界に同じフレームが現れる場合だけ二重展開が発生する。 -/
def AdjacentRequest.Goal : Prop := ∀ k n p a b, 0 < k →
  ((rangeBlocks k n p a ++ rangeBlocks k n (p+a) b).eraseDups) = rangeBlocks k n p (a+b)

/-- 独立フレームを途中からコピーできる十分条件。完全チャンクの追記は古い符号列を変えない。 -/
def AppendAtBoundary.Goal : Prop := ∀ c k a b, 0 < k → a.length%k = 0 →
  (blocks c k (a++b)).map Block.body = (blocks c k a ++ blocks c k b).map Block.body

/-- 8 バイトのオフセット、4 バイトの body 長の各フィールドが表現できる条件を一括検査する。 -/
def HeaderWidths.Goal : Prop := ∀ c k s, 0 < k → k < 2^32 → onDisk (blocks c k s) < 2^64 →
  ∀ i < (blocks c k s).length, offset (blocks c k s) i < 2^64 ∧
    (((blocks c k s)[i]?.map (·.body.length)).getD 0) < 2^32

/-- フレームが小さいほど最終ファイルも小さいという強仮説。raw と高圧縮の双方で検証する。 -/
def FinerAlwaysWins.Goal : Prop := ∀ c s k, 0 < k →
  onDisk (blocks c k s) ≤ onDisk (blocks c (2*k) s)

#guard let identity : Codec := ⟨id, some⟩
       onDisk (blocks identity 1 [3,4]) == 68 && onDisk (blocks identity 2 [3,4]) == 51
end TinylambOpt.ResearchGoals.Compression.Container
