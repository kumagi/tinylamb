import TinylambOpt.ResearchGoals.Compression.Wire
import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
The modeled encoding and decoding contracts support compressed columns without changing
the values observed by SQL scans.
-/

namespace TinylambOpt.ResearchGoals.Compression.Text
open Wire
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Lcp (common)
abbrev Words := List Bytes
def raw (ws : Words) := ws.flatten.length + 4*ws.length
def lengths (ws : Words) := ws.flatMap (fun w => uleb w.length ++ w)
def readLengths : Nat → Bytes → Option (Words × Bytes)
  | 0,bs => some ([],bs)
  | n+1,bs => do
    let (size,rest) ← readU bs
    if size > rest.length then none else
      let (tail,rem) ← readLengths n (rest.drop size)
      return (rest.take size :: tail,rem)
def entry (prev w : Bytes) :=
  let k := common prev w
  let suffix := w.drop k
  uleb k ++ uleb suffix.length ++ suffix
def entries : Bytes → Words → Bytes
  | _,[] => []
  | prev,w::ws => entry prev w ++ entries w ws
def readEntries : Nat → Bytes → Bytes → Option (Words × Bytes)
  | 0,_,bs => some ([],bs)
  | n+1,prev,bs => do
    let (k,r) ← readU bs
    let (len,r) ← readU r
    if k > prev.length || len > r.length then none else
      let w := prev.take k ++ r.take len
      let (ws,tail) ← readEntries n w (r.drop len)
      return (w::ws,tail)
def dictionary (ws : Words) := ws.eraseDups.mergeSort
def frontBody (ws : Words) :=
  let d := dictionary ws
  uleb d.length ++ entries [] d ++ pack (width (d.length-1)) (ws.map d.idxOf)
def readBody (n : Nat) (bs : Bytes) : Option Words := do
  let (size,rest) ← readU bs
  let (d,ids) ← readEntries size [] rest
  let w := width (size-1)
  if ids.length != byteCount (n*w) then none else (List.range n).mapM (fun i => d[get ids w i]?)
def packet (tag n : Nat) (body : Bytes) := [tag] ++ little n 4 ++ little body.length 4 ++ body

def readPacket (bs : Bytes) : Option Words := do
  if bs.length < 9 || bs[0]? != some 1 then none else
    let n := fromLittle ((bs.drop 1).take 4)
    let size := fromLittle ((bs.drop 5).take 4)
    let body := bs.drop 9
    if size != body.length then none else readBody n body

/-- 実測と同じフレームから行数と本体長を読み取り、空文字・重複・行順を丸ごと戻す。 -/
def FramedInverse.Goal : Prop := ∀ ws, ws.length < 2^32 → (frontBody ws).length < 2^32 →
  readPacket (packet 1 ws.length (frontBody ws)) = some ws

/-- 語末や ID 列が途中で切れたフレームを、より短い有効ページと取り違えず拒否する。 -/
def TruncatedFrame.Goal : Prop := ∀ ws n, (frontBody ws).length < 2^32 →
  let frame := packet 1 ws.length (frontBody ws)
  n < frame.length → readPacket (frame.take n) = none

/-- 終点配列を捨て、可変長の長さ接頭辞だけで空・重複を含む文字列列を復元する。 -/
def StreamOffsets.Goal : Prop := ∀ ws tail, readLengths ws.length (lengths ws ++ tail) = some (ws,tail)

/-- TLCS の 32-bit 終点より小さいかを語ごとの長さコードの総和だけで判定する。 -/
def OffsetSaving.Goal : Prop := ∀ ws overhead,
  (overhead + (lengths ws).length < raw ws ↔ overhead + streamBytes (ws.map List.length) < 4*ws.length)

/-- 長さを別ストリームに置いた場合、受け取った数値が前行と同じでも切り出しを区別できる。 -/
def LengthSeek.Goal : Prop := ∀ (ws : Words) (i : Nat), i < ws.length →
  (((ws.flatten.drop ((ws.take i).map List.length).sum).take (ws[i]?.getD []).length)) = ws[i]?.getD []

/-- 辞書語を直前語との差だけで保存し、後続の ID ストリームを食い込まずに返す。 -/
def FrontInverse.Goal : Prop := ∀ prev ws tail, readEntries ws.length prev (entries prev ws ++ tail) = some (ws,tail)

/-- 長さヘッダを含む一語の費用。短い共通 prefix は採用すると逆に大きくなりうる。 -/
def FrontEntryCharge.Goal : Prop := ∀ a b, let k := common a b
  (entry a b).length = ubytes k + ubytes (b.length-k) + b.length-k

/-- ソートした異なり語と詰めた ID の組からバイナリ照合の原列を順序どおり復元する。 -/
def FrontPageInverse.Goal : Prop := ∀ ws, readBody ws.length (frontBody ws) = some ws

/-- 実測プローブの 9 バイトフレーム、辞書の件数、差分語、ID の最後の丸めを別々に課金する。 -/
def FrontPageCharge.Goal : Prop := ∀ ws, let d := dictionary ws
  (packet 1 ws.length (frontBody ws)).length =
    9 + ubytes d.length + (entries [] d).length + byteCount (ws.length*width (d.length-1))

/-- 同じ辞書を参照する反復バッチでは語本体を増やさず、行の増分を ID 幅だけにする。 -/
def ReplicatedDictionary.Goal : Prop := ∀ ws n, 0 < n →
  let repeated := (List.replicate n ws).flatten
  let d := dictionary ws
  (frontBody repeated).length = ubytes d.length + (entries [] d).length +
    byteCount (n*ws.length*width (d.length-1))

/-- 一語だけのページでは ID 幅がゼロになり、長文を何回繰り返しても body 長が一定になる。 -/
def SingleTerm.Goal : Prop := ∀ w n, 0 < n →
  frontBody (List.replicate n w) = frontBody [w]

/-- 一括辞書と shard 別辞書のサイズ差を、コード幅の上昇と辞書再利用の収支から厳密に求める。 -/
def ShardDictionaryBudget.Goal : Prop := ∀ a b,
  let charge := fun (ws : Words) => let d := dictionary ws
                                    ubytes d.length + (entries [] d).length
  let ids := fun (ws : Words) => byteCount (ws.length*width ((dictionary ws).length-1))
  ((frontBody (a++b)).length:Int) - (frontBody a).length - (frontBody b).length =
    (charge (a++b):Int)-charge a-charge b + ids (a++b)-ids a-ids b

/-- 辞書の二語を途中で入れ替える際、その近傍以外を再符号化せずサイズ差を得る。 -/
def AdjacentExchange.Goal : Prop := ∀ prev a b tail,
  ((entries prev (a::b::tail)).length:Int) - (entries prev (b::a::tail)).length =
    ((entry prev a).length:Int)+(entry a b).length-(entry prev b).length-(entry b a).length +
    (tail.head?.map (fun w => ((entry b w).length:Int)-(entry a w).length)).getD 0

/-- 再始動のための基準語を一つ置く際の代価を、前後の差分のうち先頭一語の差で計算する。 -/
def RestartCharge.Goal : Prop := ∀ prev pre post, let last := pre.getLast?.getD prev
  ((entries prev pre).length:Int)+(entries [] post).length-(entries prev (pre++post)).length =
    (post.head?.map (fun w => ((entry [] w).length:Int)-(entry last w).length)).getD 0

/-- 共有接尾辞の多い値では語を逆順にしてから front coding し、逆順化で元の行・バイトを回収する。 -/
def SuffixDictionary.Goal : Prop := ∀ ws,
  (readBody ws.length (frontBody (ws.map List.reverse))).map (List.map List.reverse) = some ws

/-- 語の共通接頭辞をページに一度だけ置き、語ごとの差分長ヘッダも短縮する。 -/
def CommonStemBudget.Goal : Prop := ∀ stem tails,
  let ws := tails.map (stem ++ ·)
  tails.length*(stem.length) + (lengths tails).length ≤ (lengths ws).length

/-- SQL の空文字を NULL とは別に辞書の有効コードとして扱い、行数があれば辞書から復元できる。 -/
def EmptyTerm.Goal : Prop := ∀ n tail, readEntries n [] (entries [] (List.replicate n []) ++ tail) =
  some (List.replicate n [],tail)

/-- 文字列終点が 32 ビットに収まる条件を最後の累積長だけで検査する。 -/
def EndOffsetWidth.Goal : Prop := ∀ (ws : Words) w,
  (∀ i ≤ ws.length, ((ws.take i).map List.length).sum < 2^w) ↔ ws.flatten.length < 2^w

/-! Disjoint substitutions use a one-byte token 256 in the model; the wire
uses an unused byte (supplied separately) in its place. The literal case must
never contain that reserved byte after the substitution. -/
def symbols (pat : Bytes) : Nat → Bytes → Bytes
  | 0,s => s
  | fuel+1,s => match s with
    | [] => []
    | x::xs => if pat ≠ [] && pat.isPrefixOf s then 256 :: symbols pat fuel (s.drop pat.length)
               else x :: symbols pat fuel xs
def expand (pat ts : Bytes) := ts.flatMap (fun x => if x == 256 then pat else [x])

/-- URL 部分語の重ならない置換を byte 対の境界を含めて戻す。 -/
def TokenInverse.Goal : Prop := ∀ pat s fuel, (∀ x ∈ s, x < 256) →
  expand pat (symbols pat fuel s) = s

/-- トークン辞書を持つ価値があるかを、実際に置換された出現数で判定する。 -/
def TokenBreakEven.Goal : Prop := ∀ pat s fee, pat ≠ [] → (∀ x ∈ s, x < 256) →
  let ts := symbols pat s.length s
  (fee + ts.length < s.length ↔ fee < ts.count 256 * (pat.length-1))

/-- 強仮説。重なる出現をすべて数えて辞書の利得を見積もってよいか。 -/
def OverlappingSavings.Goal : Prop := ∀ pat s, 1 < pat.length →
  let allHits := (List.range s.length).countP (fun i => pat.isPrefixOf (s.drop i))
  (symbols pat s.length s).length + allHits*(pat.length-1) = s.length

/-- 同じ記号語彙でもページ末尾で置換をリセットする際は、行を連結した利得と分けて課金する。 -/
def BoundaryToken.Goal : Prop := ∀ (a b : Bytes) (x y : Nat), x < 256 → y < 256 → x ≠ y →
  (symbols [x,y] (a++b).length (a++b)).count 256 =
    (symbols [x,y] a.length a).count 256 + (symbols [x,y] b.length b).count 256 +
    (if a.getLast? == some x && b.head? == some y then 1 else 0)

#guard readBody 4 (frontBody [[],[0,255],[],[0,1]]) == some [[],[0,255],[],[0,1]]
#guard symbols [1,1] 3 [1,1,1] == [256,1]
end TinylambOpt.ResearchGoals.Compression.Text
