import TinylambOpt.ResearchGoals.Compression.Wire

namespace TinylambOpt.ResearchGoals.Compression.Directories
open Wire

/-! Random access to TLCS VARCHAR end offsets. Empty strings repeat an end.
The high stream has a one at quotient + ordinal, the low stream has fixed-width
remainders. The row count and low width belong to the enclosing column frame. -/
def ends (sizes : List Nat) := (sizes.scanl (·+·) 0).drop 1
def positions (w : Nat) (xs : List Nat) := xs.zipIdx.map (fun (x,i) => x/2^w+i)
def highBits (w : Nat) (xs : List Nat) := xs.length + peak xs / 2^w
def high (w : Nat) (xs : List Nat) := pack 1 ((List.range (highBits w xs)).map
  (fun j => if (positions w xs).contains j then 1 else 0))
def ones (bs : Bytes) := (List.range (8*bs.length)).filter (fun j => get bs 1 j == 1)
def lookup (w i : Nat) (hi lo : Bytes) : Option Nat :=
  (ones hi)[i]?.map (fun p => (p-i)*2^w + takeAt lo w i)
def directoryBytes (w : Nat) (xs : List Nat) :=
  (high w xs).length + (pack w xs).length
def suggested (xs : List Nat) := width (peak xs / xs.length)
def anchors (stride : Nat) (ps : List Nat) := ps.zipIdx.filterMap
  (fun (p,i) => if i%stride == 0 then some p else none)
def seekFrom (bs : Bytes) (start rank : Nat) :=
  ((List.range (8*bs.length-start)).map (start+·) |>.filter (fun j => get bs 1 j == 1))[rank]?
def extents (xs : List Nat) := xs.zipWith (·-·) (0::xs)

/-- 繰り返す終点も ordinal を足すことで別々の立ったビットに割り当てられる。 -/
def UniquePositions.Goal : Prop := ∀ w sizes, (positions w (ends sizes)).Pairwise (·<·)

/-- 上位ビットベクトルのバイト末尾の詰め物からは偽の終点が生じない。 -/
def UnarySelect.Goal : Prop := ∀ w sizes, ones (high w (ends sizes)) = positions w (ends sizes)

/-- 展開した全終点表を置かず、上下位二ストリームから任意の行の終点を復元する。 -/
def EndSelect.Goal : Prop := ∀ w sizes i, let xs := ends sizes
  lookup w i (high w xs) (pack w xs) = xs[i]?

/-- 隣接する二回の select で、空文字を含む対象行のバイト列を復元する。 -/
def SliceBySelect.Goal : Prop := ∀ w (rows : List Bytes) i, i < rows.length →
  let xs := ends (rows.map List.length)
  let atEnd := fun j => (lookup w j (high w xs) (pack w xs)).getD 0
  let start := if i == 0 then 0 else atEnd (i-1)
  (rows.flatten.drop start).take (atEnd i-start) = rows[i]?.getD []

/-- 部分ブロックを含め上下位の最終バイトを別々に丸めて正確に課金する。 -/
def PhysicalCharge.Goal : Prop := ∀ w sizes, let xs := ends sizes
  directoryBytes w xs = byteCount (sizes.length*w) + byteCount (sizes.length+sizes.sum/2^w)

/-- 平均バイト長から選ぶ low width では unary 側が 2 ビット/行以下で済む。 -/
def HighPlaneBudget.Goal : Prop := ∀ sizes, let xs := ends sizes
  highBits (suggested xs) xs ≤ 2*sizes.length

/-- 幅を一つ増やした時の損益を、上位面で消えるバイトと下位面の増分から判定する。 -/
def IncreaseLowWidth.Goal : Prop := ∀ w sizes, let xs := ends sizes
  (directoryBytes (w+1) xs < directoryBytes w xs ↔
    byteCount (sizes.length*w) + byteCount (highBits w xs) >
    byteCount (sizes.length*(w+1)) + byteCount (highBits (w+1) xs))

/-- 4 バイトのアンカーとヘッダまで含め、32-bit の dense な終点表より小さくなる条件。 -/
def IndexedBreakEven.Goal : Prop := ∀ w sizes stride header, 0 < stride →
  let xs := ends sizes
  (header + directoryBytes w xs + 4*(anchors stride (positions w xs)).length < 4*sizes.length ↔
    header + byteCount (sizes.length*w) + byteCount (sizes.length+sizes.sum/2^w) +
      4*((sizes.length+stride-1)/stride) < 4*sizes.length)

/-- 疎な select アンカーから再開しても、全ビットベクトルで数えた位置と同じ。 -/
def RestartSelect.Goal : Prop := ∀ w sizes stride i, 0 < stride → i < sizes.length →
  let xs := ends sizes
  let ps := positions w xs
  let anchor := ((anchors stride ps)[i/stride]?).getD 0
  seekFrom (high w xs) anchor (i%stride) = ps[i]?

/-- 長大な値による unary のゼロ連を考慮した、隣り合うアンカー間の走査ビット数。 -/
def AnchorScanBound.Goal : Prop := ∀ w sizes a i cap, a ≤ i → i < sizes.length →
  (∀ x ∈ sizes, x ≤ cap) →
  let ps := positions w (ends sizes)
  (ps[i]?.getD 0)-(ps[a]?.getD 0) ≤ (i-a) + ((i-a)*cap+2^w-1)/2^w

/-- 末尾に行を足すと、古い終点の上位 1 ビットの位置を移す必要がない。 -/
def AppendPrefix.Goal : Prop := ∀ w a b,
  (positions w (ends (a++b))).take a.length = positions w (ends a)

/-- 連結のサイズが low 幅の単位に揃うと、後半の 1 ビット位置を一律シフトできる。 -/
def AlignedShift.Goal : Prop := ∀ w (a b : List Nat), 2^w ∣ a.sum →
  (positions w (ends (a++b))).drop a.length =
    (positions w (ends b)).map (· + (a.length + a.sum/2^w))

/-- 個々の行長で再構成した終点から元の長さを復元し、同じ終点が連続しても順序を保持する。 -/
def DeltaLengths.Goal : Prop := ∀ sizes, extents (ends sizes) = sizes

/-- グループ単位で終点をゼロから振り直せば上位部を小さくできる。そのヘッダ前の損失はバイト丸め分に収まる。 -/
def ResetHighBase.Goal : Prop := ∀ w a b,
  directoryBytes w (ends a) + directoryBytes w (ends b) ≤ directoryBytes w (ends (a++b)) + 2

/-- 強仮説。平均長だけで選ぶ幅が、端数の丸めを含めた最短の物理幅にもなるか。 -/
def AverageWidthOptimal.Goal : Prop := ∀ w sizes, let xs := ends sizes
  directoryBytes (suggested xs) xs ≤ directoryBytes w xs

/-- 全行が空なら 0 幅と単項の 1 列に縮み、明示的な 32-bit 終点や低位面を必要としない。 -/
def EmptyRun.Goal : Prop := ∀ n, directoryBytes 0 (ends (List.replicate n 0)) = byteCount n

#guard let xs := ends [0,7,0,13,0]
       (List.range 6).map (fun i => lookup 2 i (high 2 xs) (pack 2 xs)) == xs.map some ++ [none]
#guard let xs := ends [1]
       directoryBytes (suggested xs) xs == 2 && directoryBytes 0 xs == 1
end TinylambOpt.ResearchGoals.Compression.Directories
