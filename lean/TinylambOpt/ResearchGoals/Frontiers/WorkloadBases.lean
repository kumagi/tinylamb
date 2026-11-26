import TinylambOpt.ResearchGoals.Frontiers.Contractions

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.
-/

namespace TinylambOpt.ResearchGoals.Frontiers.WorkloadBases

open Contractions (Vec Mat total dot)
/-! H2O H5. Left states per (output group, join key) store count and sum;
right states per key store count and sum. The two output columns have different
weight vectors. Aggregates below describe non-null integer contributions. -/
def rowBits (width : Nat) (v : Vec) := ((List.range width).map
  (fun j => if v j % 2 == 1 then 2^j else 0)).sum
def insertBits : List Nat → Nat → List Nat
  | [], x => if x == 0 then [] else [x]
  | h::t, x =>
    if x == 0 then h::t
    else if x.log2 > h.log2 then x::h::t
    else if x.log2 == h.log2 then h :: insertBits t (x ^^^ h)
    else h :: insertBits t x
def binaryRank (n m : Nat) (a : Mat) :=
  ((List.range n).foldl (fun basis i => insertBits basis (rowBits m (a i))) []).length

/-- 偶奇だけのガウス消去で整数因子の必要ランクを下から押さえ、見込みのない厳密圧縮を構築前に棄却する。 -/
def RankRejection.Goal : Prop := ∀ n m r (l t : Mat),
  binaryRank n m (fun i j => dot r (l i) (fun h => t h j)) ≤ r

/-- 同じクエリ系列の重みが少数のベクトルの線形結合なら、橋表自体がフルランクでも要約の内積だけで全回答を復元する。 -/
def QueryBasis.Goal : Prop := ∀ n k (row coeff : Vec) (basis : Mat),
  dot n row (fun j => dot k coeff (fun h => basis h j)) =
    dot k coeff (fun h => dot n row (basis h))

/-- 二行の要約が識別不能となる条件を、基底ごとの等値で特徴付け、全てのクエリ係数を列挙せず共有する。 -/
def Indistinguishable.Goal : Prop := ∀ n k (a b : Vec) (basis : Mat),
  (∀ h < k, dot n a (basis h) = dot n b (basis h)) ↔
  ∀ c : Vec, dot n a (fun j => dot k c (fun h => basis h j)) =
    dot n b (fun j => dot k c (fun h => basis h j))

def classDot (keys : List Nat) (weight value : Vec) :=
  ((keys.map weight).eraseDups.map (fun w =>
    w * ((keys.filter (fun j => weight j == w)).map value).sum)).sum

/-- H5 の二つの SUM に別々の同値類を使い、右の件数と右の売上の組が一意でも片方の状態を強く圧縮する。 -/
def SeparateMeasures.Goal : Prop := ∀ (keys : List Nat) (lc ls rc rs : Vec),
  (classDot keys rc ls, classDot keys rs lc) =
    ((keys.map (fun j => ls j*rc j)).sum, (keys.map (fun j => lc j*rs j)).sum)

/-- 強仮説。ファンアウトが同じキーを代表一つにまとめた要約で、右の値に由来する二つ目の SUM も返せるか。 -/
def FanoutOnly.Goal : Prop := ∀ n (rc rs left : Vec) (rep : Nat → Nat),
  (∀ j < n, rep j < n ∧ rc (rep j) = rc j) →
  dot n left rs = dot n left (fun j => rs (rep j))

/-- 右側の重みを下限からの少数ビット面として格納し、各面のマスク付き和だけから正確な群内積を復元する。 -/
def NarrowWeights.Goal : Prop := ∀ n bits (lo : Int) (row weight : Vec),
  (∀ j < n, lo ≤ weight j ∧ weight j < lo + 2^bits) →
  dot n row weight = lo * total n row + total bits (fun b => 2^b * total n
    (fun j => if (weight j-lo).toNat.testBit b then row j else 0))

/-- 外れ値を含む重みを代表値と残差に分け、二乗ノルムの積で任意のグループの誤差の二乗を保証する。 -/
def EnergyBound.Goal : Prop := ∀ n (row actual code : Vec),
  (dot n row actual - dot n row code)^2 ≤
    dot n row row * total n (fun j => (actual j-code j)^2)

/-- 圧縮誤差の絶対値総和を最小にする辞書代表を、グループごとの再探索なしにバケットの中央値で決定する。 -/
def MedianCode.Goal : Prop := ∀ (ws : List Int) center, ws ≠ [] →
  let m := ws.mergeSort[ws.length/2]?.getD 0
  (ws.map (fun w => (w-m).natAbs)).sum ≤ (ws.map (fun w => (w-center).natAbs)).sum

def winners (gs : List Nat) (score : Nat → Int) (k : Nat) :=
  (gs.mergeSort (fun a b => score a > score b || (score a == score b && a ≤ b))).take k

/-- 第一集約が順位を決めるとき、厳密な境界余裕がある勝者だけ第二集約を開く。勝者内部の順序は厳密値で復元する。 -/
def LateSecondMeasure.Goal : Prop := ∀ (gs : List Nat) k (approx exact payload : Nat → Int) (err : Nat),
  (∀ g ∈ gs, (exact g - approx g).natAbs ≤ err) →
  let kept := winners gs approx k
  (∀ x ∈ kept, ∀ y ∈ gs, y ∉ kept → approx y + 2*(err:Int) < approx x) →
  gs.Nodup → (winners gs exact k).map (fun g => (g,payload g)) =
    (winners kept exact k).map (fun g => (g,payload g))

/-- 収集済みの群別内積に対し右表が部分的に更新されても、差分重みがゼロのキーを丸ごと落として修復する。 -/
def DirtyWeights.Goal : Prop := ∀ n (row old new : Vec) (dirty : List Nat),
  dirty.Nodup → (∀ j ∈ dirty, j < n) → (∀ j < n, old j ≠ new j → j ∈ dirty) →
  dot n row new = dot n row old + (dirty.map (fun j => row j*(new j-old j))).sum

#guard binaryRank 3 4 (fun i j => if i == j then -1 else 0) == 3
#guard let w : Vec := fun j => if j == 0 then 5 else 8
       dot 2 (fun _ => 1) w == 13 && dot 2 (fun _ => 1) (fun _ => w 0) == 10

end TinylambOpt.ResearchGoals.Frontiers.WorkloadBases
