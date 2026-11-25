import Std

namespace TinylambOpt.ResearchGoals.Frontiers.Contractions

/-! H5, C3, L1, Q8. A matrix cell is the multiplicity of a join-key pair, or a
signed measure after delta processing. Domains below are explicit dense ID ranges. -/
abbrev Vec := Nat → Int
abbrev Mat := Nat → Nat → Int
def total (n : Nat) (v : Vec) := ((List.range n).map v).sum
def dot (n : Nat) (a b : Vec) := total n (fun i => a i * b i)
def run (n m : Nat) (a : Mat) (u v : Vec) := total n (fun i => u i * dot m (a i) v)
def cycle (n : Nat) (a b c : Mat) := total n (fun i => total n (fun j =>
  total n (fun k => a i j * b j k * c k i)))
def outer (l r : Vec) : Mat := fun i j => l i * r j
def plus (a b : Mat) : Mat := fun i j => a i j + b i j

/-- 低ランクの橋表を二組の周辺集計に変換して、結合キー対の走査を省く。 -/
def LatentJoin.Goal : Prop := ∀ n m rank (l r : Mat) u v,
  run n m (fun i j => total rank (fun h => l i h*r h j)) u v =
    total rank (fun h => dot n u (fun i => l i h) * dot m (r h) v)

/-- 一つの非零ピボットに対する小行列式の消滅をロード時に検査し、後続の任意の集約を一行一列から復元する。 -/
def PivotCertificate.Goal : Prop := ∀ n m (a : Mat) p q u v,
  p < n → q < m → a p q ≠ 0 →
  (∀ i < n, ∀ j < m, a p q * a i j = a i q * a p j) →
  run n m a u v = (dot n u (fun i => a i q) * dot m (a p) v) / a p q

/-- 上限付き誤差を許すランク一近似から、厳密な HAVING の不採用を保証する。 -/
def ResidualHaving.Goal : Prop := ∀ n m (a : Mat) l r u v (eps : Nat) threshold,
  (∀ i < n, ∀ j < m, (a i j - l i*r j).natAbs ≤ eps) →
  dot n u l * dot m r v + (eps:Int) * total n (fun i => (u i).natAbs) *
    total m (fun j => (v j).natAbs) < threshold → run n m a u v < threshold

/-- 三角結合で三辺すべてが因子化できるとき、頂点三重ループを三つの内積に落とす。 -/
def TriangleFactors.Goal : Prop := ∀ n a b c d e f,
  cycle n (outer a b) (outer c d) (outer e f) = dot n b c * dot n d e * dot n f a

/-- 橋表のランク一更新に対して三角集計を再評価する量を、変更された二端点間の経路数に限定する。 -/
def RankUpdate.Goal : Prop := ∀ n a b c u v,
  cycle n (plus a (outer u v)) b c - cycle n a b c =
    total n (fun k => dot n v (fun j => b j k) * dot n (c k) u)

/-- 基底方向に直交する売上差分は全出力群から見えないため、群ごとの更新を全廃できる。 -/
def InvisibleDelta.Goal : Prop := ∀ n m d (l r : Mat) (delta : Vec),
  (∀ h < d, dot n delta (fun i => l i h) = 0) →
  ∀ v, run n m (fun i j => total d (fun h => l i h*r h j)) delta v = 0

def step (n : Nat) (a : Mat) (v : Vec) : Vec := fun i => dot n (a i) v
def walk (n : Nat) (a : Mat) (v : Vec) : Nat → Vec
  | 0 => v
  | d+1 => step n a (walk n a v d)

/-- 同じクラスの頂点が各行先クラスへ同じ総重みを送るなら、任意段のグラフ展開を商グラフで行う。 -/
def QuotientWalk.Goal : Prop := ∀ n m (a reduced : Mat) (cls : Nat → Nat) (endWeight : Vec),
  (∀ i < n, cls i < m) →
  (∀ i < n, ∀ c < m, total n (fun j => if cls j == c then a i j else 0) = reduced (cls i) c) →
  ∀ depth i, i < n → walk n a (fun j => endWeight (cls j)) depth i =
    walk m reduced endWeight depth (cls i)

/-- 強仮説。クラス別の行和が一致する二つのグラフでは、始点に戻る長さ 3 の経路数も同じとしてよいか。 -/
def QuotientCycles.Goal : Prop := ∀ n (a b : Mat) (cls : Nat → Nat),
  (∀ i < n, ∀ c, total n (fun j => if cls j == c then a i j else 0) =
                total n (fun j => if cls j == c then b i j else 0)) →
  cycle n a a a = cycle n b b b

/-- 係数の疎な例外だけを別経路で課金する。例外座標自体が重複しても差分を重ねる。 -/
def ExceptionJoin.Goal : Prop := ∀ n m l r u v (edits : List (Nat × Nat × Int)),
  (∀ e ∈ edits, e.1 < n ∧ e.2.1 < m) →
  run n m (fun i j => l i*r j + ((edits.filter (fun (x,y,_) => x == i && y == j)).map
    (fun e => e.2.2)).sum) u v =
    dot n u l * dot m r v + (edits.map (fun (i,j,w) => u i*w*v j)).sum

/-- 二つのランク分解を内部キーで先に縮約し、中間結合を潜在次元だけの行列で共有する。 -/
def LatentBridge.Goal : Prop := ∀ n h k (l r s t : Mat) i j,
  total n (fun x => dot h (l i) (fun p => r p x) * dot k (s x) (fun q => t q j)) =
  total h (fun p => total k (fun q => l i p * dot n (r p) (fun x => s x q) * t q j))

-- Simple undirected graphs: two disjoint triangles and a ring on six vertices.
private def triangles : Mat := fun i j => if i != j && i/3 == j/3 then 1 else 0
private def ring : Mat := fun i j => if (i+1)%6 == j || (j+1)%6 == i then 1 else 0
#guard cycle 6 triangles triangles triangles == 12 && cycle 6 ring ring ring == 0
#guard (List.range 6).all (fun i => total 6 (triangles i) == 2 && total 6 (ring i) == 2)

end TinylambOpt.ResearchGoals.Frontiers.Contractions
