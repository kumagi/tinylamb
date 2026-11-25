import Std

namespace TinylambOpt.ResearchGoals.Frontiers.Arithmetic

/-! Exact integer and fixed-point aggregates after NULL filtering. Fractions
below compare sums against scaled counts. The rounding model uses ties to even. -/
def lane (m : Int) (xs : List Int) := xs.foldl (fun a x => (a+x)%m) 0
def crt (m n u v a b : Int) := (a*n*v + b*m*u) % (m*n)

/-- 二本の細い剰余レーンと群の下限から符号付き SUM を復元する。下限を負にも設定できる。 -/
def SignedCRT.Goal : Prop := ∀ (xs : List Int) (m n u v lo : Int),
  0 < m → 0 < n → m*u+n*v = 1 → lo ≤ xs.sum → xs.sum < lo+m*n →
  xs.sum = lo + crt m n u v (lane m xs-lo) (lane n xs-lo)

/-- 大きな等値結合の SUM が 0 かを、二本の剰余内積と全項の絶対値予算だけで判定する。 -/
def ZeroJoin.Goal : Prop := ∀ (terms : List (Int × Int)) (m n : Nat),
  0 < m → 0 < n → m.gcd n = 1 →
  (terms.map (fun (a,b) => a.natAbs*b.natAbs)).sum < m*n →
  let vals := terms.map (fun (a,b) => a*b)
  (vals.sum = 0 ↔ lane m vals = 0 ∧ lane n vals = 0)

def rounded (s scale : Int) :=
  let q := s/scale
  let r := s%scale
  if 2*r < scale || (2*r == scale && q%2 == 0) then q else q+1

/-- 未読ページの寄与範囲の両端が同じ値に丸まる場合、固定小数の SQL 出力を早期確定する。 -/
def RoundingCertificate.Goal : Prop := ∀ (read lo hi : Int) (tail : List Int) scale,
  0 < scale → lo ≤ tail.sum → tail.sum ≤ hi →
  rounded (read+lo) scale = rounded (read+hi) scale →
  rounded (read+tail.sum) scale = rounded (read+lo) scale

/-- 強仮説。各 morsel の丸め誤差が最大半単位なら、局所丸めした和を直接返せるか。 -/
def LocalRounding.Goal : Prop := ∀ (parts : List Int) scale, 0 < scale →
  (∀ s ∈ parts, 2*(s-scale*rounded s scale).natAbs ≤ scale) →
  (parts.map (rounded · scale)).sum = rounded parts.sum scale

def dyadic (base : Nat) (x : Int × Nat) : Int := x.1 * 2^(x.2-base)

/-- 指数帯ごとに異なる基点で整数化し、積和を一度だけ共通精度にそろえて最後に丸める。 -/
def BinnedAccumulator.Goal : Prop := ∀ (groups : List (Nat × List (Int × Nat))) base,
  (∀ g ∈ groups, base ≤ g.1 ∧ ∀ x ∈ g.2, g.1 ≤ x.2) →
  ((groups.flatMap Prod.snd).map (dyadic base)).sum =
    (groups.map (fun (e,xs) => 2^(e-base) * (xs.map (dyadic e)).sum)).sum

def prefixes (xs : List Int) := (List.range (xs.length+1)).map (fun j => (xs.take j).sum)
def safe (xs : List Int) (init lo hi : Int) :=
  let ps := prefixes xs
  lo ≤ init + ps.min?.getD 0 && init + ps.max?.getD 0 ≤ hi

/-- 並列 SUM の溢れを群単位で検査し、初めて範囲を出たブロックだけを開けば元の失敗行を得る。 -/
def FirstOverflow.Goal : Prop := ∀ (blocks : List (List Int)) start lo hi, lo ≤ start → start ≤ hi →
  let bad := fun (bj : List Int × Nat) =>
    !safe bj.1 (start + ((blocks.take bj.2).map List.sum).sum) lo hi
  let i := blocks.zipIdx.findIdx bad
  let prior := (blocks.take i).flatten
  let block := blocks[i]?.getD []
  let failAt := fun (xs : List Int) (s : Int) =>
    (xs.zipIdx.findIdx (fun (_,j) => let v := s+(xs.take (j+1)).sum; v < lo || hi < v))
  failAt blocks.flatten start = prior.length + failAt block (start+prior.sum)

/-- 二次損失の総和を両入力の件数・和・平方和から計算し、対ごとの乗算と結合を省く。 -/
def QuadraticJoin.Goal : Prop := ∀ (a b : List Int),
  (a.flatMap (fun x => b.map (fun y => (x-y)^2))).sum =
  (b.length:Int)*(a.map (·^2)).sum + (a.length:Int)*(b.map (·^2)).sum - 2*a.sum*b.sum

/-- 低精度量子化した列の整数商と非零剰余数から AVG の上界を得て、HAVING を棄却する。 -/
def QuantizedAverage.Goal : Prop := ∀ (xs : List Int) scale cut, 0 < scale → xs ≠ [] →
  let qs := (xs.map (·/scale)).sum
  let missing := xs.countP (fun x => x%scale != 0)
  scale*qs + (missing:Int)*(scale-1) < cut*xs.length → xs.sum < cut*xs.length

/-- 行群の幅の上限から必要な乗算精度を決め、積の下位ビットだけで符号付きの群内積を復元する。 -/
def CenteredProduct.Goal : Prop := ∀ (rows : List (Int × Int)) (bits a b : Nat), 0 < bits →
  (∀ r ∈ rows, r.1.natAbs ≤ a ∧ r.2.natAbs ≤ b) → rows.length*a*b < 2^(bits-1) →
  let modulus : Int := 2^bits
  let residues := rows.map (fun (x,y) => ((x%modulus)*(y%modulus))%modulus)
  let z := lane modulus residues
  (rows.map (fun (x,y) => x*y)).sum = if 2*z < modulus then z else z-modulus

/-- 二列の中心化した積和から母共分散の分子を得る。群の基点を各列の平均から遠い値に取っても正確か。 -/
def ShiftedCovariance.Goal : Prop := ∀ (rows : List (Int × Int)) (ax ay : Int),
  let n : Int := rows.length
  let center := rows.map (fun (x,y) => (x-ax,y-ay))
  let numerator := fun (rs : List (Int × Int)) =>
    n*(rs.map (fun (x,y) => x*y)).sum - (rs.map Prod.fst).sum*(rs.map Prod.snd).sum
  numerator center = numerator rows

#guard rounded 1 2 == 0 && rounded 2 2 == 1 && rounded (-3) 2 == -2
#guard crt 3 5 2 (-1) 2 4 == 14

end TinylambOpt.ResearchGoals.Frontiers.Arithmetic
