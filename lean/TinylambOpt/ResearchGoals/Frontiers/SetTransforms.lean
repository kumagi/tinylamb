import TinylambOpt.ResearchGoals.Frontiers.Contractions

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.
-/

namespace TinylambOpt.ResearchGoals.Frontiers.SetTransforms

open Contractions (total)
abbrev Hist := Nat → Int
def sub (a b : Nat) := (a &&& b) == a
def below (d : Nat) (f : Hist) (k : Nat) := total (2^d) (fun i => if sub i k then f i else 0)
def paritySign (d x : Nat) : Int := if (List.range d).countP (fun i => x.testBit i) % 2 == 0 then 1 else -1
def chi (d k x : Nat) := paritySign d (k &&& x)
def spectrum (d : Nat) (f : Hist) (k : Nat) := total (2^d) (fun x => chi d k x * f x)
def combine (d : Nat) (op : Nat → Nat → Nat) (a b : Hist) (z : Nat) :=
  total (2^d) (fun i => total (2^d) (fun j => if op i j == z then a i * b j else 0))

/-- ビット属性の OR で結合した度数表を、部分集合累積表の点ごとの積として構築する。 -/
def OrJoin.Goal : Prop := ∀ d a b k, k < 2^d →
  below d (combine d (· ||| ·) a b) k = below d a k * below d b k

/-- 部分集合別のロールアップだけを保存し、要求された完全一致セルを Möbius 反転で復元する。 -/
def RecoverCell.Goal : Prop := ∀ d f k, k < 2^d →
  f k = total (2^d) (fun x => if sub x k then paritySign d (k ^^^ x) * below d f x else 0)

/-- フラグの対称差で分類する結合を Walsh 変換し、直積の代わりに二つの係数列を乗算する。 -/
def XorJoin.Goal : Prop := ∀ d a b k,
  spectrum d (combine d (· ^^^ ·) a b) k = spectrum d a k * spectrum d b k

/-- 符号付きヒストグラムの等値結合 SUM を、変換領域の内積一回から復元する。 -/
def SpectralDot.Goal : Prop := ∀ d (a b : Hist),
  (2^d : Nat) * total (2^d) (fun i => a i*b i) =
    total (2^d) (fun k => spectrum d a k * spectrum d b k)

/-- 差分の周波数成分がクエリと交わらないとき、そのクエリのキャッシュ更新を全廃する。 -/
def InvisibleRefresh.Goal : Prop := ∀ d (query delta : Hist),
  (∀ k < 2^d, spectrum d query k = 0 ∨ spectrum d delta k = 0) →
  total (2^d) (fun i => query i * delta i) = 0

/-- 高次係数の絶対値予算から HAVING の上界を得て、残りの復号を省く。 -/
def TruncatedHaving.Goal : Prop := ∀ d (a b : Hist) (keep : Nat → Bool) (cap : Nat) (cut : Int),
  let prod := fun k => spectrum d a k * spectrum d b k
  total (2^d) (fun k => if keep k then 0 else ((prod k).natAbs:Int)) ≤ cap →
  total (2^d) (fun k => if keep k then prod k else 0) + cap < (2^d : Nat)*cut →
  total (2^d) (fun i => a i*b i) < cut

/-- 一つのフラグで群を分割するとき、二つの子の変換済み状態を加減算だけで合成する。 -/
def SplitButterfly.Goal : Prop := ∀ d (f : Hist) k, k < 2^d →
  let low := spectrum d f k
  let high := spectrum d (fun i => f (2^d+i)) k
  (spectrum (d+1) f k, spectrum (d+1) f (2^d+k)) = (low+high,low-high)

/-- ディメンションを一つ捨てる ROLLUP は、その軸の符号を反転しない係数だけを引き継ぐ。 -/
def ProjectAxis.Goal : Prop := ∀ d (f : Hist) k, k < 2^d →
  spectrum d (fun i => f (2*i) + f (2*i+1)) k = spectrum (d+1) f (2*k)

def marginal (mask val : Nat) (f : Hist) := total 8 (fun i => if (i &&& mask) == val then f i else 0)

/-- 強仮説。三つの二値属性の全二属性周辺表が一致すれば、任意の三属性フィルタも交換できるか。 -/
def PairStats.Goal : Prop := ∀ (a b : Hist),
  (∀ m ∈ [3,5,6], ∀ v < 8, marginal m v a = marginal m v b) → ∀ v < 8, a v = b v

/-- 互いに素なフラグ集合だけを通す結合を、相手側の補集合マスクに対する部分集合照会へ変換する。 -/
def DisjointJoin.Goal : Prop := ∀ d (a b : Hist),
  combine d (· &&& ·) a b 0 = total (2^d) (fun x => a x * below d b ((2^d-1) ^^^ x))

private def evenHist (x : Nat) : Int := if paritySign 3 x == 1 then 1 else 0
private def oddHist (x : Nat) : Int := 1-evenHist x
#guard [3,5,6].all (fun m => (List.range 8).all (fun v => marginal m v evenHist == marginal m v oddHist))
#guard evenHist 0 != oddHist 0

end TinylambOpt.ResearchGoals.Frontiers.SetTransforms
