import TinylambOpt.ResearchGoals.Frontiers.Contractions
import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

namespace TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-! Finite bag joins. Each atom names a physical binary relation; repeated names
are aliases. Some variables are pinned by the current GROUP BY/correlated key.
Signed multiplicities also model deltas. The cardinality bound uses set support. -/
structure Atom where
  rel : Nat
  src : Nat
  dst : Nat
  deriving Repr
abbrev Bind := Nat → Nat
abbrev DB := Nat → Nat → Nat → Int
structure Pattern where
  atoms : List Atom
  freeVars : List Nat -- also the physical elimination order

def put (b : Bind) (v x : Nat) : Bind := fun i => if i == v then x else b i
def integrate (n : Nat) : List Nat → Bind → (Bind → Int) → Int
  | [], b, f => f b
  | v::vs, b, f => ((List.range n).map (fun x => integrate n vs (put b v x) f)).sum

def value (db : DB) (a : Atom) (b : Bind) := db a.rel (b a.src) (b a.dst)
def reference (p : Pattern) (n : Nat) (db : DB) (b : Bind) :=
  integrate n p.freeVars b (fun x => (p.atoms.map (fun a => value db a x)).prod)

structure Factor where
  scope : List Nat
  get : Bind → Int

def factors (p : Pattern) (db : DB) : List Factor :=
  p.atoms.map (fun a => ⟨[a.src,a.dst], value db a⟩)
def product (fs : List Factor) (x : Bind) := (fs.map (fun f => f.get x)).prod
def eliminate (n v : Nat) (fs : List Factor) :=
  let (incident,rest) := fs.partition (fun f => f.scope.contains v)
  let boundary := (incident.flatMap Factor.scope).eraseDups.filter (· != v)
  let msg : Factor := ⟨boundary, fun x => integrate n [v] x (product incident)⟩
  msg :: rest

def elimination (p : Pattern) (n : Nat) (db : DB) (b : Bind) :=
  product (p.freeVars.foldl (fun fs v => eliminate n v fs) (factors p db)) b

/-- Pin a changed edge first. Only its other free variables are enumerated. -/
def pivotTerm (p : Pattern) (n : Nat) (old new : DB) (b : Bind) (j : Nat) (a : Atom) :=
  let candidates := fun v => if p.freeVars.contains v then List.range n else [b v]
  let vars := p.freeVars.filter (fun v => v != a.src && v != a.dst)
  ((candidates a.src).map (fun u => (((candidates a.dst).filter (fun v =>
    old a.rel u v != new a.rel u v)).map (fun v =>
      let delta := new a.rel u v - old a.rel u v
      delta * integrate n vars (put (put b a.src u) a.dst v) (fun x =>
        ((p.atoms.take j).map (fun t => value new t x)).prod *
        ((p.atoms.drop (j+1)).map (fun t => value old t x)).prod))).sum)).sum

def support (n : Nat) (db : DB) (a : Atom) (p : Pattern) (b : Bind) :=
  let domain := fun v => if p.freeVars.contains v then List.range n else [b v]
  ((domain a.src).map (fun x => (domain a.dst).countP (fun y => db a.rel x y != 0))).sum
def cover (p : Pattern) (w : List Nat) (den : Nat) : Prop :=
  0 < den ∧ w.length = p.atoms.length ∧ ∀ v ∈ p.freeVars,
    den ≤ ((p.atoms.zip w).map (fun (a,k) => if v == a.src || v == a.dst then k else 0)).sum

def slope (old delta : DB) (t : Int) : DB := fun r x y => old r x y + t*delta r x y
def diff : Nat → (Int → Int) → Int → Int
  | 0,f,t => f t
  | k+1,f,t => diff k f (t+1) - diff k f t
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples (choose)
def forecast (f : Int → Int) (degree t : Nat) :=
  ((List.range (degree+1)).map (fun k => (choose t k : Int) * diff k f 0)).sum

/-- Enumerate equality partitions by inserting the new variable in one old block
or in a fresh singleton. The signed factorial is the partition Möbius weight. -/
def partitions : List Nat → List (List (List Nat))
  | [] => [[]]
  | v::vs => (partitions vs).flatMap (fun blocks => ([v]::blocks) ::
    blocks.zipIdx.map (fun (_,i) => blocks.modify i (v::·)))
def factorial : Nat → Nat | 0 => 1 | k+1 => (k+1)*factorial k
def mobius (bs : List (List Nat)) : Int :=
  (bs.map (fun block => (-1:Int)^(block.length-1) * factorial (block.length-1))).prod
def collapsed (p : Pattern) (n : Nat) (db : DB) (b : Bind) (bs : List (List Nat)) :=
  let root := fun v => ((bs.find? (·.contains v)).bind List.head?).getD v
  integrate n (bs.filterMap List.head?) b (fun x =>
    (p.atoms.map (fun a => value db a (fun v => x (root v)))).prod)
def injective (p : Pattern) (n : Nat) (db : DB) (b : Bind) :=
  integrate n p.freeVars b (fun x => if (p.freeVars.map x).Nodup then
    (p.atoms.map (fun a => value db a x)).prod else 0)

open TinylambOpt.ResearchGoals.Frontiers.Contractions (total dot)
def bridge (n : Nat) (db : DB) (r s x y : Nat) :=
  total n (fun k => db r x k * db s k y)
def overlap (n : Nat) (db : DB) (r x y : Nat) := dot n (db r x) (db r y)
def loop3 (n : Nat) (db : DB) (r s t x : Nat) :=
  total n (fun y => bridge n db r s x y * db t y x)
def normSq (n : Nat) (db : DB) (r : Nat) := total n (fun x => dot n (db r x) (db r x))

namespace MovieCredits

/-- JOB 型の映画・人物・役柄三角結合。 -/
def pattern : Pattern :=
  ⟨[⟨0, 1, 2⟩, ⟨1, 2, 3⟩, ⟨2, 3, 1⟩], [2, 3, 1]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 三つの辺の二乗ノルムから三角結合の絶対値を制限し、展開前に HAVING を判定する。 -/
def TriangleNorm.Goal : Prop :=
  ∀ n db b, (reference pattern n db b)^2 ≤ normSq n db 0 * normSq n db 1 * normSq n db 2

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end MovieCredits

namespace KeywordCompany

/-- 映画からキーワードと制作会社に分岐して地域に再合流する。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨1, 0, 2⟩, ⟨2, 1, 3⟩, ⟨3, 2, 3⟩], [1, 2, 3]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 二枝の終点だけを共有する二つの橋表を作り、映画ごとの終点内積で合流させる。 -/
def TerminalKey.Goal : Prop :=
  ∀ n db b, reference pattern n db b =
    dot n (bridge n db 0 2 (b 0)) (bridge n db 1 3 (b 0))

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end KeywordCompany

namespace CastOverlap

/-- 同じ二人が複数映画に出演する四辺形。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 2⟩, ⟨0, 1, 2⟩, ⟨0, 1, 3⟩, ⟨0, 0, 3⟩], [2, 3, 0, 1]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 二映画の配役重複を一度だけ数え、その二乗和で全四辺形の多重度を得る。 -/
def GramSquare.Goal : Prop :=
  ∀ n db b, reference pattern n db b = total n (fun x => total n (fun y => (overlap n db 0 x y)^2))

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end CastOverlap

namespace DoubleBilling

/-- 映画の二重配役と人物属性の同時集計。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 0, 3⟩], [1, 3, 2]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 同じ配役辺の二重出現に対する二階差分を、変更辺だけの平方付き結合から計算する。 -/
def SquaredDelta.Goal : Prop :=
  ∀ n db delta b, (∀ r x y, r ≠ 0 → delta r x y = 0) →
    diff 2 (fun u => reference pattern n (slope db delta u) b) 0 =
      2*reference pattern n (fun r => if r == 0 then delta 0 else db r) b

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end DoubleBilling

namespace PostDiamond

/-- CEB の投稿リンクが分岐して同じ投稿に戻る経路。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨0, 0, 2⟩, ⟨0, 1, 3⟩, ⟨0, 2, 3⟩], [1, 2, 3]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 同一投稿から同じ終点へ至る二段の経路数を一度求め、分岐再合流の全組を二乗で得る。 -/
def SquarePathCounts.Goal : Prop :=
  ∀ n db b, reference pattern n db b = total n (fun t => (bridge n db 0 0 (b 0) t)^2)

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end PostDiamond

namespace VoteBadge

/-- 投稿・投票・バッジをユーザーで束ねる。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 1, 3⟩, ⟨3, 0, 4⟩, ⟨3, 4, 5⟩], [2, 3, 5, 4, 1]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 利用者を固定した二つの独立な結合枝を別集計し、枝同士の直積を省く。 -/
def IndependentArms.Goal : Prop :=
  ∀ n db b, reference pattern n db b =
    total n (fun u => db 0 (b 0) u * total n (db 1 u) * total n (db 2 u)) *
    total n (fun v => db 3 (b 0) v * total n (db 3 v))

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end VoteBadge

namespace ReplyCycle

/-- 同じ返信関係を四回使う閉路。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨0, 1, 2⟩, ⟨0, 2, 3⟩, ⟨0, 3, 0⟩], [1, 2, 3, 0]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 無向なら二段の到達数表の二乗和だけから長さ四の閉路の全多重度を得る。 -/
def UndirectedTrace.Goal : Prop :=
  ∀ n db b, (∀ i < n, ∀ j < n, db 0 i j = db 0 j i) →
    reference pattern n db b = total n (fun i => total n (fun j => (bridge n db 0 0 i j)^2))

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end ReplyCycle

namespace FriendBridge

/-- LDBC 型の友人・like・message・国をまたぐ経路。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 2, 3⟩, ⟨3, 3, 4⟩], [1, 2, 3]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 出発人物と終端の国が決まった四段経路を、中央キーの両方向到達質量で突き合わせる。 -/
def BidirectionalMeet.Goal : Prop :=
  ∀ n db b, reference pattern n db b =
    dot n (bridge n db 0 1 (b 0)) (fun u => bridge n db 2 3 u (b 4))

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end FriendBridge

namespace CoLikes

/-- 二人の異なる反応経路が二つの投稿で交差する。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 2⟩, ⟨0, 1, 2⟩, ⟨0, 1, 3⟩, ⟨0, 0, 3⟩, ⟨2, 0, 1⟩], [2, 3, 0]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 同じ投稿を二度数えない共通反応経路だけを残すため、同一投稿由来の二乗寄与を引く。 -/
def DistinctMessages.Goal : Prop :=
  ∀ n db b,
    integrate n pattern.freeVars b (fun x => if x 2 == x 3 then 0 else product (factors pattern db) x) =
    total n (fun u => db 2 u (b 1) * ((overlap n db 0 u (b 1))^2 -
      total n (fun j => (db 0 u j * db 0 (b 1) j)^2)))

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end CoLikes

namespace ForumBowtie

/-- 人物を共有する二つの三角関係。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 2, 0⟩, ⟨0, 0, 3⟩, ⟨1, 3, 4⟩, ⟨2, 4, 0⟩], [1, 2, 4, 3, 0]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 一頂点で接する同形の二つの三角形を一種類の三角状態に畳む。 -/
def ArticulationSquare.Goal : Prop :=
  ∀ n db b, reference pattern n db b = total n (fun x => (loop3 n db 0 1 2 x)^2)

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end ForumBowtie

namespace CountryReturn

/-- 国を固定した二方向の国別経路。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 2, 3⟩, ⟨1, 3, 4⟩, ⟨2, 0, 4⟩], [1, 2, 3, 4]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 同じ更新を t 倍した結合を、変更表が出現する回数までの前進差分から再評価する。 -/
def DeltaForecast.Goal : Prop :=
  ∀ n db delta b table (t : Nat), (∀ r x y, r ≠ table → delta r x y = 0) →
    let eval := fun u => reference pattern n (slope db delta u) b
    eval t = forecast eval (pattern.atoms.countP (fun a => a.rel == table)) t

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end CountryReturn

namespace OrderSupplier

/-- TPC-H の注文・供給者・国の二経路が地域で一致する。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 2, 3⟩, ⟨1, 0, 4⟩, ⟨2, 4, 3⟩], [1, 2, 4, 3]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 注文から地域への三段と二段の経路を、同じ地域への二つの質量から復元する。 -/
def SharedDestination.Goal : Prop :=
  ∀ n db b, reference pattern n db b = total n (fun z =>
    total n (fun y => bridge n db 0 1 (b 0) y * db 2 y z) * bridge n db 1 2 (b 0) z)

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end OrderSupplier

namespace ReturnedItem

/-- 販売と返品が同じ品目・倉庫を共有する。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 2, 3⟩, ⟨3, 3, 0⟩, ⟨2, 0, 2⟩], [1, 2, 3]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 品目と販売キーの間の弦だけをプローブし、両方向の二段経路を掛ける。 -/
def AnchoredChord.Goal : Prop :=
  ∀ n db b, reference pattern n db b = total n (fun y => db 2 (b 0) y *
    bridge n db 0 1 (b 0) y * bridge n db 2 3 y (b 0))

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end ReturnedItem

namespace ChannelBridge

/-- 三販売チャネルが商品と顧客を共有する。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 2⟩, ⟨0, 1, 2⟩, ⟨1, 0, 3⟩, ⟨1, 1, 3⟩, ⟨2, 0, 4⟩, ⟨2, 1, 4⟩], [2, 3, 4, 1]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 三販売チャネルを別々に転置し、二つの端点の重複度をチャネルごとに乗算する。 -/
def TripleOverlap.Goal : Prop :=
  ∀ n db b, reference pattern n db b = total n (fun y =>
    overlap n db 0 (b 0) y * overlap n db 1 (b 0) y * overlap n db 2 (b 0) y)

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end ChannelBridge

namespace PromoInventory

/-- 販売、販促、在庫が日付と商品を共有する。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨0, 1, 2⟩, ⟨0, 2, 3⟩, ⟨0, 3, 0⟩, ⟨2, 0, 2⟩, ⟨1, 1, 3⟩], [1, 2, 0, 3]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 同じ更新を t 倍した結合を、変更表が出現する回数までの前進差分から再評価する。 -/
def DeltaForecast.Goal : Prop :=
  ∀ n db delta b table (t : Nat), (∀ r x y, r ≠ table → delta r x y = 0) →
    let eval := fun u => reference pattern n (slope db delta u) b
    eval t = forecast eval (pattern.atoms.countP (fun a => a.rel == table)) t

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end PromoInventory

namespace TwoHopAntiWitness

/-- anti-join で非存在を調べる二種類の妨害経路。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 0, 3⟩, ⟨1, 3, 2⟩, ⟨0, 2, 4⟩], [1, 3, 4, 2]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 正の妨害経路の有無だけ必要な anti-join では、共有する終点ごとに二枝の存在を別判定する。 -/
def IndependentWitness.Goal : Prop :=
  ∀ n db b, (∀ r i j, 0 ≤ db r i j) →
    (0 < reference pattern n db b ↔ ∃ z < n, 0 < bridge n db 0 1 (b 0) z ∧
      0 < bridge n db 2 1 (b 0) z ∧ 0 < total n (db 0 z))

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end TwoHopAntiWitness

namespace HubWithChords

/-- H2O 型の中心キーから五枝を引き二組の枝を照合する。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨1, 0, 2⟩, ⟨2, 0, 3⟩, ⟨3, 0, 4⟩, ⟨4, 0, 5⟩, ⟨5, 1, 2⟩, ⟨6, 3, 4⟩], [1, 2, 5, 4, 3, 0]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 中心の左右にある二つの弦と単独の一枝を、中心ごとの三因子に分解する。 -/
def ChordComponents.Goal : Prop :=
  ∀ n db b,
    let arm := fun (x a d e : Nat) => total n (fun i => db a x i * dot n (db e i) (db d x))
    reference pattern n db b = total n (fun x => arm x 0 1 5 * arm x 2 3 6 * total n (db 4 x))

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end HubWithChords

namespace SupplierClique

/-- 同一供給網の四頂点完全結合。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨0, 0, 2⟩, ⟨0, 0, 3⟩, ⟨0, 1, 2⟩, ⟨0, 1, 3⟩, ⟨0, 2, 3⟩], [3, 2, 1, 0]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 無向四クリークでは厳密に増加する頂点 ID だけに結合を制限し、軌道の大きさ 24 で戻す。 -/
def OrbitRepresentative.Goal : Prop :=
  ∀ n db b, (∀ i < n, ∀ j < n, db 0 i j = db 0 j i) →
    injective pattern n db b = 24*integrate n pattern.freeVars b (fun x =>
      if x 0 < x 1 && x 1 < x 2 && x 2 < x 3 then product (factors pattern db) x else 0)

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end SupplierClique

namespace IntersectingCycles

/-- 時刻とキーで辺を共有する二つの再帰パス。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨0, 1, 2⟩, ⟨0, 2, 3⟩, ⟨0, 3, 0⟩, ⟨1, 2, 4⟩, ⟨2, 4, 5⟩, ⟨2, 5, 3⟩], [4, 5, 1, 2, 3, 0]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 交差する二閉路を二つの境界変数で切り、三つの独立な内部経路に分解する。 -/
def TwoSeparators.Goal : Prop :=
  ∀ n db b, reference pattern n db b = total n (fun y => total n (fun z =>
    db 0 y z * total n (fun u => bridge n db 0 0 z u * db 0 u y) *
      total n (fun v => bridge n db 1 2 y v * db 2 v z)))

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end IntersectingCycles

namespace ParallelLineage

/-- 共有元表の重複エイリアスを持つ更新系の導出。 -/
def pattern : Pattern :=
  ⟨[⟨0, 0, 1⟩, ⟨0, 1, 2⟩, ⟨0, 0, 1⟩, ⟨1, 2, 3⟩, ⟨1, 3, 0⟩, ⟨2, 3, 4⟩], [4, 1, 2, 3]⟩

/-- 境界変数だけの部分表を順次作り、袋の重み付き結果を保つ。 -/
def BoundaryMessages.Goal : Prop :=
  ∀ n db b, elimination pattern n db b = reference pattern n db b

/-- 変化した辺に端点を固定し、自己結合の複数箇所への更新も正味一度ずつ課金する。 -/
def AnchoredRepair.Goal : Prop :=
  ∀ n old new b, reference pattern n new b - reference pattern n old b =
    (pattern.atoms.zipIdx.map (fun (a,i) => pivotTerm pattern n old new b i a)).sum

/-- 射影済み辺の非零件数と任意の分数辺被覆から中間展開前に結果支持数を制限する。 -/
def FractionalCap.Goal : Prop :=
  ∀ n db b weights den, cover pattern weights den →
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
      ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod

/-- 同じ更新を t 倍した結合を、変更表が出現する回数までの前進差分から再評価する。 -/
def DeltaForecast.Goal : Prop :=
  ∀ n db delta b table (t : Nat), (∀ r x y, r ≠ table → delta r x y = 0) →
    let eval := fun u => reference pattern n (slope db delta u) b
    eval t = forecast eval (pattern.atoms.countP (fun a => a.rel == table)) t

/-- 異なる自由変数に同一 ID を許さない問い合わせを、同値類で縮約した通常結合の符号付き和へ変える。 -/
def CollisionCorrection.Goal : Prop :=
  ∀ n db b, injective pattern n db b = ((partitions pattern.freeVars).map
    (fun bs => mobius bs * collapsed pattern n db b bs)).sum

end ParallelLineage


#guard let e : DB := fun _ x y => if x != y then 1 else 0
       reference MovieCredits.pattern 3 e (fun _ => 0) == 6 &&
         elimination MovieCredits.pattern 3 e (fun _ => 0) == 6
#guard choose 5 2 == 10 && (partitions [0,1,2,3]).length == 15

end TinylambOpt.ResearchGoals.Atlas.JoinPlans
