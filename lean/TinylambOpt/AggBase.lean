import TinylambOpt.BagRel

/-
# Step 4-A: SQL 集約演算（GROUP BY / SUM / COUNT / MIN / MAX）の形式モデルと基礎定理

Join 越しの多段集約（Eager / Split Aggregation）を厳密に検証するため、
SQL 特有の **「空集合の SUM は NULL だが COUNT は 0」「GROUP BY は NULL 同士を 1 グループにまとめるが JOIN ON は NULL を弾く」**
という仕様を一切ごまかさずに定義します。
-/

namespace TinylambOpt

/-!
## 1. スカラー算術（NULL 伝播つき `+`, `*`, `COALESCE`）
-/

/-- SQL の `a + b`: どちらかが `NULL`（`none`）なら結果も `NULL`。 -/
def sqlAdd : SqlVal Int → SqlVal Int → SqlVal Int
  | some a, some b => some (a + b)
  | _,      _      => none

/-- SQL の `a * b`: どちらかが `NULL`（`none`）なら（たとえ相手が `0` でも！）結果は `NULL`。 -/
def sqlMul : SqlVal Int → SqlVal Int → SqlVal Int
  | some a, some b => some (a * b)
  | _,      _      => none

/-- SQL の `COALESCE(a, defaultVal)`: `a` が `NULL` のとき `defaultVal` に置換する。 -/
def sqlCoalesce {α : Type} (a : SqlVal α) (defaultVal : α) : α :=
  a.getD defaultVal

/-!
## 2. 可換モノイドによる集約アキュムレータの統一モデル
`tinylamb` の `AggregateAccumulator::Merge`（部分集約状態の統合）に対応する代数的構造です。
-/

/-- 集約状態のマージ則（可換モノイド）。 -/
structure AggMonoid (M : Type) where
  zero     : M
  op       : M → M → M
  op_assoc : ∀ a b c, op (op a b) c = op a (op b c)
  op_comm  : ∀ a b, op a b = op b a
  zero_op  : ∀ a, op zero a = a
  op_zero  : ∀ a, op a zero = a

/-- リスト全体のモノイド畳み込み（1 段の集約評価）。 -/
def aggFold {Row M : Type} (m : AggMonoid M) (f : Row → M) (r : Rel Row) : M :=
  r.foldr (fun x acc => m.op (f x) acc) m.zero

/-- `SUM` の状態マージ: `sqlAdd` と異なり、片方が `none`（非 NULL 行 0 件）ならもう片方の値をそのまま残す！ -/
def mergeSum : SqlVal Int → SqlVal Int → SqlVal Int
  | none,   b      => b
  | a,      none   => a
  | some a, some b => some (a + b)

def sumMonoid : AggMonoid (SqlVal Int) where
  zero := none
  op := mergeSum
  op_assoc a b c := by
    cases a <;> cases b <;> cases c <;> simp [mergeSum, Int.add_assoc]
  op_comm a b := by
    cases a <;> cases b <;> simp [mergeSum, Int.add_comm]
  zero_op a := by cases a <;> rfl
  op_zero a := by cases a <;> rfl

/-- `COUNT` の状態マージ: 初期値 `0`、加算 `+`（ただし 2 段目を SQL の `SUM` で書く場合は空集合で `NULL` になる罠あり！）。 -/
def countMonoid : AggMonoid Int where
  zero := 0
  op := (· + ·)
  op_assoc := Int.add_assoc
  op_comm := Int.add_comm
  zero_op := Int.zero_add
  op_zero := Int.add_zero

/-- `MIN` の状態マージ。 -/
def mergeMin : SqlVal Int → SqlVal Int → SqlVal Int
  | none,   b      => b
  | a,      none   => a
  | some a, some b => some (min a b)

def minMonoid : AggMonoid (SqlVal Int) where
  zero := none
  op := mergeMin
  op_assoc a b c := by
    cases a <;> cases b <;> cases c <;> simp [mergeMin, Int.min_assoc]
  op_comm a b := by
    cases a <;> cases b <;> simp [mergeMin, Int.min_comm]
  zero_op a := by cases a <;> rfl
  op_zero a := by cases a <;> rfl

/-- `MAX` の状態マージ。 -/
def mergeMax : SqlVal Int → SqlVal Int → SqlVal Int
  | none,   b      => b
  | a,      none   => a
  | some a, some b => some (max a b)

def maxMonoid : AggMonoid (SqlVal Int) where
  zero := none
  op := mergeMax
  op_assoc a b c := by
    cases a <;> cases b <;> cases c <;> simp [mergeMax, Int.max_assoc]
  op_comm a b := by
    cases a <;> cases b <;> simp [mergeMax, Int.max_comm]
  zero_op a := by cases a <;> rfl
  op_zero a := by cases a <;> rfl

/-- SQL の `SUM(expr)`: 全行 NULL または 0 行なら `none`（NULL）、そうでなければ非 NULL 値の合計 `some s`。 -/
def sqlSum {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) : SqlVal Int :=
  aggFold sumMonoid f r

/-- SQL の `COUNT(*)`: 行数をそのまま返す（0 行なら `0`）。 -/
def sqlCountStar {Row : Type} (r : Rel Row) : Int :=
  aggFold countMonoid (fun _ => 1) r

/-- SQL の `COUNT(expr)`: 非 NULL の行数だけを数える（0 行や全行 NULL なら `0`）。 -/
def sqlCountCol {Row α : Type} (f : Row → SqlVal α) (r : Rel Row) : Int :=
  aggFold countMonoid (fun x => if (f x).isSome then 1 else 0) r

/-- SQL の `MIN(expr)` / `MAX(expr)`。 -/
def sqlMin {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) : SqlVal Int :=
  aggFold minMonoid f r

def sqlMax {Row : Type} (f : Row → SqlVal Int) (r : Rel Row) : SqlVal Int :=
  aggFold maxMonoid f r

@[simp] theorem sqlSum_nil {Row : Type} (f : Row → SqlVal Int) :
    sqlSum f [] = none := rfl

@[simp] theorem sqlSum_cons {Row : Type} (f : Row → SqlVal Int) (x : Row) (xs : Rel Row) :
    sqlSum f (x :: xs) = mergeSum (f x) (sqlSum f xs) := rfl

@[simp] theorem sqlMin_nil {Row : Type} (f : Row → SqlVal Int) :
    sqlMin f [] = none := rfl

@[simp] theorem sqlMin_cons {Row : Type} (f : Row → SqlVal Int) (x : Row) (xs : Rel Row) :
    sqlMin f (x :: xs) = mergeMin (f x) (sqlMin f xs) := rfl

@[simp] theorem sqlMax_nil {Row : Type} (f : Row → SqlVal Int) :
    sqlMax f [] = none := rfl

@[simp] theorem sqlMax_cons {Row : Type} (f : Row → SqlVal Int) (x : Row) (xs : Rel Row) :
    sqlMax f (x :: xs) = mergeMax (f x) (sqlMax f xs) := rfl

@[simp] theorem sqlCountStar_nil {Row : Type} :
    sqlCountStar ([] : Rel Row) = 0 := rfl

@[simp] theorem sqlCountStar_cons {Row : Type} (x : Row) (xs : Rel Row) :
    sqlCountStar (x :: xs) = 1 + sqlCountStar xs := rfl

@[simp] theorem sqlCountCol_nil {Row α : Type} (f : Row → SqlVal α) :
    sqlCountCol f [] = 0 := rfl

@[simp] theorem sqlCountCol_cons {Row α : Type} (f : Row → SqlVal α) (x : Row) (xs : Rel Row) :
    sqlCountCol f (x :: xs) = (if (f x).isSome then 1 else 0) + sqlCountCol f xs := rfl

theorem sqlCountCol_eq_zero_of_sqlSum_none {Row : Type} (f : Row → SqlVal Int) :
    ∀ xs : Rel Row, sqlSum f xs = none → sqlCountCol f xs = 0
  | [], _ => rfl
  | x :: xs, h => by
    cases hx : f x with
    | none =>
      simp only [sqlSum_cons, hx, mergeSum] at h
      simp [hx, sqlCountCol_eq_zero_of_sqlSum_none f xs h]
    | some v =>
      simp only [sqlSum_cons, hx] at h
      cases hSub : sqlSum f xs <;> simp [hSub, mergeSum] at h

/-!
## 3. `aggFold` の基本法則（連結・`flatMap`・Bag 順列不変性）
-/

@[simp] theorem aggFold_nil {Row M : Type} (m : AggMonoid M) (f : Row → M) :
    aggFold m f [] = m.zero := rfl

@[simp] theorem aggFold_cons {Row M : Type} (m : AggMonoid M) (f : Row → M) (x : Row) (xs : Rel Row) :
    aggFold m f (x :: xs) = m.op (f x) (aggFold m f xs) := rfl

theorem aggFold_append {Row M : Type} (m : AggMonoid M) (f : Row → M) (r1 r2 : Rel Row) :
    aggFold m f (r1 ++ r2) = m.op (aggFold m f r1) (aggFold m f r2) := by
  induction r1 with
  | nil => simp [m.zero_op]
  | cons x xs ih => simp [ih, m.op_assoc]

theorem aggFold_flatMap {A B M : Type} (m : AggMonoid M) (f : B → M) (g : A → Rel B) (xs : Rel A) :
    aggFold m f (xs.flatMap g) = aggFold m (fun x => aggFold m f (g x)) xs := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.flatMap_cons, aggFold_append, ih]

/-- Bag 等価（行の並び替え `List.Perm`）な 2 つのテーブルは、どんな可換モノイド集約でも必ず同じ結果を返す。 -/
theorem aggFold_perm {Row M : Type} (m : AggMonoid M) (f : Row → M)
    {r1 r2 : Rel Row} (h : BagEq r1 r2) :
    aggFold m f r1 = aggFold m f r2 := by
  induction h with
  | nil => rfl
  | cons x _ ih => simp [ih]
  | swap x y xs =>
    simp only [aggFold_cons]
    rw [← m.op_assoc, m.op_comm (f y) (f x), m.op_assoc]
  | trans _ _ ih1 ih2 => exact ih1.trans ih2

theorem aggFold_map {A B M : Type} (m : AggMonoid M) (f : B → M) (g : A → B) (xs : Rel A) :
    aggFold m f (xs.map g) = aggFold m (fun x => f (g x)) xs := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

theorem aggFold_const_zero {A M : Type} (m : AggMonoid M) (xs : Rel A) :
    aggFold m (fun _ => m.zero) xs = m.zero := by
  induction xs with
  | nil => rfl
  | cons _ xs ih => simp [ih, m.zero_op]

theorem aggFold_op_distrib {A M : Type} (m : AggMonoid M) (g h : A → M) (xs : Rel A) :
    aggFold m (fun x => m.op (g x) (h x)) xs =
    m.op (aggFold m g xs) (aggFold m h xs) := by
  induction xs with
  | nil => simp [m.zero_op]
  | cons x xs ih =>
    simp only [aggFold_cons, ih]
    -- `(g x ⊕ h x) ⊕ (G ⊕ H) = (g x ⊕ G) ⊕ (h x ⊕ H)` を結合律と交換律で変形
    rw [m.op_assoc, ← m.op_assoc (h x), m.op_comm (h x), m.op_assoc, ← m.op_assoc]

/-- **有限 Fubini の定理（二重ループの集約順序交換）**:
    任意の可換モノイド集約において、`xs` と `ys` の二重走査の順序を入れ替えても結果は完全一致する。
    これが「非 Unique な M:N Join であっても片側事前集約が `SUM/COUNT/MIN/MAX` すべてで成り立つ」根本原理です！ -/
theorem aggFold_comm {A B M : Type} (m : AggMonoid M)
    (f : A → B → M) (xs : Rel A) (ys : Rel B) :
    aggFold m (fun x => aggFold m (fun y => f x y) ys) xs =
    aggFold m (fun y => aggFold m (fun x => f x y) xs) ys := by
  induction xs with
  | nil =>
    simp [aggFold_const_zero]
  | cons x xs ih =>
    simp only [aggFold_cons, ih, ← aggFold_op_distrib]

/-!
## 4. SQL `GROUP BY` の定義と「バケット分割の順列同値」定理
-/

/-- 出現順に重複を除いたユニークキー列を作る（`GROUP BY` のグループ一覧）。 -/
def distinctKeys {K : Type} [DecidableEq K] : List K → List K
  | [] => []
  | k :: ks => k :: (distinctKeys ks).filter (· ≠ k)

/-- SQL の `GROUP BY key`:
    入力 `r` に実在する各キー `k` について `(k, aggFn k bucket)` を 1 行ずつ出力する。
    `K = SqlVal Int` のとき `none == none` は `true` なので、SQL 仕様通り `NULL` 同士も 1 グループにまとまる！ -/
def groupBy {Row K V : Type} [DecidableEq K]
    (key : Row → K) (aggFn : K → Rel Row → V) (r : Rel Row) : Rel (K × V) :=
  (distinctKeys (r.map key)).map fun k =>
    (k, aggFn k (r.filter (fun row => key row == k)))

theorem mem_distinctKeys {K : Type} [DecidableEq K] (ks : List K) (x : K) :
    x ∈ distinctKeys ks ↔ x ∈ ks := by
  induction ks with
  | nil => simp [distinctKeys]
  | cons k ks ih =>
    simp only [distinctKeys, List.mem_cons, List.mem_filter, ne_eq, ih]
    by_cases hxk : x = k <;> simp [hxk]

theorem nodup_distinctKeys {K : Type} [DecidableEq K] (ks : List K) :
    (distinctKeys ks).Nodup := by
  induction ks with
  | nil => simp [distinctKeys]
  | cons k ks ih =>
    rw [distinctKeys, List.nodup_cons]
    refine ⟨?_, ih.filter _⟩
    simp [List.mem_filter]

theorem nodup_groupBy {Row K V : Type} [DecidableEq K]
    (key : Row → K) (fn : K → Rel Row → V) (r : Rel Row) :
    (groupBy key fn r).Nodup := by
  unfold groupBy
  have hN := nodup_distinctKeys (r.map key)
  generalize distinctKeys (r.map key) = ks at hN ⊢
  induction ks with
  | nil => simp
  | cons k ks ih =>
    rw [List.nodup_cons] at hN
    rw [List.map_cons, List.nodup_cons]
    refine ⟨?_, ih hN.2⟩
    rw [List.mem_map]
    rintro ⟨k', hk', hEq⟩
    have hkk : k' = k := congrArg Prod.fst hEq
    exact hN.1 (hkk ▸ hk')

/-- リストを述語 `p` の真偽で 2 分割して連結したものは元リストの順列（Bag 等価）。 -/
private theorem perm_filter_append_not {α : Type} (p : α → Bool) :
    ∀ r : List α, BagEq r (r.filter p ++ r.filter (fun x => !p x))
  | [] => List.Perm.nil
  | x :: xs => by
    have ih := perm_filter_append_not p xs
    cases hp : p x
    · simp only [List.filter, hp, Bool.not_false]
      exact (ih.cons x).trans List.perm_middle.symm
    · simp only [List.filter, hp, Bool.not_true, List.cons_append]
      exact ih.cons x

/-- **バケット分割の基本定理**:
    任意のテーブル `r` は、キー `key` ごとのバケット `r.filter (key · == k)` をすべて連結したものと Bag 等価！ -/
theorem perm_buckets_of_nodup {Row K : Type} [DecidableEq K]
    (key : Row → K) :
    ∀ (ks : List K) (r : Rel Row),
      ks.Nodup →
      (∀ row ∈ r, key row ∈ ks) →
      BagEq r (ks.flatMap (fun k => r.filter (fun row => key row == k)))
  | [], r, _, hCover => by
    cases r with
    | nil => exact List.Perm.nil
    | cons x _ =>
      have := hCover x (List.mem_cons_self ..)
      cases this
  | k :: ks, r, hNodup, hCover => by
    rw [List.nodup_cons] at hNodup
    obtain ⟨hkNotMem, hNodupKs⟩ := hNodup
    let rRest := r.filter (fun row => !(key row == k))
    have hSplit : BagEq r (r.filter (fun row => key row == k) ++ rRest) :=
      perm_filter_append_not (fun row => key row == k) r
    have hCoverRest : ∀ row ∈ rRest, key row ∈ ks := by
      intro row hMem
      rw [List.mem_filter] at hMem
      have hIn := hCover row hMem.1
      rw [List.mem_cons] at hIn
      cases hIn with
      | inl hEq =>
        have : (key row == k) = true := beq_iff_eq.mpr hEq
        rw [this] at hMem
        cases hMem.2
      | inr hInKs => exact hInKs
    have ih := perm_buckets_of_nodup key ks rRest hNodupKs hCoverRest
    -- `k' ∈ ks` なら `k' ≠ k` なので `rRest` からフィルタしても `r` からフィルタしても同じ
    have hSameBuckets :
        ks.flatMap (fun k' => rRest.filter (fun row => key row == k')) =
        ks.flatMap (fun k' => r.filter (fun row => key row == k')) := by
      have hElem : ∀ k' ∈ ks,
          rRest.filter (fun row => key row == k') = r.filter (fun row => key row == k') := by
        intro k' hk'
        have hne : k' ≠ k := fun hEq => hkNotMem (hEq ▸ hk')
        simp only [rRest, List.filter_filter]
        apply List.filter_congr
        intro row _
        by_cases hEq : key row = k'
        · simp [hEq, hne]
        · simp [beq_iff_eq, hEq]
      clear ih hCoverRest hCover hNodupKs hkNotMem hSplit
      induction ks with
      | nil => rfl
      | cons k1 ks1 ihk =>
        have hk1 := hElem k1 (List.mem_cons_self ..)
        have hks1 := ihk (fun z hz => hElem z (List.mem_cons_of_mem k1 hz))
        simp [List.flatMap_cons, hk1, hks1]
    rw [hSameBuckets] at ih
    simp only [List.flatMap_cons]
    exact hSplit.trans (ih.append_left _)

theorem perm_distinctKey_buckets {Row K : Type} [DecidableEq K]
    (key : Row → K) (r : Rel Row) :
    BagEq r ((distinctKeys (r.map key)).flatMap (fun k => r.filter (fun row => key row == k))) := by
  apply perm_buckets_of_nodup key (distinctKeys (r.map key)) r (nodup_distinctKeys _)
  intro row hMem
  rw [mem_distinctKeys]
  exact List.mem_map_of_mem hMem

/-- **2 段集約の基本分解定理**:
    どんな可換モノイド集約でも、全体を 1 度に畳み込んだ結果と、
    `distinctKeys` ごとの部分集約結果をさらに畳み込んだ結果は完全一致する！ -/
theorem aggFold_split_by_key {Row K M : Type} [DecidableEq K]
    (m : AggMonoid M) (key : Row → K) (f : Row → M) (r : Rel Row) :
    aggFold m f r =
    aggFold m Prod.snd (groupBy key (fun _ bucket => aggFold m f bucket) r) := by
  have hPerm := perm_distinctKey_buckets key r
  rw [aggFold_perm m f hPerm, aggFold_flatMap]
  unfold groupBy
  clear hPerm
  induction (distinctKeys (r.map key)) with
  | nil => rfl
  | cons k ks ih => simp [ih]

/-- 2 つの `Nodup` リストが同じ要素集合を持てば順列同値（`BagEq`）。 -/
theorem nodup_perm_of_iff {α : Type} [DecidableEq α]
    {l1 l2 : List α} (h1 : l1.Nodup) (h2 : l2.Nodup)
    (hMem : ∀ x, x ∈ l1 ↔ x ∈ l2) :
    BagEq l1 l2 := by
  induction l1 generalizing l2 with
  | nil =>
    cases l2 with
    | nil => exact List.Perm.nil
    | cons y ys =>
      have := (hMem y).mpr (List.mem_cons_self ..)
      cases this
  | cons x xs ih =>
    rw [List.nodup_cons] at h1
    have hx2 : x ∈ l2 := (hMem x).mp (List.mem_cons_self ..)
    have hSplit := perm_filter_append_not (· == x) l2
    -- `l2` は `Nodup` かつ `x ∈ l2` なので、`l2.filter (· == x)` はちょうど `[x]`！
    have hFilterEq : l2.filter (· == x) = [x] := by
      clear ih hMem hSplit
      induction l2 with
      | nil => cases hx2
      | cons y ys ihy =>
        rw [List.nodup_cons] at h2
        rw [List.mem_cons] at hx2
        cases hx2 with
        | inl hxy =>
          subst hxy
          have hNone : ys.filter (· == x) = [] := by
            rw [List.filter_eq_nil_iff]
            intro z hz
            simp only [beq_iff_eq]
            intro hzx
            exact h2.1 (hzx ▸ hz)
          simp [List.filter, hNone]
        | inr hxys =>
          have hne : (y == x) = false := beq_eq_false_iff_ne.mpr (fun hyx => h2.1 (hyx ▸ hxys))
          simp [List.filter, hne, ihy h2.2 hxys]
    rw [hFilterEq, List.singleton_append] at hSplit
    let l2Rest := l2.filter (fun y => !(y == x))
    have h2Rest : l2Rest.Nodup := h2.filter _
    have hMemRest : ∀ z, z ∈ xs ↔ z ∈ l2Rest := by
      intro z
      simp only [l2Rest, List.mem_filter, Bool.not_eq_true', beq_eq_false_iff_ne]
      constructor
      · intro hzxs
        have hzx : z ≠ x := fun hEq => h1.1 (hEq ▸ hzxs)
        exact ⟨(hMem z).mp (List.mem_cons_of_mem x hzxs), hzx⟩
      · rintro ⟨hzl2, hzx⟩
        have hzcons := (hMem z).mpr hzl2
        rw [List.mem_cons] at hzcons
        exact hzcons.resolve_left hzx
    exact (ih h1.2 h2Rest hMemRest).cons x |>.trans hSplit.symm

/-- **`GROUP BY` 同値判定マスター補題**:
    2 つのクエリ結果 `groupBy k1 f1 r1` と `groupBy k2 f2 r2` が Bag 等価であることを示すには：
    (1) 現れるグループキー集合が一致すること（幻影グループも欠落グループも無いこと）
    (2) 実在する各グループキー `g` に対して集約値が一致すること
    の 2 点だけを示せばよい！ -/
theorem groupBy_bagEq_of_keys_and_vals {R1 R2 K V : Type} [DecidableEq K]
    (k1 : R1 → K) (f1 : K → Rel R1 → V) (r1 : Rel R1)
    (k2 : R2 → K) (f2 : K → Rel R2 → V) (r2 : Rel R2)
    (hKeys : ∀ g : K, (∃ x ∈ r1, k1 x = g) ↔ (∃ y ∈ r2, k2 y = g))
    (hVals : ∀ g : K, (∃ x ∈ r1, k1 x = g) →
      f1 g (r1.filter (k1 · == g)) = f2 g (r2.filter (k2 · == g))) :
    BagEq (groupBy k1 f1 r1) (groupBy k2 f2 r2) := by
  unfold groupBy
  have hPermKeys : BagEq (distinctKeys (r1.map k1)) (distinctKeys (r2.map k2)) := by
    apply nodup_perm_of_iff (nodup_distinctKeys _) (nodup_distinctKeys _)
    intro g
    simp [mem_distinctKeys, List.mem_map, hKeys g]
  have hMapCongr :
      (distinctKeys (r1.map k1)).map (fun g => (g, f1 g (r1.filter (k1 · == g)))) =
      (distinctKeys (r1.map k1)).map (fun g => (g, f2 g (r2.filter (k2 · == g)))) := by
    apply List.map_congr_left
    intro g hg
    rw [mem_distinctKeys, List.mem_map] at hg
    rw [hVals g hg]
  rw [hMapCongr]
  exact hPermKeys.map _

end TinylambOpt
