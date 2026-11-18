import TinylambOpt.AggBase
import TinylambOpt.TpcdsDiscovery
import TinylambOpt.ScalarOverflowAndCoreAlgebra

/-
# Step 13: TPC-DS 全勝に向けた証明付きアグレッシブ実行カーネル・ローワリング規則

TPC-DS ベンチマークのプロファイリングで判明した主要ボトルネック群を解消する **6 系統の実行コード生成・
物理ローワリング最適化** と、うっかり踏むと誤答を起こす **三値論理 / NULL / 非単射トラップ** を
Lean 4 で機械検証（`sorry` ゼロ）します。

## 収録する 6 大実行最適化定理

1. **定数集合 `IN (...)` の事前コンパイル O(1) ハッシュ集合 + NULL フラグ三値評価 (`in_const_set_3vl_sound`)**
   - 対象: **Q08**（400 郵便番号 `IN`）、**Q15**, **Q48**, **Q10**, **Q69**
2. **ASCII 1-始まり `SUBSTR(s, pos, len)` のゼロコピー・スライス還元 (`ascii_substr_1based_slice_sound`)**
   - 対象: **Q08**, **Q15**, **Q79**, **Q62**, **Q99**
3. **二値全域述語（`IS [NOT] NULL / TRUE / FALSE`）上の `NOT` 対合正規化 (`two_valued_predicate_not_involution_sound`)**
   - 対象: **Q97**, **Q72**, **Q51**, **Q83**
4. **事前構築 `EXISTS / NOT EXISTS` の外側非 NULL ドメイン制限 ＆ 結合前プッシュダウン (`prebuilt_exists_outer_domain_and_pushdown_sound`)**
   - 対象: **Q10**, **Q35**, **Q69**, **Q16**
5. **融合 `AntiJoin` グループ集約カーネル (`fused_antijoin_groupby_sound`)**
   - 対象: **Q78**（`(sales ⋈ date_dim) ▷ returns` の中間表マテリアライズ完全排除・`GroupedOutputSink` 直結）
6. **`UNION DISTINCT / ALL` + `GROUP BY` 共有 CTE への選言グループキー制限貫通 (`shared_grouped_union_cte_disjunctive_pushdown_sound`) ＆ `EXCEPT / INTERSECT` サロゲートキー先行重複排除 (`eager_distinct_surrogate_keys_setop_sound`)**
   - 対象: **Q75**, **Q87**, **Q38**, **Q02**
-/

namespace TinylambOpt

/-!
## 1. 定数リスト `v IN (c_1, ..., c_n)` の O(1) ハッシュ集合 + NULL フラグ三値評価
-/

/-- SQL の基準セマンティクス: `v IN (c_1, ..., c_n)` の逐次 OR 評価。
    空リストなら `.tFalse`、非空リストなら `sqlEq v c_1 OR ... OR sqlEq v c_n`。 -/
def evalInSequential {α : Type} [DecidableEq α] (v : SqlVal α) : List (SqlVal α) → SqlBool
  | [] => .tFalse
  | [c] => sqlEq v c
  | c :: rest => (sqlEq v c).or (evalInSequential v rest)

/-- バイトコード `BytecodeOp::kInStringSet` / `FlatIntSet` コンパイル時表現:
    非 NULL 定数リスト `nonNulls`（実行時は O(1) フラットハッシュ集合）と `hasNull` フラグ、および元リスト非空フラグ `nonEmpty`。 -/
structure CompiledInConstSet (α : Type) where
  nonNulls : List α
  hasNull  : Bool
  nonEmpty : Bool

def compileInConstSet {α : Type} (cs : List (SqlVal α)) : CompiledInConstSet α :=
  { nonNulls := cs.filterMap id
    hasNull  := cs.any Option.isNone
    nonEmpty := !cs.isEmpty }

/-- O(1) ハッシュ集合メンバシップによる三値評価カーネル。 -/
def evalCompiledInConstSet {α : Type} [DecidableEq α]
    (v : SqlVal α) (s : CompiledInConstSet α) : SqlBool :=
  if !s.nonEmpty then
    .tFalse
  else
    match v with
    | none => .tNull
    | some x =>
      if s.nonNulls.contains x then .tTrue
      else if s.hasNull then .tNull
      else .tFalse

/-- ❌ **罠 1 (`in_const_set_null_probe_trap`)**:
    `v = NULL` のとき、リストが空なら `FALSE` だが、非空リスト `['CA']` に対しては `hasNull = false` でも
    **`NULL`（`.tNull`）** を返さなければならない！（うっかり `hasNull` だけ見て `.tFalse` を返すと `NOT (NULL IN ('CA'))` が `TRUE` に化けてバグる！） -/
theorem in_const_set_null_probe_trap :
    evalInSequential (none : SqlVal Int) [] = .tFalse ∧
    evalInSequential (none : SqlVal Int) [some 1] = .tNull ∧
    (evalInSequential (none : SqlVal Int) [some 1]).not = .tNull := by
  decide

private theorem evalInSequential_null_nonempty {α : Type} [DecidableEq α] :
    ∀ (c : SqlVal α) (cs : List (SqlVal α)),
      evalInSequential none (c :: cs) = .tNull
  | c, [] => by cases c <;> rfl
  | c, d :: cs => by
    have ih := evalInSequential_null_nonempty d cs
    change (sqlEq none c).or (evalInSequential none (d :: cs)) = .tNull
    rw [ih]
    cases c <;> rfl

private theorem contains_filterMap_id_some {α : Type} [DecidableEq α]
    (x y : α) (cs : List (SqlVal α)) :
    (List.filterMap id (some y :: cs)).contains x =
      (decide (x = y) || (List.filterMap id cs).contains x) := by
  change List.contains (y :: List.filterMap id cs) x =
    (decide (x = y) || (List.filterMap id cs).contains x)
  rw [List.contains_cons]
  by_cases h : x = y <;> simp [h]

private theorem evalCompiled_nonempty_cons {α : Type} [DecidableEq α]
    (x : α) (c : SqlVal α) (cs : List (SqlVal α)) :
    evalCompiledInConstSet (some x) (compileInConstSet (c :: cs)) =
      if (List.filterMap id (c :: cs)).contains x = true then .tTrue
      else if (c :: cs).any Option.isNone = true then .tNull
      else .tFalse := rfl

/-- ✅ **定理 1 (`in_const_set_3vl_sound` — Q08, Q15, Q48, Q10, Q69)**:
    任意の型 `α`（バイナリ照合文字列・整数・日付）、任意の NULL 許容プローブ値 `v`、
    および任意の定数リスト `cs`（重複や `NULL` 要素を含んでよい）に対し、
    コンパイル時に構築した `(nonNulls, hasNull, nonEmpty)` による O(1) 三値評価は
    逐次 `OR` チェーン評価 `evalInSequential` と **三値すべて（`.tTrue / .tFalse / .tNull`）で完全一致** する！ -/
theorem in_const_set_3vl_sound {α : Type} [DecidableEq α]
    (v : SqlVal α) (cs : List (SqlVal α)) :
    evalCompiledInConstSet v (compileInConstSet cs) = evalInSequential v cs := by
  cases cs with
  | nil =>
    simp [compileInConstSet, evalCompiledInConstSet, evalInSequential]
  | cons c rest =>
    cases v with
    | none =>
      rw [evalInSequential_null_nonempty]
      simp [compileInConstSet, evalCompiledInConstSet]
    | some x =>
      induction rest generalizing c with
      | nil =>
        cases c with
        | none =>
          simp [compileInConstSet, evalCompiledInConstSet, evalInSequential, sqlEq]
        | some y =>
          by_cases hxy : x = y
          · subst hxy
            simp [compileInConstSet, evalCompiledInConstSet, evalInSequential, sqlEq]
          · simp [compileInConstSet, evalCompiledInConstSet, evalInSequential, sqlEq, hxy]
      | cons d ds ih =>
        have hStep : evalInSequential (some x) (c :: d :: ds) =
            (sqlEq (some x) c).or (evalInSequential (some x) (d :: ds)) := rfl
        rw [hStep, ← ih d, evalCompiled_nonempty_cons, evalCompiled_nonempty_cons]
        cases c with
        | none =>
          have hEqNull : sqlEq (some x) none = .tNull := rfl
          have hFiltNone : List.filterMap id (none :: d :: ds) = List.filterMap id (d :: ds) := rfl
          have hAnyNone : (none :: d :: ds).any Option.isNone = true := rfl
          rw [hEqNull, hFiltNone, hAnyNone]
          cases (List.filterMap id (d :: ds)).contains x <;>
            cases (d :: ds).any Option.isNone <;> rfl
        | some y =>
          rw [contains_filterMap_id_some]
          have hAnySome : (some y :: d :: ds).any Option.isNone = (d :: ds).any Option.isNone := rfl
          rw [hAnySome]
          by_cases hxy : x = y
          · subst hxy
            simp [sqlEq, SqlBool.or]
          · have hEqFalse : sqlEq (some x) (some y) = .tFalse := by simp [sqlEq, hxy]
            rw [hEqFalse]
            simp only [hxy, decide_false, Bool.false_or]
            cases (List.filterMap id (d :: ds)).contains x <;>
              cases (d :: ds).any Option.isNone <;> rfl

/-!
## 2. ASCII 1-始まり `SUBSTR(s, pos, len)` のゼロコピー・スライス還元
-/

/-- SQL 1-始まり `SUBSTR` の文字単位参照モデル（`pos ≥ 1` のとき `pos - 1` 文字目から最大 `len` 文字、`pos = 0` は先頭 0 始まり相当の GoogleSQL/標準境界として `drop 0` と区別するため `pos ≥ 1` ガード下で証明）。 -/
def sqlSubstr1Based {CharT : Type} (s : List CharT) (pos len : Nat) : List CharT :=
  (s.drop (pos - 1)).take len

/-- バイト列スライス（C++ `std::string_view::substr(start, count)` モデル）。 -/
def zeroCopyByteSlice (bytes : List UInt8) (start count : Nat) : List UInt8 :=
  (bytes.drop start).take count

/-- ✅ **定理 2 (`ascii_substr_1based_slice_sound` — Q08, Q15, Q79, Q62, Q99)**:
    すべてのバイトが `< 0x80`（シングルバイト UTF-8 / ASCII）である文字列 `bytes` と `pos ≥ 1` に対し、
    1 文字 = 1 バイトの全単射 `decodeAscii : UInt8 → Char` のもとで、
    文字単位の `SUBSTR(s, pos, len)` とバイト列ゼロコピースライス `zeroCopyByteSlice bytes (pos - 1) len` は完全一致する！ -/
theorem ascii_substr_1based_slice_sound {CharT : Type}
    (decodeAscii : UInt8 → CharT) (bytes : List UInt8) (pos len : Nat) :
    sqlSubstr1Based (bytes.map decodeAscii) pos len =
      (zeroCopyByteSlice bytes (pos - 1) len).map decodeAscii := by
  simp [sqlSubstr1Based, zeroCopyByteSlice, List.map_drop, List.map_take]

/-!
## 3. 二値全域述語（`IS [NOT] NULL / TRUE / FALSE`）上の `NOT` 対合正規化
-/

def sqlIsFalse : SqlBool → SqlBool
  | .tFalse => .tTrue
  | _       => .tFalse

def sqlIsNotFalse : SqlBool → SqlBool
  | .tFalse => .tFalse
  | _       => .tTrue

/-- ❌ **罠 3 (`sqlNot_eq_not_neq_on_null_trap`)**:
    一般の比較 `a = b` は `tNull` を返し得るが、`IS NULL` 系は決して `tNull` を返さない。 -/
theorem sqlIsNull_never_tNull {α : Type} (v : SqlVal α) :
    sqlIsNull v ≠ .tNull ∧ sqlIsNotNull v ≠ .tNull := by
  cases v <;> simp [sqlIsNull, sqlIsNotNull]

/-- ✅ **定理 3 (`two_valued_predicate_not_involution_sound` — Q97, Q72, Q51, Q83)**:
    `NOT (x IS NULL) = x IS NOT NULL`、`NOT (x IS NOT NULL) = x IS NULL`、
    `NOT (b IS TRUE) = b IS NOT TRUE`、`NOT (b IS FALSE) = b IS NOT FALSE`（および逆向き）は、
    `WHERE` 文脈だけでなく `SELECT` や `CASE WHEN` 内のスカラー三値ブール値としても **100% 完全一致** する！ -/
theorem two_valued_predicate_not_involution_sound {α : Type}
    (v : SqlVal α) (b : SqlBool) :
    (sqlIsNull v).not = sqlIsNotNull v ∧
    (sqlIsNotNull v).not = sqlIsNull v ∧
    b.isTrue.not = b.isNotTrue ∧
    b.isNotTrue.not = b.isTrue ∧
    (sqlIsFalse b).not = sqlIsNotFalse b ∧
    (sqlIsNotFalse b).not = sqlIsFalse b := by
  cases v <;> cases b <;>
    simp [sqlIsNull, sqlIsNotNull, SqlBool.isTrue, SqlBool.isNotTrue,
      sqlIsFalse, sqlIsNotFalse, SqlBool.not]

/-!
## 4. 事前構築 `EXISTS / NOT EXISTS` の外側非 NULL ドメイン制限 ＆ 結合前プッシュダウン
-/

/-- 相関 `EXISTS (SELECT 1 FROM innerRel r WHERE outerKey o = innerKey r AND innerPred r)`。 -/
def evalCorrelatedExists {O I K : Type} [DecidableEq K]
    (outerKey : O → SqlVal K) (innerKey : I → SqlVal K) (innerPred : I → SqlBool)
    (innerRel : Rel I) (negated : Bool) (o : O) : SqlBool :=
  let hasMatch := innerRel.any (fun r =>
    (sqlEq (outerKey o) (innerKey r)).inWhere && (innerPred r).inWhere)
  let existsBool := if negated then !hasMatch else hasMatch
  if existsBool then .tTrue else .tFalse

/-- 外側テーブル `outerRel` に現れる非 NULL 相関キー集合 `D` で内側テーブルを事前セミ制限してから
    メンバシップ集合を構築する `TryCompilePrebuiltExists` の高速評価モデル。 -/
def evalDomainRestrictedPrebuiltExists {O I K : Type} [DecidableEq K]
    (outerKey : O → SqlVal K) (innerKey : I → SqlVal K) (innerPred : I → SqlBool)
    (outerRel : Rel O) (innerRel : Rel I) (negated : Bool) (o : O) : SqlBool :=
  let outerDomain : List K := outerRel.filterMap outerKey
  let restrictedMembers : List K :=
    (innerRel.filter (fun r =>
      match innerKey r with
      | none => false
      | some k => outerDomain.contains k && (innerPred r).inWhere)).filterMap innerKey
  let hasMatch :=
    match outerKey o with
    | none => false
    | some k => restrictedMembers.contains k
  let existsBool := if negated then !hasMatch else hasMatch
  if existsBool then .tTrue else .tFalse

/-- ❌ **罠 4 (`not_exists_vs_not_in_null_probe_trap`)**:
    `NOT EXISTS (SELECT 1 FROM R WHERE o.k = r.k)` は `o.k = NULL` のとき **`TRUE`** になる（`o.k NOT IN (SELECT r.k FROM R)` の `NULL` とは挙動が逆！）。
    `evalDomainRestrictedPrebuiltExists` は `outerKey o = none` で `hasMatch = false` -> `negated = true` なら `.tTrue` を返すため、この境界も厳密に一致する。 -/
theorem not_exists_on_null_outer_key_is_true :
    evalCorrelatedExists (fun x : SqlVal Int => x) (fun y : SqlVal Int => y)
      (fun _ => .tTrue) [some 1, none] true none = .tTrue := by
  decide

/-- ✅ **定理 4A (`prebuilt_exists_outer_domain_filter_sound` — Q10, Q35, Q69, Q16)**:
    任意の外側行 `o ∈ outerRel` について、`EXISTS` でも `NOT EXISTS`（`negated = true`）でも、
    また `outerKey o` や `innerKey r` に `NULL` が混在していても、
    内側スキャンを外側非 NULL ドメイン `outerDomain` で事前フィルタして構築したメンバシップ判定は
    元の相関サブクエリ評価 `evalCorrelatedExists` と **100% 完全一致** する！ -/
theorem prebuilt_exists_outer_domain_filter_sound {O I K : Type} [DecidableEq K]
    (outerKey : O → SqlVal K) (innerKey : I → SqlVal K) (innerPred : I → SqlBool)
    (outerRel : Rel O) (innerRel : Rel I) (negated : Bool)
    (o : O) (ho : o ∈ outerRel) :
    evalDomainRestrictedPrebuiltExists outerKey innerKey innerPred outerRel innerRel negated o =
      evalCorrelatedExists outerKey innerKey innerPred innerRel negated o := by
  unfold evalDomainRestrictedPrebuiltExists evalCorrelatedExists
  dsimp only
  suffices hMatch :
      (match outerKey o with
       | none => false
       | some k =>
         ((innerRel.filter (fun r =>
           match innerKey r with
           | none => false
           | some k' => (outerRel.filterMap outerKey).contains k' && (innerPred r).inWhere)).filterMap
           innerKey).contains k) =
      innerRel.any (fun r => (sqlEq (outerKey o) (innerKey r)).inWhere && (innerPred r).inWhere) by
    rw [hMatch]
  cases hok : outerKey o with
  | none =>
    symm
    rw [Bool.eq_false_iff]
    intro hAny
    rw [List.any_eq_true] at hAny
    obtain ⟨r, _, hr⟩ := hAny
    cases innerKey r <;> simp [sqlEq, SqlBool.inWhere] at hr
  | some kv =>
    have hkvInOuter : (outerRel.filterMap outerKey).contains kv = true := by
      rw [List.contains_eq_mem, decide_eq_true_eq, List.mem_filterMap]
      exact ⟨o, ho, hok⟩
    apply Bool.eq_iff_iff.mpr
    simp only [List.contains_eq_mem, decide_eq_true_eq, List.mem_filterMap, List.mem_filter,
      List.any_eq_true, Bool.and_eq_true]
    constructor
    · rintro ⟨r, ⟨hrMem, hrCond⟩, hrKey⟩
      refine ⟨r, hrMem, ?_, ?_⟩
      · simp [hrKey, sqlEq, SqlBool.inWhere]
      · simp [hrKey] at hrCond
        exact hrCond.2
    · rintro ⟨r, hrMem, hEq, hPred⟩
      cases hik : innerKey r with
      | none => simp [hik, sqlEq, SqlBool.inWhere] at hEq
      | some kv' =>
        have hEqKv : kv = kv' := by
          by_cases h : kv = kv'
          · exact h
          · simp [hik, sqlEq, SqlBool.inWhere, h] at hEq
        subst hEqKv
        refine ⟨r, ⟨hrMem, ?_⟩, hik⟩
        rw [hik, Bool.and_eq_true]
        exact ⟨decide_eq_true ⟨o, ho, hok⟩, hPred⟩

/-- ✅ **定理 4B (`single_source_exists_pushdown_before_inner_join_sound` — Q10, Q35, Q69, Q16)**:
    外側テーブル `O` の列のみを参照する `EXISTS / NOT EXISTS` 述語 `existsPred : O → SqlBool` は、
    任意の他テーブル `D`（例: `customer_demographics` 192 万行）との `InnerJoin` の **前** に押し込んで
    先に `O` を絞ってから結合しても、行順序・重複度まで含めて **100% 完全一致** する！ -/
theorem single_source_exists_pushdown_before_inner_join_sound {O D : Type}
    (existsPred : O → SqlBool) (onPred : O → D → SqlBool)
    (outerRel : Rel O) (dimRel : Rel D) :
    select (fun od => existsPred od.1) (innerJoin onPred outerRel dimRel) =
      innerJoin onPred (select existsPred outerRel) dimRel := by
  induction outerRel with
  | nil => rfl
  | cons o os ih =>
    simp only [innerJoin, select, List.flatMap_cons, List.filter_append] at ih ⊢
    cases he : (existsPred o).inWhere
    · have hEmpty :
          ((dimRel.filter (fun d => (onPred o d).inWhere)).map (fun d => (o, some d))).filter
            (fun od => (existsPred od.1).inWhere) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro od hod
        rw [List.mem_map] at hod
        obtain ⟨_, _, rfl⟩ := hod
        simp [he]
      simp [List.filter, he, hEmpty, ih]
    · have hKeep :
          ((dimRel.filter (fun d => (onPred o d).inWhere)).map (fun d => (o, some d))).filter
            (fun od => (existsPred od.1).inWhere) =
          (dimRel.filter (fun d => (onPred o d).inWhere)).map (fun d => (o, some d)) := by
        rw [List.filter_eq_self]
        intro od hod
        rw [List.mem_map] at hod
        obtain ⟨_, _, rfl⟩ := hod
        exact he
      simp [List.filter, he, hKeep, List.flatMap_cons, ih]

/-!
## 5. 融合 `AntiJoin` グループ集約カーネル（TPC-DS Q78）
-/

/-- ✅ **定理 5 (`fused_antijoin_groupby_sound` — TPC-DS Q78)**:
    `(L LEFT JOIN R ON p) WHERE keyR(r) IS NULL` の直後に `GROUP BY k(l), aggFn(l)` を行うクエリにおいて、
    結合条件 `p` が検査列 `keyR` に関して Null-Rejecting（例: `wr_order_number = ws_order_number`）ならば、
    `LEFT JOIN` のマッチ行や NULL パディング行を 1 行もマテリアライズせず、
    **`R` のハッシュ表をプローブして `¬∃ r ∈ R, p(l, r)` だった左行 `l` のみを直接グループ集約器へ流し込む
    融合 `AntiJoin` グループ集約** は、元の `LEFT JOIN + WHERE IS NULL + GROUP BY` と **完全一致** する！ -/
theorem fused_antijoin_groupby_sound {L R K KeyG AggV : Type} [DecidableEq KeyG]
    (p : L → R → SqlBool) (keyR : R → SqlVal K)
    (hNullRejR : ∀ l r, keyR r = none → (p l r).inWhere = false)
    (groupKey : L → KeyG) (aggOverLeft : KeyG → Rel L → AggV)
    (lRel : Rel L) (rRel : Rel R) :
    let materializedFiltered : Rel (L × Option R) :=
      select (fun lr => match lr.2 with
        | none => .tTrue
        | some r => match keyR r with | none => .tTrue | some _ => .tFalse)
      (leftOuterJoin p lRel rRel)
    groupBy (fun lr => groupKey lr.1)
      (fun g bucket => aggOverLeft g (bucket.map Prod.fst))
      materializedFiltered =
    groupBy groupKey aggOverLeft (antiJoin p lRel rRel) := by
  intro materializedFiltered
  have hProj : materializedFiltered.map Prod.fst = antiJoin p lRel rRel := by
    dsimp [materializedFiltered]
    clear materializedFiltered
    induction lRel with
    | nil => rfl
    | cons l ls ih =>
      simp only [leftOuterJoin, antiJoin, select, List.flatMap_cons, List.filter_append,
        List.map_append] at ih ⊢
      have hHead :
          List.map Prod.fst
            (List.filter
              (fun row =>
                (match row.2 with
                 | none => SqlBool.tTrue
                 | some r => match keyR r with | none => .tTrue | some _ => .tFalse).inWhere)
              (if (rRel.filter (fun y => (p l y).inWhere)).isEmpty = true then [(l, none)]
               else (rRel.filter (fun y => (p l y).inWhere)).map (fun y => (l, some y)))) =
          if !(rRel.any (fun y => (p l y).inWhere)) then [l] else [] := by
        cases hm : rRel.filter (fun r => (p l r).inWhere) with
        | nil =>
          have hAnyFalse : rRel.any (fun r => (p l r).inWhere) = false := by
            cases hAny : rRel.any (fun r => (p l r).inWhere)
            · rfl
            · rw [List.any_eq_true] at hAny
              obtain ⟨r, hr, hpr⟩ := hAny
              have : r ∈ rRel.filter (fun r => (p l r).inWhere) := List.mem_filter.mpr ⟨hr, hpr⟩
              rw [hm] at this
              nomatch this
          simp only [List.isEmpty, ↓reduceIte, hAnyFalse, Bool.not_false]
          rfl
        | cons r0 rs =>
          have hAnyTrue : rRel.any (fun r => (p l r).inWhere) = true := by
            have hr0 : r0 ∈ rRel.filter (fun r => (p l r).inWhere) := by
              rw [hm]; exact List.mem_cons_self ..
            rw [List.mem_filter] at hr0
            exact List.any_eq_true.mpr ⟨r0, hr0.1, hr0.2⟩
          have hDrop :
              (((r0 :: rs).map (fun r => (l, some r))).filter (fun lr =>
                (match lr.2 with
                 | none => SqlBool.tTrue
                 | some r => match keyR r with | none => .tTrue | some _ => .tFalse).inWhere)) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro lr hlr
            rw [List.mem_map] at hlr
            obtain ⟨r, hrCons, rfl⟩ := hlr
            have hrFilt : r ∈ rRel.filter (fun r' => (p l r').inWhere) := by
              rw [hm]; exact hrCons
            rw [List.mem_filter] at hrFilt
            cases hkr : keyR r with
            | none =>
              rw [hNullRejR l r hkr] at hrFilt
              simp at hrFilt
            | some _ =>
              simp [hkr, SqlBool.inWhere]
          simp only [List.isEmpty, Bool.false_eq_true, ↓reduceIte, hDrop, List.map_nil,
            hAnyTrue, Bool.not_true]
      rw [hHead, ih]
      simp only [List.filter]
      cases !(rRel.any (fun y => (p l y).inWhere)) <;> rfl
  unfold groupBy
  have hKeys : materializedFiltered.map (fun lr => groupKey lr.1) =
      (antiJoin p lRel rRel).map groupKey := by
    rw [← hProj, List.map_map]
    rfl
  rw [hKeys]
  apply List.map_congr_left
  intro g _
  congr 1
  rw [← hProj, List.filter_map]
  rfl

/-!
## 6. `UNION DISTINCT / ALL` + `GROUP BY` 共有 CTE への選言グループキー制限貫通（Q75）＆
     `EXCEPT / INTERSECT` サロゲートキー先行重複排除（Q87, Q38）
-/

private theorem filter_distinctKeys_comm {α : Type} [DecidableEq α]
    (p : α → Bool) :
    ∀ (xs : List α), (distinctKeys xs).filter p = distinctKeys (xs.filter p)
  | [] => rfl
  | x :: xs => by
    have ih := filter_distinctKeys_comm p xs
    cases hp : p x
    · simp only [distinctKeys, List.filter, hp, List.filter_filter]
      have hSame :
          (distinctKeys xs).filter (fun y => p y && decide (y ≠ x)) =
          (distinctKeys xs).filter p := by
        apply List.filter_congr
        intro y _
        cases hpy : p y
        · rfl
        · have hne : y ≠ x := fun hEq => by rw [hEq, hp] at hpy; nomatch hpy
          simp [hne]
      rw [hSame, ih]
    · simp only [distinctKeys, List.filter, hp, List.filter_filter]
      congr 1
      rw [← ih, List.filter_filter]
      apply List.filter_congr
      intro y _
      exact Bool.and_comm _ _

/-- ✅ **定理 6A (`shared_grouped_union_distinct_cte_disjunctive_pushdown_sound` — TPC-DS Q75)**:
    Q75 の `all_sales` のように、`UNION DISTINCT`（`distinctKeys (b1 ++ b2 ++ b3)`）の上に
    `GROUP BY key` を載せた共有 CTE を複数箇所（例: `curr_yr.d_year = 2002`, `prev_yr.d_year = 2001`）から読むとき、
    各消費箇所のグループキー条件の選言 `keepKey(k) := p1(k) || p2(k)` を
    **`GROUP BY` と `UNION DISTINCT` を一気に貫通して各チャネル枝 `b1, b2, b3` の内側へ直接プッシュダウン** しても、
    すべての消費箇所の結果は **行順序・重複度まで含めて 100% 完全一致** する！ -/
theorem shared_grouped_union_distinct_cte_disjunctive_pushdown_sound
    {Row K V : Type} [DecidableEq Row] [DecidableEq K]
    (key : Row → K) (aggFn : K → Rel Row → V)
    (p1 p2 : K → SqlBool)
    (b1 b2 b3 : Rel Row) :
    let keepRow (r : Row) : Bool := ((p1 (key r)).or (p2 (key r))).inWhere
    let fullUnion := distinctKeys (b1 ++ b2 ++ b3)
    let pushedUnion := distinctKeys (b1.filter keepRow ++ b2.filter keepRow ++ b3.filter keepRow)
    let fullCte := groupBy key aggFn fullUnion
    let pushedCte := groupBy key aggFn pushedUnion
    select (fun kv => p1 kv.1) fullCte = select (fun kv => p1 kv.1) pushedCte ∧
    select (fun kv => p2 kv.1) fullCte = select (fun kv => p2 kv.1) pushedCte := by
  intro keepRow fullUnion pushedUnion fullCte pushedCte
  have hPushUnion : pushedUnion = fullUnion.filter keepRow := by
    dsimp [pushedUnion, fullUnion]
    rw [← List.filter_append, ← List.filter_append, ← filter_distinctKeys_comm]
  have hGroupComm : ∀ (q : K → SqlBool),
      (∀ k, (q k).inWhere = true → ((p1 k).or (p2 k)).inWhere = true) →
      select (fun kv => q kv.1) fullCte = select (fun kv => q kv.1) pushedCte := by
    intro q hImp
    dsimp [fullCte, pushedCte, groupBy, select]
    rw [hPushUnion]
    have hMapFilter : (fullUnion.filter keepRow).map key =
        (fullUnion.map key).filter (fun k => ((p1 k).or (p2 k)).inWhere) := by
      induction fullUnion with
      | nil => rfl
      | cons r rs ih =>
        dsimp [keepRow] at ih ⊢
        cases hKeep : ((p1 (key r)).or (p2 (key r))).inWhere <;>
          simp [List.filter, hKeep, ih]
    rw [hMapFilter, ← filter_distinctKeys_comm]
    have hKeysEq :
        ∀ ks : List K,
          ((ks.map (fun k => (k, aggFn k (fullUnion.filter (fun r => key r == k))))).filter
            (fun kv => (q kv.1).inWhere)) =
          (((ks.filter (fun k => ((p1 k).or (p2 k)).inWhere)).map
            (fun k => (k, aggFn k ((fullUnion.filter keepRow).filter (fun r => key r == k))))).filter
            (fun kv => (q kv.1).inWhere)) := by
      intro ks
      induction ks with
      | nil => rfl
      | cons k rest ih =>
        by_cases hqk : (q k).inWhere = true
        · have hDisj : ((p1 k).or (p2 k)).inWhere = true := hImp k hqk
          have hBucketEq :
              (fullUnion.filter keepRow).filter (fun r => key r == k) =
              fullUnion.filter (fun r => key r == k) := by
            rw [List.filter_filter]
            apply List.filter_congr
            intro r _
            by_cases hkr : (key r == k) = true
            · have hkEq : key r = k := eq_of_beq hkr
              simp [keepRow, hkEq, hDisj]
            · simp [hkr]
          simp only [List.filter, List.map, hqk, hDisj]
          rw [hBucketEq, ih]
        · simp only [Bool.not_eq_true] at hqk
          cases hDisj : ((p1 k).or (p2 k)).inWhere <;>
            simp [List.filter, hqk, hDisj, ih]
    exact hKeysEq (distinctKeys (fullUnion.map key))
  refine ⟨hGroupComm p1 ?_, hGroupComm p2 ?_⟩
  · intro k hk; rw [or_inWhere]; simp [hk]
  · intro k hk; rw [or_inWhere]; simp [hk]

/-- ❌ **罠 6B (`except_below_non_injective_dim_unsound`)**:
    Q87 / Q38 で `EXCEPT DISTINCT` や `INTERSECT DISTINCT` 自体を `customer` 結合の下へ押し込んで
    `ss_customer_sk EXCEPT cs_bill_customer_sk` にしてしまうと、
    異なる `c_customer_sk`（例: `1` と `2`）が同姓同名 `(c_last_name, c_first_name)` を持つ場合に誤答になる！ -/
theorem except_below_non_injective_dim_unsound :
    let custName (sk : Int) : String := if sk == 1 || sk == 2 then "Alice Smith" else "Bob"
    let ssSk : List Int := [1]
    let csSk : List Int := [2]
    let trueExcept := (distinctKeys (ssSk.map custName)).filter
      (fun nm => !(distinctKeys (csSk.map custName)).contains nm)
    let badPushedExcept := distinctKeys ((ssSk.filter (fun k => !csSk.contains k)).map custName)
    trueExcept = [] ∧ badPushedExcept = ["Alice Smith"] ∧ trueExcept ≠ badPushedExcept := by
  decide

/-- ✅ **定理 6B (`eager_distinct_surrogate_keys_setop_sound` — TPC-DS Q87, Q38)**:
    `EXCEPT` 自体は `customer` の下に押し込めないが、**各チャネル内でサロゲートキー組 `sk(f)` の重複排除
    `distinctKeys (F.map sk)` を `customer` 結合より先に実行** することは、`customer` に同姓同名や重複があっても
    `EXCEPT DISTINCT` / `INTERSECT DISTINCT` の結果集合・件数を **100% 完全保存** する！ -/
theorem eager_distinct_surrogate_keys_setop_sound {F SK C Out : Type}
    [DecidableEq SK] [DecidableEq Out]
    (sk1 sk2 : F → SK) (onC : SK → C → SqlBool) (outFn : SK → C → Out)
    (F1 F2 : Rel F) (C_rel : Rel C) :
    let branchFull (sk : F → SK) (F_rel : Rel F) : List Out :=
      distinctKeys (F_rel.flatMap fun f =>
        (C_rel.filter (fun c => (onC (sk f) c).inWhere)).map fun c => outFn (sk f) c)
    let branchEager (sk : F → SK) (F_rel : Rel F) : List Out :=
      distinctKeys ((distinctKeys (F_rel.map sk)).flatMap fun k =>
        (C_rel.filter (fun c => (onC k c).inWhere)).map fun c => outFn k c)
    BagEq
      ((branchFull sk1 F1).filter (fun o => !(branchFull sk2 F2).contains o))
      ((branchEager sk1 F1).filter (fun o => !(branchEager sk2 F2).contains o)) ∧
    BagEq
      ((branchFull sk1 F1).filter (fun o => (branchFull sk2 F2).contains o))
      ((branchEager sk1 F1).filter (fun o => (branchEager sk2 F2).contains o)) := by
  intro branchFull branchEager
  have hMemEq : ∀ (sk : F → SK) (F_rel : Rel F) (o : Out),
      o ∈ branchFull sk F_rel ↔ o ∈ branchEager sk F_rel := by
    intro sk F_rel o
    simp only [branchFull, branchEager, mem_distinctKeys, List.mem_flatMap, List.mem_map]
    constructor
    · rintro ⟨f, hf, c, hc, rfl⟩
      exact ⟨sk f, ⟨f, hf, rfl⟩, c, hc, rfl⟩
    · rintro ⟨k, ⟨f, hf, rfl⟩, c, hc, rfl⟩
      exact ⟨f, hf, c, hc, rfl⟩
  refine ⟨?_, ?_⟩
  · apply perm_of_nodup_iff_mem
      ((nodup_distinctKeys _).filter _) ((nodup_distinctKeys _).filter _)
    intro o
    simp only [List.mem_filter, List.contains_eq_mem, Bool.not_eq_true', decide_eq_false_iff_not]
    rw [hMemEq sk1 F1 o, hMemEq sk2 F2 o]
  · apply perm_of_nodup_iff_mem
      ((nodup_distinctKeys _).filter _) ((nodup_distinctKeys _).filter _)
    intro o
    simp only [List.mem_filter, List.contains_eq_mem, decide_eq_true_eq]
    rw [hMemEq sk1 F1 o, hMemEq sk2 F2 o]

/-!
## 7. 非シャドウイング `GROUP BY <select_alias>` インライン化による融合 Group-Join 解禁（Q06, Q08）
-/

/-- ✅ **定理 7 (`select_alias_group_by_canonicalization_sound` — TPC-DS Q06, Q08)**:
    入力スキーマ列と衝突しない `SELECT expr(r) AS alias` を `GROUP BY alias` で参照するクエリにおいて、
    結合融合前に `GROUP BY expr(r)` へ正規化することは、後段でエイリアス解決してから集約する評価と **完全一致** する！ -/
theorem select_alias_group_by_canonicalization_sound {Row K V : Type} [DecidableEq K]
    (aliasExpr : Row → K) (aggFn : K → Rel Row → V) (r : Rel Row) :
    let withAliasCol : Rel (Row × K) := r.map (fun row => (row, aliasExpr row))
    groupBy Prod.snd (fun k b => aggFn k (b.map Prod.fst)) withAliasCol =
      groupBy aliasExpr aggFn r := by
  intro withAliasCol
  unfold groupBy
  have hKeys : withAliasCol.map Prod.snd = r.map aliasExpr := by
    dsimp [withAliasCol]
    rw [List.map_map]
    rfl
  rw [hKeys]
  apply List.map_congr_left
  intro k _
  congr 1
  dsimp [withAliasCol]
  rw [List.filter_map, List.map_map]
  simp [Function.comp_def]

end TinylambOpt
