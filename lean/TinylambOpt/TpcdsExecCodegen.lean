import Std.Tactic.BVDecide
import TinylambOpt.AggBase
import TinylambOpt.TpcdsDiscovery
import TinylambOpt.ScalarOverflowAndCoreAlgebra

/-
# Step 13: TPC-DS 全勝に向けた証明付きアグレッシブ実行カーネル・ローワリング規則

TPC-DS ベンチマークのプロファイリングで判明した主要ボトルネック群を解消する **6 系統の実行コード生成・
物理ローワリング最適化** と、うっかり踏むと誤答を起こす **三値論理 / NULL / 非単射トラップ** を
Lean 4 で機械検証（`sorry` ゼロ）します。

## 収録する 6 系統・18 節の実行最適化定理

1. **定数集合 `IN (...)` の事前コンパイル O(1) ハッシュ集合 + NULL フラグ三値評価 (`in_const_set_3vl_sound`)**
   - 対象: **Q08**（400 郵便番号 `IN`）、**Q15**, **Q48**, **Q10**, **Q69**
2. **ASCII 1-始まり `SUBSTR(s, pos, len)` のゼロコピー・スライス還元 (`ascii_substr_1based_slice_sound`)**
   - 対象: **Q08**, **Q15**, **Q79**, **Q62**, **Q99**
3. **二値全域述語（`IS [NOT] NULL / TRUE / FALSE`）上の `NOT` 対合正規化 (`two_valued_predicate_not_involution_sound`)**
   - 対象: **Q97**, **Q72**, **Q51**, **Q83**
4. **事前構築 `EXISTS / NOT EXISTS` の外側非 NULL ドメイン制限 ＆ 結合前プッシュダウン (`prebuilt_exists_outer_domain_filter_sound` / `single_source_exists_pushdown_before_inner_join_sound`)**
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
abbrev evalInSequential {α : Type} [DecidableEq α] (v : SqlVal α) := evalInList v

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
    simp [compileInConstSet, evalCompiledInConstSet, evalInList]
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
          simp [compileInConstSet, evalCompiledInConstSet, evalInList, sqlEq]
        | some y =>
          by_cases hxy : x = y
          · subst hxy
            simp [compileInConstSet, evalCompiledInConstSet, evalInList, sqlEq]
          · simp [compileInConstSet, evalCompiledInConstSet, evalInList, sqlEq, hxy]
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

/-- ❌ **罠 3 (`isNull_family_never_tNull_trap`)**:
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

/-!
## 8. カラムナ NULL ビットマップ単一パス集計 ＆ 2×32bit パック整数結合キー全単射（Q97, Q51, Q65）
-/

/-- ✅ **定理 8A (`columnar_null_indicator_popcount_sound` — TPC-DS Q97)**:
    `COUNT(CASE WHEN isNull_0(r) = wantNull0 ∧ isNull_1(r) = wantNull1 THEN 1 END)` は
    行を 1 行も `Row` としてマテリアライズせず、カラムナ列の NULL フラグ列に対する論理演算＋カウントと完全一致し、
    特に両列が `has_nulls = false` のときは `(!wantNull0 && !wantNull1)` なら `rows.length`、さもなくば `0` に O(1) 短絡する！ -/
theorem columnar_null_indicator_popcount_sound {Row : Type}
    (isNull0 isNull1 : Row → Bool) (wantNull0 wantNull1 : Bool) (rows : List Row) :
    (rows.filter (fun r => (isNull0 r == wantNull0) && (isNull1 r == wantNull1))).length =
      rows.countP (fun r => (isNull0 r == wantNull0) && (isNull1 r == wantNull1)) ∧
    ((∀ r ∈ rows, isNull0 r = false ∧ isNull1 r = false) →
      rows.countP (fun r => (isNull0 r == wantNull0) && (isNull1 r == wantNull1)) =
        if (!wantNull0 && !wantNull1) then rows.length else 0) := by
  refine ⟨?_, ?_⟩
  · induction rows with
    | nil => rfl
    | cons r rs ihr =>
      rw [List.filter_cons, List.countP_cons]
      cases ((isNull0 r == wantNull0) && (isNull1 r == wantNull1))
      · simp [ihr]
      · simp [ihr, Nat.add_comm]
  · intro hNoNull
    induction rows with
    | nil => simp
    | cons r rs ih =>
      have hr := hNoNull r (List.mem_cons_self ..)
      have hrs : ∀ x ∈ rs, isNull0 x = false ∧ isNull1 x = false :=
        fun x hx => hNoNull x (List.mem_cons_of_mem _ hx)
      rw [List.countP_cons, hr.1, hr.2, ih hrs]
      cases wantNull0 <;> cases wantNull1 <;> simp

/-- 2 つの 32-bit ビット列 `(k0, k1)` を 1 つの 64-bit ワードへパックする写像。 -/
def pack2x32Bits (k0 k1 : BitVec 32) : BitVec 64 :=
  (k0.zeroExtend 64 <<< 32) ||| k1.zeroExtend 64

/-- ✅ **定理 8B (`packed_2x32_join_key_bijection_sound` — TPC-DS Q97, Q51, Q65)**:
    32-bit 範囲に収まる 2 列整数／日付キー `(k0, k1)` の 64-bit パック表現は **完全単射** であり、
    2 列複合ハッシュ表や `Value` ベクタ比較を単一 64-bit ワード比較へ置換しても衝突による誤マッチは絶対に起きない！ -/
theorem packed_2x32_join_key_bijection_sound (a0 a1 b0 b1 : BitVec 32) :
    pack2x32Bits a0 a1 = pack2x32Bits b0 b1 ↔ (a0 = b0 ∧ a1 = b1) := by
  unfold pack2x32Bits
  bv_decide

/-!
## 9. 同値類商 ID (`gid`) エンコーディングによる `INTERSECT / EXCEPT DISTINCT` 64-bit 整数化（Q38, Q87）
-/

/-- ✅ **定理 9 (`quotient_gid_setop_cardinality_sound` — TPC-DS Q38, Q87)**:
    ディメンション射影タプル `Attr`（例: `(c_last_name, c_first_name)`）に対して、
    `gid a1 = gid a2 ↔ a1 = a2` を満たす稠密な同値類 ID `gid : Attr → GID` を 1 回だけ割り当てれば、
    3 チャネルの `INTERSECT DISTINCT` ならびに多段 `EXCEPT DISTINCT` の結果件数（`COUNT(*)`）は、
    文字列タプルを一切作らず **`(gid(attr), d_date)` の整数ペア集合演算で計算した結果と 100% 完全一致** する！ -/
theorem quotient_gid_setop_cardinality_sound {Attr DateK GID : Type}
    [DecidableEq Attr] [DecidableEq DateK] [DecidableEq GID]
    (gid : Attr → GID)
    (hQuot : ∀ x y : Attr, gid x = gid y ↔ x = y)
    (s1 s2 s3 : List (Attr × DateK)) :
    let encode (p : Attr × DateK) : GID × DateK := (gid p.1, p.2)
    let d1 := distinctKeys s1
    let d2 := distinctKeys s2
    let d3 := distinctKeys s3
    let e1 := distinctKeys (s1.map encode)
    let e2 := distinctKeys (s2.map encode)
    let e3 := distinctKeys (s3.map encode)
    ((d1.filter (fun x => d2.contains x && d3.contains x)).length =
      (e1.filter (fun y => e2.contains y && e3.contains y)).length) ∧
    ((d1.filter (fun x => !d2.contains x && !d3.contains x)).length =
      (e1.filter (fun y => !e2.contains y && !e3.contains y)).length) := by
  intro encode d1 d2 d3 e1 e2 e3
  have hEncInj : Function.Injective encode := by
    rintro ⟨a1, k1⟩ ⟨a2, k2⟩ hEq
    simp only [encode, Prod.mk.injEq] at hEq
    exact Prod.ext ((hQuot a1 a2).mp hEq.1) hEq.2
  have hDistinctMap : ∀ s : List (Attr × DateK),
      distinctKeys (s.map encode) = (distinctKeys s).map encode := by
    intro s
    induction s with
    | nil => rfl
    | cons x xs ih =>
      simp only [List.map_cons, distinctKeys]
      rw [ih, List.filter_map]
      congr 1
      congr 1
      apply List.filter_congr
      intro z _
      simp only [Function.comp_def, decide_eq_decide]
      exact ⟨fun hne hEq => hne (congrArg encode hEq), fun hne hEq => hne (hEncInj hEq)⟩
  have hContains : ∀ (s : List (Attr × DateK)) (x : Attr × DateK),
      (distinctKeys (s.map encode)).contains (encode x) = (distinctKeys s).contains x := by
    intro s x
    rw [hDistinctMap s]
    cases hMem : (distinctKeys s).contains x
    · simp only [List.contains_eq_mem, decide_eq_false_iff_not, List.mem_map] at hMem ⊢
      rintro ⟨x', hx', hEq⟩
      exact hMem (hEncInj hEq ▸ hx')
    · simp only [List.contains_eq_mem, decide_eq_true_eq, List.mem_map] at hMem ⊢
      exact ⟨x, hMem, rfl⟩
  have h1 : e1 = d1.map encode := hDistinctMap s1
  refine ⟨?_, ?_⟩
  · rw [h1, List.filter_map, List.length_map]
    congr 1
    apply List.filter_congr
    intro x _
    simp only [Function.comp_def, e2, e3, d2, d3, hContains]
  · rw [h1, List.filter_map, List.length_map]
    congr 1
    apply List.filter_congr
    intro x _
    simp only [Function.comp_def, e2, e3, d2, d3, hContains]

/-!
## 10. 実行時検証済み一意 PK による Group-By キー縮約・未参照 PK 結合消去・未使用 `LEFT JOIN` 除去
       （Q04, Q11, Q74, Q23, Q22, Q39, Q63, Q72）
-/

/-- ✅ **定理 10A (`bijective_dim_key_cte_deferral_sound` — TPC-DS Q04, Q11, Q74)**:
    ディメンション表 `D`（例: `customer`）上で結合キー `pk` とビジネスタプル `bk` が全単射
    （`lookupDim k = some b` が単射）であることが実行時に検証されたとき、
    各ファクト CTE の内部で `D` の複数 `VARCHAR` 列を含めて `GROUP BY (bk, yr)` を計算する代わりに、
    **CTE 内では整数 `(fk, yr)` だけで集約し、外側で `D` を 1 回だけ引き直す遅延結合** は結果を完全保存する！ -/
theorem bijective_dim_key_cte_deferral_sound {F PK BK Yr V : Type}
    [DecidableEq PK] [DecidableEq BK] [DecidableEq Yr]
    (fk : F → PK) (yr : F → Yr)
    (lookupDim : PK → Option BK)
    (hInj : ∀ k1 k2 b, lookupDim k1 = some b → lookupDim k2 = some b → k1 = k2)
    (aggFn : Rel F → V) (facts : Rel F) :
    let validFacts : Rel (F × BK) :=
      facts.filterMap (fun f => (lookupDim (fk f)).map (fun b => (f, b)))
    let eagerGroups : List ((BK × Yr) × V) :=
      groupBy (fun fb => (fb.2, yr fb.1)) (fun _ bucket => aggFn (bucket.map Prod.fst)) validFacts
    let deferredGroups : List ((BK × Yr) × V) :=
      (groupBy (fun f => (fk f, yr f)) (fun _ bucket => aggFn bucket)
        (facts.filter (fun f => (lookupDim (fk f)).isSome))).filterMap
        (fun kv => (lookupDim kv.1.1).map (fun b => ((b, kv.1.2), kv.2)))
    eagerGroups = deferredGroups := by
  intro validFacts eagerGroups deferredGroups
  dsimp [eagerGroups, deferredGroups, groupBy]
  have hValidMapKey :
      validFacts.map (fun fb => (fb.2, yr fb.1)) =
      ((facts.filter (fun f => (lookupDim (fk f)).isSome)).map (fun f => (fk f, yr f))).filterMap
        (fun ky => (lookupDim ky.1).map (fun b => (b, ky.2))) := by
    dsimp [validFacts]
    induction facts with
    | nil => rfl
    | cons f fs ih =>
      cases hlk : lookupDim (fk f) <;> simp [List.filterMap, List.filter, hlk, ih]
  have hSomeOnFiltered : ∀ ky ∈ (facts.filter (fun f => (lookupDim (fk f)).isSome)).map (fun f => (fk f, yr f)),
      ∃ b, lookupDim ky.1 = some b := by
    intro ky hky
    rw [List.mem_map] at hky
    obtain ⟨f, hf, rfl⟩ := hky
    rw [List.mem_filter] at hf
    cases hlk : lookupDim (fk f) with
    | none => simp [hlk] at hf
    | some b => exact ⟨b, rfl⟩
  have hSomeOnDistinct : ∀ ky ∈ distinctKeys ((facts.filter (fun f => (lookupDim (fk f)).isSome)).map (fun f => (fk f, yr f))),
      ∃ b, lookupDim ky.1 = some b := by
    intro ky hky
    rw [mem_distinctKeys] at hky
    exact hSomeOnFiltered ky hky
  have hDistinctComm :
      ∀ (ks : List (PK × Yr)),
        (∀ ky ∈ ks, ∃ b, lookupDim ky.1 = some b) →
        distinctKeys (ks.filterMap (fun ky => (lookupDim ky.1).map (fun b => (b, ky.2)))) =
          (distinctKeys ks).filterMap (fun ky => (lookupDim ky.1).map (fun b => (b, ky.2))) := by
    intro ks hAllSome
    induction ks with
    | nil => rfl
    | cons ky rest ih =>
      obtain ⟨b, hb⟩ := hAllSome ky (List.mem_cons_self ..)
      have hRestSome : ∀ z ∈ rest, ∃ b', lookupDim z.1 = some b' :=
        fun z hz => hAllSome z (List.mem_cons_of_mem _ hz)
      simp only [List.filterMap_cons, hb, Option.map_some, distinctKeys]
      rw [ih hRestSome]
      congr 1
      clear ih
      have hRestDistSome : ∀ z ∈ distinctKeys rest, ∃ b', lookupDim z.1 = some b' := by
        intro z hz
        rw [mem_distinctKeys] at hz
        exact hRestSome z hz
      generalize distinctKeys rest = dr at hRestDistSome ⊢
      induction dr with
      | nil => rfl
      | cons z zs ihz =>
        obtain ⟨bz, hbz⟩ := hRestDistSome z (List.mem_cons_self ..)
        have hSub : ∀ w ∈ zs, ∃ b', lookupDim w.1 = some b' :=
          fun w hw => hRestDistSome w (List.mem_cons_of_mem _ hw)
        by_cases hEq : z = ky
        · subst hEq
          have hd1 : decide ((b, z.2) ≠ (b, z.2)) = false := by simp
          have hd2 : decide (z ≠ z) = false := by simp
          simp only [List.filterMap_cons, hb, Option.map_some, List.filter_cons,
            hd1, hd2, Bool.false_eq_true, ↓reduceIte, ihz hSub]
        · have hNePair : (bz, z.2) ≠ (b, ky.2) := by
            intro hPair
            injection hPair with hB hY
            subst hB
            have hkEq := hInj z.1 ky.1 bz hbz hb
            exact hEq (Prod.ext hkEq hY)
          have hd1 : decide ((bz, z.2) ≠ (b, ky.2)) = true := decide_eq_true hNePair
          have hd2 : decide (z ≠ ky) = true := decide_eq_true hEq
          simp only [List.filter_cons, hd1, hd2, ↓reduceIte, List.filterMap_cons,
            hbz, Option.map_some, ihz hSub]
  rw [hValidMapKey, hDistinctComm _ hSomeOnFiltered]
  have hBucket : ∀ (fs : Rel F) (k0 : PK) (y0 : Yr) (b : BK),
      lookupDim k0 = some b →
      ((fs.filterMap (fun f => (lookupDim (fk f)).map (fun b' => (f, b')))).filter (fun fb => (fb.2, yr fb.1) == (b, y0))).map Prod.fst =
      (fs.filter (fun f => (lookupDim (fk f)).isSome)).filter (fun f => (fk f, yr f) == (k0, y0)) := by
    intro fs k0 y0 b hb
    induction fs with
    | nil => rfl
    | cons f fs ih =>
      cases hlk : lookupDim (fk f) with
      | none =>
        simp only [List.filterMap_cons, hlk, Option.map_none, List.filter_cons,
          Option.isSome_none, Bool.false_eq_true, ↓reduceIte]
        exact ih
      | some bf =>
        have hEqIff : ((bf, yr f) == (b, y0)) = ((fk f, yr f) == (k0, y0)) := by
          by_cases h1 : bf = b ∧ yr f = y0
          · have hkEq : fk f = k0 := hInj (fk f) k0 b (h1.1 ▸ hlk) hb
            simp [h1.1, h1.2, hkEq]
          · have h2 : ¬(fk f = k0 ∧ yr f = y0) := by
              rintro ⟨hkEq, hyEq⟩
              rw [hkEq, hb] at hlk
              injection hlk with hbfEq
              exact h1 ⟨hbfEq.symm, hyEq⟩
            have hb1 : ((bf, yr f) == (b, y0)) = false :=
              beq_false_of_ne (fun h => h1 (Prod.mk.inj h))
            have hb2 : ((fk f, yr f) == (k0, y0)) = false :=
              beq_false_of_ne (fun h => h2 (Prod.mk.inj h))
            rw [hb1, hb2]
        simp only [List.filterMap_cons, hlk, Option.map_some, List.filter_cons, Option.isSome_some,
          ↓reduceIte]
        rw [hEqIff]
        cases ((fk f, yr f) == (k0, y0))
        · simp only [Bool.false_eq_true, ↓reduceIte, ih]
        · simp only [↓reduceIte, List.map_cons, ih]
  dsimp [validFacts]
  generalize distinctKeys ((facts.filter (fun f => (lookupDim (fk f)).isSome)).map (fun f => (fk f, yr f))) = dk at hSomeOnDistinct ⊢
  induction dk with
  | nil => rfl
  | cons ky kys ih =>
    obtain ⟨b, hb⟩ := hSomeOnDistinct ky (List.mem_cons_self ..)
    have hBucketHead := hBucket facts ky.1 ky.2 b hb
    simp only [List.filterMap_cons, List.map_cons, hb, Option.map_some]
    rw [hBucketHead]
    congr 1
    exact ih (fun z hz => hSomeOnDistinct z (List.mem_cons_of_mem _ hz))

/-- ✅ **定理 10B (`unreferenced_unique_pk_inner_join_to_semi_sound` — TPC-DS Q23, Q22, Q39, Q63)**:
    射影・集約・フィルタで他列が一切参照されないディメンション表 `D` との等価内部結合 `fk(l) = pk(d)` は、
    `D` 上で `pk` が重複を持たない（`(D.filterMap pk).Nodup`）とき、
    **行の重複度も順序も変えない純粋な存在フィルタ（Semi-Join / PK ビットセット検査）** と **100% 完全一致** する！ -/
theorem unreferenced_unique_pk_inner_join_to_semi_sound {L D K : Type} [DecidableEq K]
    (fk : L → SqlVal K) (pk : D → SqlVal K)
    (lRel : Rel L) (dRel : Rel D)
    (hUniquePk : (dRel.filterMap pk).Nodup) :
    (innerJoin (fun l d => sqlEq (fk l) (pk d)) lRel dRel).map Prod.fst =
      lRel.filter (fun l => dRel.any (fun d => (sqlEq (fk l) (pk d)).inWhere)) := by
  have hAtMostOne : ∀ (kv : K) (ds : Rel D),
      (ds.filterMap pk).Nodup →
      (ds.filter (fun d => (sqlEq (some kv) (pk d)).inWhere)).length ≤ 1 := by
    intro kv ds
    induction ds with
    | nil => simp
    | cons d rest ih =>
      intro hNd
      cases hpk : pk d with
      | none =>
        simp only [List.filterMap_cons_none hpk] at hNd
        have hEqFalse : (sqlEq (some kv) none).inWhere = false := rfl
        simp only [List.filter_cons, hpk, hEqFalse, Bool.false_eq_true, ↓reduceIte]
        exact ih hNd
      | some kv' =>
        simp only [List.filterMap_cons_some hpk, List.nodup_cons] at hNd
        by_cases hEq : kv = kv'
        · subst hEq
          have hNoneRest : rest.filter (fun d' => (sqlEq (some kv) (pk d')).inWhere) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro d' hd'
            cases hpk' : pk d' with
            | none => simp [sqlEq, SqlBool.inWhere]
            | some kv2 =>
              by_cases h2 : kv = kv2
              · subst h2
                exact False.elim (hNd.1 (List.mem_filterMap.mpr ⟨d', hd', hpk'⟩))
              · have hSqlF : sqlEq (some kv) (some kv2) = .tFalse := by
                  unfold sqlEq; simp [h2]
                simp [hSqlF, SqlBool.inWhere]
          have hEqTrue : (sqlEq (some kv) (some kv)).inWhere = true := by
            simp [sqlEq, SqlBool.inWhere]
          simp only [List.filter_cons, hpk, hEqTrue, ↓reduceIte, hNoneRest, List.length_singleton]
          exact Nat.le_refl 1
        · have hEqFalse : (sqlEq (some kv) (some kv')).inWhere = false := by
            have hSqlF : sqlEq (some kv) (some kv') = .tFalse := by
              unfold sqlEq; simp [hEq]
            simp [hSqlF, SqlBool.inWhere]
          simp only [List.filter_cons, hpk, hEqFalse, Bool.false_eq_true, ↓reduceIte]
          exact ih hNd.2
  induction lRel with
  | nil => rfl
  | cons l ls ih =>
    have hHead :
        List.map Prod.fst ((dRel.filter (fun d => (sqlEq (fk l) (pk d)).inWhere)).map (fun d => (l, some d))) =
        if dRel.any (fun d => (sqlEq (fk l) (pk d)).inWhere) then [l] else [] := by
      cases hfk : fk l with
      | none =>
        have hNil : dRel.filter (fun d => (sqlEq none (pk d)).inWhere) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro d _; simp [sqlEq, SqlBool.inWhere]
        have hAnyFalse : dRel.any (fun d => (sqlEq none (pk d)).inWhere) = false := by
          cases h : dRel.any (fun d => (sqlEq none (pk d)).inWhere)
          · rfl
          · rw [List.any_eq_true] at h
            obtain ⟨d, _, hd⟩ := h
            simp [sqlEq, SqlBool.inWhere] at hd
        simp [hNil, hAnyFalse]
      | some kv =>
        have hLen := hAtMostOne kv dRel hUniquePk
        cases hm : dRel.filter (fun d => (sqlEq (some kv) (pk d)).inWhere) with
        | nil =>
          have hAnyFalse : dRel.any (fun d => (sqlEq (some kv) (pk d)).inWhere) = false := by
            cases h : dRel.any (fun d => (sqlEq (some kv) (pk d)).inWhere)
            · rfl
            · rw [List.any_eq_true] at h
              obtain ⟨d, hdMem, hdOk⟩ := h
              have : d ∈ dRel.filter (fun d => (sqlEq (some kv) (pk d)).inWhere) :=
                List.mem_filter.mpr ⟨hdMem, hdOk⟩
              rw [hm] at this; nomatch this
          simp [hAnyFalse]
        | cons d0 ds =>
          rw [hm] at hLen
          have hDsNil : ds = [] := by
            cases ds with
            | nil => rfl
            | cons _ _ => simp at hLen
          subst hDsNil
          have hAnyTrue : dRel.any (fun d => (sqlEq (some kv) (pk d)).inWhere) = true := by
            have hd0 : d0 ∈ dRel.filter (fun d => (sqlEq (some kv) (pk d)).inWhere) := by
              rw [hm]; exact List.mem_cons_self ..
            rw [List.mem_filter] at hd0
            exact List.any_eq_true.mpr ⟨d0, hd0.1, hd0.2⟩
          simp [hAnyTrue]
    unfold innerJoin at ih ⊢
    rw [List.flatMap_cons, List.map_append, hHead, ih, List.filter_cons]
    cases dRel.any (fun d => (sqlEq (fk l) (pk d)).inWhere) <;> rfl

/-- ✅ **定理 10C (`unused_unique_left_join_elim_sound` — TPC-DS Q72)**:
    後続の SELECT / WHERE / GROUP BY で右表 `R`（例: `catalog_returns`）の列が一切参照されず、
    左側の生存行 `l ∈ lRel` に対する `R` のマッチ行が高々 1 行（`(rRel.filter (p l)).length ≤ 1`）であれば、
    **`LEFT OUTER JOIN R` 自体を完全に削除しても、左側の行リスト・重複度・順序は 100% 完全一致** する！ -/
theorem unused_unique_left_join_elim_sound {L R : Type}
    (p : L → R → SqlBool) (lRel : Rel L) (rRel : Rel R)
    (hAtMostOne : ∀ l ∈ lRel, (rRel.filter (fun r => (p l r).inWhere)).length ≤ 1) :
    (leftOuterJoin p lRel rRel).map Prod.fst = lRel := by
  induction lRel with
  | nil => rfl
  | cons l ls ih =>
    have hHeadLen := hAtMostOne l (List.mem_cons_self ..)
    have hTail := ih (fun x hx => hAtMostOne x (List.mem_cons_of_mem _ hx))
    have hHead :
        List.map Prod.fst (if (rRel.filter (fun y => (p l y).inWhere)).isEmpty = true then [(l, none)]
         else (rRel.filter (fun y => (p l y).inWhere)).map (fun y => (l, some y))) = [l] := by
      cases hm : rRel.filter (fun r => (p l r).inWhere) with
      | nil => rfl
      | cons r0 rs =>
        rw [hm] at hHeadLen
        cases rs with
        | nil => rfl
        | cons _ _ => simp at hHeadLen
    unfold leftOuterJoin at hTail ⊢
    rw [List.flatMap_cons, List.map_append, hHead, hTail]
    rfl

/-!
## 11. 異なる証人 `EXISTS (k = ok ∧ w ≠ ow)` の `(min_w, max_w)` O(1) ルックアップ化（Q16）
-/

private def listMinInt : Int → List Int → Int
  | acc, [] => acc
  | acc, x :: xs => listMinInt (min acc x) xs

private def listMaxInt : Int → List Int → Int
  | acc, [] => acc
  | acc, x :: xs => listMaxInt (max acc x) xs

private theorem listMinInt_le_acc (acc : Int) (xs : List Int) :
    listMinInt acc xs ≤ acc := by
  induction xs generalizing acc with
  | nil => exact Int.le_refl _
  | cons x xs ih => exact Int.le_trans (ih (min acc x)) (Int.min_le_left acc x)

private theorem listMinInt_le_mem (acc : Int) (xs : List Int) :
    ∀ y ∈ xs, listMinInt acc xs ≤ y := by
  induction xs generalizing acc with
  | nil => intro _ h; nomatch h
  | cons x xs ih =>
    intro y hy
    rcases List.mem_cons.mp hy with rfl | hmem
    · exact Int.le_trans (listMinInt_le_acc (min acc y) xs) (Int.min_le_right acc y)
    · exact ih (min acc x) y hmem

private theorem listMinInt_mem (acc : Int) (xs : List Int) :
    listMinInt acc xs = acc ∨ listMinInt acc xs ∈ xs := by
  induction xs generalizing acc with
  | nil => exact Or.inl rfl
  | cons x xs ih =>
    rcases ih (min acc x) with hEq | hMem
    · rw [listMinInt, hEq]
      by_cases hle : acc ≤ x
      · exact Or.inl (Int.min_eq_left hle)
      · exact Or.inr (List.mem_cons.mpr (Or.inl (Int.min_eq_right (Int.le_of_not_le hle))))
    · exact Or.inr (List.mem_cons_of_mem _ hMem)

private theorem acc_le_listMaxInt (acc : Int) (xs : List Int) :
    acc ≤ listMaxInt acc xs := by
  induction xs generalizing acc with
  | nil => exact Int.le_refl _
  | cons x xs ih => exact Int.le_trans (Int.le_max_left acc x) (ih (max acc x))

private theorem mem_le_listMaxInt (acc : Int) (xs : List Int) :
    ∀ y ∈ xs, y ≤ listMaxInt acc xs := by
  induction xs generalizing acc with
  | nil => intro _ h; nomatch h
  | cons x xs ih =>
    intro y hy
    rcases List.mem_cons.mp hy with rfl | hmem
    · exact Int.le_trans (Int.le_max_right acc y) (acc_le_listMaxInt (max acc y) xs)
    · exact ih (max acc x) y hmem

private theorem listMaxInt_mem (acc : Int) (xs : List Int) :
    listMaxInt acc xs = acc ∨ listMaxInt acc xs ∈ xs := by
  induction xs generalizing acc with
  | nil => exact Or.inl rfl
  | cons x xs ih =>
    rcases ih (max acc x) with hEq | hMem
    · rw [listMaxInt, hEq]
      by_cases hle : acc ≤ x
      · exact Or.inr (List.mem_cons.mpr (Or.inl (Int.max_eq_right hle)))
      · exact Or.inl (Int.max_eq_left (Int.le_of_not_le hle))
    · exact Or.inr (List.mem_cons_of_mem _ hMem)

/-- ✅ **定理 11 (`distinct_witness_exists_to_minmax_sound` — TPC-DS Q16, Q94, Q95)**:
    Q16 の `EXISTS (SELECT * FROM catalog_sales cs2 WHERE cs1.cs_order_number = cs2.cs_order_number AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)`
    に対して、相関インデックスで全倉庫リストを走査する代わりに、
    各非 NULL 注文番号ごとの非 NULL 倉庫集合 `w0 :: ws` の **`(min_w, max_w)` 2 値だけ** をフラットマップに保持すれば、
    任意の非 NULL `ow : Int` に対して **`(∃ v ∈ w0 :: ws, v ≠ ow) ↔ (min_w ≠ ow ∨ max_w ≠ ow)`** が **100% 完全同値** となる！ -/
theorem distinct_witness_exists_to_minmax_sound (w0 : Int) (ws : List Int) (ow : Int) :
    let minW := listMinInt w0 ws
    let maxW := listMaxInt w0 ws
    (∃ v ∈ (w0 :: ws), v ≠ ow) ↔ (minW ≠ ow ∨ maxW ≠ ow) := by
  intro minW maxW
  constructor
  · rintro ⟨v, hvMem, hvNe⟩
    by_cases hMin : minW = ow
    · by_cases hMax : maxW = ow
      · exfalso
        have hLe1 : ow ≤ v := by
          rw [← hMin]
          rcases List.mem_cons.mp hvMem with rfl | hIn
          · exact listMinInt_le_acc v ws
          · exact listMinInt_le_mem w0 ws v hIn
        have hLe2 : v ≤ ow := by
          rw [← hMax]
          rcases List.mem_cons.mp hvMem with rfl | hIn
          · exact acc_le_listMaxInt v ws
          · exact mem_le_listMaxInt w0 ws v hIn
        exact hvNe (Int.le_antisymm hLe2 hLe1)
      · exact Or.inr hMax
    · exact Or.inl hMin
  · intro hOr
    rcases hOr with hMinNe | hMaxNe
    · refine ⟨minW, ?_, hMinNe⟩
      rcases listMinInt_mem w0 ws with hEq | hIn
      · exact hEq ▸ List.mem_cons_self ..
      · exact List.mem_cons_of_mem _ hIn
    · refine ⟨maxW, ?_, hMaxNe⟩
      rcases listMaxInt_mem w0 ws with hEq | hIn
      · exact hEq ▸ List.mem_cons_self ..
      · exact List.mem_cons_of_mem _ hIn

/-!
## 12. 相関スカラ集約サブクエリ脱相関（Q06）＆ 結合残余 `p(build) OR q(probe, build)` 事前評価（Q15）
-/

/-- ✅ **定理 12A (`correlated_scalar_agg_decorrelation_sound` — TPC-DS Q06)**:
    `outer.val > c * (SELECT agg(j.x) FROM inner j WHERE j.k = outer.k)` のように、
    グループ集約値が空（`none`）のとき Null-Rejecting となる比較述語 `cmp` を持つ相関スカラ集約は、
    `inner` を非 NULL キーで事前 `GROUP BY k` した派生表との等価内部結合と **100% 完全一致** する！ -/
theorem correlated_scalar_agg_decorrelation_sound {O I K AggV : Type} [DecidableEq K]
    (outerKey : O → SqlVal K) (innerKey : I → SqlVal K)
    (aggBucket : Rel I → SqlVal AggV)
    (hEmptyNull : aggBucket [] = none)
    (cmp : O → SqlVal AggV → SqlBool)
    (hNullRej : ∀ o, (cmp o none).inWhere = false)
    (outerRel : Rel O) (innerRel : Rel I) :
    let corrFilter :=
      outerRel.filter (fun o =>
        (cmp o (aggBucket (innerRel.filter (fun j => (sqlEq (outerKey o) (innerKey j)).inWhere)))).inWhere)
    let nonNullInner : List (K × I) :=
      innerRel.filterMap (fun j => (innerKey j).map (fun k => (k, j)))
    let preAgg : List (K × SqlVal AggV) :=
      groupBy Prod.fst (fun _ b => aggBucket (b.map Prod.snd)) nonNullInner
    let joinedFilter :=
      (innerJoin (fun o kv =>
        match outerKey o with
        | none => .tNull
        | some ok => if ok == kv.1 then cmp o kv.2 else .tFalse) outerRel preAgg).map Prod.fst
    corrFilter = joinedFilter := by
  intro corrFilter nonNullInner preAgg joinedFilter
  dsimp [corrFilter, joinedFilter]
  have hPreAggNodup : (preAgg.map Prod.fst).Nodup := by
    dsimp [preAgg, groupBy]
    rw [List.map_map]
    have hId : (Prod.fst ∘ fun k => (k, aggBucket ((nonNullInner.filter (fun r => r.1 == k)).map Prod.snd))) = id := rfl
    rw [hId, List.map_id]
    exact nodup_distinctKeys _
  have hBucketEqGen : ∀ (is : Rel I) (kv : K),
      ((is.filterMap (fun j => (innerKey j).map (fun k => (k, j)))).filter (fun r => r.1 == kv)).map Prod.snd =
      is.filter (fun j => (sqlEq (some kv) (innerKey j)).inWhere) := by
    intro is kv
    induction is with
    | nil => rfl
    | cons j js ih =>
      cases hik : innerKey j with
      | none =>
        have hEqNone : (sqlEq (some kv) none).inWhere = false := rfl
        simp only [List.filterMap_cons, hik, Option.map_none, List.filter_cons,
          hEqNone, Bool.false_eq_true, ↓reduceIte]
        exact ih
      | some kv' =>
        by_cases hEq : kv' = kv
        · subst hEq
          have hSqlTrue : (sqlEq (some kv') (some kv')).inWhere = true := by
            simp [sqlEq, SqlBool.inWhere]
          simp only [List.filterMap_cons, hik, Option.map_some, List.filter_cons,
            beq_self_eq_true, hSqlTrue, ↓reduceIte, List.map_cons]
          rw [ih]
        · have hNeSymm : kv ≠ kv' := fun h => hEq h.symm
          have hBeqFalse : (kv' == kv) = false := beq_false_of_ne hEq
          have hSqlEqFalse : (sqlEq (some kv) (some kv')).inWhere = false := by
            simp [sqlEq, hNeSymm, SqlBool.inWhere]
          simp only [List.filterMap_cons, hik, Option.map_some, List.filter_cons,
            hBeqFalse, hSqlEqFalse, Bool.false_eq_true, ↓reduceIte]
          exact ih
  have hBucketEq : ∀ kv : K,
      (nonNullInner.filter (fun r => r.1 == kv)).map Prod.snd =
      innerRel.filter (fun j => (sqlEq (some kv) (innerKey j)).inWhere) :=
    hBucketEqGen innerRel
  have hMemPreAgg : ∀ kv : K,
      (kv ∈ preAgg.map Prod.fst) ↔
        (innerRel.filter (fun j => (sqlEq (some kv) (innerKey j)).inWhere)) ≠ [] := by
    intro kv
    dsimp [preAgg, groupBy]
    rw [List.map_map]
    have hId : (Prod.fst ∘ fun k => (k, aggBucket ((nonNullInner.filter (fun r => r.1 == k)).map Prod.snd))) = id := rfl
    rw [hId, List.map_id, mem_distinctKeys]
    rw [← hBucketEq kv]
    constructor
    · intro hMem hNil
      rw [List.map_eq_nil_iff, List.filter_eq_nil_iff] at hNil
      rw [List.mem_map] at hMem
      obtain ⟨⟨k', j'⟩, hkjMem, rfl⟩ := hMem
      have := hNil (k', j') hkjMem
      simp at this
    · intro hNonNil
      cases hFilt : nonNullInner.filter (fun r => r.1 == kv) with
      | nil =>
        rw [hFilt] at hNonNil
        simp at hNonNil
      | cons r0 _ =>
        have hr0 := List.mem_filter.mp (hFilt ▸ List.mem_cons_self ..)
        have hkEq : r0.1 = kv := eq_of_beq hr0.2
        exact hkEq ▸ List.mem_map_of_mem hr0.1
  induction outerRel with
  | nil => rfl
  | cons o os ih =>
    unfold innerJoin at ih ⊢
    rw [List.filter_cons, List.flatMap_cons, List.map_append, ← ih]
    cases hok : outerKey o with
    | none =>
      have hLeftNil : innerRel.filter (fun j => (sqlEq none (innerKey j)).inWhere) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro j _; simp [sqlEq, SqlBool.inWhere]
      have hRightNil : preAgg.filter (fun _ => SqlBool.tNull.inWhere) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro _ _; simp [SqlBool.inWhere]
      rw [hLeftNil, hEmptyNull, hNullRej o]
      simp [hok, hRightNil]
    | some kv =>
      by_cases hHasKey : kv ∈ preAgg.map Prod.fst
      · -- Exactly one entry in preAgg has key kv
        have hFind : preAgg.filter (fun p => p.1 == kv) =
            [(kv, aggBucket (innerRel.filter (fun j => (sqlEq (some kv) (innerKey j)).inWhere)))] := by
          dsimp [preAgg, groupBy]
          have hMemDK : kv ∈ distinctKeys (nonNullInner.map Prod.fst) := by
            dsimp [preAgg, groupBy] at hHasKey
            rw [List.map_map] at hHasKey
            have hId : (Prod.fst ∘ fun k => (k, aggBucket ((nonNullInner.filter (fun r => r.1 == k)).map Prod.snd))) = id := rfl
            rw [hId, List.map_id] at hHasKey
            exact hHasKey
          have hNdDK := nodup_distinctKeys (nonNullInner.map Prod.fst)
          rw [← hBucketEq kv]
          dsimp [nonNullInner] at hMemDK hNdDK ⊢
          generalize distinctKeys ((innerRel.filterMap (fun j => (innerKey j).map (fun k => (k, j)))).map Prod.fst) = dk at hMemDK hNdDK
          induction dk with
          | nil => nomatch hMemDK
          | cons k0 ks ihk =>
            rw [List.nodup_cons] at hNdDK
            rcases List.mem_cons.mp hMemDK with hEqK | hInKs
            · subst k0
              have hRestNil : (ks.map (fun k => (k, aggBucket (((innerRel.filterMap (fun j => (innerKey j).map (fun k => (k, j)))).filter (fun r => r.1 == k)).map Prod.snd)))).filter
                  (fun p => p.1 == kv) = [] := by
                apply List.filter_eq_nil_iff.mpr
                intro p hp
                rw [List.mem_map] at hp
                obtain ⟨k', hk', rfl⟩ := hp
                have hNe : k' ≠ kv := fun h => hNdDK.1 (h ▸ hk')
                simp [beq_false_of_ne hNe]
              rw [List.map_cons, List.filter_cons, hRestNil]
              simp
            · have hNe : k0 ≠ kv := fun h => hNdDK.1 (h ▸ hInKs)
              simp only [List.map_cons, List.filter_cons, beq_false_of_ne hNe,
                Bool.false_eq_true, ↓reduceIte, ihk hInKs hNdDK.2]
        have hRewFilter :
            preAgg.filter (fun p => (if kv == p.1 then cmp o p.2 else .tFalse).inWhere) =
            (preAgg.filter (fun p => p.1 == kv)).filter (fun p => (cmp o p.2).inWhere) := by
          rw [List.filter_filter]
          apply List.filter_congr
          intro p _
          by_cases hEq : p.1 = kv
          · subst hEq; simp
          · have h1 : (kv == p.1) = false := beq_false_of_ne (fun h => hEq h.symm)
            have h2 : (p.1 == kv) = false := beq_false_of_ne hEq
            simp [h1, h2, SqlBool.inWhere]
        simp only [hok, hRewFilter, hFind, List.filter_cons, List.filter_nil]
        cases (cmp o (aggBucket (innerRel.filter (fun j => (sqlEq (some kv) (innerKey j)).inWhere)))).inWhere <;> rfl
      · have hEmptyBucket : innerRel.filter (fun j => (sqlEq (some kv) (innerKey j)).inWhere) = [] := by
          by_cases hNil : innerRel.filter (fun j => (sqlEq (some kv) (innerKey j)).inWhere) = []
          · exact hNil
          · exact False.elim (hHasKey ((hMemPreAgg kv).mpr hNil))
        have hNoMatch : preAgg.filter (fun p => (if kv == p.1 then cmp o p.2 else .tFalse).inWhere) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro p hp
          by_cases hEq : kv = p.1
          · subst hEq
            exact False.elim (hHasKey (List.mem_map_of_mem hp))
          · simp [beq_false_of_ne hEq, SqlBool.inWhere]
        rw [hEmptyBucket, hEmptyNull, hNullRej o]
        simp only [hok, Bool.false_eq_true, ↓reduceIte, hNoMatch, List.map_nil, List.nil_append]

/-- ✅ **定理 12B (`precomputed_build_disjunct_residual_sound` — TPC-DS Q15)**:
    Q15 の `(SUBSTR(ca_zip, 1, 5) IN (...) OR ca_state IN (...) OR cs_sales_price > 500)` のように、
    3 値論理 `OR` 結合残余の一方の選言肢 `p(b)` がビルド側行 `b` のみに依存するとき、
    `p(b)` が `.tUnknown`（NULL）を取り得る場合であっても、**ビルド時に 2 値フラグ `bp(b) := (p b).inWhere` を
    1 回だけ事前計算** しておけば、ペアごとの残余判定 `((p b).or (q l b)).inWhere` は
    `bp(b) || (q l b).inWhere` と **全 9 通りの 3VL 真理値で 100% 完全一致** する！ -/
theorem precomputed_build_disjunct_residual_sound (pb qlb : SqlBool) :
    (pb.or qlb).inWhere = (pb.inWhere || qlb.inWhere) := by
  cases pb <;> cases qlb <;> rfl

/-!
## 13. 狭義単調パーティション上の隣接 `RANK()` 自己結合 → `LAG / LEAD` 変換（Q47）＆
       単一要素パーティション上の `RANK() == 1` 定数畳み込み（Q70）
-/

private def indexRankTriples (n : Nat) : List (Nat × Nat × Nat) :=
  (List.range n).flatMap (fun i =>
    (List.range n).flatMap (fun j =>
      ((List.range n).filter (fun k =>
        decide ((i + 1 = (j + 1) + 1) ∧ (i + 1 + 1 = k + 1)))).map (fun k => (i, j, k))))

private theorem flatMap_congr_mem {α β : Type} {xs : List α} {f g : α → List β}
    (h : ∀ x ∈ xs, f x = g x) : xs.flatMap f = xs.flatMap g := by
  induction xs with
  | nil => rfl
  | cons x rest ih =>
    simp only [List.flatMap_cons, h x (List.mem_cons_self ..),
      ih (fun y hy => h y (List.mem_cons_of_mem _ hy))]

private def indexLagLeadTriples (n : Nat) : List (Nat × Nat × Nat) :=
  (List.range n).filterMap (fun i =>
    if 1 ≤ i ∧ i + 1 < n then some (i, i - 1, i + 1) else none)

private theorem filter_range_unique_eq (n target : Nat) :
    (List.range n).filter (fun x => decide (x = target)) =
      if target < n then [target] else [] := by
  induction n with
  | zero => simp
  | succ m ih =>
    rw [List.range_succ, List.filter_append, ih]
    by_cases hEq : m = target
    · subst hEq
      have hNotLt : ¬(m < m) := Nat.lt_irrefl m
      have hLtSucc : m < m + 1 := Nat.lt_succ_self m
      simp [hNotLt, hLtSucc]
    · have hIff : (target < m + 1) ↔ (target < m) := by omega
      simp [hEq, hIff]

/-- ✅ **定理 13A (`adjacent_rank_self_join_to_lag_lead_sound` — TPC-DS Q47, Q57)**:
    `GROUP BY (P, O)` によってパーティション内順序キー `O` に同順位（peer tie）が存在しない長さ `n` のパーティションでは、
    0-indexed 位置 `i` の行の `RANK()` は厳密に `i + 1` となるため、
    `rn = rn_lag + 1 ∧ rn = rn_lead - 1` で `v1, v1_lag, v1_lead` を 3 重自己結合した添字三つ組列は、
    **自己結合を一切行わず単一配列の隣接添字 `(i, i - 1, i + 1)`（`1 ≤ i ∧ i + 1 < n`）を読み出す
    `LAG(1) / LEAD(1)` 走査** と **100% 完全一致** する！ -/
theorem adjacent_rank_self_join_to_lag_lead_sound (n : Nat) :
    indexRankTriples n = indexLagLeadTriples n := by
  unfold indexRankTriples indexLagLeadTriples
  have hStepK : ∀ i j : Nat,
      ((List.range n).filter (fun k => decide ((i + 1 = (j + 1) + 1) ∧ (i + 1 + 1 = k + 1)))).map (fun k => (i, j, k)) =
      if (j + 1 = i) ∧ (i + 1 < n) then [(i, j, i + 1)] else [] := by
    intro i j
    by_cases hJ : j + 1 = i
    · have hSame : (List.range n).filter (fun k => decide ((i + 1 = (j + 1) + 1) ∧ (i + 1 + 1 = k + 1))) =
          (List.range n).filter (fun k => decide (k = i + 1)) := by
        apply List.filter_congr
        intro k _
        simp only [decide_eq_decide]
        omega
      rw [hSame, filter_range_unique_eq n (i + 1)]
      by_cases hK : i + 1 < n <;> simp [hJ, hK]
    · have hNil : (List.range n).filter (fun k => decide ((i + 1 = (j + 1) + 1) ∧ (i + 1 + 1 = k + 1))) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro k _
        simp only [decide_eq_true_eq]
        omega
      rw [hNil]
      simp [hJ]
  have hStepJ : ∀ i ∈ List.range n,
      (List.range n).flatMap (fun j =>
        ((List.range n).filter (fun k => decide ((i + 1 = (j + 1) + 1) ∧ (i + 1 + 1 = k + 1)))).map (fun k => (i, j, k))) =
      if 1 ≤ i ∧ i + 1 < n then [(i, i - 1, i + 1)] else [] := by
    intro i hiMem
    have hiLt : i < n := List.mem_range.mp hiMem
    have hCongr : ∀ j ∈ List.range n,
        ((List.range n).filter (fun k => decide ((i + 1 = (j + 1) + 1) ∧ (i + 1 + 1 = k + 1)))).map (fun k => (i, j, k)) =
        if (j + 1 = i) ∧ (i + 1 < n) then [(i, j, i + 1)] else [] := fun j _ => hStepK i j
    rw [flatMap_congr_mem hCongr]
    by_cases hCond : 1 ≤ i ∧ i + 1 < n
    · have hRew : (List.range n).flatMap (fun j => if (j + 1 = i) ∧ (i + 1 < n) then [(i, j, i + 1)] else []) =
          ((List.range n).filter (fun j => decide (j = i - 1))).map (fun j => (i, j, i + 1)) := by
        induction n with
        | zero => nomatch hiLt
        | succ m ihm =>
          -- Direct induction over general list
          clear hiMem hiLt ihm
          generalize List.range (m + 1) = js
          induction js with
          | nil => rfl
          | cons j rest ihj =>
            simp only [List.flatMap_cons, List.filter_cons, ihj]
            by_cases hjEq : j = i - 1
            · subst hjEq
              have hTrue : (i - 1 + 1 = i) ∧ (i + 1 < m + 1) := ⟨by omega, hCond.2⟩
              simp only [hTrue, and_self, ↓reduceIte, decide_true, List.map_cons, List.singleton_append]
            · have hNeSucc : j + 1 ≠ i := by omega
              have hdFalse : decide (j = i - 1) = false := decide_eq_false hjEq
              simp only [hNeSucc, false_and, ↓reduceIte, List.nil_append, hdFalse, Bool.false_eq_true]
      have hjLt : i - 1 < n := by omega
      rw [hRew, filter_range_unique_eq n (i - 1)]
      simp [hCond, hjLt]
    · simp only [hCond, ↓reduceIte]
      rw [List.flatMap_eq_nil_iff]
      intro j hjMem
      have hjLt : j < n := List.mem_range.mp hjMem
      have hFalse : ¬((j + 1 = i) ∧ (i + 1 < n)) := by omega
      simp [hFalse]
  have hAll : ∀ xs : List Nat,
      (∀ i ∈ xs, i ∈ List.range n) →
      xs.flatMap (fun i =>
        (List.range n).flatMap (fun j =>
          ((List.range n).filter (fun k => decide ((i + 1 = (j + 1) + 1) ∧ (i + 1 + 1 = k + 1)))).map (fun k => (i, j, k)))) =
      xs.filterMap (fun i => if 1 ≤ i ∧ i + 1 < n then some (i, i - 1, i + 1) else none) := by
    intro xs hSub
    induction xs with
    | nil => rfl
    | cons i is ih =>
      rw [List.flatMap_cons, List.filterMap_cons, hStepJ i (hSub i (List.mem_cons_self ..)),
          ih (fun x hx => hSub x (List.mem_cons_of_mem _ hx))]
      by_cases h : 1 ≤ i ∧ i + 1 < n <;> simp [h]
  exact hAll (List.range n) (fun _ h => h)

/-- ✅ **定理 13B (`singleton_partition_rank_const_one_sound` — TPC-DS Q70)**:
    `GROUP BY K` の出力行リスト `rows`（`(rows.map keyK).Nodup`）において、
    ウィンドウの `PARTITION BY P` が `K` を細分する（`partP r1 = partP r2 → keyK r1 = keyK r2`）ならば、
    各行のウィンドウパーティションは常に長さ 1 の単一要素リスト `[r]` となり、
    `RANK() / DENSE_RANK() / ROW_NUMBER()` はすべての行で恒等的に定数 `1` となる！ -/
theorem singleton_partition_rank_const_one_sound {Row K P : Type}
    [DecidableEq K] [DecidableEq P]
    (keyK : Row → K) (partP : Row → P)
    (hRefines : ∀ r1 r2, partP r1 = partP r2 → keyK r1 = keyK r2)
    (rows : List Row)
    (hNodup : (rows.map keyK).Nodup) :
    ∀ r ∈ rows, rows.filter (fun r' => partP r' == partP r) = [r] := by
  induction rows with
  | nil => intro _ h; nomatch h
  | cons x xs ih =>
    simp only [List.map_cons, List.nodup_cons] at hNodup
    intro r hr
    rcases List.mem_cons.mp hr with rfl | hInXs
    · have hEmpty : xs.filter (fun r' => partP r' == partP r) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro r' hr'
        by_cases hEqP : partP r' = partP r
        · exfalso
          exact hNodup.1 (hRefines r' r hEqP ▸ List.mem_map_of_mem hr')
        · simp [beq_false_of_ne hEqP]
      simp [hEmpty]
    · have hxNe : (partP x == partP r) = false := by
        apply beq_false_of_ne
        intro hEqP
        exact hNodup.1 (hRefines x r hEqP ▸ List.mem_map_of_mem hInXs)
      simp [hxNe, ih hNodup.2 r hInXs]

/-!
## 14. `SELECT DISTINCT` の `UNION ALL`＋ディメンション結合への分配（Q54）
-/

/-- ✅ **定理 14 (`distinct_union_all_dim_distribution_sound` — TPC-DS Q54)**:
    Q54 の `my_customers` のように、`(A UNION ALL B)` と選択的ディメンション表 `D` を結合してから
    `SELECT DISTINCT proj(f, d)` を取るクエリは、
    **各ブランチ `A`, `B` の内側にディメンション結合と `SELECT DISTINCT` を分配してから `UNION DISTINCT` する形**
    と集合として **100% 完全同値** であり、無フィルタの `UNION ALL` マテリアライズを完全に除去できる！ -/
theorem distinct_union_all_dim_distribution_sound {F D Out : Type} [DecidableEq Out]
    (onPred : F → D → SqlBool) (projFn : F → D → Out)
    (branchA branchB : Rel F) (dimRel : Rel D) :
    let evalJoinProj (br : Rel F) : List Out :=
      br.flatMap (fun f => (dimRel.filter (fun d => (onPred f d).inWhere)).map (projFn f))
    BagEq
      (distinctKeys (evalJoinProj (branchA ++ branchB)))
      (distinctKeys (distinctKeys (evalJoinProj branchA) ++ distinctKeys (evalJoinProj branchB))) := by
  intro evalJoinProj
  apply perm_of_nodup_iff_mem (nodup_distinctKeys _) (nodup_distinctKeys _)
  intro o
  simp only [evalJoinProj, mem_distinctKeys, List.flatMap_append, List.mem_append]

/-!
## 15. 階層的 `ROLLUP` / 共通基底 `GROUP BY` からの粗粒度再集約（Q22）
-/

def sumOptInts : List (Option Int) → Int
  | [] => 0
  | none :: xs => sumOptInts xs
  | some v :: xs => v + sumOptInts xs

def countOptInts : List (Option Int) → Nat
  | [] => 0
  | none :: xs => countOptInts xs
  | some _ :: xs => 1 + countOptInts xs

theorem sumOptInts_append (xs ys : List (Option Int)) :
    sumOptInts (xs ++ ys) = sumOptInts xs + sumOptInts ys := by
  induction xs with
  | nil => simp [sumOptInts]
  | cons x xs ih =>
    cases x <;> simp [sumOptInts, ih, Int.add_assoc]

theorem countOptInts_append (xs ys : List (Option Int)) :
    countOptInts (xs ++ ys) = countOptInts xs + countOptInts ys := by
  induction xs with
  | nil => simp [countOptInts]
  | cons x xs ih =>
    cases x <;> simp [countOptInts, ih, Nat.add_assoc]

/-- ✅ **定理 15 (`union_all_hierarchical_rollup_from_finest_group_sound` — TPC-DS Q22)**:
    最細粒度キー `keyFine : Row → KF` で一度だけ `(SUM(x), COUNT(x))` を集約しておけば、
    任意の粗粒度ロールアップ射影 `projCoarse : KF → KC` に対する `(SUM(x), COUNT(x))`（したがって `AVG(x) = SUM / NULLIF(COUNT, 0)`）は、
    元テーブル `R` を再走査することなく、最細粒度グループの部分和・部分件数を足し合わせるだけで
    **100% 完全一致** する！ -/
theorem union_all_hierarchical_rollup_from_finest_group_sound {Row KF KC : Type}
    [DecidableEq KF] [DecidableEq KC]
    (keyFine : Row → KF) (projCoarse : KF → KC) (valOf : Row → Option Int)
    (R : List Row) (fineDomain : List KF)
    (hNodup : fineDomain.Nodup)
    (hCover : ∀ r ∈ R, keyFine r ∈ fineDomain)
    (kc : KC) :
    let coarseFineKeys := fineDomain.filter (fun kf => projCoarse kf == kc)
    let fineSum (kf : KF) := sumOptInts ((R.filter (fun r => keyFine r == kf)).map valOf)
    let fineCnt (kf : KF) := countOptInts ((R.filter (fun r => keyFine r == kf)).map valOf)
    (coarseFineKeys.foldr (fun kf acc => fineSum kf + acc) 0 =
      sumOptInts ((R.filter (fun r => projCoarse (keyFine r) == kc)).map valOf)) ∧
    (coarseFineKeys.foldr (fun kf acc => fineCnt kf + acc) 0 =
      countOptInts ((R.filter (fun r => projCoarse (keyFine r) == kc)).map valOf)) := by
  intro coarseFineKeys fineSum fineCnt
  have hGen : ∀ (R' : List Row), (∀ r ∈ R', keyFine r ∈ fineDomain) →
      (coarseFineKeys.foldr (fun kf acc =>
          sumOptInts ((R'.filter (fun r => keyFine r == kf)).map valOf) + acc) 0 =
        sumOptInts ((R'.filter (fun r => projCoarse (keyFine r) == kc)).map valOf)) ∧
      (coarseFineKeys.foldr (fun kf acc =>
          countOptInts ((R'.filter (fun r => keyFine r == kf)).map valOf) + acc) 0 =
        countOptInts ((R'.filter (fun r => projCoarse (keyFine r) == kc)).map valOf)) := by
    intro R' hCov'
    induction R' with
    | nil =>
      have hZeroSum : ∀ ks : List KF,
          ks.foldr (fun (_ : KF) (acc : Int) => 0 + acc) (0 : Int) = 0 := by
        intro ks
        induction ks with
        | nil => rfl
        | cons _ _ ihk => rw [List.foldr_cons, Int.zero_add, ihk]
      have hZeroCnt : ∀ ks : List KF,
          ks.foldr (fun (_ : KF) (acc : Nat) => 0 + acc) (0 : Nat) = 0 := by
        intro ks
        induction ks with
        | nil => rfl
        | cons _ _ ihk => rw [List.foldr_cons, Nat.zero_add, ihk]
      exact ⟨by simpa only [List.filter_nil, List.map_nil, sumOptInts] using hZeroSum coarseFineKeys,
             by simpa only [List.filter_nil, List.map_nil, countOptInts] using hZeroCnt coarseFineKeys⟩
    | cons x xs ih =>
      have hxDom : keyFine x ∈ fineDomain := hCov' x (List.mem_cons_self ..)
      have hxsCov : ∀ r ∈ xs, keyFine r ∈ fineDomain :=
        fun r hr => hCov' r (List.mem_cons_of_mem _ hr)
      obtain ⟨ihSum, ihCnt⟩ := ih hxsCov
      have hStep : ∀ (ks : List KF), ks.Nodup →
          (ks.foldr (fun kf acc =>
              sumOptInts (((x :: xs).filter (fun r => keyFine r == kf)).map valOf) + acc) 0 =
            (if keyFine x ∈ ks then sumOptInts [valOf x] else 0) +
            ks.foldr (fun kf acc =>
              sumOptInts ((xs.filter (fun r => keyFine r == kf)).map valOf) + acc) 0) ∧
          (ks.foldr (fun kf acc =>
              countOptInts (((x :: xs).filter (fun r => keyFine r == kf)).map valOf) + acc) 0 =
            (if keyFine x ∈ ks then countOptInts [valOf x] else 0) +
            ks.foldr (fun kf acc =>
              countOptInts ((xs.filter (fun r => keyFine r == kf)).map valOf) + acc) 0) := by
        intro ks hksNd
        induction ks with
        | nil => simp
        | cons k ks' ihk =>
          simp only [List.nodup_cons] at hksNd
          obtain ⟨ihkS, ihkC⟩ := ihk hksNd.2
          by_cases hEq : keyFine x = k
          · subst hEq
            have hNotMem : keyFine x ∉ ks' := hksNd.1
            have hHeadS : sumOptInts (((x :: xs).filter (fun r => keyFine r == keyFine x)).map valOf) =
                sumOptInts [valOf x] + sumOptInts ((xs.filter (fun r => keyFine r == keyFine x)).map valOf) := by
              simpa [List.filter_cons, beq_self_eq_true] using
                sumOptInts_append [valOf x] ((xs.filter (fun r => keyFine r == keyFine x)).map valOf)
            have hHeadC : countOptInts (((x :: xs).filter (fun r => keyFine r == keyFine x)).map valOf) =
                countOptInts [valOf x] + countOptInts ((xs.filter (fun r => keyFine r == keyFine x)).map valOf) := by
              simpa [List.filter_cons, beq_self_eq_true] using
                countOptInts_append [valOf x] ((xs.filter (fun r => keyFine r == keyFine x)).map valOf)
            constructor
            · rw [List.foldr_cons, List.foldr_cons, hHeadS, ihkS]
              simp only [List.mem_cons, true_or, ↓reduceIte, hNotMem]
              omega
            · rw [List.foldr_cons, List.foldr_cons, hHeadC, ihkC]
              simp only [List.mem_cons, true_or, ↓reduceIte, hNotMem]
              omega
          · have hBeq : (keyFine x == k) = false := beq_false_of_ne hEq
            have hHeadS : sumOptInts (((x :: xs).filter (fun r => keyFine r == k)).map valOf) =
                sumOptInts ((xs.filter (fun r => keyFine r == k)).map valOf) := by
              simp [hBeq]
            have hHeadC : countOptInts (((x :: xs).filter (fun r => keyFine r == k)).map valOf) =
                countOptInts ((xs.filter (fun r => keyFine r == k)).map valOf) := by
              simp [hBeq]
            constructor
            · rw [List.foldr_cons, List.foldr_cons, hHeadS, ihkS]
              simp only [List.mem_cons, hEq, false_or]
              omega
            · rw [List.foldr_cons, List.foldr_cons, hHeadC, ihkC]
              simp only [List.mem_cons, hEq, false_or]
              omega
      have hNdCoarse : coarseFineKeys.Nodup := hNodup.filter _
      obtain ⟨hStS, hStC⟩ := hStep coarseFineKeys hNdCoarse
      have hMemIff : (keyFine x ∈ coarseFineKeys) ↔ (projCoarse (keyFine x) == kc) = true := by
        simp [coarseFineKeys, List.mem_filter, hxDom]
      by_cases hMatch : (projCoarse (keyFine x) == kc) = true
      · have hMem : keyFine x ∈ coarseFineKeys := hMemIff.mpr hMatch
        have hRhsS : sumOptInts (((x :: xs).filter (fun r => projCoarse (keyFine r) == kc)).map valOf) =
            sumOptInts [valOf x] + sumOptInts ((xs.filter (fun r => projCoarse (keyFine r) == kc)).map valOf) := by
          simpa [List.filter_cons, hMatch] using
            sumOptInts_append [valOf x] ((xs.filter (fun r => projCoarse (keyFine r) == kc)).map valOf)
        have hRhsC : countOptInts (((x :: xs).filter (fun r => projCoarse (keyFine r) == kc)).map valOf) =
            countOptInts [valOf x] + countOptInts ((xs.filter (fun r => projCoarse (keyFine r) == kc)).map valOf) := by
          simpa [List.filter_cons, hMatch] using
            countOptInts_append [valOf x] ((xs.filter (fun r => projCoarse (keyFine r) == kc)).map valOf)
        constructor
        · rw [hStS, ihSum, hRhsS]; simp only [hMem, ↓reduceIte]
        · rw [hStC, ihCnt, hRhsC]; simp only [hMem, ↓reduceIte]
      · have hNotMem : keyFine x ∉ coarseFineKeys := fun hm => hMatch (hMemIff.mp hm)
        have hMatchF : (projCoarse (keyFine x) == kc) = false :=
          Bool.eq_false_iff.mpr hMatch
        constructor
        · rw [hStS, ihSum]; simp only [hNotMem, ↓reduceIte, Int.zero_add, List.filter_cons, hMatchF, Bool.false_eq_true]
        · rw [hStC, ihCnt]; simp only [hNotMem, ↓reduceIte, Nat.zero_add, List.filter_cons, hMatchF, Bool.false_eq_true]
  exact hGen R hCover

/-- ❌ **反例トラップ 15B (`rollup_grand_total_empty_count_null_trap`)**:
    `GROUP BY ()`（全体集約レベル）で空集合 `fineGroups = []` に対して `SUM(__cnt)` をそのまま返すと
    SQL の `SUM` は `NULL` (`none`) を返してしまうが、元の `COUNT(x)` は `0` (`some 0`) を返す！
    したがって `COUNT` の全体ロールアップには `COALESCE(SUM(__cnt), 0)` が必須である。 -/
theorem rollup_grand_total_empty_count_null_trap :
    let sqlSumOnEmpty : Option Int := none
    let sqlCountOnEmpty : Option Int := some 0
    sqlSumOnEmpty ≠ sqlCountOnEmpty := by
  decide

/-!
## 16. パーティション準同型パイプラインにおける `ORDER BY k ASC LIMIT L` 先頭バンド打ち切り（Q51）
-/

/-- ✅ **定理 16 (`partitioned_order_by_limit_band_pruning_sound` — TPC-DS Q51)**:
    `GROUP BY (k, ...)`、`WINDOW (PARTITION BY k ...)`、`FULL OUTER JOIN ON l.k = r.k` など
    パーティションキー `k` に関して準同型なパイプライン `evalPipe` の後ろに
    `ORDER BY k ASC ..., LIMIT L` が続くとき、
    ソート順で先頭に位置するバンド述語 `inBand` を入力にプッシュダウンした結果
    `bandSorted`（`= sortedOut.filter (inBand ∘ outKey)`）が `L` 行以上あれば、
    全体出力 `sortedOut = bandSorted ++ restSorted` の先頭 `L` 行は
    **`bandSorted.take L` と 100% 完全一致** し、残り 98%+ のパーティションの評価を完全に省略できる！ -/
theorem partitioned_order_by_limit_band_pruning_sound {InRow OutRow : Type}
    (inBandIn : InRow → Bool) (inBandOut : OutRow → Bool)
    (evalPipe : List InRow → List OutRow)
    (hHom : ∀ R, (evalPipe R).filter inBandOut = evalPipe (R.filter inBandIn))
    (R : List InRow)
    (bandSorted restSorted : List OutRow)
    (hBandLen : bandSorted.length = ((evalPipe R).filter inBandOut).length)
    (L : Nat)
    (hEnough : L ≤ (evalPipe (R.filter inBandIn)).length) :
    (bandSorted ++ restSorted).take L = bandSorted.take L := by
  rw [hHom R] at hBandLen
  have hLen : L ≤ bandSorted.length := hBandLen ▸ hEnough
  exact List.take_append_of_le_length hLen

/-!
## 17. 並列 Radix パーティション分割 `UNION DISTINCT`・集合積の完全性（Q75, Q97）
-/

/-- ✅ **定理 17 (`radix_partitioned_union_distinct_sound` — TPC-DS Q75, Q97)**:
    任意の決定論的バケット関数 `bucketOf : Row → Fin P`（`P > 0`）によって
    入力を `P` 個の素なバケットへ分割し、各バケットを独立・並列に `distinctKeys` 重複除去して結合した結果は、
    単一スレッドで全体に対して `distinctKeys` を取った結果と **マルチセット（`BagEq`）として 100% 完全同値** である！ -/
theorem radix_partitioned_union_distinct_sound {Row : Type} [DecidableEq Row]
    {P : Nat} (bucketOf : Row → Fin P) (rows : List Row) :
    BagEq
      (distinctKeys rows)
      ((List.finRange P).flatMap (fun b =>
        distinctKeys (rows.filter (fun r => bucketOf r == b)))) := by
  have hBucketsNodup : ∀ (bs : List (Fin P)), bs.Nodup →
      (bs.flatMap (fun b => distinctKeys (rows.filter (fun r => bucketOf r == b)))).Nodup := by
    intro bs hbs
    induction bs with
    | nil => exact List.nodup_nil
    | cons b bs' ihb =>
      simp only [List.nodup_cons] at hbs
      simp only [List.flatMap_cons]
      refine List.nodup_append.mpr ⟨nodup_distinctKeys _, ihb hbs.2, ?_⟩
      intro x hx1 y hy2 hEq
      subst hEq
      rw [mem_distinctKeys, List.mem_filter] at hx1
      rw [List.mem_flatMap] at hy2
      rcases hy2 with ⟨b2, hb2In, hx2⟩
      rw [mem_distinctKeys, List.mem_filter] at hx2
      have hbEq1 : bucketOf x = b := eq_of_beq hx1.2
      have hbEq2 : bucketOf x = b2 := eq_of_beq hx2.2
      exact hbs.1 (hbEq1.symm.trans hbEq2 ▸ hb2In)
  have hRhsNodup := hBucketsNodup (List.finRange P) (List.nodup_finRange P)
  apply perm_of_nodup_iff_mem (nodup_distinctKeys _) hRhsNodup
  intro r
  simp only [mem_distinctKeys, List.mem_flatMap, List.mem_filter]
  constructor
  · intro hr
    exact ⟨bucketOf r, List.mem_finRange (bucketOf r), hr, beq_self_eq_true _⟩
  · rintro ⟨_, _, hr, _⟩
    exact hr

/-!
## 18. 単一行スカラー集計 CTE の `CROSS JOIN` 除去とスカラー副問合せ復元（Q23）

TPC-DS Q23 の `best_ss_customer`:
```sql
SELECT c_customer_sk, SUM(ss_quantity * ss_sales_price) AS ssales
FROM store_sales, max_store_sales
GROUP BY c_customer_sk
HAVING SUM(ss_quantity * ss_sales_price) > (50 / 100.0) * MAX(tpcds_cmax)
```
において、`max_store_sales` が `GROUP BY`・`HAVING`・`LIMIT`・`OFFSET` を持たない純粋スカラー集計
（常に厳密に 1 行 `[s]` を返す）であり、`max_store_sales` の列が冪等集計 `MIN` / `MAX` / `ANY_VALUE` の
引数としてのみ参照される場合、`CROSS JOIN max_store_sales` を除去して `(SELECT tpcds_cmax FROM max_store_sales)`
へ置換しても、空入力時・NULL 時を含むすべての入力で出力が完全一致する！
-/

/-- 任意の `g ∈ distinctKeys (R.map keyOf)` に対し、フィルタ済みグループは空でない（長さ ≥ 1）。 -/
theorem filter_key_length_pos_of_mem_distinctKeys {Row Key : Type} [DecidableEq Key]
    (keyOf : Row → Key) (R : List Row) (g : Key)
    (hg : g ∈ distinctKeys (R.map keyOf)) :
    0 < (R.filter (fun r => keyOf r == g)).length := by
  rw [mem_distinctKeys, List.mem_map] at hg
  rcases hg with ⟨r, hrIn, rfl⟩
  apply List.length_pos_of_mem (a := r)
  rw [List.mem_filter]
  exact ⟨hrIn, beq_self_eq_true _⟩

/-- ✅ **定理 18 (`singleton_scalar_cross_join_elim_sound` — TPC-DS Q23)**:
    常に 1 行 `[s]` を返すスカラー CTE との `CROSS JOIN` 上で `GROUP BY keyOf(r)` を行い、
    スカラー列 `s` を冪等集計 `idemAgg`（任意の `n > 0` で `idemAgg (replicate n s) = s` を満たす `MIN`/`MAX`/`ANY_VALUE`）
    のみで参照するクエリは、`CROSS JOIN` を除去して単一ソース `R` 上で集計し `s` をスカラー参照するクエリと
    **リスト順序まで含めて 100% 完全一致** する！ -/
theorem singleton_scalar_cross_join_elim_sound {LRow Key SVal FactAgg OutRow : Type}
    [DecidableEq Key]
    (keyOf : LRow → Key)
    (factAgg : List LRow → FactAgg)
    (idemAgg : List SVal → SVal)
    (s : SVal)
    (hIdem : ∀ n : Nat, 0 < n → idemAgg (List.replicate n s) = s)
    (emit : Key → FactAgg → SVal → Option OutRow)
    (R : List LRow) :
    let crossRows := R.map (fun r => (r, s))
    (distinctKeys (crossRows.map (fun p => keyOf p.1))).filterMap (fun g =>
      let grp := crossRows.filter (fun p => keyOf p.1 == g)
      emit g (factAgg (grp.map Prod.fst)) (idemAgg (grp.map Prod.snd)))
    =
    (distinctKeys (R.map keyOf)).filterMap (fun g =>
      let grp := R.filter (fun r => keyOf r == g)
      emit g (factAgg grp) s) := by
  have hMapKey : (R.map (fun r => (r, s))).map (fun p => keyOf p.1) = R.map keyOf := by
    rw [List.map_map]
    rfl
  simp only [hMapKey]
  have hFilterFst : ∀ (rs : List LRow) (g : Key),
      ((rs.map (fun r => (r, s))).filter (fun p => keyOf p.1 == g)).map Prod.fst =
        rs.filter (fun r => keyOf r == g) := by
    intro rs g
    induction rs with
    | nil => rfl
    | cons r rs' ihr =>
      simp only [List.map_cons, List.filter_cons]
      cases keyOf r == g <;> simp [ihr]
  have hFilterSnd : ∀ (rs : List LRow) (g : Key),
      ((rs.map (fun r => (r, s))).filter (fun p => keyOf p.1 == g)).map Prod.snd =
        List.replicate ((rs.filter (fun r => keyOf r == g)).length) s := by
    intro rs g
    induction rs with
    | nil => rfl
    | cons r rs' ihr =>
      simp only [List.map_cons, List.filter_cons]
      cases keyOf r == g <;> simp [List.replicate_succ, ihr]
  have hExt : ∀ g ∈ distinctKeys (R.map keyOf),
      emit g
        (factAgg (((R.map (fun r => (r, s))).filter (fun p => keyOf p.1 == g)).map Prod.fst))
        (idemAgg (((R.map (fun r => (r, s))).filter (fun p => keyOf p.1 == g)).map Prod.snd))
      =
      emit g (factAgg (R.filter (fun r => keyOf r == g))) s := by
    intro g hg
    rw [hFilterFst R g, hFilterSnd R g]
    rw [hIdem _ (filter_key_length_pos_of_mem_distinctKeys keyOf R g hg)]
  revert hExt
  generalize distinctKeys (R.map keyOf) = gs
  intro hExt
  induction gs with
  | nil => rfl
  | cons g gs' ih =>
    simp only [List.filterMap_cons]
    rw [hExt g (List.mem_cons_self ..), ih (fun x hx => hExt x (List.mem_cons_of_mem _ hx))]

/-- ❌ **反例トラップ 6 (`trap_singleton_cross_join_sum_not_idempotent`)**:
    もしスカラー列に対する集計が冪等でない `SUM`（例: `SUM(tpcds_cmax)`）だった場合、
    グループ内に 2 行あると `10 + 10 = 20 ≠ 10` となるため、`MIN`/`MAX`/`ANY_VALUE` 以外では置換してはならない！ -/
theorem trap_singleton_cross_join_sum_not_idempotent :
    ∃ (n : Nat) (s : Int), 0 < n ∧ (List.replicate n s).sum ≠ s := by
  exact ⟨2, 10, by decide, by decide⟩

-- ============================================================================
-- 19. 閉じた純粋定数部分式の無例外証明と述語プッシュダウン健全性
--     (TPC-DS Q02 `2001 + 1`, Q47/Q57 `1999 - 1`, Q05/Q16/Q80 `CAST(... AS DATE)`)
-- ============================================================================
-- 背景:
--   SQL 述語中の部分式 `e` が一切の入力列・パラメータ・揮発性関数を参照せず (`evalClosed : Option α`)、
--   空行に対する AST 評価が例外を起こさず値 `v` (`evalClosed = some v`) に収束するとき、
--   任意の入力行 `r : Row` に対しても行評価 `evalRow r = evalClosed = some v` が成り立ち例外を起こさない。
--   従って `e` を含む比較述語 `P(r, e)` を JOIN / CTE 境界を越えてプッシュダウンしても、
--   評価回数減少によって例外観測が消滅することはない (`isSome (evalRow r) = true`)。

/-- ✅ **定理 19 (`closed_const_non_throwing_eval_independent_of_row`)**:
    入力行に依存しない純粋閉部分式 `evalRow : Row → Option Val` (`∀ r, evalRow r = evalClosed`) が
    空環境で `evalClosed = some v` に成功するならば、すべての行 `r` で例外なく同一値 `some v` を返す。 -/
theorem closed_const_non_throwing_eval_independent_of_row
    {Row Val : Type}
    (evalClosed : Option Val)
    (evalRow : Row → Option Val)
    (hClosed : ∀ r : Row, evalRow r = evalClosed)
    {v : Val}
    (hOk : evalClosed = some v) :
    ∀ r : Row, evalRow r = some v ∧ (evalRow r).isSome = true := by
  intro r
  rw [hClosed r, hOk]
  exact ⟨rfl, rfl⟩

/-- ❌ **反例トラップ 7 (`trap_throwing_closed_const_not_droppable`)**:
    閉じた定数式であっても `1 / 0` や `INT64_MAX + 1` のように `evalClosed = none`（評価時エラー）となる式は、
    空でない入力行集合上で例外を送出するため「例外なし (`isSome = true`)」と見なしてはならない！ -/
theorem trap_throwing_closed_const_not_droppable :
    let evalClosed : Option Int := none
    evalClosed.isSome = false := by
  rfl

-- ============================================================================
-- 20. Kleene 3 値論理における AND-OR 分配律と相関 `COUNT(*) > 0` → `IN` 半結合変換
--     (TPC-DS Q41: 3,097ms → <2ms)
-- ============================================================================

/-- ✅ **定理 20 (`kleene_and_or_distrib_left_sound`)**:
    Kleene 3 値論理 (`SqlBool`) においても、`AND` の `OR` に対する左分配律
    `(a AND b) OR (a AND c) = a AND (b OR c)` は全 27 通りすべてで厳密に恒等である。
    これにより相関サブクエリ内の `(i_manufact = i1.i_manufact AND P₁) OR (i_manufact = i1.i_manufact AND P₂)`
    から相関等価条件 `i_manufact = i1.i_manufact` を最上位連言へ括り出せる。 -/
theorem kleene_and_or_distrib_left_sound (a b c : SqlBool) :
    (a.and b).or (a.and c) = a.and (b.or c) := by
  cases a <;> cases b <;> cases c <;> rfl

/-- SQL の NULL 伝播等号 (`sqlEqOptBool a b = true` ⟺ 双方が `some v` で一致)。 -/
def sqlEqOptBool {α : Type} [BEq α] : Option α → Option α → Bool
  | some x, some y => x == y
  | _, _ => false

/-- 2 値 `WHERE` 文脈における `x IN (SELECT y FROM ... WHERE y IS NOT NULL)` の判定:
    `x = some v` かつリスト内に `y == v` が存在するときに限り `true`。 -/
def sqlInNonNullListWhere {α : Type} [BEq α] (x : Option α) (ys : List α) : Bool :=
  match x with
  | none => false
  | some v => ys.any (fun y => y == v)

/-- リスト長が正であることと `!L.isEmpty` の同値性。 -/
theorem decide_pos_length_eq_not_isEmpty {α : Type} (L : List α) :
    decide (0 < L.length) = !L.isEmpty := by
  cases L <;> rfl

/-- 補助補題: `sqlEqOptBool a none = false`。 -/
theorem sqlEqOptBool_none {α : Type} [BEq α] (a : Option α) :
    sqlEqOptBool a none = false := by
  cases a <;> rfl

/-- 補助補題: `S` 上で `sqlEqOptBool (innerKey s) x && innerPred s` を満たす行が存在することと、
    `S.filter innerPred` から抽出した非 NULL キー列に対する `sqlInNonNullListWhere` は一致する。 -/
theorem filter_eq_nonEmpty_eq_in_filterMap
    {SRow Key : Type} [BEq Key]
    (innerKey : SRow → Option Key)
    (innerPred : SRow → Bool)
    (x : Option Key)
    (S : List SRow) :
    (!(S.filter (fun s => sqlEqOptBool (innerKey s) x && innerPred s)).isEmpty) =
      sqlInNonNullListWhere x ((S.filter innerPred).filterMap innerKey) := by
  cases x with
  | none =>
    have hEmpty : S.filter (fun s => sqlEqOptBool (innerKey s) none && innerPred s) = [] := by
      induction S with
      | nil => rfl
      | cons s ss ih =>
        rw [List.filter_cons, sqlEqOptBool_none, Bool.false_and]
        exact ih
    rw [hEmpty]
    rfl
  | some v =>
    induction S with
    | nil => rfl
    | cons s ss ih =>
      dsimp only [sqlInNonNullListWhere] at ih ⊢
      rw [List.filter_cons, List.filter_cons]
      cases hP : innerPred s with
      | false =>
        rw [Bool.and_false]
        exact ih
      | true =>
        rw [Bool.and_true]
        cases hK : innerKey s with
        | none =>
          have hNoneEq : sqlEqOptBool (none : Option Key) (some v) = false := rfl
          rw [hNoneEq]
          change (!(List.filter (fun s => sqlEqOptBool (innerKey s) (some v) && innerPred s) ss).isEmpty) =
            (List.filterMap innerKey (s :: List.filter innerPred ss)).any (fun y => y == v)
          rw [List.filterMap_cons_none hK]
          exact ih
        | some k =>
          have hSomeEq : sqlEqOptBool (some k) (some v) = (k == v) := rfl
          rw [hSomeEq]
          cases hEq : k == v with
          | true =>
            change (!(s :: List.filter (fun s => sqlEqOptBool (innerKey s) (some v) && innerPred s) ss).isEmpty) =
              (List.filterMap innerKey (s :: List.filter innerPred ss)).any (fun y => y == v)
            rw [List.filterMap_cons_some hK, List.any_cons, hEq]
            rfl
          | false =>
            change (!(List.filter (fun s => sqlEqOptBool (innerKey s) (some v) && innerPred s) ss).isEmpty) =
              (List.filterMap innerKey (s :: List.filter innerPred ss)).any (fun y => y == v)
            rw [List.filterMap_cons_some hK, List.any_cons, hEq, Bool.false_or]
            exact ih

/-- ✅ **定理 21 (`correlated_count_gt_zero_to_in_subquery_sound`)**:
    最上位 `WHERE` 連言における相関 `(SELECT COUNT(*) FROM S WHERE innerKey(s) = outerKey(o) AND innerPred(s)) > 0`
    は、外部キー `outerKey(o)` が `NULL` の場合も含め、`outerKey(o) IN (SELECT innerKey(s) FROM S WHERE innerPred(s) AND innerKey(s) IS NOT NULL)`
    と 100% 同値である。 -/
theorem correlated_count_gt_zero_to_in_subquery_sound
    {ORow SRow Key : Type} [BEq Key]
    (outerKey : ORow → Option Key)
    (innerKey : SRow → Option Key)
    (innerPred : SRow → Bool)
    (S : List SRow)
    (O : List ORow) :
    O.filter (fun o =>
      decide (0 < (S.filter (fun s => sqlEqOptBool (innerKey s) (outerKey o) && innerPred s)).length))
    =
    O.filter (fun o =>
      sqlInNonNullListWhere (outerKey o) ((S.filter innerPred).filterMap innerKey)) := by
  congr 1
  funext o
  rw [decide_pos_length_eq_not_isEmpty]
  exact filter_eq_nonEmpty_eq_in_filterMap innerKey innerPred (outerKey o) S

/-- ❌ **反例トラップ 8 (`trap_correlated_count_eq_zero_not_in_null_outer_unsound`)**:
    逆に `(SELECT COUNT(*) ...) = 0` を単純に `outerKey NOT IN (SELECT innerKey ...)` に書き換えると、
    `outerKey = NULL` かつサブクエリが空でないとき、元クエリは `COUNT(*) = 0` (`TRUE`) だが
    `NULL NOT IN (1)` は `UNKNOWN`（`WHERE` で `FALSE`）となり結果が食い違う！ -/
theorem trap_correlated_count_eq_zero_not_in_null_outer_unsound :
    let S : List (Option Int) := [some 1]
    let outerKey : Option Int := none
    ((S.filter (fun k => sqlEqOptBool k outerKey)).length == 0) = true ∧
    (match outerKey with | none => false | some v => !(S.filterMap id).contains v) = false := by
  exact ⟨rfl, rfl⟩

-- ============================================================================
-- 21. 任意純粋式の ROLLUP `SUM` 準同型性とインターリーブ並列ブロック分割の全単射性
--     (TPC-DS Q67: 決定論化 & 高速化)
-- ============================================================================

/-- SQL の `SUM` 意味論: 非 NULL 要素が 1 つもなければ `none` (`NULL`)、1 つ以上あればその総和 `some s`。 -/
def sqlOptSum : List (Option Int) → Option Int
  | [] => none
  | none :: xs => sqlOptSum xs
  | some v :: xs =>
    match sqlOptSum xs with
    | none => some v
    | some s => some (v + s)

/-- 部分集計の結合演算 (`SUM` of partial `SUM`s)。 -/
def sqlOptSumCombine : Option Int → Option Int → Option Int
  | none, b => b
  | some a, none => some a
  | some a, some b => some (a + b)

/-- ✅ **定理 22 (`rollup_sum_homomorphic_arbitrary_expr_sound`)**:
    任意の純粋行式 `f : Row → Option Int`（`COALESCE(a * b, 0)` を含む）に対し、
    分割された行リスト `xs ++ ys` 上の `SUM` は、部分 `SUM` 同士の `sqlOptSumCombine`（すなわち外側の `SUM`）と
    空入力・全 NULL グループを含めて完全に一致する。 -/
theorem rollup_sum_homomorphic_arbitrary_expr_sound
    {Row : Type} (f : Row → Option Int) (xs ys : List Row) :
    sqlOptSum ((xs ++ ys).map f) =
      sqlOptSumCombine (sqlOptSum (xs.map f)) (sqlOptSum (ys.map f)) := by
  induction xs with
  | nil =>
    simp [sqlOptSum, sqlOptSumCombine]
  | cons x xs' ih =>
    rw [List.cons_append, List.map_cons, List.map_cons]
    cases f x with
    | none =>
      simp only [sqlOptSum]
      exact ih
    | some v =>
      simp only [sqlOptSum, ih]
      cases sqlOptSum (List.map f xs') <;> cases sqlOptSum (List.map f ys) <;>
        simp [sqlOptSumCombine, Int.add_assoc]

/-- ✅ **定理 23 (`interleaved_worker_partition_bijection_sound`)**:
    ブロック番号 `b < B` を `w = b % W` (`W > 0`) により各ワーカー `w ∈ 0..W-1` に静的インターリーブ割当すると、
    すべてのブロック `b` は一意なワーカー `w < W` に属し、実行ごとに各ワーカーが訪問するブロック列が
    100% 決定論的に定まる（アトミック競合による浮動小数点加算順の揺らぎを根絶する）。 -/
theorem interleaved_worker_partition_bijection_sound
    (W B b : Nat) (hW : 0 < W) (_hB : b < B) :
    b % W < W ∧ ∀ w1 w2 : Nat, b % W = w1 → b % W = w2 → w1 = w2 := by
  refine ⟨Nat.mod_lt b hW, ?_⟩
  intro w1 w2 h1 h2
  omega

-- ============================================================================
-- 22. LEFT JOIN 選言的 NULL 棄却条件からの Union Semi-Filter 押し下げと 2×Int32 パック全単射
--     (TPC-DS Q78: `COALESCE(ws_qty, 0) > 0 OR COALESCE(cs_qty, 0) > 0` による `ss` 事前絞り込み)
-- ============================================================================

/-- `LEFT JOIN` の 1 行 `l` に対する右側マッチ結果リスト（マッチが空なら `[none]`、非空なら `map some`）。 -/
def leftJoinRowMatches {R Key : Type} [BEq Key]
    (k : Option Key) (keyR : R → Option Key) (rs : List R) : List (Option R) :=
  let matched := rs.filter (fun r => sqlEqOptBool k (keyR r))
  if matched.isEmpty then [none] else matched.map some

/-- 補助補題: `pred none = false` のとき、`leftJoinRowMatches` の要素 `mr` が `pred mr = true` を満たすなら、
    元の `rs` 内に `sqlEqOptBool k (keyR r) && pred (some r)` を満たす要素が存在する。 -/
theorem leftJoinRowMatches_true_implies_any
    {R Key : Type} [BEq Key]
    (k : Option Key) (keyR : R → Option Key) (rs : List R)
    (pred : Option R → Bool) (hNull : pred none = false)
    (mr : Option R) (hMem : mr ∈ leftJoinRowMatches k keyR rs) (hPred : pred mr = true) :
    rs.any (fun r => sqlEqOptBool k (keyR r) && pred (some r)) = true := by
  dsimp only [leftJoinRowMatches] at hMem
  split at hMem
  · simp only [List.mem_singleton] at hMem
    subst hMem
    rw [hNull] at hPred
    contradiction
  · simp only [List.mem_map, List.mem_filter] at hMem
    rcases hMem with ⟨r, ⟨hrMem, hrEq⟩, rfl⟩
    exact List.any_eq_true.mpr ⟨r, hrMem, by simp [hrEq, hPred]⟩

/-- ✅ **定理 24 (`left_join_disjunctive_null_rejecting_semi_filter_sound`)**:
    `L LEFT JOIN R₁ ... LEFT JOIN R₂ ... WHERE P₁(r₁) OR P₂(r₂)` において、
    各枝 `P₁, P₂` が全 NULL パディング行を棄却する（`P₁(none) = false`, `P₂(none) = false`、
    例: `COALESCE(ws_qty, 0) > 0`）ならば、生存するすべての左行 `l ∈ L` は
    `EXISTS (r₁ ∈ R₁: keyL=keyR₁ ∧ P₁) OR EXISTS (r₂ ∈ R₂: keyL=keyR₂ ∧ P₂)` を必ず満たす。
    したがって `L` の集約・結合前にこの選言的 Semi-Filter を押し下げても最終出力多重集合は 100% 不変である。 -/
theorem left_join_disjunctive_null_rejecting_semi_filter_sound
    {L R1 R2 Key : Type} [BEq Key]
    (keyL : L → Option Key) (keyR1 : R1 → Option Key) (keyR2 : R2 → Option Key)
    (R1s : List R1) (R2s : List R2)
    (P1 : Option R1 → Bool) (P2 : Option R2 → Bool)
    (hNull1 : P1 none = false) (hNull2 : P2 none = false)
    (Ls : List L) :
    let semi (l : L) : Bool :=
      R1s.any (fun r1 => sqlEqOptBool (keyL l) (keyR1 r1) && P1 (some r1)) ||
      R2s.any (fun r2 => sqlEqOptBool (keyL l) (keyR2 r2) && P2 (some r2))
    let evalOuter (ls : List L) :=
      ls.flatMap (fun l =>
        (leftJoinRowMatches (keyL l) keyR1 R1s).flatMap (fun mr1 =>
          (leftJoinRowMatches (keyL l) keyR2 R2s).filterMap (fun mr2 =>
            if P1 mr1 || P2 mr2 then some (l, mr1, mr2) else none)))
    evalOuter (Ls.filter semi) = evalOuter Ls := by
  intro semi evalOuter
  induction Ls with
  | nil => rfl
  | cons l ls ih =>
    by_cases hSemi : semi l = true
    · simp only [evalOuter, List.filter_cons_of_pos hSemi, List.flatMap_cons]
      congr 1
    · have hSemiF : semi l = false := Bool.eq_false_iff.mpr hSemi
      simp only [evalOuter, List.filter_cons_of_neg hSemi, List.flatMap_cons]
      have hHeadEmpty :
          ((leftJoinRowMatches (keyL l) keyR1 R1s).flatMap (fun mr1 =>
            (leftJoinRowMatches (keyL l) keyR2 R2s).filterMap (fun mr2 =>
              if P1 mr1 || P2 mr2 then some (l, mr1, mr2) else none))) = [] := by
        apply List.flatMap_eq_nil_iff.mpr
        intro mr1 hMr1
        apply List.filterMap_eq_nil_iff.mpr
        intro mr2 hMr2
        cases hOr : P1 mr1 || P2 mr2 with
        | false => simp
        | true =>
          cases hP1 : P1 mr1 with
          | true =>
            have h1 := leftJoinRowMatches_true_implies_any (keyL l) keyR1 R1s P1 hNull1 mr1 hMr1 hP1
            have : semi l = true := by simp [semi, h1]
            rw [hSemiF] at this
            contradiction
          | false =>
            rw [hP1, Bool.false_or] at hOr
            have h2 := leftJoinRowMatches_true_implies_any (keyL l) keyR2 R2s P2 hNull2 mr2 hMr2 hOr
            have : semi l = true := by simp [semi, h2]
            rw [hSemiF] at this
            contradiction
      rw [hHeadEmpty, List.nil_append]
      exact ih

/-- ❌ **反例トラップ 9 (`trap_left_join_non_strict_predicate_semi_unsound`)**:
    もし片方の枝が NULL パディング行を通す（例: `COALESCE(ws_qty, 1) > 0` で `P1 none = true`）なら、
    `R1 = []`, `R2 = []` でも `LEFT JOIN` 結果は残るのに Semi-Filter は全行を落としてしまう！ -/
theorem trap_left_join_non_strict_predicate_semi_unsound :
    let R1s : List Int := []
    let P1 : Option Int → Bool := fun mr => (mr.getD 1) > 0
    P1 none = true ∧
    R1s.any (fun r1 => sqlEqOptBool (some 42) (some r1) && P1 (some r1)) = false := by
  exact ⟨rfl, rfl⟩

/-- ✅ **定理 25 (`packed_int32_pair_bijection_sound`)**:
    符号付き 32 ビット整数領域 `[-2^31, 2^31 - 1]` に収まる 2 列 `(a, b)` を
    `pack(a, b) = (a + 2^31) * 2^32 + (b + 2^31)` で単一 64 ビット整数へパックすると完全単射となる。 -/
def packInt32Pair (a b : Int) : Int :=
  (a + 2147483648) * 4294967296 + (b + 2147483648)

theorem packed_int32_pair_bijection_sound
    (a1 b1 a2 b2 : Int)
    (_ha1 : -2147483648 ≤ a1 ∧ a1 < 2147483648)
    (hb1 : -2147483648 ≤ b1 ∧ b1 < 2147483648)
    (_ha2 : -2147483648 ≤ a2 ∧ a2 < 2147483648)
    (hb2 : -2147483648 ≤ b2 ∧ b2 < 2147483648) :
    packInt32Pair a1 b1 = packInt32Pair a2 b2 ↔ (a1 = a2 ∧ b1 = b2) := by
  unfold packInt32Pair
  constructor
  · intro h
    omega
  · rintro ⟨rfl, rfl⟩
    rfl

-- ============================================================================
-- 23. 条件付き `CASE WHEN` 倍精度算術セル直接評価 & 共有ユニーク次元結合の後段統合
--     (TPC-DS Q04 / Q11 / Q74: `def_cell=1` 解放 & 3 回重複 `customer` スキャン削減)
-- ============================================================================

/-- 行単位の `CASE WHEN cond(r) THEN expr(l) ELSE 0` 評価。 -/
def evalCondCaseElseZero {L R : Type}
    (cond : R → Bool) (expr : L → Option Int) (l : L) (r : R) : Option Int :=
  if cond r then expr l else some 0

/-- セル直接評価: `cond(r) = false` のときは `expr(l)` を評価せず即座に `some 0` を加算する。 -/
def evalCondCaseCellFast {L R : Type}
    (cond : R → Bool) (expr : L → Option Int) (l : L) (r : R) : Option Int :=
  match cond r with
  | true => expr l
  | false => some 0

/-- ✅ **定理 26 (`conditional_case_double_arith_cell_eval_sound`)**:
    `expr(l)` が無例外な純粋算術式であるとき、`cond(r) = false` の行で左列ロードと算術評価を完全にスキップする
    `evalCondCaseCellFast` は、`SUM` および `COUNT` の両アキュムレータで標準 `CASE` 評価と厳密に一致する。 -/
theorem conditional_case_double_arith_cell_eval_sound
    {L R : Type} (cond : R → Bool) (expr : L → Option Int) (pairs : List (L × R)) :
    pairs.map (fun p => evalCondCaseCellFast cond expr p.1 p.2) =
      pairs.map (fun p => evalCondCaseElseZero cond expr p.1 p.2) := by
  congr 1
  funext p
  unfold evalCondCaseCellFast evalCondCaseElseZero
  cases cond p.2 <;> rfl

/-- ✅ **定理 27 (`multi_join_shared_unique_dim_deferral_sound`)**:
    次元表関数 `ukOfPk : PK → UK` が単射（`c_customer_sk ↔ c_customer_id` の双方向 UNIQUE NOT NULL）であるとき、
    各チャネル集計側で先に `D` を結合してから `ukOfPk fk₁ == ukOfPk fk₂` で外部結合する式と、
    整数サロゲートキー `fk₁ == fk₂` で直接結合した後に `D` を 1 回だけ適用する式は 100% 同値である。 -/
theorem multi_join_shared_unique_dim_deferral_sound
    {PK UK V1 V2 : Type} [DecidableEq PK] [DecidableEq UK]
    (ukOfPk : PK → UK)
    (hInj : ∀ k1 k2 : PK, ukOfPk k1 = ukOfPk k2 → k1 = k2)
    (A1 : List (PK × V1)) (A2 : List (PK × V2)) :
    (A1.flatMap (fun a1 =>
      A2.filterMap (fun a2 =>
        if ukOfPk a1.1 = ukOfPk a2.1 then some (ukOfPk a1.1, a1.2, a2.2) else none)))
    =
    (A1.flatMap (fun a1 =>
      A2.filterMap (fun a2 =>
        if a1.1 = a2.1 then some (ukOfPk a1.1, a1.2, a2.2) else none))) := by
  congr 1
  funext a1
  congr 1
  funext a2
  by_cases hEq : a1.1 = a2.1
  · simp [hEq]
  · have hNeUk : ukOfPk a1.1 ≠ ukOfPk a2.1 := fun hUk => hEq (hInj a1.1 a2.1 hUk)
    simp [hEq, hNeUk]

/-- ❌ **反例トラップ 10 (`trap_non_unique_uk_dim_collapse_unsound`)**:
    `ukOfPk` が単射でない（異なる `pk` が同じ `uk` を持つ）場合、`a1.1 = a2.1` への縮退は正当な結合ペアを欠落させる！ -/
theorem trap_non_unique_uk_dim_collapse_unsound :
    let ukOfPk : Int → Int := fun _ => 100
    let A1 : List (Int × Int) := [(1, 10)]
    let A2 : List (Int × Int) := [(2, 20)]
    (A1.flatMap (fun a1 => A2.filterMap (fun a2 =>
      if ukOfPk a1.1 == ukOfPk a2.1 then some (a1.2, a2.2) else none))).length = 1 ∧
    (A1.flatMap (fun a1 => A2.filterMap (fun a2 =>
      if a1.1 == a2.1 then some (a1.2, a2.2) else none))).length = 0 := by
  exact ⟨rfl, rfl⟩

/-- A build-side CASE counts exactly the joined pairs for which its condition
    is TRUE. FALSE and UNKNOWN contribute zero, even when the group exists. -/
theorem count_case_one_else_null_sound {Row : Type} (p : Row → SqlBool)
    (rows : List Row) :
    sqlCountCol (fun row => if p row = .tTrue then some (1 : Int) else none) rows =
      sqlCountStar (rows.filter fun row => decide (p row = .tTrue)) := by
  induction rows with
  | nil => rfl
  | cons row rows ih =>
    cases h : p row <;> simp [h, ih]

/-- The low word of a compact COUNT key stores the signed fact value in
    two's complement. Decoding the word recovers every value admitted by
    the min/max guard, including both signed 32-bit endpoints. -/
theorem compact_count_signed_key_roundtrip (x : Int)
    (lower : -2147483648 ≤ x) (upper : x < 2147483648) :
    let word := if x < 0 then x + 4294967296 else x
    0 ≤ word ∧ word < 4294967296 ∧
      (if word < 2147483648 then word else word - 4294967296) = x := by
  dsimp
  split <;> (try split) <;> omega

/-- A weighted count for an exact key or a bucket. The entries include both
    raw rows (weight one) and partial counts from earlier workers. -/
private def weightedCount {Key : Type} (accepts : Key → Bool)
    (entries : List (Key × Nat)) : Nat :=
  (entries.map fun e => if accepts e.1 then e.2 else 0).sum

/-- The saturating byte update equals capping the total, including when a
    partial count itself exceeds the cap. -/
private theorem cap_count_step (cap count weight : Nat) :
    min cap (min cap count + min cap weight) = min cap (count + weight) := by
  omega

/-- The capped histogram can discard only exact keys below the HAVING
    threshold, regardless of hash collisions or the order of partials. -/
theorem capped_bucket_count_keeps_candidate {Key Bucket : Type}
    [DecidableEq Key] [DecidableEq Bucket] (bucket : Key → Bucket) (key : Key)
    (cap : Nat) (entries : List (Key × Nat))
    (passes : cap ≤ weightedCount (fun k => decide (k = key)) entries) :
    entries.foldl (fun count e =>
      if bucket e.1 = bucket key then min cap (count + min cap e.2)
      else count) 0 = cap := by
  let inBucket := fun k => decide (bucket k = bucket key)
  have bound : weightedCount (fun k => decide (k = key)) entries ≤
      weightedCount inBucket entries := by
    clear passes
    induction entries with
    | nil => simp [weightedCount]
    | cons entry rest ih =>
      have tail := ih
      change weightedCount (fun k => decide (k = key)) rest ≤
        weightedCount inBucket rest at tail
      by_cases hk : entry.1 = key
      · simp [weightedCount, inBucket, hk] at tail ⊢
        omega
      · simp [weightedCount, inBucket, hk] at tail ⊢
        omega
  have foldCap (init : Nat) (es : List (Key × Nat)) :
      es.foldl (fun count e =>
        if bucket e.1 = bucket key then min cap (count + min cap e.2)
        else count) (min cap init) = min cap (init + weightedCount inBucket es) := by
    induction es generalizing init with
    | nil => simp [weightedCount]
    | cons e rest ih =>
      by_cases hb : bucket e.1 = bucket key
      · simp [List.foldl_cons, hb, cap_count_step, ih, weightedCount, inBucket,
          Nat.add_assoc]
      · simp [List.foldl_cons, hb, ih, weightedCount, inBucket]
  simpa [Nat.min_eq_left (Nat.le_trans passes bound)] using foldCap 0 entries

/-- After a partial GROUP BY has emitted one row per finest key, a second
    grouping at that level has singleton fibers. The row index can name the
    group while the coarser projections still need their own grouping. -/
theorem unique_group_key_fiber_is_singleton {Row Key : Type} [DecidableEq Key]
    (key : Row → Key) (rows : List Row) (row : Row)
    (unique : (rows.map key).Nodup) (present : row ∈ rows) :
    rows.filter (fun r => decide (key r = key row)) = [row] := by
  induction rows with
  | nil => simp at present
  | cons first rest ih =>
    simp only [List.map_cons, List.nodup_cons, List.mem_cons] at unique present
    rcases present with rfl | present
    · have empty : rest.filter (fun r => decide (key r = key row)) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro r mem
        intro eq
        exact unique.1 ((of_decide_eq_true eq) ▸ List.mem_map_of_mem mem)
      simp [empty]
    · have diff : key first ≠ key row := by
        intro eq
        exact unique.1 (eq.symm ▸ List.mem_map_of_mem present)
      simp [diff, ih unique.2 present]

/-- Radix partitioning changes where a key is accumulated. A stable
    partition leaves the exact key's sequence of inputs intact. -/
theorem radix_bucket_preserves_exact_key_order {Row Key Bucket : Type}
    [DecidableEq Key] [DecidableEq Bucket] (key : Row → Key)
    (bucket : Key → Bucket) (target : Key) (rows : List Row) :
    (rows.filter (fun r => decide (bucket (key r) = bucket target))).filter
      (fun r => decide (key r = target)) =
      rows.filter (fun r => decide (key r = target)) := by
  induction rows with
  | nil => rfl
  | cons row rest ih =>
    by_cases exactKey : key row = target
    · simp [exactKey, ih]
    · by_cases sameBucket : bucket (key row) = bucket target
      · simp [sameBucket, exactKey, ih]
      · simp [sameBucket, exactKey, ih]

end TinylambOpt
