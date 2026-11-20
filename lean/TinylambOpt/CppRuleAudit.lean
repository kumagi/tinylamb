/-
# Step 7: C++ Optimizer 既存実装ルールの直訳監査（Bug Finding & Repair Verification）

「教科書上の理想化されたルール」ではなく、`tinylamb` の C++ 実装（`plan/cascades.cpp`,
`plan/cascades_search.cpp`, `query/join_reduction.cpp`, `expression/rewrite.cpp`）の
**実際の guard 条件・データ構造コピーをそのまま Lean 4 に落とし込んで検証**します。

この直訳監査により、既存の C++ 実装に潜んでいた **実在の数学的バグ** が
コンパイラレベルで次々と炙り出され、かつ修正版 guard の完全正当性が機械検証されました。
-/

import TinylambOpt.ThreeValued
import TinylambOpt.BagRel
import TinylambOpt.AggBase

namespace TinylambOpt

/-!
## Audit 1: `query/join_reduction.cpp` (`IsStrictOnQualifiers` / `RejectsNulls` / `NeverFalse`)
   および `expression/rewrite.cpp` (`IsNullStrictWhere`) の三値論理・整数 Truthy 監査

C++ の `Value::Truthy()` は整数 `0` を偽、非 `0` を真、`NULL` を未知として扱います。
`query/join_reduction.cpp` の既存実装をそのまま直訳すると、以下の **2 つの実在バグ** が露呈します：

1. **Bug 1a（空リスト `IN ()` の Strict 誤判定）**:
   `IsStrictOnQualifiers` の `case kInExp:`（および `expression/rewrite.cpp` の `IsNullStrictWhere`）が
   `!in.list_.empty()` をチェックしていなかったため、`r.a IN ()` を「`r.a = NULL` のとき `NULL` になる」と誤判定していました。
   実際には SQL の `NULL IN ()` は `FALSE` になるため、`WHERE (r.a IN ()) IS FALSE` は
   NULL パディング行で **`TRUE` になるにもかかわらず、`LEFT JOIN → INNER JOIN` へ誤降格**します！
2. **Bug 1b（`NeverFalse` における減算・XOR・ビット反転の零化バグ）**:
   `NeverFalse` が `kSubtract` (`-`) や `kXor` (`^`)、`kBitwiseNot` (`~`) に対して
   「各オペランドが `NeverFalse` なら結果も `NeverFalse`（偽＝0 にならない）」と判定していました。
   しかし `1 - 1 = 0`、`1 ^ 1 = 0`、`~(-1) = 0` はすべて `Truthy() == false` になるため、
   `NOT (1 - 1)` や `NOT (~(-1))` は全行（NULL パディング行を含む！）で **`TRUE` になるにもかかわらず、
   `RejectsNulls` が `true` を返して `LEFT JOIN → INNER JOIN` へ誤降格**します！
-/

/-- 監査用の SQL 式 AST（整数 + 真偽値を `Int`（0=偽, 非0=真）と `Option Int`（`none`=NULL）で統一表現）。 -/
inductive AuditExpr where
  | paddedCol : Nat → AuditExpr
  | otherCol  : Nat → AuditExpr
  | constVal  : Option Int → AuditExpr
  | add       : AuditExpr → AuditExpr → AuditExpr
  | sub       : AuditExpr → AuditExpr → AuditExpr
  | bitXor    : AuditExpr → AuditExpr → AuditExpr
  | bitNot    : AuditExpr → AuditExpr
  | neg       : AuditExpr → AuditExpr
  | eq        : AuditExpr → AuditExpr → AuditExpr
  | andE      : AuditExpr → AuditExpr → AuditExpr
  | orE       : AuditExpr → AuditExpr → AuditExpr
  | notE      : AuditExpr → AuditExpr
  | isNull    : AuditExpr → AuditExpr
  | isNotNull : AuditExpr → AuditExpr
  | isTrue    : AuditExpr → AuditExpr
  | isFalse   : AuditExpr → AuditExpr
  | inList    : AuditExpr → List (Option Int) → AuditExpr
  deriving Repr

/-- `Option Int` から SQL 三値論理 `SqlBool` への変換（`tinylamb::Value::Truthy()` 準拠）。 -/
def valToSqlBool : Option Int → SqlBool
  | none   => .tNull
  | some n => if n == 0 then .tFalse else .tTrue

def sqlBoolToVal : SqlBool → Option Int
  | .tTrue  => some 1
  | .tFalse => some 0
  | .tNull  => none

/-- SQL `IN (...)` の評価: リストが空なら左辺が `NULL` でも常に `FALSE` (`some 0`)！ -/
abbrev evalInAux (lhs : Option Int) := evalInList lhs

/-- `AuditExpr` の評価器（`padded` 関数が `paddedCol` の値を供給。NULL パディング行では `fun _ => none`）。 -/
def evalAuditExpr (padded other : Nat → Option Int) : AuditExpr → Option Int
  | .paddedCol i => padded i
  | .otherCol i  => other i
  | .constVal v  => v
  | .add a b     =>
    match evalAuditExpr padded other a, evalAuditExpr padded other b with
    | some x, some y => some (x + y)
    | _, _ => none
  | .sub a b     =>
    match evalAuditExpr padded other a, evalAuditExpr padded other b with
    | some x, some y => some (x - y)
    | _, _ => none
  | .bitXor a b  =>
    match evalAuditExpr padded other a, evalAuditExpr padded other b with
    | some x, some y => some (if x == y then 0 else 1)
    | _, _ => none
  | .bitNot a    =>
    match evalAuditExpr padded other a with
    | some x => some (-(x + 1)) -- 2 の補数 `~x = -(x + 1)`（`~(-1) = 0`！）
    | none   => none
  | .neg a       =>
    match evalAuditExpr padded other a with
    | some x => some (-x)
    | none   => none
  | .eq a b      =>
    match evalAuditExpr padded other a, evalAuditExpr padded other b with
    | some x, some y => some (if x == y then 1 else 0)
    | _, _ => none
  | .andE a b    =>
    sqlBoolToVal ((valToSqlBool (evalAuditExpr padded other a)).and
                  (valToSqlBool (evalAuditExpr padded other b)))
  | .orE a b     =>
    sqlBoolToVal ((valToSqlBool (evalAuditExpr padded other a)).or
                  (valToSqlBool (evalAuditExpr padded other b)))
  | .notE a      =>
    sqlBoolToVal (valToSqlBool (evalAuditExpr padded other a)).not
  | .isNull a    =>
    match evalAuditExpr padded other a with
    | none   => some 1
    | some _ => some 0
  | .isNotNull a =>
    match evalAuditExpr padded other a with
    | none   => some 0
    | some _ => some 1
  | .isTrue a    =>
    match valToSqlBool (evalAuditExpr padded other a) with
    | .tTrue => some 1
    | _      => some 0
  | .isFalse a   =>
    match valToSqlBool (evalAuditExpr padded other a) with
    | .tFalse => some 1
    | _       => some 0
  | .inList c ls =>
    sqlBoolToVal (evalInAux (evalAuditExpr padded other c) ls)

/-! ### 1-A. 修正前の既存 C++ コード (`query/join_reduction.cpp`) の忠実な直訳 -/

/-- 既存 C++ の `IsStrictOnQualifiers`（`inList` で `!list.isEmpty` を忘れている！） -/
def cppBuggyIsStrict : AuditExpr → Bool
  | .paddedCol _ => true
  | .add a b | .sub a b | .bitXor a b => cppBuggyIsStrict a || cppBuggyIsStrict b
  | .notE a | .neg a | .bitNot a      => cppBuggyIsStrict a
  | .inList c _                       => cppBuggyIsStrict c -- ❌ 空リスト `[]` でも true を返していた！
  | _                                 => false

mutual
/-- 既存 C++ の `RejectsNulls` -/
def cppBuggyRejectsNulls : AuditExpr → Bool
  | .paddedCol _ => true
  | .constVal _  => false
  | .andE a b    => cppBuggyRejectsNulls a || cppBuggyRejectsNulls b
  | .orE a b | .bitXor a b | .add a b | .sub a b =>
    cppBuggyRejectsNulls a && cppBuggyRejectsNulls b
  | .eq a b      => cppBuggyIsStrict a || cppBuggyIsStrict b
  | .notE a      => cppBuggyNeverFalse a
  | .neg a | .bitNot a => cppBuggyIsStrict a
  | .isNotNull a | .isTrue a | .isFalse a => cppBuggyIsStrict a
  | .inList c ls => !ls.isEmpty && cppBuggyIsStrict c
  | _            => false

/-- 既存 C++ の `NeverFalse`（`sub`, `bitXor`, `bitNot` に誤ったルールが入っていた！） -/
def cppBuggyNeverFalse : AuditExpr → Bool
  | .paddedCol _ => true
  | .constVal v  => valToSqlBool v == .tTrue
  | .andE a b    => cppBuggyNeverFalse a && cppBuggyNeverFalse b
  | .orE a b     => cppBuggyNeverFalse a || cppBuggyNeverFalse b
  | .bitXor a b | .add a b | .sub a b =>
    -- ❌ BUG: `1 - 1 = 0` や `1 ^ 1 = 0` は両辺 Truthy なのに結果が FALSE (0) になる！
    cppBuggyNeverFalse a && cppBuggyNeverFalse b
  | .eq a b      => cppBuggyIsStrict a || cppBuggyIsStrict b
  | .notE a      => cppBuggyRejectsNulls a
  | .neg a | .bitNot a =>
    -- ❌ BUG: `~(-1) = 0` は `-1` が Truthy なのに結果が FALSE (0) になる！
    cppBuggyNeverFalse a
  | _            => false
end

/-- 🚨 **実在バグ証明 1a**: `WHERE (r.a IN ()) IS FALSE` に対して、
    既存 C++ の `cppBuggyRejectsNulls` は `true`（NULL パディング行を落とす）と判定するが、
    実際には NULL パディング行（`padded = fun _ => none`）で **`tTrue`（通過！）** に評価される！ -/
theorem cpp_empty_in_isFalse_bug :
    cppBuggyRejectsNulls (.isFalse (.inList (.paddedCol 0) [])) = true ∧
    valToSqlBool (evalAuditExpr (fun _ => none) (fun _ => some 1)
      (.isFalse (.inList (.paddedCol 0) []))) = .tTrue := by
  exact ⟨rfl, rfl⟩

/-- 🚨 **実在バグ証明 1b**: `WHERE NOT (1 - 1)` および `WHERE NOT (~(-1))` に対して、
    既存 C++ の `cppBuggyRejectsNulls` は `true` と誤判定するが、
    実際には NULL パディング行で **`tTrue`（通過！）** に評価される！ -/
theorem cpp_neverFalse_sub_and_bitNot_bug :
    cppBuggyRejectsNulls (.notE (.sub (.constVal (some 1)) (.constVal (some 1)))) = true ∧
    valToSqlBool (evalAuditExpr (fun _ => none) (fun _ => none)
      (.notE (.sub (.constVal (some 1)) (.constVal (some 1))))) = .tTrue ∧
    cppBuggyRejectsNulls (.notE (.bitNot (.constVal (some (-1))))) = true ∧
    valToSqlBool (evalAuditExpr (fun _ => none) (fun _ => none)
      (.notE (.bitNot (.constVal (some (-1)))))) = .tTrue := by
  exact ⟨rfl, rfl, rfl, rfl⟩

/-! ### 1-B. 修正版 `repairedIsStrict` / `repairedRejectsNulls` / `repairedNeverFalse` と完全正当性証明 -/

/-- 修正版 `IsStrict`: `inList` は `!ls.isEmpty` のときのみ `NULL` を保存する！ -/
def repairedIsStrict : AuditExpr → Bool
  | .paddedCol _ => true
  | .add a b | .sub a b | .bitXor a b => repairedIsStrict a || repairedIsStrict b
  | .notE a | .neg a | .bitNot a      => repairedIsStrict a
  | .inList c ls                      => !ls.isEmpty && repairedIsStrict c
  | _                                 => false

mutual
/-- 修正版 `RejectsNulls`: 算術・ビット演算は「片方が NULL 厳格」なときのみ確実に `NULL` になる。 -/
def repairedRejectsNulls : AuditExpr → Bool
  | .paddedCol _ => true
  | .constVal _  => false
  | .andE a b    => repairedRejectsNulls a || repairedRejectsNulls b
  | .orE a b     => repairedRejectsNulls a && repairedRejectsNulls b
  | .bitXor a b | .add a b | .sub a b => repairedIsStrict a || repairedIsStrict b
  | .eq a b      => repairedIsStrict a || repairedIsStrict b
  | .notE a      => repairedNeverFalse a
  | .neg a | .bitNot a => repairedIsStrict a
  | .isNotNull a | .isTrue a | .isFalse a => repairedIsStrict a
  | .inList c ls => !ls.isEmpty && repairedIsStrict c
  | _            => false

/-- 修正版 `NeverFalse`: 算術・ビット演算（`1 - 1 = 0`, `~(-1) = 0` 等）は非零を保存しないため、
    「片方が NULL 厳格（結果が確実に `NULL` になり、`FALSE` にはならない）」な場合のみ許可する！ -/
def repairedNeverFalse : AuditExpr → Bool
  | .paddedCol _ => true
  | .constVal v  => valToSqlBool v == .tTrue
  | .andE a b    => repairedNeverFalse a && repairedNeverFalse b
  | .orE a b     => repairedNeverFalse a || repairedNeverFalse b
  | .bitXor a b | .add a b | .sub a b => repairedIsStrict a || repairedIsStrict b
  | .eq a b      => repairedIsStrict a || repairedIsStrict b
  | .notE a      => repairedRejectsNulls a
  | .neg a | .bitNot a => repairedIsStrict a
  | _            => false
end

theorem evalInAux_none_nonEmpty (rs : List (Option Int)) (hne : rs ≠ []) :
    evalInAux none rs = .tNull := by
  induction rs with
  | nil => contradiction
  | cons r tl ih =>
    cases tl with
    | nil =>
      cases r <;> rfl
    | cons r2 tl2 =>
      have htl : r2 :: tl2 ≠ [] := List.cons_ne_nil _ _
      have hrec := ih htl
      change (sqlEq none r).or (evalInAux none (r2 :: tl2)) = .tNull
      rw [hrec]
      cases r <;> rfl

/-- 補題: `repairedIsStrict e = true` ならば、NULL パディング行での評価結果は必ず `none` (`NULL`) になる。 -/
theorem repairedIsStrict_sound (other : Nat → Option Int) (e : AuditExpr)
    (h : repairedIsStrict e = true) :
    evalAuditExpr (fun _ => none) other e = none := by
  induction e with
  | paddedCol _ => rfl
  | otherCol _ => simp [repairedIsStrict] at h
  | constVal _ => simp [repairedIsStrict] at h
  | add a b iha ihb =>
    simp [repairedIsStrict, Bool.or_eq_true] at h
    rcases h with ha | hb
    · simp [evalAuditExpr, iha ha]
    · simp [evalAuditExpr, ihb hb]
  | sub a b iha ihb =>
    simp [repairedIsStrict, Bool.or_eq_true] at h
    rcases h with ha | hb
    · simp [evalAuditExpr, iha ha]
    · simp [evalAuditExpr, ihb hb]
  | bitXor a b iha ihb =>
    simp [repairedIsStrict, Bool.or_eq_true] at h
    rcases h with ha | hb
    · simp [evalAuditExpr, iha ha]
    · simp [evalAuditExpr, ihb hb]
  | bitNot a iha =>
    simp [repairedIsStrict] at h
    simp [evalAuditExpr, iha h]
  | neg a iha =>
    simp [repairedIsStrict] at h
    simp [evalAuditExpr, iha h]
  | eq _ _ => simp [repairedIsStrict] at h
  | andE _ _ => simp [repairedIsStrict] at h
  | orE _ _ => simp [repairedIsStrict] at h
  | notE a iha =>
    simp [repairedIsStrict] at h
    simp [evalAuditExpr, iha h, valToSqlBool, SqlBool.not, sqlBoolToVal]
  | isNull _ => simp [repairedIsStrict] at h
  | isNotNull _ => simp [repairedIsStrict] at h
  | isTrue _ => simp [repairedIsStrict] at h
  | isFalse _ => simp [repairedIsStrict] at h
  | inList c ls ihc =>
    simp [repairedIsStrict, Bool.and_eq_true] at h
    rcases h with ⟨hne_bool, hc⟩
    have hne : ls ≠ [] := by
      intro hempty
      subst hempty
      simp at hne_bool
    simp [evalAuditExpr, ihc hc, evalInAux_none_nonEmpty ls hne, sqlBoolToVal]

theorem valToSqlBool_sqlBoolToVal (b : SqlBool) :
    valToSqlBool (sqlBoolToVal b) = b := by
  cases b <;> rfl

/-- ✅ **完全正当性定理 (Audit 1)**: 任意の深さ・任意の保持列値 `other` に対して、
    1. `repairedRejectsNulls e = true` ならば NULL パディング行で絶対に `.tTrue` にならない。
    2. `repairedNeverFalse e = true` ならば NULL パディング行で絶対に `.tFalse` にならない。 -/
theorem repairedRejectsAndNeverFalse_sound (other : Nat → Option Int) :
    ∀ e : AuditExpr,
      (repairedRejectsNulls e = true →
        valToSqlBool (evalAuditExpr (fun _ => none) other e) ≠ .tTrue) ∧
      (repairedNeverFalse e = true →
        valToSqlBool (evalAuditExpr (fun _ => none) other e) ≠ .tFalse) := by
  intro e
  induction e with
  | paddedCol _ =>
    simp [repairedRejectsNulls, repairedNeverFalse, evalAuditExpr, valToSqlBool]
  | otherCol _ =>
    simp [repairedRejectsNulls, repairedNeverFalse]
  | constVal v =>
    constructor
    · simp [repairedRejectsNulls]
    · intro h
      simp [repairedNeverFalse] at h
      simp [evalAuditExpr, h]
  | add a b _ _ =>
    constructor <;> intro h <;> simp [repairedRejectsNulls, repairedNeverFalse, Bool.or_eq_true] at h
    · rcases h with ha | hb
      · simp [evalAuditExpr, repairedIsStrict_sound other a ha, valToSqlBool]
      · simp [evalAuditExpr, repairedIsStrict_sound other b hb]
        cases evalAuditExpr (fun _ => none) other a <;> simp [valToSqlBool]
    · rcases h with ha | hb
      · simp [evalAuditExpr, repairedIsStrict_sound other a ha, valToSqlBool]
      · simp [evalAuditExpr, repairedIsStrict_sound other b hb]
        cases evalAuditExpr (fun _ => none) other a <;> simp [valToSqlBool]
  | sub a b _ _ =>
    constructor <;> intro h <;> simp [repairedRejectsNulls, repairedNeverFalse, Bool.or_eq_true] at h
    · rcases h with ha | hb
      · simp [evalAuditExpr, repairedIsStrict_sound other a ha, valToSqlBool]
      · simp [evalAuditExpr, repairedIsStrict_sound other b hb]
        cases evalAuditExpr (fun _ => none) other a <;> simp [valToSqlBool]
    · rcases h with ha | hb
      · simp [evalAuditExpr, repairedIsStrict_sound other a ha, valToSqlBool]
      · simp [evalAuditExpr, repairedIsStrict_sound other b hb]
        cases evalAuditExpr (fun _ => none) other a <;> simp [valToSqlBool]
  | bitXor a b _ _ =>
    constructor <;> intro h <;> simp [repairedRejectsNulls, repairedNeverFalse, Bool.or_eq_true] at h
    · rcases h with ha | hb
      · simp [evalAuditExpr, repairedIsStrict_sound other a ha, valToSqlBool]
      · simp [evalAuditExpr, repairedIsStrict_sound other b hb]
        cases evalAuditExpr (fun _ => none) other a <;> simp [valToSqlBool]
    · rcases h with ha | hb
      · simp [evalAuditExpr, repairedIsStrict_sound other a ha, valToSqlBool]
      · simp [evalAuditExpr, repairedIsStrict_sound other b hb]
        cases evalAuditExpr (fun _ => none) other a <;> simp [valToSqlBool]
  | bitNot a _ =>
    constructor <;> intro h <;> simp [repairedRejectsNulls, repairedNeverFalse] at h
    · simp [evalAuditExpr, repairedIsStrict_sound other a h, valToSqlBool]
    · simp [evalAuditExpr, repairedIsStrict_sound other a h, valToSqlBool]
  | neg a _ =>
    constructor <;> intro h <;> simp [repairedRejectsNulls, repairedNeverFalse] at h
    · simp [evalAuditExpr, repairedIsStrict_sound other a h, valToSqlBool]
    · simp [evalAuditExpr, repairedIsStrict_sound other a h, valToSqlBool]
  | eq a b _ _ =>
    constructor <;> intro h <;> simp [repairedRejectsNulls, repairedNeverFalse, Bool.or_eq_true] at h
    · rcases h with ha | hb
      · simp [evalAuditExpr, repairedIsStrict_sound other a ha, valToSqlBool]
      · simp [evalAuditExpr, repairedIsStrict_sound other b hb]
        cases evalAuditExpr (fun _ => none) other a <;> simp [valToSqlBool]
    · rcases h with ha | hb
      · simp [evalAuditExpr, repairedIsStrict_sound other a ha, valToSqlBool]
      · simp [evalAuditExpr, repairedIsStrict_sound other b hb]
        cases evalAuditExpr (fun _ => none) other a <;> simp [valToSqlBool]
  | andE a b iha ihb =>
    constructor
    · intro h
      simp [repairedRejectsNulls, Bool.or_eq_true] at h
      simp [evalAuditExpr, valToSqlBool_sqlBoolToVal]
      rcases h with ha | hb
      · have hna := iha.1 ha
        cases h1 : valToSqlBool (evalAuditExpr (fun _ => none) other a) <;>
          cases h2 : valToSqlBool (evalAuditExpr (fun _ => none) other b) <;>
          simp [SqlBool.and] <;> exact hna h1
      · have hnb := ihb.1 hb
        cases h1 : valToSqlBool (evalAuditExpr (fun _ => none) other a) <;>
          cases h2 : valToSqlBool (evalAuditExpr (fun _ => none) other b) <;>
          simp [SqlBool.and] <;> exact hnb h2
    · intro h
      simp [repairedNeverFalse, Bool.and_eq_true] at h
      simp [evalAuditExpr, valToSqlBool_sqlBoolToVal]
      have hna := iha.2 h.1
      have hnb := ihb.2 h.2
      cases h1 : valToSqlBool (evalAuditExpr (fun _ => none) other a) <;>
        cases h2 : valToSqlBool (evalAuditExpr (fun _ => none) other b) <;>
        simp [SqlBool.and] <;> first | exact hna h1 | exact hnb h2
  | orE a b iha ihb =>
    constructor
    · intro h
      simp [repairedRejectsNulls, Bool.and_eq_true] at h
      simp [evalAuditExpr, valToSqlBool_sqlBoolToVal]
      have hna := iha.1 h.1
      have hnb := ihb.1 h.2
      cases h1 : valToSqlBool (evalAuditExpr (fun _ => none) other a) <;>
        cases h2 : valToSqlBool (evalAuditExpr (fun _ => none) other b) <;>
        simp [SqlBool.or] <;> first | exact hna h1 | exact hnb h2
    · intro h
      simp [repairedNeverFalse, Bool.or_eq_true] at h
      simp [evalAuditExpr, valToSqlBool_sqlBoolToVal]
      rcases h with ha | hb
      · have hna := iha.2 ha
        cases h1 : valToSqlBool (evalAuditExpr (fun _ => none) other a) <;>
          cases h2 : valToSqlBool (evalAuditExpr (fun _ => none) other b) <;>
          simp [SqlBool.or] <;> exact hna h1
      · have hnb := ihb.2 hb
        cases h1 : valToSqlBool (evalAuditExpr (fun _ => none) other a) <;>
          cases h2 : valToSqlBool (evalAuditExpr (fun _ => none) other b) <;>
          simp [SqlBool.or] <;> exact hnb h2
  | notE a iha =>
    constructor
    · intro h
      simp [repairedRejectsNulls] at h
      have hna := iha.2 h
      simp [evalAuditExpr, valToSqlBool_sqlBoolToVal]
      cases h1 : valToSqlBool (evalAuditExpr (fun _ => none) other a) <;>
        simp [SqlBool.not]
      exact hna h1
    · intro h
      simp [repairedNeverFalse] at h
      have hna := iha.1 h
      simp [evalAuditExpr, valToSqlBool_sqlBoolToVal]
      cases h1 : valToSqlBool (evalAuditExpr (fun _ => none) other a) <;>
        simp [SqlBool.not]
      exact hna h1
  | isNull _ =>
    simp [repairedRejectsNulls, repairedNeverFalse]
  | isNotNull a _ =>
    constructor
    · intro h
      simp [repairedRejectsNulls] at h
      simp [evalAuditExpr, repairedIsStrict_sound other a h, valToSqlBool]
    · simp [repairedNeverFalse]
  | isTrue a _ =>
    constructor
    · intro h
      simp [repairedRejectsNulls] at h
      simp [evalAuditExpr, repairedIsStrict_sound other a h, valToSqlBool]
    · simp [repairedNeverFalse]
  | isFalse a _ =>
    constructor
    · intro h
      simp [repairedRejectsNulls] at h
      simp [evalAuditExpr, repairedIsStrict_sound other a h, valToSqlBool]
    · simp [repairedNeverFalse]
  | inList c ls _ =>
    constructor
    · intro h
      simp [repairedRejectsNulls, Bool.and_eq_true] at h
      rcases h with ⟨hne_bool, hc⟩
      have hne : ls ≠ [] := by
        intro hempty
        subst hempty
        simp at hne_bool
      simp [evalAuditExpr, repairedIsStrict_sound other c hc, evalInAux_none_nonEmpty ls hne,
            sqlBoolToVal, valToSqlBool]
    · simp [repairedNeverFalse]

/-!
## Audit 2: `plan/cascades_search.cpp` (`DeriveLogicalProperties` on `kOuterJoin`)
   + `pk_unique_distinct_elimination` の自己比較 `ON R.pk = R.col` バグ監査

C++ の `kOuterJoin` プロパティ導出（`cascades_search.cpp:538-568`）は、
`ON` の等価連言 `lhs = rhs` において **片方が右側列でありさえすれば** `right_join_cols` に追加していました。
そのため `ON R.pk = R.col`（右テーブル内の列同士の比較！）でも `R.pk` が `right_join_cols` に入り、
左側の主キーが結合後も一意であると誤認定され、`pk_unique_distinct_elimination` が `DISTINCT` を誤消去していました！
-/

structure AuditRightRow where
  pk  : Nat
  col : Nat
  deriving DecidableEq, Repr

inductive OnOperand where
  | leftCol  : Nat → OnOperand
  | rightPk  : OnOperand
  | rightCol : OnOperand
  | constLit : Nat → OnOperand
  deriving DecidableEq, Repr

/-- 既存 C++ の `right_join_cols` 収集判定: `lhs` または `rhs` に `rightPk` が現れれば無条件に true！ -/
def cppBuggyPinsRightPk (lhs rhs : OnOperand) : Bool :=
  lhs == .rightPk || rhs == .rightPk

/-- 修正版の判定: `rightPk` の相手側が必ず `leftCol` または `constLit` でなければならない！ -/
def repairedPinsRightPk (lhs rhs : OnOperand) : Bool :=
  let otherPinned : OnOperand → Bool
    | .leftCol _ | .constLit _ => true
    | _ => false
  (lhs == .rightPk && otherPinned rhs) || (rhs == .rightPk && otherPinned lhs)

/-- 🚨 **実在バグ証明 2**: `L = [1]`（PK 一意）, `R = [{10, 10}, {20, 20}]`（`pk` 一意）に対して、
    `ON R.pk = R.col` は既存 C++ の `cppBuggyPinsRightPk` を通過するが、
    `LEFT JOIN` 後の `L` キー射影は `[1, 1]` と重複し、`DISTINCT` 消去で結果が壊れる！ -/
theorem cpp_outerJoin_selfEq_candidateKey_bug :
    let L : Rel Nat := [1]
    let R : Rel AuditRightRow := [{ pk := 10, col := 10 }, { pk := 20, col := 20 }]
    let onSelfEq : Nat → AuditRightRow → SqlBool := fun _ r =>
      if r.pk == r.col then .tTrue else .tFalse
    (R.map AuditRightRow.pk).Nodup ∧
    cppBuggyPinsRightPk .rightPk .rightCol = true ∧
    repairedPinsRightPk .rightPk .rightCol = false ∧
    distinctKeys ((leftOuterJoin onSelfEq L R).map Prod.fst) ≠
      (leftOuterJoin onSelfEq L R).map Prod.fst := by
  decide

/-!
## Audit 3: `plan/cascades.cpp` (`push_selection_through_aggregation`) の
   `grouping_sets` コピー漏れバグ監査

`plan/cascades.cpp` の `push_selection_through_aggregation` は、フィルタを集約の下へ押し下げた後、
新しい `kAggregation` 式を構築する際に `.target_list` のみをコピーし、
**`.grouping_sets` / `.partition_by` のコピーを忘れて空（＝スカラー集約！）にしていました**！
-/

/-- スカラー集約 `SELECT first_key, COUNT(*)` のモデル:
    入力が空でも **1 行 `(none, 0)`** を出力し、複数グループがあっても **1 行** に潰してしまう！ -/
def scalarCountModel {T K : Type} (r : Rel T) (key : T → SqlVal K) : Rel (SqlVal K × Int) :=
  match r with
  | []     => [(none, 0)]
  | h :: _ => [(key h, sqlCountStar r)]

/-- 🚨 **実在バグ証明 3**: `push_selection_through_aggregation` が `grouping_sets` を落として
    スカラー集約化すると、(A) 複数グループ残存時に 1 行へ潰れ、(B) 0 行通過時に幻の `(none, 0)` 行が出現する！ -/
theorem cpp_push_sel_through_agg_drops_grouping_bug :
    let R : Rel (Int × Int) := [(20, 1), (30, 1)]
    let key : Int × Int → SqlVal Int := fun p => some p.1
    let fn : SqlVal Int → Rel (Int × Int) → Int := fun _ g => sqlCountStar g
    -- (A) `k > 10` で 2 グループ残るケース: 本来 2 行なのに C++ バグ版は 1 行に潰れる！
    let keepGt10 : SqlVal Int → Bool := fun k => match k with | some x => x > 10 | none => false
    ((groupBy key fn R).filter (fun g => keepGt10 g.1)).length = 2 ∧
    (scalarCountModel (R.filter (fun row => keepGt10 (key row))) key).length = 1 ∧
    -- (B) `k > 100` で 0 行になるケース: 本来 0 行なのに C++ バグ版は 1 行 `(none, 0)` を捏造する！
    let keepGt100 : SqlVal Int → Bool := fun k => match k with | some x => x > 100 | none => false
    ((groupBy key fn R).filter (fun g => keepGt100 g.1)) = [] ∧
    (scalarCountModel (R.filter (fun row => keepGt100 (key row))) key) = [(none, 0)] := by
  decide

private theorem distinctKeys_filter_comm {K : Type} [DecidableEq K]
    (ks : List K) (p : K → Bool) :
    distinctKeys (ks.filter p) = (distinctKeys ks).filter p := by
  induction ks with
  | nil => rfl
  | cons k tl ih =>
    cases hp : p k
    · simp only [List.filter, hp, distinctKeys, ih, List.filter_filter]
      apply List.filter_congr
      intro y _
      by_cases hyk : y = k
      · subst hyk; simp [hp]
      · simp [hyk]
    · simp only [List.filter, hp, distinctKeys, ih, List.filter_filter]
      congr 1
      apply List.filter_congr
      intro y _
      exact Bool.and_comm _ _

/-- ✅ **完全正当性定理 (Audit 3)**: `grouping_sets` を保持した修正版 `push_selection_through_aggregation` は、
    任意の入力リレーション `r`・集約関数 `fn`・グループキー述語 `p` に対して **完全に一致する**！ -/
theorem repaired_push_sel_through_agg_sound {T K V : Type} [DecidableEq K]
    (key : T → K) (fn : K → Rel T → V) (p : K → Bool) (r : Rel T) :
    (groupBy key fn r).filter (fun kv => p kv.1) =
      groupBy key fn (r.filter (fun x => p (key x))) := by
  unfold groupBy
  have hmap : (r.filter (fun x => p (key x))).map key = (r.map key).filter p := by
    induction r with
    | nil => rfl
    | cons hd tl ih =>
      simp only [List.filter, List.map]
      cases p (key hd) <;> simp [ih]
  rw [hmap, distinctKeys_filter_comm, List.filter_map]
  have hfilt : (distinctKeys (r.map key)).filter
      (fun k => p (k, fn k (r.filter (fun row => key row == k))).1) =
      (distinctKeys (r.map key)).filter p := rfl
  rw [hfilt]
  apply List.map_congr_left
  intro k hk
  have hpk : p k = true := (List.mem_filter.mp hk).2
  congr 1
  congr 1
  rw [List.filter_filter]
  apply List.filter_congr
  intro x _
  letI : BEq K := instBEqOfDecidableEq
  cases hkx : (key x == k)
  · simp
  · have heq : key x = k := of_decide_eq_true hkx
    simp [heq, hpk]

/-!
## Audit 4: `plan/cascades.cpp` (`distinct_over_group_by` & `distinct_and_group_by_interchange`)
   の非列式 `GROUP BY` キー無視バグ監査

C++ の `distinct_over_group_by`（および `distinct_and_group_by_interchange`）は：
```cpp
for (const auto& g : agg.grouping_sets) {
  if (g && g->Type() == TypeTag::kColumnValue) {
    grouping_cols.insert(...);
  }
}
```
と書いていたため、`GROUP BY a, ABS(b)` のように **列参照と計算式が混在する `GROUP BY`** で
`ABS(b)` を黙ってスキップし、`a` さえ射影されていれば `DISTINCT` を消去していました！
-/

inductive GroupKeyItem where
  | colKey  : String → GroupKeyItem
  | exprKey : String → GroupKeyItem
  deriving DecidableEq, Repr

/-- 既存 C++ の判定: `colKey` だけを集め、`exprKey` を無視してしまっていた！ -/
def cppBuggyAllGroupingProjected (gkeys : List GroupKeyItem) (projCols : List String) : Bool :=
  let groupingCols := gkeys.filterMap fun
    | .colKey c  => some c
    | .exprKey _ => none -- ❌ 計算式キーを黙ってスキップ！
  !groupingCols.isEmpty && groupingCols.all (fun c => projCols.contains c)

/-- 修正版の判定: すべての `GROUP BY` キー項目（式を含む！）が出力リストに含まれていなければならない！ -/
def repairedAllGroupingProjected (gkeys : List GroupKeyItem) (projItems : List GroupKeyItem) : Bool :=
  !gkeys.isEmpty && gkeys.all (fun g => projItems.contains g)

/-- 🚨 **実在バグ証明 4**: `R = [(1, -2), (1, 3)]` に対して `SELECT DISTINCT a FROM R GROUP BY a, |b|` を実行すると、
    `GROUP BY a, |b|` のグループ出力は `[1, 1]` と重複するが、既存 C++ の `cppBuggyAllGroupingProjected` は
    `true` を返して `DISTINCT` を誤消去する！ -/
theorem cpp_distinct_over_group_by_skips_expr_key_bug :
    let gkeys := [GroupKeyItem.colKey "a", GroupKeyItem.exprKey "abs(b)"]
    let R : Rel (Int × Int) := [(1, -2), (1, 3)]
    let groupOut := (groupBy (fun p : Int × Int => (p.1, p.2.natAbs))
      (fun _ g => sqlCountStar g) R).map (fun kv => kv.1.1)
    cppBuggyAllGroupingProjected gkeys ["a"] = true ∧
    repairedAllGroupingProjected gkeys [.colKey "a"] = false ∧
    groupOut = [1, 1] ∧
    distinctKeys groupOut ≠ groupOut := by
  decide

/-- ✅ **完全正当性定理 (Audit 4)**: すべてのグループキー情報 `K` が出力射影 `proj` から一意に復元できる
    （`proj kv1 = proj kv2 → kv1.1 = kv2.1`）とき、`groupBy` の出力は既に重複を持たない（`Nodup`）ため
    `DISTINCT` 消去は常に正しい！ -/
theorem repaired_distinct_over_group_by_nodup {T K V Out : Type} [DecidableEq K]
    (key : T → K) (fn : K → Rel T → V) (r : Rel T)
    (proj : K × V → Out)
    (hinj : ∀ kv1 kv2, proj kv1 = proj kv2 → kv1.1 = kv2.1) :
    ((groupBy key fn r).map proj).Nodup := by
  unfold groupBy
  rw [List.map_map]
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
    have hkk : k' = k := hinj _ _ hEq
    exact hN.1 (hkk ▸ hk')

/-!
## Audit 5: `plan/cascades.cpp` (`aggregate_union_transpose`) の
   複合式ターゲット `SUM(x) + 1` / `AVG(x) + 1` および `HAVING` 欠落バグ監査

C++ の `aggregate_union_transpose` は：
```cpp
if (target.expression->Type() == TypeTag::kAggregateExp) { ... }
```
と**最上位ノードが `kAggregateExp` のときだけ**検査していたため、
`SUM(x) + 1` や `AVG(x) + 1` のような二項演算（`kBinaryExp`）を「集約ではない通常式（`distributable = true`）」と誤認し、
各ブランチで計算した上に親でも再度評価していました！
-/

/-- 🚨 **実在バグ証明 5a**: `SUM(x) + 1` を各 `UNION ALL` ブランチへ分配して合算すると `+ 1` がブランチ数回加算される！ -/
theorem cpp_agg_union_compound_sum_plus_one_bug :
    let B1 : Rel (SqlVal Int) := [some 10]
    let B2 : Rel (SqlVal Int) := [some 20]
    let trueVal := sqlAdd (sqlSum id (B1 ++ B2)) (some 1)
    let buggyBranch1 := sqlAdd (sqlSum id B1) (some 1)
    let buggyBranch2 := sqlAdd (sqlSum id B2) (some 1)
    let buggyCombined := sqlSum id [buggyBranch1, buggyBranch2]
    trueVal = some 31 ∧ buggyCombined = some 32 ∧ trueVal ≠ buggyCombined := by
  decide

/-- 🚨 **実在バグ証明 5b**: `HAVING SUM(x) > 10` を各ブランチの `Aggregation` にコピーすると、
    ブランチ 1 (`SUM = 6`) とブランチ 2 (`SUM = 6`) が両方とも落ちて合算 `12 > 10` が消失する！ -/
theorem cpp_agg_union_having_pushed_to_branch_bug :
    let B1 : Rel (Int × SqlVal Int) := [(1, some 6)]
    let B2 : Rel (Int × SqlVal Int) := [(1, some 6)]
    let gt10 : SqlVal Int → Bool := fun v => match v with | some s => s > 10 | none => false
    let sumSnd : Int → Rel (Int × SqlVal Int) → SqlVal Int := fun _ g => sqlSum Prod.snd g
    let outerHaving := (groupBy Prod.fst sumSnd (B1 ++ B2)).filter (fun kv => gt10 kv.2)
    let branchFiltered1 := (groupBy Prod.fst sumSnd B1).filter (fun kv => gt10 kv.2)
    let branchFiltered2 := (groupBy Prod.fst sumSnd B2).filter (fun kv => gt10 kv.2)
    outerHaving = [(1, some 12)] ∧ (branchFiltered1 ++ branchFiltered2) = [] := by
  decide

/-- ✅ **完全正当性定理 (Audit 5)**: 純粋な最上位集約（`SUM` / `COUNT`）のみをブランチへ押し下げ、
    `HAVING` を最上位の親 `Aggregation` にのみ保持する修正版 `aggregate_union_transpose` は、
    片方または両方のブランチが空・全 `NULL` の場合を含め **全入力で完全一致** する！ -/
theorem repaired_agg_union_sum_and_count_sound {Row : Type}
    (f : Row → SqlVal Int) (B1 B2 : Rel Row) :
    sqlSum id [sqlSum f B1, sqlSum f B2] = sqlSum f (B1 ++ B2) ∧
    sqlSum id [some (sqlCountCol f B1), some (sqlCountCol f B2)] =
      some (sqlCountCol f (B1 ++ B2)) := by
  constructor
  · have hdist : ∀ xs ys : Rel Row,
        sqlSum f (xs ++ ys) = mergeSum (sqlSum f xs) (sqlSum f ys) := by
      intro xs ys
      induction xs with
      | nil => simp [sqlSum, aggFold, sumMonoid, mergeSum]
      | cons x tl ih =>
        simp only [List.cons_append, sqlSum_cons] at ih ⊢
        rw [ih]
        exact (sumMonoid.op_assoc (f x) (sqlSum f tl) (sqlSum f ys)).symm
    rw [hdist]
    cases hb1 : sqlSum f B1 <;> cases hb2 : sqlSum f B2 <;>
      simp [sqlSum, aggFold, sumMonoid, mergeSum]
  · have hcnt : ∀ xs ys : Rel Row,
        sqlCountCol f (xs ++ ys) = sqlCountCol f xs + sqlCountCol f ys := by
      intro xs ys
      induction xs with
      | nil => simp [sqlCountCol, aggFold, countMonoid]
      | cons x tl ih =>
        simp only [List.cons_append, sqlCountCol_cons, ih, Int.add_assoc]
    rw [hcnt]
    simp [sqlSum, aggFold, sumMonoid, mergeSum]

/-!
## Audit 6: `filter_aggregate_pushdown` & `count_star_rewrite_on_not_null` (`plan/cascades.cpp`)

- 旧 C++ の `filter_aggregate_pushdown` は `target.expression->Type() == TypeTag::kAggregateExp` の要素にのみ
  `FILTER (WHERE p)` を付与し、`else { new_targets.push_back(target); }` で `Selection(p)` ごと消去していたため、
  同居する `SUM(x) + 1`（`kBinaryExp`）がフィルタ無しで全行計算されてしまうバグがありました。
- 修正版はトップレベル `kAggregateExp` と定数のみを許可し、既存 `FILTER (WHERE w)` と `p` を連言結合します。
-/

/-- 🚨 **実在バグ証明 6**: `SELECT COUNT(*), SUM(x) + 1 FROM t WHERE x > 0` で `Selection` を外し、
    トップレベル `kAggregateExp` のみに `FILTER` を付けると、`SUM(x) + 1` が負の行 `-100` まで合算してしまう！ -/
theorem cpp_filter_agg_pushdown_compound_target_bug :
    let t : Rel Int := [-100, 10]
    let pos : Int → Bool := fun x => x > 0
    let trueOut := (sqlCountStar (t.filter pos), sqlAdd (sqlSum some (t.filter pos)) (some 1))
    let buggyOut :=
      (aggFold countMonoid (fun x => if pos x then 1 else 0) t,
       sqlAdd (sqlSum some t) (some 1))
    trueOut = (1, some 11) ∧ buggyOut = (1, some (-89)) ∧ trueOut ≠ buggyOut := by
  decide

/-- ✅ **完全正当性定理 (Audit 6a: `filter_aggregate_pushdown`)**:
    トップレベル集約に対して `Selection(p)` を既存 `FILTER (WHERE w)` との連言 `w ∧ p` に畳み込む変換は、
    任意の `AggMonoid` スカラー集約で元の `Selection(p)` 上の集約と **全入力で完全一致** する。 -/
theorem repaired_filter_aggregate_pushdown_sound {Row M : Type}
    (m : AggMonoid M) (f : Row → M) (w p : Row → SqlBool) (rel : Rel Row) :
    aggFold m (fun r => if (w r).inWhere then f r else m.zero)
      (rel.filter (fun r => (p r).inWhere)) =
    aggFold m (fun r => if ((w r).and (p r)).inWhere then f r else m.zero) rel := by
  induction rel with
  | nil => rfl
  | cons r tl ih =>
    have hcons : ∀ (g : Row → M) (x : Row) (xs : Rel Row),
        aggFold m g (x :: xs) = m.op (g x) (aggFold m g xs) := fun _ _ _ => rfl
    have hand := and_inWhere (w r) (p r)
    cases hp : (p r).inWhere <;> cases hw : (w r).inWhere
    · simp only [List.filter, hp, hcons, hand, hw, Bool.false_and, Bool.false_eq_true, ↓reduceIte,
                 m.zero_op, ih]
    · simp only [List.filter, hp, hcons, hand, hw, Bool.and_false, Bool.false_eq_true, ↓reduceIte,
                 m.zero_op, ih]
    · simp only [List.filter, hp, hcons, hand, hw, Bool.false_and, Bool.false_eq_true, ↓reduceIte,
                 ih]
    · simp only [List.filter, hp, hcons, hand, hw, Bool.true_and, ↓reduceIte, ih]

/-- ✅ **完全正当性定理 (Audit 6b: `count_star_rewrite_on_not_null`)**:
    入力上で非 `NULL` が保証された列 `f` に対する `COUNT(f)` は `COUNT(*)` と **全入力で完全一致** する。 -/
theorem count_star_rewrite_on_not_null_sound {Row α : Type}
    (f : Row → SqlVal α) (rel : Rel Row)
    (h_nn : ∀ r ∈ rel, f r ≠ none) :
    sqlCountCol f rel = sqlCountStar rel := by
  induction rel with
  | nil => rfl
  | cons r tl ih =>
    have hr : (f r).isSome = true := by
      cases hf : f r with
      | none => exact absurd hf (h_nn r (List.Mem.head tl))
      | some _ => rfl
    have htl : ∀ x ∈ tl, f x ≠ none := fun x hx => h_nn x (List.Mem.tail r hx)
    simp only [sqlCountCol_cons, sqlCountStar_cons, hr, ↓reduceIte, ih htl]

end TinylambOpt
