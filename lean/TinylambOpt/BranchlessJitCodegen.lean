import Std.Tactic.BVDecide
import TinylambOpt.ClickBenchDiscovery

/-
# Step 12: 物理プラン後段（Bytecode → Branchless LLVM JIT / SIMD）の形式検証と積極的コード生成

論理・物理プラン最適化（Cascades）のさらに下流、`BytecodeProgram` から LLVM ORC JIT ネイティブコードへ
落とすレイヤ（`expression/jit.cpp`, `executor/selection.cpp`, `executor/projection.cpp`,
`executor/aggregation.cpp`）において、以下の **4 大積極的最適化** の健全性・完全性を機械証明します：

1. **2レーン `(bits : BitVec 64, isNull : Bool)` & 64行ビットスライス Kleene 3値論理 (`bv_decide`)**
   - NULL 混在バッチでもフォールバックせず分岐ゼロで `AND / OR / XOR / NOT / IS [NOT] NULL / TRUE / FALSE` を評価
2. **ハードウェア例外ゼロの投機的算術・ビット演算 (`bv_decide`)**
   - 分岐なし `ABS` (`(x + (x >> 63)) ^ (x >> 63)`)
   - 投機実行しても x86 `idivq` が絶対に `SIGFPE` を起こさない除数サニタイズ (`safeDivisorBV`)
   - `ColumnVector::IntegerData()` の NULL スロットにどんなゴミビットが入っていても結果・エラーが汚染されない不変条件
3. **前方分岐スタック Bytecode DAG の単一基本ブロック If-Conversion 定理 (`if_conversion_sound_and_complete`)**
   - `AND / OR` 短絡ジャンプ、`Searched CASE WHEN`、`COALESCE`、`IN (...)`（`kDup` / `kPop` 含む）を持つ
     任意のスタックバイトコードを **ループ内分岐ゼロ（`llvm::SelectInst` のみ）** に平坦化しつつ、
     AST の厳密な短絡エラー抑止（D7）と 100% 一致させる一般カット不変条件の完全証明
4. **ZoneMap 区間伝播によるオーバーフロー検査除去 & NULL マスク付き SIMD `SUM` 定理**
   - バッチ単位 `O(1)` の 4 端点区間検査で `llvm.s{add,sub,mul}.with.overflow` を純粋な `nsw` SIMD 命令へ昇格
   - 累積 `SUM` の `[MAX, 5, -5]` 途中オーバーフロー罠を完全に防ぐプレフィックス包絡線ガードと NULL ゼロ置換の同値性
-/

namespace TinylambOpt

/-!
## 1. ブランチレス 3値論理（単一 SSA レーン & 64行ビットスライス）の証明
-/

/-- LLVM JIT の単一 SSA 値レーン表現: `(bits : i64, isNull : i1)`。 -/
structure JitLane where
  bits   : BitVec 64
  isNull : Bool
  deriving DecidableEq, Repr

/-- `JitLane` から SQL 三値論理 `SqlBool` への意味写像（`FastTruthy` と同一：`bits != 0` が真）。 -/
def JitLane.toSqlBool (x : JitLane) : SqlBool :=
  if x.isNull then .tNull
  else if x.bits != 0#64 then .tTrue
  else .tFalse

/-- ブランチレス SSA 実装：論理 `NOT` -/
def jitNot (x : JitLane) : JitLane :=
  { bits := if x.bits == 0#64 then 1#64 else 0#64
    isNull := x.isNull }

/-- ブランチレス SSA 実装：Kleene 3値 `AND`（分岐ゼロ） -/
def jitAnd (l r : JitLane) : JitLane :=
  let lTrue  := (!l.isNull) && (l.bits != 0#64)
  let rTrue  := (!r.isNull) && (r.bits != 0#64)
  let lFalse := (!l.isNull) && (l.bits == 0#64)
  let rFalse := (!r.isNull) && (r.bits == 0#64)
  let resFalse := lFalse || rFalse
  let resNull  := (!resFalse) && (l.isNull || r.isNull)
  { bits := if lTrue && rTrue then 1#64 else 0#64
    isNull := resNull }

/-- ブランチレス SSA 実装：Kleene 3値 `OR`（分岐ゼロ） -/
def jitOr (l r : JitLane) : JitLane :=
  let lTrue := (!l.isNull) && (l.bits != 0#64)
  let rTrue := (!r.isNull) && (r.bits != 0#64)
  let resTrue := lTrue || rTrue
  let resNull := (!resTrue) && (l.isNull || r.isNull)
  { bits := if resTrue then 1#64 else 0#64
    isNull := resNull }

/-- ブランチレス SSA 実装：3値 `XOR` -/
def jitXor (l r : JitLane) : JitLane :=
  let lTrue := l.bits != 0#64
  let rTrue := r.bits != 0#64
  { bits := if lTrue != rTrue then 1#64 else 0#64
    isNull := l.isNull || r.isNull }

/-- ブランチレス SSA 実装：`IS NULL / IS NOT NULL / IS [NOT] TRUE / IS [NOT] FALSE` -/
def jitIsNull     (x : JitLane) : JitLane := ⟨if x.isNull then 1#64 else 0#64, false⟩
def jitIsNotNull  (x : JitLane) : JitLane := ⟨if !x.isNull then 1#64 else 0#64, false⟩
def jitIsTrue     (x : JitLane) : JitLane := ⟨if (!x.isNull) && (x.bits != 0#64) then 1#64 else 0#64, false⟩
def jitIsNotTrue  (x : JitLane) : JitLane := ⟨if x.isNull || (x.bits == 0#64) then 1#64 else 0#64, false⟩
def jitIsFalse    (x : JitLane) : JitLane := ⟨if (!x.isNull) && (x.bits == 0#64) then 1#64 else 0#64, false⟩
def jitIsNotFalse (x : JitLane) : JitLane := ⟨if x.isNull || (x.bits != 0#64) then 1#64 else 0#64, false⟩

/-- ブランチレス SSA 実装：`WHERE` フィルタ出力バイト (`!isNull && bits != 0`) -/
def jitKeepRow (x : JitLane) : Bool :=
  (!x.isNull) && (x.bits != 0#64)

/-- ✅ **定理 1-A (`jit_3vl_lane_sound`)**:
    任意の `64-bit` ペイロード（`-1`, `0`, `1`, `42`, ゴミビット含む）と NULL フラグに対して、
    ブランチレス SSA の `NOT / AND / OR / IS TRUE / IS NOT TRUE / WHERE keep` は
    基準の `SqlBool` 三値論理と **全ビットパターンで完全一致** する。 -/
theorem jit_3vl_lane_sound (l r : JitLane) :
    (jitNot l).toSqlBool = l.toSqlBool.not ∧
    (jitAnd l r).toSqlBool = l.toSqlBool.and r.toSqlBool ∧
    (jitOr l r).toSqlBool = l.toSqlBool.or r.toSqlBool ∧
    (jitIsTrue l).toSqlBool = l.toSqlBool.isTrue ∧
    (jitIsNotTrue l).toSqlBool = l.toSqlBool.isNotTrue ∧
    jitKeepRow l = l.toSqlBool.inWhere := by
  cases l with
  | mk lb ln =>
    cases r with
    | mk rb rn =>
      cases ln <;> cases rn <;>
        by_cases hlb : lb = 0#64 <;> by_cases hrb : rb = 0#64 <;>
        simp [JitLane.toSqlBool, jitNot, jitAnd, jitOr, jitIsTrue, jitIsNotTrue,
              jitKeepRow, SqlBool.not, SqlBool.and, SqlBool.or,
              SqlBool.isTrue, SqlBool.isNotTrue, SqlBool.inWhere, hlb, hrb]

/-- 64行一括ビットスライス三値論理表現：
    `trueMask`（真の行が 1）と `nullMask`（NULL の行が 1）。 -/
structure BitMask64 where
  trueMask : BitVec 64
  nullMask : BitVec 64

def BitMask64.laneAt (m : BitMask64) (i : Nat) : SqlBool :=
  if m.nullMask.getLsbD i then .tNull
  else if m.trueMask.getLsbD i then .tTrue
  else .tFalse

def BitMask64.not (m : BitMask64) : BitMask64 :=
  { trueMask := (~~~m.trueMask) &&& (~~~m.nullMask)
    nullMask := m.nullMask }

def BitMask64.and (a b : BitMask64) : BitMask64 :=
  let aFalse := (~~~a.trueMask) &&& (~~~a.nullMask)
  let bFalse := (~~~b.trueMask) &&& (~~~b.nullMask)
  let resFalse := aFalse ||| bFalse
  { trueMask := (a.trueMask &&& b.trueMask) &&& (~~~(a.nullMask ||| b.nullMask))
    nullMask := (~~~resFalse) &&& (a.nullMask ||| b.nullMask) }

def BitMask64.or (a b : BitMask64) : BitMask64 :=
  let aTrue := a.trueMask &&& (~~~a.nullMask)
  let bTrue := b.trueMask &&& (~~~b.nullMask)
  let resTrue := aTrue ||| bTrue
  { trueMask := resTrue
    nullMask := (~~~resTrue) &&& (a.nullMask ||| b.nullMask) }

/-- ✅ **定理 1-B (`bitmask64_3vl_sound`)**:
    64行分の `BitVec 64` ビット演算だけで計算した 3値 `NOT / AND / OR / WHERE` が、
    任意のレーン `i < 64` で `SqlBool` の行単位評価と **1ビットの狂いもなく一致** する！ -/
theorem bitmask64_3vl_sound (a b : BitMask64) (i : Nat) (hi : i < 64) :
    (a.not).laneAt i = (a.laneAt i).not ∧
    (a.and b).laneAt i = (a.laneAt i).and (b.laneAt i) ∧
    (a.or b).laneAt i = (a.laneAt i).or (b.laneAt i) ∧
    ((a.trueMask &&& (~~~a.nullMask)).getLsbD i = (a.laneAt i).inWhere) := by
  simp only [BitMask64.laneAt, BitMask64.not, BitMask64.and, BitMask64.or,
             BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_not, hi, decide_true, Bool.true_and]
  cases haN : a.nullMask.getLsbD i <;> cases haT : a.trueMask.getLsbD i <;>
    cases hbN : b.nullMask.getLsbD i <;> cases hbT : b.trueMask.getLsbD i <;>
    simp [SqlBool.not, SqlBool.and, SqlBool.or, SqlBool.inWhere]


/-!
## 2. ハードウェア例外ゼロの投機的ビット演算・除数サニタイズ・NULLゴミ耐性 (`bv_decide`)
-/

/-- 符号付き 64-bit 最小値 `INT64_MIN = -2^63 = 0x8000000000000000`。 -/
def i64MinBV : BitVec 64 := 0x8000000000000000#64

/-- ブランチレス `ABS` カーネル: `mask = x >>s 63; (x + mask) ^ mask`（分岐命令ゼロ）。 -/
def branchlessAbsBV (x : BitVec 64) : BitVec 64 :=
  let mask := x.sshiftRight 63
  (x + mask) ^^^ mask

/-- ✅ **定理 2-A (`branchlessAbsBV_exact` via `bv_decide`)**:
    全 $2^{64}$ 通りの `BitVec 64` に対して：
    1. `branchlessAbsBV x` は `x <s 0` のとき `-x`、`x ≥s 0` のとき `x` に完全一致する。
    2. `x ≠ INT64_MIN` なら符号付き非負（`¬ slt _ 0`）であり、`x = INT64_MIN` のみオーバーフローする。 -/
theorem branchlessAbsBV_exact (x : BitVec 64) :
    (branchlessAbsBV x = if BitVec.slt x 0#64 then -x else x) ∧
    (x ≠ i64MinBV → BitVec.slt (branchlessAbsBV x) 0#64 = false) := by
  constructor
  · unfold branchlessAbsBV
    bv_decide
  · unfold branchlessAbsBV i64MinBV
    bv_decide

/-- 投機的実行用の安全な除数サニタイザ:
    NULL レーン・ゼロ除算・`INT64_MIN / -1` のいずれかに該当するレーンでは除数を `1` に置換する。 -/
def safeDivisorBV (l r : BitVec 64) (lNull rNull : Bool) : BitVec 64 :=
  if lNull || rNull || (r == 0#64) || (l == i64MinBV && r == -1#64) then
    1#64
  else
    r

private theorem safeDivisorBV_no_trap_aux (l r : BitVec 64) (lNull rNull : Bool) :
    safeDivisorBV l r lNull rNull ≠ 0#64 ∧
    ¬(l = i64MinBV ∧ safeDivisorBV l r lNull rNull = -1#64) := by
  cases lNull <;> cases rNull <;>
    (unfold safeDivisorBV i64MinBV; bv_decide)

private theorem safeDivisorBV_eq_when_valid (l r : BitVec 64)
    (hr : r ≠ 0#64) (hmin : ¬(l = i64MinBV ∧ r = -1#64)) :
    safeDivisorBV l r false false = r := by
  unfold safeDivisorBV i64MinBV at *
  bv_decide

/-- ✅ **定理 2-B (`safeDivisorBV_never_traps_and_exact` via `bv_decide`)**:
    全 $2^{128} \times 4$ 通りの入力 `(l, r, lNull, rNull)` に対して：
    1. **ハードウェア無トラップ保証**: `safeDivisorBV` は絶対に `0` にならず、かつ
       `(l = INT64_MIN ∧ safeDivisorBV = -1)` も絶対に起きない（x86 `idivq` の `SIGFPE` 完全消滅！）。
    2. **値保存性**: 正常レーンでは `srem l (safeDivisorBV ...)` は本来の `srem l r` と完全一致する。 -/
theorem safeDivisorBV_never_traps_and_exact (l r : BitVec 64) (lNull rNull : Bool) :
    let d := safeDivisorBV l r lNull rNull
    (d ≠ 0#64) ∧
    ¬(l = i64MinBV ∧ d = -1#64) ∧
    (lNull = false ∧ rNull = false ∧ r ≠ 0#64 ∧ ¬(l = i64MinBV ∧ r = -1#64) →
      BitVec.srem l d = BitVec.srem l r) := by
  have hTrap := safeDivisorBV_no_trap_aux l r lNull rNull
  refine ⟨hTrap.1, hTrap.2, ?_⟩
  rintro ⟨rfl, rfl, hr, hmin⟩
  rw [safeDivisorBV_eq_when_valid l r hr hmin]

/-- ブランチレス符号付き加算（NULL マスク付きエラー生成）:
    `ColumnVector::IntegerData()` の NULL 位置に `INT64_MAX` 等のゴミが入っていても
    `(!l.isNull && !r.isNull)` でエラービットをマスクする。 -/
def jitCheckedAdd (l r : JitLane) : JitLane × Bool :=
  let rawSum := l.bits + r.bits
  let ovf    := BitVec.saddOverflow l.bits r.bits
  let isNull := l.isNull || r.isNull
  let err    := (!isNull) && ovf
  ({ bits := rawSum, isNull := isNull }, err)

/-- ✅ **定理 2-C (`jitCheckedAdd_null_garbage_immune`)**:
    片方が `NULL`（`l.isNull = true`）である限り、その背後にある生メモリ `g1, g2 : BitVec 64` に
    どんなゴミ値（`INT64_MAX` 等）が入っていようと、エラーフラグは常に `false` であり、
    出力の NULL 状態は一切影響を受けない！ -/
theorem jitCheckedAdd_null_garbage_immune (g1 g2 : BitVec 64) (r : JitLane) :
    let (out1, err1) := jitCheckedAdd ⟨g1, true⟩ r
    let (out2, err2) := jitCheckedAdd ⟨g2, true⟩ r
    err1 = false ∧ err2 = false ∧ out1.isNull = true ∧ out2.isNull = true := by
  simp [jitCheckedAdd]


/-!
## 3. 前方分岐スタック Bytecode DAG の単一基本ブロック If-Conversion 定理

`BytecodeCompiler` が出力する全制御フロー（`AND`, `OR`, `Searched CASE`, `COALESCE`, `IN (...)`）は
**すべて前方ジャンプ (`offset ≥ 1`) のスタックマシン DAG** です。
ここでは「PC ごとにジャンプする逐次実行 VM (`runSeqFrom`)」と、
「ループ内分岐ゼロで `pc = 0 .. M-1` を 1 パス直進し `select` でスタックを合流させる
Predicated SSA 実行 (`runPredicated`)」が **値・NULL・短絡エラーの有無まで 100% 同値** であることを証明します。
-/

/-- 抽象化された前方分岐スタック Bytecode 命令セット（`expression/bytecode.hpp` に対応）。 -/
inductive DagInst (Row : Type) where
  | pushLeaf      : (Row → JitLane × Bool) → DagInst Row
  | unaryOp       : (JitLane → JitLane × Bool) → DagInst Row
  | binaryOp      : (JitLane → JitLane → JitLane × Bool) → DagInst Row
  | popAt         : Nat → DagInst Row
  | dupAt         : Nat → DagInst Row
  | jump          : Nat → DagInst Row   -- 相対ジャンプ幅 `step + 1`（常に `≥ 1` ステップ前進！）
  | jumpIfFalse   : Nat → DagInst Row   -- top が非NULL偽なら top を `0` に正規化して `pc + step + 1` へ
  | jumpIfTrue    : Nat → DagInst Row   -- top が非NULL真なら top を `1` に正規化して `pc + step + 1` へ
  | jumpIfNotNull : Nat → DagInst Row   -- top が非NULLならそのまま `pc + step + 1` へ

/-- スタック先頭から `idx` 番目を取り除く（`BytecodeOp::kPop` の `operand` 対応）。 -/
def eraseFromTop {α : Type} (stk : List α) (idx : Nat) : List α :=
  stk.eraseIdx idx

/-- スタック先頭から `idx` 番目を複製して先頭に積む（`BytecodeOp::kDup` の `operand` 対応）。 -/
def dupFromTop (stk : List JitLane) (idx : Nat) : List JitLane :=
  match stk[idx]? with
  | some v => v :: stk
  | none   => ⟨0#64, true⟩ :: stk

/-- 2 つの同長スタックをブランチレス `select` (`llvm::SelectInst`) で要素ごとに合流させる。 -/
def selectStack (cond : Bool) (onTrue onFalse : List JitLane) : List JitLane :=
  match onTrue, onFalse with
  | x :: xs, y :: ys => (if cond then x else y) :: selectStack cond xs ys
  | _, _             => if cond then onTrue else onFalse

@[simp] theorem selectStack_true (a b : List JitLane) (h : a.length = b.length) :
    selectStack true a b = a := by
  induction a generalizing b with
  | nil => cases b <;> simp [selectStack] at h ⊢
  | cons x xs ih =>
    cases b with
    | nil => simp at h
    | cons y ys =>
      simp only [List.length_cons, Nat.add_right_cancel_iff] at h
      simp [selectStack, ih ys h]

@[simp] theorem selectStack_false (a b : List JitLane) (h : a.length = b.length) :
    selectStack false a b = b := by
  induction a generalizing b with
  | nil => cases b <;> simp [selectStack] at h ⊢
  | cons x xs ih =>
    cases b with
    | nil => simp at h
    | cons y ys =>
      simp only [List.length_cons, Nat.add_right_cancel_iff] at h
      simp [selectStack, ih ys h]

/-- 1 命令の局所実行結果：`(局所エラー発生?, 遷移先1, オプション遷移先2)`。 -/
inductive StepOut where
  | one : Bool → Nat → List JitLane → StepOut
  | two : Bool → Bool → (Nat × List JitLane) → (Nat × List JitLane) → StepOut

/-- `pc` における 1 命令の意味（逐次 VM と Predicated SSA の両方が共有する単一命令セマンティクス）。 -/
def stepInst {Row : Type} (inst : DagInst Row) (row : Row) (pc : Nat) (stk : List JitLane) : StepOut :=
  match inst with
  | .pushLeaf f =>
    let (v, err) := f row
    .one err (pc + 1) (v :: stk)
  | .unaryOp f =>
    match stk with
    | x :: rest =>
      let (v, err) := f x
      .one err (pc + 1) (v :: rest)
    | [] => .one true (pc + 1) []
  | .binaryOp f =>
    match stk with
    | r :: l :: rest =>
      let (v, err) := f l r
      .one err (pc + 1) (v :: rest)
    | _ => .one true (pc + 1) stk
  | .popAt idx =>
    .one false (pc + 1) (eraseFromTop stk idx)
  | .dupAt idx =>
    .one false (pc + 1) (dupFromTop stk idx)
  | .jump step =>
    .one false (pc + step + 1) stk
  | .jumpIfFalse step =>
    match stk with
    | top :: rest =>
      let decides := (!top.isNull) && (top.bits == 0#64)
      let normFalse : JitLane := ⟨0#64, false⟩
      .two false decides (pc + step + 1, normFalse :: rest) (pc + 1, top :: rest)
    | [] => .one true (pc + 1) []
  | .jumpIfTrue step =>
    match stk with
    | top :: rest =>
      let decides := (!top.isNull) && (top.bits != 0#64)
      let normTrue : JitLane := ⟨1#64, false⟩
      .two false decides (pc + step + 1, normTrue :: rest) (pc + 1, top :: rest)
    | [] => .one true (pc + 1) []
  | .jumpIfNotNull step =>
    match stk with
    | top :: rest =>
      let decides := !top.isNull
      .two false decides (pc + step + 1, top :: rest) (pc + 1, top :: rest)
    | [] => .one true (pc + 1) []

/-- すべての命令は常に前方 (`pc + 1 ≤ nextPc`) にのみ遷移する（DAG 性の根拠）。 -/
private theorem stepInst_forward {Row : Type} (inst : DagInst Row) (row : Row)
    (pc : Nat) (stk : List JitLane) :
    match stepInst inst row pc stk with
    | .one _ nextPc _ => pc + 1 ≤ nextPc
    | .two _ _ (tPc, _) (fPc, _) => pc + 1 ≤ tPc ∧ pc + 1 ≤ fPc := by
  cases inst with
  | pushLeaf _ => simp [stepInst]
  | popAt _ => simp [stepInst]
  | dupAt _ => simp [stepInst]
  | jump _ => simp [stepInst]
  | unaryOp _ => cases stk <;> simp [stepInst]
  | binaryOp _ => rcases stk with _ | ⟨_, _ | _⟩ <;> simp [stepInst]
  | jumpIfFalse _ => cases stk <;> simp [stepInst]
  | jumpIfTrue _ => cases stk <;> simp [stepInst]
  | jumpIfNotNull _ => cases stk <;> simp [stepInst]

/-- **基準（Reference）逐次短絡 VM**:
    `pc` から実際にジャンプを辿り、到達した命令だけを評価する。未到達分岐のエラーは絶対に踏まない！ -/
def runSeqFrom {Row : Type} (prog : Array (DagInst Row)) (row : Row)
    (pc : Nat) (stk : List JitLane) : Option (List JitLane) :=
  if h : pc < prog.size then
    have hFwd := stepInst_forward prog[pc] row pc stk
    match hOut : stepInst prog[pc] row pc stk with
    | .one err nextPc nextStk =>
      have : pc + 1 ≤ nextPc := by rw [hOut] at hFwd; exact hFwd
      if err then none
      else runSeqFrom prog row nextPc nextStk
    | .two err decides (tPc, tStk) (fPc, fStk) =>
      have hPair : pc + 1 ≤ tPc ∧ pc + 1 ≤ fPc := by rw [hOut] at hFwd; exact hFwd
      if err then none
      else if decides then
        have : pc + 1 ≤ tPc := hPair.1
        runSeqFrom prog row tPc tStk
      else
        have : pc + 1 ≤ fPc := hPair.2
        runSeqFrom prog row fPc fStk
  else
    some stk
termination_by prog.size - pc
decreasing_by
  all_goals simp_wf; omega

/-- Predicated If-Conversion が管理する前方カット辺 `(targetPC, edgeActive, edgeStack)`。 -/
structure CutEdge where
  target : Nat
  cond   : Bool
  stk    : List JitLane

/-- `pc` に到着した辺をブランチレス合流した結果。 -/
structure MergeOut where
  active : Bool
  stk    : List JitLane
  rem    : List CutEdge

/-- `pc` に到着するカット辺群を `selectStack` だけでブランチレス合流させ、残りの辺と分離する。 -/
def mergeIncoming (pc : Nat) (defaultDepth : Nat) : List CutEdge → MergeOut
  | [] => ⟨false, List.replicate defaultDepth ⟨0#64, true⟩, []⟩
  | e :: es =>
    let r := mergeIncoming pc defaultDepth es
    if e.target == pc then
      ⟨e.cond || r.active, selectStack e.cond e.stk r.stk, r.rem⟩
    else
      ⟨r.active, r.stk, e :: r.rem⟩

/-- **積極的最適化（Predicated Single-Block If-Conversion 実行器）**:
    `pc = 0, 1, ..., prog.size - 1` を **一切分岐せず直線的に 1 回ずつすべて実行** し、
    エラーは `anyErr || (active && localErr)` でマスク集約する（`expression/jit.cpp` の生成 IR と 1 対 1 対応！）。 -/
def runPredLoop {Row : Type} (prog : Array (DagInst Row)) (depths : Nat → Nat) (row : Row)
    (pc : Nat) (edges : List CutEdge) (anyErr : Bool) : (Bool × List CutEdge) :=
  if h : pc < prog.size then
    let merged := mergeIncoming pc (depths pc) edges
    match stepInst prog[pc] row pc merged.stk with
    | .one localErr nextPc nextStk =>
      let anyErr' := anyErr || (merged.active && localErr)
      let edges'  := ⟨nextPc, merged.active, nextStk⟩ :: merged.rem
      runPredLoop prog depths row (pc + 1) edges' anyErr'
    | .two localErr decides (tPc, tStk) (fPc, fStk) =>
      let anyErr' := anyErr || (merged.active && localErr)
      let edges'  := ⟨tPc, merged.active && decides, tStk⟩ ::
                     ⟨fPc, merged.active && !decides, fStk⟩ :: merged.rem
      runPredLoop prog depths row (pc + 1) edges' anyErr'
  else
    (anyErr, edges)
termination_by prog.size - pc
decreasing_by
  all_goals simp_wf; omega

/-- プログラム `prog` が静的スタック深さ整合性（Bytecode Verifier）を満たす条件。 -/
def ValidStackDepths {Row : Type} (prog : Array (DagInst Row)) (depths : Nat → Nat) : Prop :=
  ∀ pc (h : pc < prog.size) (row : Row) (stk : List JitLane),
    stk.length = depths pc →
    match stepInst prog[pc] row pc stk with
    | .one _ nextPc nextStk =>
      nextPc ≤ prog.size ∧ nextStk.length = depths nextPc
    | .two _ _ (tPc, tStk) (fPc, fStk) =>
      tPc ≤ prog.size ∧ tStk.length = depths tPc ∧
      fPc ≤ prog.size ∧ fStk.length = depths fPc

/-- カット不変条件：`cond = true` である辺は **ただ 1 本 `(uTarget, true, uStk)` のみ** 存在する！ -/
inductive UniqueActiveEdge (depths : Nat → Nat) (uTarget : Nat) (uStk : List JitLane) :
    List CutEdge → Prop where
  | here (es : List CutEdge) :
      uStk.length = depths uTarget →
      (∀ e ∈ es, e.cond = false ∧ e.stk.length = depths e.target) →
      UniqueActiveEdge depths uTarget uStk (⟨uTarget, true, uStk⟩ :: es)
  | there (e : CutEdge) (es : List CutEdge) :
      e.cond = false →
      e.stk.length = depths e.target →
      UniqueActiveEdge depths uTarget uStk es →
      UniqueActiveEdge depths uTarget uStk (e :: es)

private theorem uniqueActiveEdge_len {depths : Nat → Nat} {uTarget : Nat} {uStk : List JitLane}
    {es : List CutEdge} (h : UniqueActiveEdge depths uTarget uStk es) :
    uStk.length = depths uTarget := by
  induction h with
  | here _ hL _ => exact hL
  | there _ _ _ _ _ ih => exact ih

private theorem mergeIncoming_all_false (pc d : Nat) :
    ∀ (es : List CutEdge),
      (∀ e ∈ es, e.cond = false ∧ (e.target = pc → e.stk.length = d)) →
      (mergeIncoming pc d es).active = false ∧
      (mergeIncoming pc d es).stk.length = d ∧
      (∀ e ∈ (mergeIncoming pc d es).rem, e.cond = false ∧ e ∈ es ∧ e.target ≠ pc)
  | [], _hAll => by simp [mergeIncoming]
  | e :: es, hAll => by
    have hHead := hAll e (by simp)
    have ih := mergeIncoming_all_false pc d es (fun x hx => hAll x (by simp [hx]))
    rcases ih with ⟨ihAct, ihLen, ihRem⟩
    rcases heq : (e.target == pc) with _ | _
    · simp only [mergeIncoming, heq, Bool.false_eq_true, ↓reduceIte]
      refine ⟨ihAct, ihLen, ?_⟩
      intro x hx
      simp only [List.mem_cons] at hx
      rcases hx with rfl | hx
      · exact ⟨hHead.1, by simp, by intro hc; subst hc; simp at heq⟩
      · rcases ihRem x hx with ⟨hc, hm, hne⟩
        exact ⟨hc, by simp [hm], hne⟩
    · have hTargetEq : e.target = pc := beq_iff_eq.mp heq
      have hELen : e.stk.length = d := hHead.2 hTargetEq
      simp only [mergeIncoming, heq, ↓reduceIte, hHead.1, ihAct, Bool.or_self]
      refine ⟨trivial, ?_, ?_⟩
      · rw [selectStack_false e.stk _ (by omega)]
        exact ihLen
      · intro x hx
        rcases ihRem x hx with ⟨hc, hm, hne⟩
        exact ⟨hc, by simp [hm], hne⟩

private theorem mergeIncoming_unique_hit (depths : Nat → Nat) (pc : Nat) (uStk : List JitLane) :
    ∀ (es : List CutEdge),
      UniqueActiveEdge depths pc uStk es →
      (mergeIncoming pc (depths pc) es).active = true ∧
      (mergeIncoming pc (depths pc) es).stk = uStk ∧
      (∀ e ∈ (mergeIncoming pc (depths pc) es).rem,
        e.cond = false ∧ e.stk.length = depths e.target ∧ e.target ≠ pc) := by
  intro es hUniq
  induction hUniq with
  | here tail hULen hTail =>
    have hSub := mergeIncoming_all_false pc (depths pc) tail
      (fun x hx => ⟨(hTail x hx).1, fun hEq => by rw [← hEq]; exact (hTail x hx).2⟩)
    rcases hSub with ⟨_, hSubLen, hSubRem⟩
    simp only [mergeIncoming, beq_self_eq_true, ↓reduceIte, Bool.true_or]
    refine ⟨trivial, selectStack_true uStk _ (by omega), ?_⟩
    intro x hx
    rcases hSubRem x hx with ⟨hc, hm, hne⟩
    exact ⟨hc, (hTail x hm).2, hne⟩
  | there e tail hFalse hELen hUniqTail ih =>
    rcases ih with ⟨ihAct, ihStk, ihRem⟩
    rcases heq : (e.target == pc) with _ | _
    · simp only [mergeIncoming, heq, Bool.false_eq_true, ↓reduceIte]
      refine ⟨ihAct, ihStk, ?_⟩
      intro x hx
      simp only [List.mem_cons] at hx
      rcases hx with rfl | hx
      · exact ⟨hFalse, hELen, by intro hc; subst hc; simp at heq⟩
      · exact ihRem x hx
    · have hTargetEq : e.target = pc := beq_iff_eq.mp heq
      subst hTargetEq
      have hULen : uStk.length = depths e.target := uniqueActiveEdge_len hUniqTail
      simp only [mergeIncoming, beq_self_eq_true, ↓reduceIte, hFalse, Bool.false_or, ihStk]
      exact ⟨ihAct, selectStack_false e.stk uStk (by omega), ihRem⟩

private theorem mergeIncoming_unique_miss (depths : Nat → Nat) (pc uTarget : Nat)
    (uStk : List JitLane) (hNe : uTarget ≠ pc) :
    ∀ (es : List CutEdge),
      UniqueActiveEdge depths uTarget uStk es →
      (mergeIncoming pc (depths pc) es).active = false ∧
      (mergeIncoming pc (depths pc) es).stk.length = depths pc ∧
      UniqueActiveEdge depths uTarget uStk (mergeIncoming pc (depths pc) es).rem := by
  intro es hUniq
  induction hUniq with
  | here tail hULen hTail =>
    have hBeq : (uTarget == pc) = false := beq_false_of_ne hNe
    have hSub := mergeIncoming_all_false pc (depths pc) tail
      (fun x hx => ⟨(hTail x hx).1, fun hEq => by rw [← hEq]; exact (hTail x hx).2⟩)
    rcases hSub with ⟨hSubAct, hSubLen, hSubRem⟩
    simp only [mergeIncoming, hBeq, Bool.false_eq_true, ↓reduceIte]
    refine ⟨hSubAct, hSubLen, UniqueActiveEdge.here _ hULen ?_⟩
    intro x hx
    rcases hSubRem x hx with ⟨hc, hm, _⟩
    exact ⟨hc, (hTail x hm).2⟩
  | there e tail hFalse hELen _ ih =>
    rcases ih with ⟨ihAct, ihLen, ihUniq⟩
    rcases heq : (e.target == pc) with _ | _
    · simp only [mergeIncoming, heq, Bool.false_eq_true, ↓reduceIte]
      exact ⟨ihAct, ihLen, UniqueActiveEdge.there e _ hFalse hELen ihUniq⟩
    · have hTargetEq : e.target = pc := beq_iff_eq.mp heq
      have hELen' : e.stk.length = depths pc := by rw [← hTargetEq]; exact hELen
      simp only [mergeIncoming, heq, ↓reduceIte, hFalse, Bool.false_or]
      exact ⟨ihAct, by rw [selectStack_false e.stk _ (by omega)]; exact ihLen, ihUniq⟩

/-- エラーが一度 `true` になった後は、`runPredLoop` を最後まで回しても `anyErr = true` が絶対に消えない単調性補題。 -/
private theorem runPredLoop_err_monotone {Row : Type} (prog : Array (DagInst Row))
    (depths : Nat → Nat) (row : Row) (pc : Nat) (edges : List CutEdge) :
    (runPredLoop prog depths row pc edges true).1 = true := by
  if h : pc < prog.size then
    unfold runPredLoop
    simp only [h, ↓reduceDIte]
    rcases stepInst prog[pc] row pc (mergeIncoming pc (depths pc) edges).stk with
      ⟨localErr, nextPc, nextStk⟩ | ⟨localErr, decides, ⟨tPc, tStk⟩, ⟨fPc, fStk⟩⟩
    · simp only [Bool.true_or]
      exact runPredLoop_err_monotone prog depths row (pc + 1) _
    · simp only [Bool.true_or]
      exact runPredLoop_err_monotone prog depths row (pc + 1) _
  else
    unfold runPredLoop
    simp [h]
termination_by prog.size - pc
decreasing_by
  all_goals simp_wf; omega

/-- カット主補題: `pc ≤ uTarget ≤ prog.size` かつ `UniqueActiveEdge` が成り立つ状態から `runPredLoop` を回すと、
    `runSeqFrom prog row uTarget uStk` と **エラー有無・最終合流スタックが完全一致** する！ -/
private theorem runPredLoop_simulates_seq {Row : Type} (prog : Array (DagInst Row))
    (depths : Nat → Nat) (hValid : ValidStackDepths prog depths) (row : Row)
    (pc uTarget : Nat) (uStk : List JitLane) (edges : List CutEdge)
    (hOrder : pc ≤ uTarget ∧ uTarget ≤ prog.size)
    (hUniq : UniqueActiveEdge depths uTarget uStk edges) :
    let (errOut, finalEdges) := runPredLoop prog depths row pc edges false
    match runSeqFrom prog row uTarget uStk with
    | none => errOut = true
    | some finalStk =>
      errOut = false ∧
      (mergeIncoming prog.size (depths prog.size) finalEdges).active = true ∧
      (mergeIncoming prog.size (depths prog.size) finalEdges).stk = finalStk := by
  if h : pc < prog.size then
    by_cases hHit : uTarget = pc
    · subst hHit
      rcases mergeIncoming_unique_hit depths uTarget uStk edges hUniq with
        ⟨hAct, hStkEq, hRemFalse⟩
      have hULen : uStk.length = depths uTarget := uniqueActiveEdge_len hUniq
      have hV := hValid uTarget h row uStk hULen
      have hFwd := stepInst_forward prog[uTarget] row uTarget uStk
      unfold runPredLoop
      simp only [h, ↓reduceDIte, hAct, hStkEq, Bool.true_and, Bool.false_or]
      unfold runSeqFrom
      simp only [h, ↓reduceDIte]
      revert hV hFwd
      rcases hStep : stepInst prog[uTarget] row uTarget uStk with
        ⟨localErr, nextPc, nextStk⟩ | ⟨localErr, decides, ⟨tPc, tStk⟩, ⟨fPc, fStk⟩⟩
      · intro hV hFwd
        cases localErr
        · simp only [Bool.false_eq_true, ↓reduceIte]
          have hNewUniq : UniqueActiveEdge depths nextPc nextStk
              (⟨nextPc, true, nextStk⟩ :: (mergeIncoming uTarget (depths uTarget) edges).rem) :=
            UniqueActiveEdge.here _ hV.2 (fun e he => ⟨(hRemFalse e he).1, (hRemFalse e he).2.1⟩)
          exact runPredLoop_simulates_seq prog depths hValid row (uTarget + 1) nextPc nextStk _
            ⟨hFwd, hV.1⟩ hNewUniq
        · simp only [↓reduceIte]
          exact runPredLoop_err_monotone prog depths row (uTarget + 1) _
      · intro hV hFwd
        cases localErr
        · cases decides
          · simp only [Bool.not_false, Bool.false_eq_true, ↓reduceIte]
            have hNewUniq : UniqueActiveEdge depths fPc fStk
                (⟨tPc, false, tStk⟩ :: ⟨fPc, true, fStk⟩ ::
                  (mergeIncoming uTarget (depths uTarget) edges).rem) :=
              UniqueActiveEdge.there _ _ rfl hV.2.1
                (UniqueActiveEdge.here _ hV.2.2.2 (fun e he => ⟨(hRemFalse e he).1, (hRemFalse e he).2.1⟩))
            exact runPredLoop_simulates_seq prog depths hValid row (uTarget + 1) fPc fStk _
              ⟨hFwd.2, hV.2.2.1⟩ hNewUniq
          · simp only [Bool.not_true, Bool.false_eq_true, ↓reduceIte]
            have hNewUniq : UniqueActiveEdge depths tPc tStk
                (⟨tPc, true, tStk⟩ :: ⟨fPc, false, fStk⟩ ::
                  (mergeIncoming uTarget (depths uTarget) edges).rem) :=
              UniqueActiveEdge.here _ hV.2.1 (fun e he => by
                simp only [List.mem_cons] at he
                rcases he with rfl | he
                · exact ⟨rfl, hV.2.2.2⟩
                · exact ⟨(hRemFalse e he).1, (hRemFalse e he).2.1⟩)
            exact runPredLoop_simulates_seq prog depths hValid row (uTarget + 1) tPc tStk _
              ⟨hFwd.1, hV.1⟩ hNewUniq
        · simp only [↓reduceIte]
          exact runPredLoop_err_monotone prog depths row (uTarget + 1) _
    · -- `pc < uTarget`: `pc` はスキップされたデッド領域！局所エラーが起きてもマスクされ無視される！
      have hNe : uTarget ≠ pc := hHit
      rcases mergeIncoming_unique_miss depths pc uTarget uStk hNe edges hUniq with
        ⟨hAct, hStkLen, hRemUniq⟩
      have hV := hValid pc h row (mergeIncoming pc (depths pc) edges).stk hStkLen
      unfold runPredLoop
      simp only [h, ↓reduceDIte]
      revert hV
      rcases hStep : stepInst prog[pc] row pc (mergeIncoming pc (depths pc) edges).stk with
        ⟨localErr, nextPc, nextStk⟩ | ⟨localErr, decides, ⟨tPc, tStk⟩, ⟨fPc, fStk⟩⟩
      · intro hV
        simp only [hAct, Bool.false_and, Bool.or_self]
        have hNextUniq : UniqueActiveEdge depths uTarget uStk
            (⟨nextPc, false, nextStk⟩ :: (mergeIncoming pc (depths pc) edges).rem) :=
          UniqueActiveEdge.there _ _ rfl hV.2 hRemUniq
        exact runPredLoop_simulates_seq prog depths hValid row (pc + 1) uTarget uStk _
          ⟨by omega, hOrder.2⟩ hNextUniq
      · intro hV
        simp only [hAct, Bool.false_and, Bool.or_self]
        have hNextUniq : UniqueActiveEdge depths uTarget uStk
            (⟨tPc, false, tStk⟩ :: ⟨fPc, false, fStk⟩ :: (mergeIncoming pc (depths pc) edges).rem) :=
          UniqueActiveEdge.there _ _ rfl hV.2.1
            (UniqueActiveEdge.there _ _ rfl hV.2.2.2 hRemUniq)
        exact runPredLoop_simulates_seq prog depths hValid row (pc + 1) uTarget uStk _
          ⟨by omega, hOrder.2⟩ hNextUniq
  else
    have hEq : uTarget = prog.size := by omega
    have hEnd : pc = prog.size := by omega
    subst hEq; subst hEnd
    unfold runPredLoop
    simp only [Nat.lt_irrefl, ↓reduceDIte]
    unfold runSeqFrom
    simp only [Nat.lt_irrefl, ↓reduceDIte]
    rcases mergeIncoming_unique_hit depths prog.size uStk edges hUniq with ⟨hActEnd, hStkEnd, _⟩
    exact ⟨trivial, hActEnd, hStkEnd⟩
termination_by prog.size - pc
decreasing_by
  all_goals simp_wf; omega

/-- 単一ブロック Predicated JIT エントリポイント（初期カット `[(0, true, [])]` から開始）。 -/
def runPredicated {Row : Type} (prog : Array (DagInst Row)) (depths : Nat → Nat) (row : Row) :
    Bool × List JitLane :=
  let (err, finalEdges) := runPredLoop prog depths row 0 [⟨0, true, []⟩] false
  let mergedEnd         := mergeIncoming prog.size (depths prog.size) finalEdges
  (err, mergedEnd.stk)

/-- ✅ **主定理 3 (`if_conversion_sound_and_complete`)**:
    静的スタック深さ検査を通過した **任意の前方分岐 Bytecode プログラム `prog`**（`AND / OR / CASE / COALESCE / IN` の
    あらゆる入れ子を含む！）に対して、ループ内分岐ゼロの `runPredicated` は逐次短絡 VM `runSeqFrom` と：
    1. エラー発生時は **必ず `err = true` を返す（エラー見逃しゼロ）**
    2. 正常終了時は **デッド分岐の偽エラーを一切発火せず `err = false` を返し、最終出力スタックが完全一致する！** -/
theorem if_conversion_sound_and_complete {Row : Type}
    (prog : Array (DagInst Row)) (depths : Nat → Nat)
    (hEntry : depths 0 = 0)
    (hValid : ValidStackDepths prog depths) (row : Row) :
    let (predErr, predStk) := runPredicated prog depths row
    match runSeqFrom prog row 0 [] with
    | none          => predErr = true
    | some finalStk => predErr = false ∧ predStk = finalStk := by
  unfold runPredicated
  have hInit : UniqueActiveEdge depths 0 [] [⟨0, true, []⟩] :=
    UniqueActiveEdge.here [] (by simp [hEntry]) (by intro e he; simp at he)
  have hSim := runPredLoop_simulates_seq prog depths hValid row 0 0 [] [⟨0, true, []⟩]
    ⟨Nat.le_refl 0, Nat.zero_le _⟩ hInit
  rcases hSeq : runSeqFrom prog row 0 [] with _ | finalStk
  · rw [hSeq] at hSim; exact hSim
  · rw [hSeq] at hSim
    exact ⟨hSim.1, hSim.2.2⟩

/-- ✅ **具体検証デモ (`short_circuit_error_suppression_demo`)**:
    `FALSE AND <overflowing_rhs>` において、右辺が `localErr = true` を出していても
    `runPredicated` は `active = false` でそれを完全にマスクし、`(false, [FALSE])` を返す！ -/
theorem short_circuit_error_suppression_demo :
    let prog : Array (DagInst Unit) := #[
      .pushLeaf (fun _ => (⟨0#64, false⟩, false)), -- PC 0: FALSE
      .jumpIfFalse 2,                               -- PC 1 -> PC 4 (右辺スキップ！)
      .pushLeaf (fun _ => (⟨0#64, false⟩, true)),  -- PC 2: オーバーフローする右辺！
      .binaryOp (fun l r => (jitAnd l r, false))    -- PC 3
    ]
    let depths := fun pc => if pc == 0 then 0 else if pc == 3 then 2 else 1
    runSeqFrom prog () 0 [] = some [⟨0#64, false⟩] ∧
    runPredicated prog depths () = (false, [⟨0#64, false⟩]) := by
  dsimp only
  let demoProg : Array (DagInst Unit) := #[
    DagInst.pushLeaf (fun _ => (⟨0#64, false⟩, false)),
    DagInst.jumpIfFalse 2,
    DagInst.pushLeaf (fun _ => (⟨0#64, false⟩, true)),
    DagInst.binaryOp (fun l r => (jitAnd l r, false))
  ]
  let demoDepths := fun pc => if pc == 0 then 0 else if pc == 3 then 2 else 1
  have hValid : ValidStackDepths demoProg demoDepths := by
    intro pc h _ stk hLen
    have : pc = 0 ∨ pc = 1 ∨ pc = 2 ∨ pc = 3 := by simp [demoProg] at h; omega
    rcases this with rfl | rfl | rfl | rfl
    · simp [demoProg, demoDepths, stepInst] at hLen ⊢; omega
    · rcases stk with _ | ⟨x, _ | _⟩ <;> simp [demoDepths] at hLen
      simp [demoProg, demoDepths, stepInst]
    · rcases stk with _ | ⟨x, _ | _⟩ <;> simp [demoDepths] at hLen
      simp [demoProg, demoDepths, stepInst]
    · rcases stk with _ | ⟨x, _ | ⟨y, _ | _⟩⟩ <;> simp [demoDepths] at hLen
      simp [demoProg, demoDepths, stepInst]
  have hSeq : runSeqFrom demoProg () 0 [] = some [⟨0#64, false⟩] := by
    unfold runSeqFrom; simp [demoProg, stepInst]
    unfold runSeqFrom; simp [stepInst]
    unfold runSeqFrom; simp
  have hMain := if_conversion_sound_and_complete demoProg demoDepths rfl hValid ()
  rw [hSeq] at hMain
  refine ⟨hSeq, ?_⟩
  rcases hPred : runPredicated demoProg demoDepths () with ⟨e, s⟩
  rw [hPred] at hMain
  simp [hMain.1, hMain.2]


/-!
## 4. ZoneMap 区間解析によるオーバーフロー検査除去 & NULL ゼロ置換 SIMD `SUM` の証明

`llvm.smul.with.overflow.i64` や逐次 `sadd.with.overflow.i64` は x86-64 AVX2 でベクトル化を阻害します。
バッチごとに既知の `DataChunk::ZoneMapAt(col)` `[lo, hi]` を使って **`O(1)` でオーバーフロー不能性を証明** し、
純粋な `nsw` SIMD カーネル（`vpaddq` / `vpmullq`）へ昇格させる規則を機械証明します。
-/

def i64MinInt : Int := -(2 ^ 63)
def i64MaxInt : Int := (2 ^ 63) - 1

abbrev InI64 (x : Int) : Prop := i64MinInt ≤ x ∧ x ≤ i64MaxInt

private theorem mul_le_mul_of_nonpos_right' {a b c : Int} (hab : a ≤ b) (hc : c ≤ 0) :
    b * c ≤ a * c := by
  have h1 : 0 ≤ -c := by omega
  have h2 : a * (-c) ≤ b * (-c) := Int.mul_le_mul_of_nonneg_right hab h1
  simp only [Int.mul_neg] at h2
  omega

private theorem mul_le_mul_of_nonpos_left' {a b c : Int} (hab : a ≤ b) (hc : c ≤ 0) :
    c * b ≤ c * a := by
  have h1 : 0 ≤ -c := by omega
  have h2 : (-c) * a ≤ (-c) * b := Int.mul_le_mul_of_nonneg_left hab h1
  simp only [Int.neg_mul] at h2
  omega

/-- ✅ **定理 4-A (`zonemap_interval_mul_4corners_sound`)**:
    `x ∈ [aLo, aHi]`, `y ∈ [bLo, bHi]` のとき、4 端点積
    `{aLo*bLo, aLo*bHi, aHi*bLo, aHi*bHi}` が `[mn, mx]` に収まり `InI64 mn ∧ InI64 mx` ならば、
    **バッチ内のあらゆる組 `(x, y)` で符号付き 64-bit 乗算 `x * y` は絶対にオーバーフローしない！** -/
theorem zonemap_interval_mul_4corners_sound
    (aLo aHi bLo bHi mn mx x y : Int)
    (hx : aLo ≤ x ∧ x ≤ aHi) (hy : bLo ≤ y ∧ y ≤ bHi)
    (hCornersLo : mn ≤ aLo * bLo ∧ mn ≤ aLo * bHi ∧ mn ≤ aHi * bLo ∧ mn ≤ aHi * bHi)
    (hCornersHi : aLo * bLo ≤ mx ∧ aLo * bHi ≤ mx ∧ aHi * bLo ≤ mx ∧ aHi * bHi ≤ mx)
    (hFits : InI64 mn ∧ InI64 mx) :
    mn ≤ x * y ∧ x * y ≤ mx ∧ InI64 (x * y) := by
  have hBounds : mn ≤ x * y ∧ x * y ≤ mx := by
    by_cases hy0 : 0 ≤ y
    · have h1 : aLo * y ≤ x * y := Int.mul_le_mul_of_nonneg_right hx.1 hy0
      have h2 : x * y ≤ aHi * y := Int.mul_le_mul_of_nonneg_right hx.2 hy0
      have hLoBound : mn ≤ aLo * y := by
        by_cases ha0 : 0 ≤ aLo
        · have := Int.mul_le_mul_of_nonneg_left hy.1 ha0; omega
        · have := mul_le_mul_of_nonpos_left' hy.2 (by omega : aLo ≤ 0); omega
      have hHiBound : aHi * y ≤ mx := by
        by_cases ha1 : 0 ≤ aHi
        · have := Int.mul_le_mul_of_nonneg_left hy.2 ha1; omega
        · have := mul_le_mul_of_nonpos_left' hy.1 (by omega : aHi ≤ 0); omega
      omega
    · have hyNeg : y ≤ 0 := by omega
      have h1 : aHi * y ≤ x * y := mul_le_mul_of_nonpos_right' hx.2 hyNeg
      have h2 : x * y ≤ aLo * y := mul_le_mul_of_nonpos_right' hx.1 hyNeg
      have hLoBound : mn ≤ aHi * y := by
        by_cases ha1 : 0 ≤ aHi
        · have := Int.mul_le_mul_of_nonneg_left hy.1 ha1; omega
        · have := mul_le_mul_of_nonpos_left' hy.2 (by omega : aHi ≤ 0); omega
      have hHiBound : aLo * y ≤ mx := by
        by_cases ha0 : 0 ≤ aLo
        · have := Int.mul_le_mul_of_nonneg_left hy.2 ha0; omega
        · have := mul_le_mul_of_nonpos_left' hy.1 (by omega : aLo ≤ 0); omega
      omega
  exact ⟨hBounds.1, hBounds.2, ⟨by unfold InI64 at hFits; omega, by unfold InI64 at hFits; omega⟩⟩

/-- ✅ **定理 4-B (`zonemap_interval_add_sub_abs_neg_sound`)**:
    加算・減算・単項マイナス・`ABS` の区間伝播が `InI64` に収まれば、
    バッチ内の全要素で 64-bit オーバーフローが絶対に発生しない。 -/
theorem zonemap_interval_add_sub_abs_neg_sound
    (aLo aHi bLo bHi x y : Int)
    (hx : aLo ≤ x ∧ x ≤ aHi) (hy : bLo ≤ y ∧ y ≤ bHi) :
    (InI64 (aLo + bLo) ∧ InI64 (aHi + bHi) → InI64 (x + y)) ∧
    (InI64 (aLo - bHi) ∧ InI64 (aHi - bLo) → InI64 (x - y)) ∧
    (i64MinInt < aLo ∧ InI64 aLo ∧ InI64 aHi → InI64 (-x) ∧ InI64 (if x < 0 then -x else x)) := by
  unfold InI64 i64MinInt i64MaxInt
  refine ⟨by omega, by omega, ?_⟩
  intro _
  omega

/-- 基準（Reference）の逐次オーバーフロー検査付き NULL スキップ `SUM` アキュムレータ
    各行を加算するたびに InI64 を検査する参照モデル。C++ は安全性を示せるバッチをまとめて処理し、必要な場合に逐次の検査に戻る。 -/
def foldCheckedSum : Int → List (Option Int) → Option Int
  | acc, [] => some acc
  | acc, none :: xs => foldCheckedSum acc xs
  | acc, some v :: xs =>
    let next := acc + v
    if i64MinInt ≤ next ∧ next ≤ i64MaxInt then
      foldCheckedSum next xs
    else
      none

/-- SUM after the join sees only key 2. Eagerly computing the key-1 partial
    instead raises an overflow, even with a unique key on the opposite side. -/
theorem eager_sum_unmatched_key_overflow :
    let fact : List (Nat × Int) := [(1, i64MaxInt), (1, 1), (2, 7)]
    let groupValues := fun k => (fact.filter (fun r => r.1 == k)).map (fun r => some r.2)
    let aggregate := fun k => (foldCheckedSum 0 (groupValues k)).map (k, ·)
    let matched := fun (row : Nat × Int) => row.1 == 2
    ([2] : List Nat).mapM aggregate = some [(2, 7)] ∧
      (([1, 2] : List Nat).mapM aggregate).map (List.filter matched) = none ∧
      ([1, 2] : List Nat).mapM aggregate = none := by decide

/-- ブランチレス SIMD `SUM` カーネル：NULL スロットを `0` にマスク置換して単純総和する。 -/
def simdZeroMaskedSum (acc : Int) (xs : List (Option Int)) : Int :=
  xs.foldl (fun s o => s + o.getD 0) acc

/-- ✅ **定理 4-C (`zonemap_prefix_sum_masked_sound`)**:
    バッチ `xs`（行数 `xs.length`）の全非 NULL 値が ZoneMap `[lo, hi]` に入り、かつ初期累積値 `acc` に対し
    包絡線条件 `InI64 acc ∧ i64MinInt ≤ acc + xs.length * min 0 lo ∧ acc + xs.length * max 0 hi ≤ i64MaxInt`
    が成り立つならば：
    1. 逐次検査 `foldCheckedSum acc xs` は **`[MAX, 5, -5]` のような途中オーバーフローも絶対に起こさず成功** し、
    2. その結果はブランチレス・オーバーフロー検査なしの `simdZeroMaskedSum acc xs` と **完全一致** する！ -/
theorem zonemap_prefix_sum_masked_sound
    (lo hi : Int) :
    ∀ (xs : List (Option Int)) (acc : Int),
      (∀ o ∈ xs, ∀ v, o = some v → lo ≤ v ∧ v ≤ hi) →
      InI64 acc →
      i64MinInt ≤ acc + (xs.length : Int) * min 0 lo →
      acc + (xs.length : Int) * max 0 hi ≤ i64MaxInt →
      foldCheckedSum acc xs = some (simdZeroMaskedSum acc xs)
  | [], acc, _, _, _, _ => rfl
  | none :: xs, acc, hRange, hAcc, hEnvLo, hEnvHi => by
    simp only [List.length_cons, Int.natCast_add, Int.natCast_one, Int.add_mul, Int.one_mul] at hEnvLo hEnvHi
    have hMinNonPos : min 0 lo ≤ 0 := Int.min_le_left 0 lo
    have hMaxNonNeg : 0 ≤ max 0 hi := Int.le_max_left 0 hi
    have hSubLo : i64MinInt ≤ acc + (xs.length : Int) * min 0 lo := by omega
    have hSubHi : acc + (xs.length : Int) * max 0 hi ≤ i64MaxInt := by omega
    simpa [foldCheckedSum, simdZeroMaskedSum] using
      zonemap_prefix_sum_masked_sound lo hi xs acc
        (fun o ho v hv => hRange o (by simp [ho]) v hv) hAcc hSubLo hSubHi
  | some v :: xs, acc, hRange, hAcc, hEnvLo, hEnvHi => by
    simp only [List.length_cons, Int.natCast_add, Int.natCast_one, Int.add_mul, Int.one_mul] at hEnvLo hEnvHi
    have hv : lo ≤ v ∧ v ≤ hi := hRange (some v) (by simp) v rfl
    have hvLo : min 0 lo ≤ v := Int.le_trans (Int.min_le_right 0 lo) hv.1
    have hvHi : v ≤ max 0 hi := Int.le_trans hv.2 (Int.le_max_right 0 hi)
    have hMinNonPos : min 0 lo ≤ 0 := Int.min_le_left 0 lo
    have hMaxNonNeg : 0 ≤ max 0 hi := Int.le_max_left 0 hi
    have hNextLo : i64MinInt ≤ (acc + v) + (xs.length : Int) * min 0 lo := by omega
    have hNextHi : (acc + v) + (xs.length : Int) * max 0 hi ≤ i64MaxInt := by omega
    have hTailLoNonPos : (xs.length : Int) * min 0 lo ≤ 0 :=
      Int.mul_nonpos_of_nonneg_of_nonpos (Int.natCast_nonneg xs.length) hMinNonPos
    have hTailHiNonNeg : 0 ≤ (xs.length : Int) * max 0 hi :=
      Int.mul_nonneg (Int.natCast_nonneg xs.length) hMaxNonNeg
    have hNextBounds : i64MinInt ≤ acc + v ∧ acc + v ≤ i64MaxInt := ⟨by omega, by omega⟩
    simp only [foldCheckedSum, hNextBounds, and_self, ↓reduceIte, simdZeroMaskedSum,
               List.foldl_cons, Option.getD_some]
    exact zonemap_prefix_sum_masked_sound lo hi xs (acc + v)
      (fun o ho w hw => hRange o (by simp [ho]) w hw) hNextBounds hNextLo hNextHi

end TinylambOpt
