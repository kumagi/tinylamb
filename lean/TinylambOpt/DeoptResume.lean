import Std.Tactic

/-
# Deopt 後のスカラ再開 (deopt resume)

バッチ実行の途中でガード（INT64 加算オーバーフロー）が失敗したとき、
コミット済みの接頭辞状態と保存したカーソルからスカラ経路を再開すると、
最初から参照経路を走らせた結果（エラーを含む）と一致することを証明する。

## モデルと前提

* 行は `Option Int`（`none` は NULL）。状態 `State` は
  `output`（確定済み出力行）、`sum`（検査付き SUM）、`count`（件数）、
  `nullSeen`（NULL フラグ）の 4 つを明示的に持つ。
* `stepRaw` は 1 行の投機実行で、ガード失敗時も `output` / `count` の
  部分書き込みを残す（`ok = false`）。`step` はスナップショットに
  巻き戻して失敗を `none` として報告する。
* 参照 `refRun` は先頭から順に `step` を適用する逐次経路。
* 最適化経路 `batchExecute` は、成功した行だけをコミットし、
  最初の失敗行で停止して `cursor = 最後に成功した行の次` を保存する。
  `resumeScalar` はそこから `rows.drop cursor` を参照経路で再開する。
* 主定理は「再開結果 = 参照全走結果」で、`none` はエラーを表すので
  エラーの一致も含まれる。カーソルをバッチ先頭 (0) に戻すと
  確定済み接頭辞を二重に処理してしまう（下の境界例）。

C++ 側の対応: JIT バッチのガード失敗時に deopt し、確定済みアキュムレータと
カーソルを保持したままスカラ経路へ戻る処理を想定。
-/

namespace TinylambOpt.DeoptResume

def i64Min : Int := -9223372036854775808
def i64Max : Int :=  9223372036854775807

def inI64 (z : Int) : Prop := i64Min ≤ z ∧ z ≤ i64Max

instance (z : Int) : Decidable (inI64 z) := by
  unfold inI64
  infer_instance

/-- 確定済みの実行状態。出力・SUM・件数・NULL フラグを明示する。 -/
structure State where
  output : List (Option Int)
  sum : Option Int
  count : Nat
  nullSeen : Bool
  deriving DecidableEq, Repr

/-- 部分書き込みを巻き戻すためのスナップショット。出力は長さだけ覚える。 -/
structure Snapshot where
  outputLen : Nat
  sum : Option Int
  count : Nat
  nullSeen : Bool
  deriving DecidableEq, Repr

def save (st : State) : Snapshot :=
  ⟨st.output.length, st.sum, st.count, st.nullSeen⟩

def restore (st : State) (snap : Snapshot) : State :=
  { st with
    output := st.output.take snap.outputLen
    sum := snap.sum
    count := snap.count
    nullSeen := snap.nullSeen }

theorem restore_save (st : State) : restore st (save st) = st := by
  simp [restore, save]

/-! ## 1 行の処理と部分書き込みの退避・復元 -/

/-- 投機的な 1 行実行。ガード失敗 (`ok = false`) でも書きかけの状態を返す。 -/
def stepRaw (st : State) (row : Option Int) : State × Bool :=
  match row with
  | none =>
      ({ st with
         output := st.output ++ [none]
         count := st.count + 1
         nullSeen := true }, true)
  | some v =>
      let written := { st with output := st.output ++ [some v], count := st.count + 1 }
      match st.sum with
      | none => (written, false)
      | some s =>
          let next := s + v
          if inI64 next then ({ written with sum := some next }, true) else (written, false)

/-- ガード付き 1 行実行。失敗ならスナップショットへ戻して `none`。 -/
def step (st : State) (row : Option Int) : Option State :=
  let _snap := save st
  let (part, ok) := stepRaw st row
  if ok then some part else none

theorem step_eq (st : State) (row : Option Int) :
    step st row = if (stepRaw st row).2 = true then some (stepRaw st row).1 else none := by
  rcases hraw : stepRaw st row with ⟨part, ok⟩
  simp [step, hraw]

/-- 明示的に退避・復元する経路。失敗なら元の状態に戻る。 -/
def stepRolled (st : State) (row : Option Int) : State :=
  let snap := save st
  let (part, ok) := stepRaw st row
  if ok then part else restore st snap

theorem stepRolled_eq_of_ok (st : State) (row : Option Int)
    (h : (stepRaw st row).2 = true) :
    stepRolled st row = (stepRaw st row).1 := by
  rcases hraw : stepRaw st row with ⟨part, ok⟩
  have hok : ok = true := by simpa [hraw] using h
  simp [stepRolled, hraw, hok]

theorem stepRolled_eq_of_failed (st : State) (row : Option Int)
    (h : (stepRaw st row).2 = false) :
    stepRolled st row = st := by
  rcases hraw : stepRaw st row with ⟨part, ok⟩
  have hok : ok = false := by simpa [hraw] using h
  simp [stepRolled, hraw, hok, restore_save]

theorem step_eq_stepRolled_on_ok (st : State) (row : Option Int)
    (h : (stepRaw st row).2 = true) :
    step st row = some (stepRolled st row) := by
  rw [step_eq, stepRolled_eq_of_ok st row h]
  simp [h]

/-- ガード失敗時に `output` には部分書き込みが残っている。 -/
theorem stepRaw_failure_partial_write (st : State) (row : Option Int)
    (h : (stepRaw st row).2 = false) :
    (stepRaw st row).1.output = st.output ++ [row] := by
  cases row with
  | none => simp [stepRaw]
  | some v =>
      cases hsum : st.sum with
      | none => simp [stepRaw, hsum]
      | some s =>
          by_cases hg : inI64 (s + v)
          · simp [stepRaw, hsum, hg] at h
          · simp [stepRaw, hsum, hg]

/-- 部分書き込みをスナップショットで巻き戻せば元の状態に戻る。 -/
theorem restore_discards_partial_write (st : State) (row : Option Int) :
    restore (stepRaw st row).1 (save st) = st := by
  cases st with
  | mk output sum count nullSeen =>
      cases row with
      | none =>
          simp [stepRaw, restore, save]
      | some v =>
          cases sum with
          | none =>
              simp [stepRaw, restore, save]
          | some s =>
              by_cases hg : inI64 (s + v) <;> simp [stepRaw, restore, save, hg]

/-! ## 参照経路とバッチ経路 -/

/-- 参照: バッチ先頭から順にガード付き 1 行実行を適用する。 -/
def refRun : List (Option Int) → State → Option State
  | [], st => some st
  | row :: rest, st => match step st row with
      | none => none
      | some st' => refRun rest st'

/-- バッチ実行の結果。`cursor` は次に処理すべき行位置（最後に成功した行の次）。 -/
structure BatchOutcome where
  committed : State
  cursor : Nat
  failed : Bool
  deriving DecidableEq, Repr

def batchExecuteAux (rows : List (Option Int)) (st : State) (cursor : Nat) : BatchOutcome :=
  match rows with
  | [] => ⟨st, cursor, false⟩
  | row :: rest =>
      match step st row with
      | none => ⟨st, cursor, true⟩
      | some st' => batchExecuteAux rest st' (cursor + 1)

def batchExecute (rows : List (Option Int)) (st : State) : BatchOutcome :=
  batchExecuteAux rows st 0

/-- 保存したカーソルからスカラ経路を再開する。 -/
def resumeScalar (rows : List (Option Int)) (out : BatchOutcome) : Option State :=
  refRun (rows.drop out.cursor) out.committed

theorem batchExecuteAux_cursor_ge (rows : List (Option Int)) (st : State) (cursor : Nat) :
    cursor ≤ (batchExecuteAux rows st cursor).cursor := by
  induction rows generalizing cursor st with
  | nil => simp [batchExecuteAux]
  | cons row rest ih =>
      cases h : step st row with
      | none => simp [batchExecuteAux, h]
      | some st' =>
          have hge := ih st' (cursor + 1)
          simp [batchExecuteAux, h]
          omega

/-- ガード位置ごとの対応する参照状態を与える核定理。
    `cursor` 行目から再開した結果は、残りを参照経路で走らせた結果と等しい。 -/
theorem batchExecuteAux_sound (rows : List (Option Int)) (st : State) (cursor : Nat) :
    let out := batchExecuteAux rows st cursor
    refRun (rows.drop (out.cursor - cursor)) out.committed = refRun rows st := by
  induction rows generalizing cursor st with
  | nil =>
      simp [batchExecuteAux]
  | cons row rest ih =>
      cases h : step st row with
      | none =>
          simp [batchExecuteAux, h, refRun]
      | some st' =>
          have hge := batchExecuteAux_cursor_ge rest st' (cursor + 1)
          have ih' := ih st' (cursor + 1)
          dsimp at ih'
          simp only [batchExecuteAux, h]
          change refRun ((row :: rest).drop
              ((batchExecuteAux rest st' (cursor + 1)).cursor - cursor))
              (batchExecuteAux rest st' (cursor + 1)).committed =
            refRun (row :: rest) st
          have hsub :
              (batchExecuteAux rest st' (cursor + 1)).cursor - cursor =
                ((batchExecuteAux rest st' (cursor + 1)).cursor - (cursor + 1)) + 1 := by
            omega
          rw [hsub]
          change refRun (rest.drop
              ((batchExecuteAux rest st' (cursor + 1)).cursor - (cursor + 1)))
              (batchExecuteAux rest st' (cursor + 1)).committed =
            refRun (row :: rest) st
          simpa [refRun, h] using ih'

/-- **定理 (`deopt_resume_sound`)**: 確定済み接頭辞と保存カーソルからの再開は、
    先頭からの参照全走と同じ結果（エラー含む）を返す。 -/
theorem deopt_resume_sound (rows : List (Option Int)) (st : State) :
    resumeScalar rows (batchExecute rows st) = refRun rows st := by
  simpa [resumeScalar, batchExecute] using batchExecuteAux_sound rows st 0

/-- バッチが失敗したなら参照全走も失敗する。 -/
theorem batchExecuteAux_ref_of_failed (rows : List (Option Int)) (st : State) (cursor : Nat) :
    (batchExecuteAux rows st cursor).failed = true → refRun rows st = none := by
  induction rows generalizing cursor st with
  | nil => simp [batchExecuteAux]
  | cons row rest ih =>
      cases h : step st row with
      | none => simp [batchExecuteAux, h, refRun]
      | some st' =>
          simp [batchExecuteAux, h]
          intro hf
          simpa [refRun, h] using ih st' (cursor + 1) hf

/-- ガード失敗時の再開は必ずエラー (`none`) になる。 -/
theorem deopt_resume_failure_returns_none (rows : List (Option Int)) (st : State) :
    let out := batchExecute rows st
    out.failed = true → resumeScalar rows out = none := by
  dsimp only
  intro hf
  have hnone : refRun rows st = none := by
    simpa [batchExecute] using batchExecuteAux_ref_of_failed rows st 0 hf
  simpa [hnone] using deopt_resume_sound rows st

/-! ## 境界例 -/

/-- `[1, 2, INT64_MAX]` は最後の行の直前にガード失敗する。
    確定済み状態 (sum = 3) からバッチ先頭を再開すると 1 と 2 を二重に加算し
    sum = 6 になる。正しいカーソル (2) からの再開だけが参照と一致する。 -/
theorem restart_from_batch_head_unsound :
    let rows : List (Option Int) := [some 1, some 2, some i64Max]
    let st0 : State := ⟨[], some 0, 0, false⟩
    let out := batchExecute rows st0
    out.cursor = 2 ∧ out.failed = true ∧ out.committed.sum = some 3 ∧
    (refRun [some 1, some 2] out.committed).map State.sum = some (6 : Int) ∧
    (refRun [some 1, some 2] st0).map State.sum = some (3 : Int) ∧
    refRun rows st0 = none ∧
    resumeScalar rows out = none := by
  decide

/-- A JIT-specific guard may reject a row even when checked scalar addition
    succeeds. Undo speculative writes before re-evaluating the unconsumed row. -/
def runGuarded (guard : State → Option Int → Bool) : List (Option Int) → State → Option State
  | [], st => some st
  | row :: rest, st =>
    let part := (stepRaw st row).1
    if guard st row && (stepRaw st row).2 then runGuarded guard rest part
    else refRun (row :: rest) (restore part (save st))

theorem arbitrary_guard_resume (guard : State → Option Int → Bool)
    (rows : List (Option Int)) (st : State) : runGuarded guard rows st = refRun rows st := by
  induction rows generalizing st with
  | nil => rfl
  | cons row rest ih =>
    simp only [runGuarded, restore_discards_partial_write]
    split
    · rename_i h
      simp only [Bool.and_eq_true] at h
      rw [refRun, step_eq, if_pos h.2]
      apply ih
    · rfl

end TinylambOpt.DeoptResume
