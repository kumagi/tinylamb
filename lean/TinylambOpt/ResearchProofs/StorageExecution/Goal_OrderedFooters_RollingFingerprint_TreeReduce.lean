import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `RollingFingerprint.TreeReduce` — Merge an arbitrary spill tree without visiting the
  raw rows again.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_TreeReduce

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.RollingFingerprint

/-- 小さな入力での真偽確認（探索的チェック）。 -/
private def samples : List (List Nat) :=
  [[], [0], [1], [257], [65521], [0, 0], [1, 2], [257, 65521], [3, 0, 257, 9]]

private def smallTrees : List Tree :=
  (samples.map Tree.leaf) ++
  (samples.map Tree.leaf).flatMap (fun a =>
    (samples.map Tree.leaf).map (fun b => Tree.node a b)) ++
  (samples.map Tree.leaf).flatMap (fun a =>
    (samples.map Tree.leaf).map (fun b =>
      Tree.node (Tree.node a b) (Tree.node b a)))

#eval (smallTrees.all (fun t => rootState agg t == agg.reference (plain t)), smallTrees.length)

/-- 法（modulus）と基数。 -/
private def M : Nat := 65521
private def B : Nat := 257

/-- アキュムレータ付きの多項式ローリングハッシュ。 -/
private def F (xs : List Nat) (h : Nat) : Nat := xs.foldl (fun h x => (B * h + x) % M) h
private def V (xs : List Nat) : Nat := F xs 0
private def P (xs : List Nat) : Nat := B ^ xs.length % M

/-- `% M` の下で剰余を外す基本的な補題。 -/
private theorem mr (a b : Nat) : (a * (b % M)) % M = (a * b) % M := by
  simp only [Nat.mul_mod, Nat.mod_mod]

private theorem ml (a b : Nat) : ((a % M) * b) % M = (a * b) % M := by
  simp only [Nat.mul_mod, Nat.mod_mod]

private theorem ar (a b : Nat) : (a + b % M) % M = (a + b) % M := by
  simp only [Nat.add_mod, Nat.mod_mod]

private theorem al (a b : Nat) : (a % M + b) % M = (a + b) % M := by
  simp only [Nat.add_mod, Nat.mod_mod]

private theorem add2 (a b : Nat) : (a % M + b % M) % M = (a + b) % M := by
  simp only [Nat.add_mod, Nat.mod_mod]

/-- アキュムレータが `M` 未満なら、畳み込み結果も `M` 未満。 -/
private theorem F_lt (xs : List Nat) (h : Nat) (hh : h < M) : F xs h < M := by
  induction xs generalizing h with
  | nil => simpa [F] using hh
  | cons x xs ih =>
      simp only [F, List.foldl_cons]
      exact ih _ (Nat.mod_lt _ (by decide : 0 < M))

private theorem V_lt (xs : List Nat) : V xs < M :=
  F_lt xs 0 (by decide : 0 < M)

private theorem inner (x P0 V0 : Nat) :
    ((x % M) * P0 + V0) % M = (x * P0 + V0) % M := by
  calc
    ((x % M) * P0 + V0) % M = (((x % M) * P0) % M + V0) % M := by
      rw [Nat.add_mod]
      exact ar ((x % M) * P0 % M) V0
    _ = ((x * P0) % M + V0) % M := by rw [ml]
    _ = (x * P0 + V0) % M := by rw [al]

/-- 連結境界での多項式ハッシュの合成則。 -/
private theorem key (P0 V0 h x : Nat) (hV : V0 < M) :
    (((B * h + x) % M) * P0 + V0) % M =
      (h * (P0 * B % M) + ((x % M) * P0 + V0) % M) % M := by
  calc
    (((B * h + x) % M) * P0 + V0) % M
        = ((((B * h + x) % M) * P0) % M + V0) % M := by
            rw [Nat.add_mod, Nat.mod_eq_of_lt hV]
    _ = (((B * h + x) * P0) % M + V0) % M := by rw [ml]
    _ = ((P0 * (B * h + x)) % M + V0) % M := by rw [Nat.mul_comm (B * h + x) P0]
    _ = (P0 * (B * h + x) + V0) % M := by rw [al]
    _ = (P0 * B * h + P0 * x + V0) % M := by rw [Nat.mul_add]; ac_rfl
    _ = (h * P0 * B + x * P0 + V0) % M := by ac_rfl
    _ = (h * (P0 * B % M) + ((x % M) * P0 + V0) % M) % M := by
      symm
      calc
        (h * (P0 * B % M) + ((x % M) * P0 + V0) % M) % M
            = ((h * (P0 * B % M)) % M + ((x % M) * P0 + V0) % M) % M := by
                rw [Nat.add_mod]
                simp only [Nat.mod_mod]
        _ = ((h * (P0 * B)) % M + ((x % M) * P0 + V0) % M) % M := by rw [mr]
        _ = ((h * P0 * B) % M + ((x % M) * P0 + V0) % M) % M := by rw [Nat.mul_assoc]
        _ = ((h * P0 * B) % M + (x * P0 + V0) % M) % M := by rw [inner]
        _ = (h * P0 * B + x * P0 + V0) % M := by
              rw [add2]
              rw [Nat.add_assoc]

/-- 任意のアキュムレータについての畳み込み公式。 -/
private theorem F_mod (xs : List Nat) (h : Nat) :
    F xs h % M = (h * P xs + V xs) % M := by
  induction xs generalizing h with
  | nil => simp [F, V, P]
  | cons x xs ih =>
      change F xs ((B * h + x) % M) % M = (h * P (x :: xs) + V (x :: xs)) % M
      rw [ih ((B * h + x) % M)]
      have hPc : P (x :: xs) = P xs * B % M := by
        simp [P, Nat.pow_succ, Nat.mul_mod,
          Nat.mod_eq_of_lt (by decide : B < M)]
      have hVc : V (x :: xs) = ((x % M) * P xs + V xs) % M := by
        calc
          V (x :: xs) = F xs (x % M) := by simp [V, F, B]
          _ = F xs (x % M) % M :=
            (Nat.mod_eq_of_lt
              (F_lt xs (x % M) (Nat.mod_lt _ (by decide : 0 < M)))).symm
          _ = ((x % M) * P xs + V xs) % M := ih (x % M)
      rw [hPc, hVc]
      exact key (P xs) (V xs) h x (V_lt xs)

private theorem P_append (a b : List Nat) : P (a ++ b) = P a * P b % M := by
  simp [P, List.length_append, Nat.pow_add, Nat.mul_mod]

/-- `join` が到達可能な要約について連結と一致する。 -/
private theorem stitch_agg : stitch agg := by
  intro a b
  simp only [agg]
  change (P a * P b % M, (V a * P b + V b) % M) = (P (a ++ b), V (a ++ b))
  refine Prod.ext (P_append a b).symm ?_
  calc
    (V a * P b + V b) % M = V (a ++ b) := by
      symm
      calc
        V (a ++ b) = F b (V a) := by simp [V, F, List.foldl_append]
        _ = F b (V a) % M := (Nat.mod_eq_of_lt (F_lt b (V a) (V_lt a))).symm
        _ = (V a * P b + V b) % M := F_mod b (V a)

/-- 木を辿る要約計算は、葉を連結した生データの要約と一致する。 -/
theorem sound_TreeReduce : TreeReduce.Goal := by
  intro t
  induction t with
  | leaf xs => rfl
  | node a b iha ihb =>
      simp only [rootState, plain, iha, ihb]
      exact stitch_agg _ _

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_RollingFingerprint_TreeReduce
