import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_ShardAdmission

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

-- 真偽確認: 小さい全探索では常に成り立つ。
private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs, 3 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun a => ls.all (fun b =>
    (List.range 12).all (fun c =>
      let credit := (c : Int) - 4
      ((0 ≤ credit + (agg.reference (a ++ b)).2) ==
       (0 ≤ credit + (agg.reference a).2 ∧
        0 ≤ credit + (agg.reference a).1 + (agg.reference b).2)))))

#eval probe 3
#eval probe 4

-- The signed steps used by BalanceFloor.agg.reference.
private def steps (xs : List Nat) : List Int :=
  xs.map (fun x => if x % 2 == 0 then ((x / 2 : Nat) : Int) else -((x / 2 + 1 : Nat) : Int))

-- A fold maintaining (running sum, minimum prefix sum) for a list of steps.
private def smStep (p : Int × Int) (x : Int) : Int × Int :=
  (p.1 + x, min p.2 (p.1 + x))

private def smFrom (s m : Int) (xs : List Int) : Int × Int :=
  xs.foldl smStep (s, m)

private def sm (xs : List Int) : Int × Int := smFrom 0 0 xs

private theorem smFrom_cons (s m : Int) (x : Int) (xs : List Int) :
    smFrom s m (x :: xs) = smFrom (s + x) (min m (s + x)) xs := by
  simp [smFrom, List.foldl_cons, smStep]

private theorem sm_cons (x : Int) (xs : List Int) :
    sm (x :: xs) = smFrom x (min 0 x) xs := by
  simpa [sm] using smFrom_cons 0 0 x xs

private theorem smFrom_fst (s m : Int) (xs : List Int) :
    (smFrom s m xs).1 = s + xs.sum := by
  induction xs generalizing s m with
  | nil => simp [smFrom]
  | cons x xs ih =>
      rw [smFrom_cons, ih]
      simp [List.sum_cons]
      omega

private theorem smFrom_snd_le_init (s m : Int) (xs : List Int) :
    (smFrom s m xs).2 ≤ m := by
  induction xs generalizing s m with
  | nil => simp [smFrom]
  | cons x xs ih =>
      rw [smFrom_cons]
      have h := ih (s + x) (min m (s + x))
      omega

private theorem sm_snd_le_zero (xs : List Int) : (sm xs).2 ≤ 0 := by
  simpa [sm] using smFrom_snd_le_init 0 0 xs

private theorem smFrom_snd_le_fst (s m : Int) (xs : List Int) (h : m ≤ s) :
    (smFrom s m xs).2 ≤ (smFrom s m xs).1 := by
  induction xs generalizing s m with
  | nil => simp [smFrom]; omega
  | cons x xs ih =>
      rw [smFrom_cons]
      have hc : min m (s + x) ≤ s + x := by omega
      have hih := ih (s + x) (min m (s + x)) hc
      omega

private theorem sm_snd_le_fst (xs : List Int) : (sm xs).2 ≤ (sm xs).1 := by
  exact smFrom_snd_le_fst 0 0 xs (by omega)

private theorem smFrom_formula (s m : Int) (xs : List Int) (h : m ≤ s) :
    smFrom s m xs = (s + xs.sum, min m (s + (sm xs).2)) := by
  induction xs generalizing s m with
  | nil =>
      simp [smFrom, sm]
      omega
  | cons x xs ih =>
      rw [smFrom_cons]
      have hc : min m (s + x) ≤ s + x := by omega
      have hih := ih (s + x) (min m (s + x)) hc
      rw [hih]
      have hc2 : min 0 x ≤ x := by omega
      have hsm := ih x (min 0 x) hc2
      rw [show (x :: xs).sum = x + xs.sum by simp [List.sum_cons]]
      rw [sm_cons, hsm]
      dsimp
      have hz := sm_snd_le_zero xs
      apply Prod.ext <;> omega

private theorem sm_append (ss tt : List Int) :
    sm (ss ++ tt) =
      ((sm ss).1 + (sm tt).1, min (sm ss).2 ((sm ss).1 + (sm tt).2)) := by
  have hf : smFrom (sm ss).1 (sm ss).2 tt =
      ((sm ss).1 + tt.sum, min (sm ss).2 ((sm ss).1 + (sm tt).2)) :=
    smFrom_formula (sm ss).1 (sm ss).2 tt (sm_snd_le_fst ss)
  rw [show sm (ss ++ tt) = smFrom (sm ss).1 (sm ss).2 tt by
    simp [sm, smFrom, List.foldl_append]]
  rw [hf]
  have htt : (sm tt).1 = tt.sum := by
    simpa [sm] using smFrom_fst 0 0 tt
  rw [htt]

-- Relate the fold state to the scanl/min? definition used by agg.reference.
private theorem scanl_fold (ss : List Int) (c d : Int) :
    (ss.scanl (· + ·) c).foldl min d = min d (c + (sm ss).2) := by
  induction ss generalizing c d with
  | nil =>
      simp [sm, smFrom, List.scanl_nil]
  | cons x xs ih =>
      rw [List.scanl_cons, List.foldl_cons]
      rw [ih (c + x) (min d c)]
      have hc2 : min 0 x ≤ x := by omega
      have hsmexpr : sm (x :: xs) =
          (x + xs.sum, min (min 0 x) (x + (sm xs).2)) := by
        rw [sm_cons]
        exact smFrom_formula x (min 0 x) xs hc2
      rw [hsmexpr]
      have hz := sm_snd_le_zero xs
      omega

private theorem zero_mem_scanl (ss : List Int) : 0 ∈ ss.scanl (· + ·) 0 := by
  cases ss with
  | nil => simp [List.scanl_nil]
  | cons x xs => simp [List.scanl_cons]

private theorem scanl_min_eq_snd (ss : List Int) :
    (ss.scanl (· + ·) 0).min?.getD 0 = (sm ss).2 := by
  let l := ss.scanl (· + ·) 0
  change l.min?.getD 0 = (sm ss).2
  have hzero : 0 ∈ l := by
    dsimp [l]
    exact zero_mem_scanl ss
  have hle : l.min?.getD 0 ≤ 0 := by
    cases h : l.min? with
    | none =>
        have hl : l = [] := List.min?_eq_none_iff.mp h
        simp [hl] at hzero
    | some m =>
        have hm := (List.min?_eq_some_iff.mp h).2 0 hzero
        simpa [h] using hm
  have hfold : l.foldl min 0 = (sm ss).2 := by
    dsimp [l]
    have hf := scanl_fold ss 0 0
    have hz := sm_snd_le_zero ss
    omega
  have hfmin := List.foldl_min (l := l) (a := (0 : Int))
  omega

private theorem sm_steps_eq_reference (xs : List Nat) :
    sm (steps xs) = agg.reference xs := by
  apply Prod.ext
  · simp [sm, smFrom_fst, steps, agg]
  · simp [agg, steps, scanl_min_eq_snd]

theorem sound_ShardAdmission : BalanceFloor.ShardAdmission.Goal := by
  intro a b credit
  rw [← sm_steps_eq_reference a, ← sm_steps_eq_reference b,
    ← sm_steps_eq_reference (a ++ b)]
  have happ : sm (steps (a ++ b)) =
      ((sm (steps a)).1 + (sm (steps b)).1,
        min (sm (steps a)).2 ((sm (steps a)).1 + (sm (steps b)).2)) := by
    rw [show steps (a ++ b) = steps a ++ steps b by simp [steps]]
    exact sm_append (steps a) (steps b)
  rw [happ]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_ShardAdmission
