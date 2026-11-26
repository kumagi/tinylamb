import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `Cooldown.PrefixRange` — Accepted completions of a fixed bitmap prefix occupy a
  contiguous interval of ranks.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_PrefixRange

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.Cooldown

private theorem lowCount_eq (n q : Nat) : lowCount model n q = count model n (q - 1) := by
  simp [lowCount, model]

private theorem count_succ (n q : Nat) :
    count model (n + 1) q = count model n (q - 1) + (if q == 0 then count model n 2 else 0) := by
  by_cases hq : q = 0 <;> simp [count, model, hq]

private theorem step_false (q : Nat) : model.step q false = some (q - 1) := by
  simp [model]

private theorem step_true_zero : model.step 0 true = some 2 := by
  simp [model]

private theorem step_true_ne (q : Nat) (hq : q ≠ 0) : model.step q true = none := by
  simp [model, hq]

private theorem unrank_succ_false (n q r : Nat) (hr : r < lowCount model n q) :
    unrank model (n + 1) q r = (unrank model n (q - 1) r).map (fun bs => false :: bs) := by
  have hb : decide (lowCount model n q ≤ r) = false := by
    simp [Nat.not_le_of_gt hr]
  simp [unrank, hb, step_false]
  cases unrank model n (q - 1) r <;> simp

private theorem unrank_succ_true (n r : Nat) (hr : lowCount model n 0 ≤ r) :
    unrank model (n + 1) 0 r = (unrank model n 2 (r - lowCount model n 0)).map (fun bs => true :: bs) := by
  have hb : decide (lowCount model n 0 ≤ r) = true := by
    simp [hr]
  simp [unrank, hb, step_true_zero]
  cases unrank model n 2 (r - lowCount model n 0) <;> simp

private theorem descend_succ_false (n q : Nat) (bs : List Bool) (q' off : Nat) :
    descend model (n + 1) q (false :: bs) = some (q', off) ↔
      descend model n (q - 1) bs = some (q', off) := by
  cases hd : descend model n (q - 1) bs with
  | none => simp [descend, step_false, hd]
  | some p => rcases p with ⟨a, b⟩; simp [descend, step_false, hd]

private theorem unrank_isSome : ∀ n q r, (unrank model n q r).isSome = decide (r < count model n q) := by
  intro n
  induction n with
  | zero =>
      intro q r
      by_cases hr : r = 0
      · subst r
        simp [unrank, count, model]
      · have hnot : ¬ r < 1 := by omega
        simp [unrank, count, model, hr, hnot]
  | succ n ih =>
      intro q r
      by_cases hr : r < lowCount model n q
      · rw [unrank_succ_false n q r hr]
        simp only [Option.isSome_map]
        rw [ih]
        rw [lowCount_eq] at hr
        by_cases hq : q = 0
        · subst q
          simp at hr
          simp [count_succ]
          omega
        · rw [count_succ]
          simp [hq]
      · have hge : lowCount model n q ≤ r := Nat.le_of_not_gt hr
        by_cases hq : q = 0
        · subst q
          rw [unrank_succ_true n r hge]
          simp only [Option.isSome_map]
          rw [ih]
          simp [lowCount_eq, count_succ] at hge ⊢
          omega
        · have hb : decide (lowCount model n q ≤ r) = true := by simp [hge]
          simp [unrank, hb, step_true_ne q hq]
          rw [count_succ]
          simp [hq]
          rw [← lowCount_eq]
          omega

private theorem descend_bound : ∀ n q pre q' off,
    descend model n q pre = some (q', off) →
      off + count model (n - pre.length) q' ≤ count model n q := by
  intro n q pre
  induction pre generalizing n q with
  | nil =>
      intro q' off h
      simp [descend] at h
      rcases h with ⟨rfl, rfl⟩
      simp
  | cons b bs ih =>
      intro q' off h
      cases n with
      | zero => simp [descend] at h
      | succ n =>
          cases b with
          | false =>
              have h' : descend model n (q - 1) bs = some (q', off) :=
                (descend_succ_false n q bs q' off).mp h
              have hi := ih n (q - 1) q' off h'
              rw [count_succ]
              simp
              omega
          | true =>
              by_cases hq : q = 0
              · subst q
                cases hd : descend model n 2 bs with
                | none => simp [descend, step_true_zero, hd] at h
                | some p =>
                    rcases p with ⟨q1, off1⟩
                    have h' : descend model n 2 bs = some (q1, off1) := hd
                    have hi := ih n 2 q1 off1 h'
                    have hres : q1 = q' ∧ off1 + lowCount model n 0 = off := by
                      simpa [descend, step_true_zero, hd] using h
                    rcases hres with ⟨rfl, rfl⟩
                    rw [count_succ]
                    simp [lowCount_eq]
                    omega
              · simp [descend, step_true_ne q hq] at h

private theorem prefixRangeAll : ∀ n q pre q' off r,
    descend model n q pre = some (q', off) →
      (unrank model n q r).any (fun bs => pre.isPrefixOf bs) =
        decide (off ≤ r ∧ r < off + count model (n - pre.length) q') := by
  intro n q pre
  induction pre generalizing n q with
  | nil =>
      intro q' off r h
      simp [descend] at h
      rcases h with ⟨rfl, rfl⟩
      have hany : (unrank model n q r).any (fun _ : List Bool => true) =
          (unrank model n q r).isSome := by
        cases unrank model n q r <;> simp
      simpa using (hany.trans (unrank_isSome n q r))
  | cons b bs ih =>
      intro q' off r h
      cases n with
      | zero => simp [descend] at h
      | succ n =>
          cases b with
          | false =>
              have h' : descend model n (q - 1) bs = some (q', off) :=
                (descend_succ_false n q bs q' off).mp h
              have hi := ih n (q - 1) q' off r h'
              have hbnd := descend_bound n (q - 1) bs q' off h'
              have hlen : (n + 1) - (false :: bs).length = n - bs.length := by
                simp
              rw [hlen]
              by_cases hr : r < lowCount model n q
              · rw [unrank_succ_false n q r hr]
                simp only [Option.any_map]
                have hpref : (fun x : List Bool => (false :: bs).isPrefixOf (false :: x)) =
                    fun x => bs.isPrefixOf x := by
                  funext x
                  simp
                rw [hpref, hi]
              · have hge : lowCount model n q ≤ r := Nat.le_of_not_gt hr
                by_cases hq : q = 0
                · subst q
                  rw [unrank_succ_true n r (by simpa [lowCount_eq] using hge)]
                  simp only [Option.any_map]
                  have hpref : (fun x : List Bool => (false :: bs).isPrefixOf (true :: x)) =
                      fun _ => false := by
                    funext x
                    simp [List.isPrefixOf]
                  rw [hpref]
                  have hanyfalse : Option.any (fun _ : List Bool => false)
                      (unrank model n 2 (r - lowCount model n 0)) = false := by
                    cases unrank model n 2 (r - lowCount model n 0) <;> simp
                  rw [hanyfalse]
                  simp [lowCount_eq] at hbnd hge
                  have hno : ¬ (off ≤ r ∧ r < off + count model (n - bs.length) q') := by omega
                  simp [hno]
                · have hb : decide (lowCount model n q ≤ r) = true := by simp [hge]
                  simp [unrank, hb, step_true_ne q hq]
                  rw [← lowCount_eq] at hbnd
                  omega
          | true =>
              by_cases hq : q = 0
              · subst q
                cases hd : descend model n 2 bs with
                | none => simp [descend, step_true_zero, hd] at h
                | some p =>
                    rcases p with ⟨q1, off1⟩
                    have h' : descend model n 2 bs = some (q1, off1) := hd
                    have hi := ih n 2 q1 off1 (r - lowCount model n 0) h'
                    have hbnd := descend_bound n 2 bs q1 off1 h'
                    have hres : q1 = q' ∧ off1 + lowCount model n 0 = off := by
                      simpa [descend, step_true_zero, hd] using h
                    rcases hres with ⟨rfl, rfl⟩
                    have hlen : (n + 1) - (true :: bs).length = n - bs.length := by
                      simp
                    rw [hlen]
                    by_cases hr : lowCount model n 0 ≤ r
                    · rw [unrank_succ_true n r hr]
                      simp only [Option.any_map]
                      have hpref : (fun x : List Bool => (true :: bs).isPrefixOf (true :: x)) =
                          fun x => bs.isPrefixOf x := by
                        funext x
                        simp
                      rw [hpref, hi]
                      rw [lowCount_eq] at hr
                      rw [lowCount_eq]
                      simp only [Nat.zero_sub] at hr ⊢
                      have hiff :
                          (off1 ≤ r - count model n 0 ∧
                            r - count model n 0 < off1 + count model (n - bs.length) q1) ↔
                          (off1 + count model n 0 ≤ r ∧
                            r < off1 + count model n 0 + count model (n - bs.length) q1) := by
                        omega
                      simp [hiff]
                    · rw [unrank_succ_false n 0 r (Nat.lt_of_not_ge hr)]
                      simp only [Option.any_map]
                      have hpref : (fun x : List Bool => (true :: bs).isPrefixOf (false :: x)) =
                          fun _ => false := by
                        funext x
                        simp [List.isPrefixOf]
                      rw [hpref]
                      have hanyfalse : Option.any (fun _ : List Bool => false)
                          (unrank model n 0 r) = false := by
                        cases unrank model n 0 r <;> simp
                      rw [hanyfalse]
                      have hno : ¬ (off1 + lowCount model n 0 ≤ r ∧
                          r < off1 + lowCount model n 0 + count model (n - bs.length) q1) := by omega
                      simp [hno]
              · simp [descend, step_true_ne q hq] at h

theorem sound_PrefixRange : Cooldown.PrefixRange.Goal := by
  intro n q pre q' off r h _
  exact prefixRangeAll n q pre q' off r h

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_Cooldown_PrefixRange
