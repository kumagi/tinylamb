import TinylambOpt.ResearchGoals.EngineDecisions.Optimizer
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Opt_JoinPrefix

open TinylambOpt.ResearchGoals.EngineDecisions.Optimizer.JoinPrefix

theorem drop_take_prefix (p q : List Nat) (off n : Nat) (h : off+n ≤ p.length) :
  ((p++q).drop off).take n = (p.drop off).take n := by
  rw [List.drop_append]
  have hoff : off - p.length = 0 := by omega
  rw [hoff]
  simp only [List.drop_zero]
  rw [List.take_append]
  have hlen : (p.drop off).length = p.length - off := by rw [List.length_drop]
  have hn : n - (p.drop off).length = 0 := by omega
  rw [hn]
  simp

theorem prefixFor_append (lower : Nat → Nat) (k : Nat) :
    ∀ ls, ∃ suf, ls = prefixFor lower k ls ++ suf := by
  intro ls
  induction ls generalizing k with
  | nil =>
    cases k with
    | zero => exact ⟨[], by simp [prefixFor]⟩
    | succ k => exact ⟨[], by simp [prefixFor]⟩
  | cons l ls ih =>
    cases k with
    | zero => exact ⟨l::ls, by simp [prefixFor]⟩
    | succ k =>
      obtain ⟨suf, hsuf⟩ := ih (k+1 - lower l)
      refine ⟨suf, ?_⟩
      rw [prefixFor]
      simp only [List.cons_append]
      congr 1

theorem prefixFor_len (lower : Nat → Nat) (matchesFor : Nat → List Nat) :
    ∀ ls, (∀ l ∈ ls, lower l ≤ (matchesFor l).length) → ∀ k,
      prefixFor lower k ls = ls ∨
        k ≤ ((prefixFor lower k ls).flatMap matchesFor).length := by
  intro ls
  induction ls with
  | nil =>
    intro h k
    left
    cases k <;> simp [prefixFor]
  | cons l ls ih =>
    intro h k
    cases k with
    | zero =>
      right
      simp [prefixFor]
    | succ k =>
      have hl : lower l ≤ (matchesFor l).length := h l (by simp)
      have hls : ∀ x ∈ ls, lower x ≤ (matchesFor x).length := by
        intro x hx; exact h x (by simp [hx])
      have hres := ih hls (k+1 - lower l)
      rcases hres with heq | hle
      · left
        simp [prefixFor, heq]
      · right
        simp only [prefixFor, List.flatMap_cons, List.length_append, List.length_map]
        omega

theorem sound : Goal := by
  intro matchesFor lower ls off n h
  let k := off + n
  obtain ⟨suf, hsuf⟩ := prefixFor_append lower k ls
  have hlen := prefixFor_len lower matchesFor ls h k
  have hgoal : (((prefixFor lower k ls).flatMap matchesFor).drop off).take n =
      ((ls.flatMap matchesFor).drop off).take n := by
    rcases hlen with hpref | hklen
    · rw [hpref]
    · have hfull : ls.flatMap matchesFor =
          ((prefixFor lower k ls).flatMap matchesFor) ++ (suf.flatMap matchesFor) := by
        calc
          ls.flatMap matchesFor = (prefixFor lower k ls ++ suf).flatMap matchesFor :=
            congrArg (fun l => l.flatMap matchesFor) hsuf
          _ = (prefixFor lower k ls).flatMap matchesFor ++ suf.flatMap matchesFor :=
            List.flatMap_append
      rw [hfull]
      symm
      apply drop_take_prefix
      simpa [k] using hklen
  simpa [k] using hgoal

end TinylambOpt.ResearchProofs.EngineDecisions.Opt_JoinPrefix
