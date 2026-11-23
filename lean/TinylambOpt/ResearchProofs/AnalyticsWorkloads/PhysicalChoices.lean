import TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.PhysicalChoices

open TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices

/-! Proofs for the five reusable physical-plan / execution-kernel propositions
in `TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices`.

`EdgeMorsels` is proved by a generic "chunk then flatten" lemma
(`chunks_flatten`) plus per-element size/count bookkeeping over `List.sum`. -/

-- chunks of `vs` into pieces of size `cap` concatenate back to `vs`
private theorem chunks_flatten (vs : List Nat) (cap : Nat) (hcap : 0 < cap) :
    (List.range ((vs.length + cap - 1) / cap)).flatMap
      (fun j => (vs.drop (j * cap)).take cap) = vs := by
  let P : Nat → Prop := fun n => ∀ vs : List Nat, vs.length = n → ∀ cap : Nat, 0 < cap →
    (List.range ((vs.length + cap - 1) / cap)).flatMap
      (fun j => (vs.drop (j * cap)).take cap) = vs
  have key : ∀ n, P n := fun n => Nat.strongRecOn n (fun n ih vs hn cap hcap => by
    cases vs with
    | nil =>
        have hc : (List.length ([] : List Nat) + cap - 1) / cap = 0 := by
          rw [List.length_nil]
          have : (0 : Nat) + cap - 1 < cap := by omega
          exact Nat.div_eq_of_lt this
        rw [hc]
        simp
    | cons x xs =>
        by_cases hsmall : xs.length + 1 ≤ cap
        · -- whole list fits in one chunk: quotient is 1
          have harg : (x :: xs).length + cap - 1 = xs.length + cap := by
            simp only [List.length_cons]; omega
          rw [harg]
          have hc : (xs.length + cap) / cap = 1 := by
            rw [Nat.div_eq_iff hcap]
            constructor <;> omega
          rw [hc, List.range_one]
          simp only [List.flatMap_cons, List.flatMap_nil, Nat.zero_mul, List.drop_zero,
            List.append_nil]
          rw [List.take_of_length_le (by simpa [List.length_cons] using hsmall)]
        · -- cap < xs.length + 1, i.e. cap ≤ xs.length: peel the first chunk
          have hcaple : cap ≤ xs.length := by omega
          have harg : (x :: xs).length + cap - 1 = xs.length + cap := by
            simp only [List.length_cons]; omega
          rw [harg]
          have hc : (xs.length + cap) / cap = xs.length / cap + 1 := by
            have h2 := Nat.add_mul_div_right xs.length 1 hcap
            rw [Nat.one_mul] at h2
            exact h2
          rw [hc, List.range_succ_eq_map, List.flatMap_cons, List.flatMap_map]
          -- remaining flatMap function: fun a => ((x::xs).drop (a.succ * cap)).take cap
          have hfun : (fun a => ((x :: xs).drop (a.succ * cap)).take cap)
              = (fun a => (((x :: xs).drop cap).drop (a * cap)).take cap) := by
            funext a
            have e1 : a.succ * cap = a * cap + cap := by
              rw [Nat.succ_eq_add_one, Nat.add_mul, Nat.one_mul]
            have e2 : (x :: xs).drop (a * cap + cap)
                = ((x :: xs).drop cap).drop (a * cap) := by
              rw [Nat.add_comm (a * cap) cap]
              exact (List.drop_drop).symm
            rw [e1, e2]
          rw [hfun]
          have hdrop_len : ((x :: xs).drop cap).length = xs.length + 1 - cap := by
            rw [List.length_drop, List.length_cons]
          have hlt : ((x :: xs).drop cap).length < n := by
            rw [hdrop_len]
            have : xs.length + 1 = n := by
              have hn' := hn
              simp [List.length_cons] at hn'
              omega
            omega
          have hq : (((x :: xs).drop cap).length + cap - 1) / cap = xs.length / cap := by
            rw [hdrop_len]
            congr 1
            omega
          have hih := ih _ hlt ((x :: xs).drop cap) rfl cap hcap
          rw [hq] at hih
          rw [hih]
          simp [Nat.zero_mul, List.drop_zero, List.take_append_drop])
  exact key vs.length vs rfl cap hcap

-- sum helpers over Nat lists
private theorem sum_le_sum {α : Type} {l : List α} {f g : α → Nat}
    (h : ∀ a ∈ l, f a ≤ g a) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha : f a ≤ g a := h a List.mem_cons_self
      have hl : (l.map f).sum ≤ (l.map g).sum :=
        ih fun b hb => h b (List.mem_cons_of_mem a hb)
      omega

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map fun a => f a + g a).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      omega

private theorem sum_map_mul_left {α : Type} (l : List α) (c : Nat) (f : α → Nat) :
    (l.map fun a => c * f a).sum = c * (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      rw [Nat.mul_add]

private theorem sum_map_mul_right {α : Type} (l : List α) (f : α → Nat) (n : Nat) :
    (l.map fun a => f a * n).sum = (l.map f).sum * n := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      rw [Nat.add_mul]

private theorem sum_indicator {α : Type} (l : List α) (p : α → Bool) :
    (l.map fun a => (if p a then 1 else 0)).sum = l.countP p := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons, ih, List.countP_cons]
      exact Nat.add_comm _ _


private theorem flatMap_congr' {α : Type} {β : Type} {l : List α} {f g : α → List β}
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      have ha := h a List.mem_cons_self
      have hl := ih fun b hb => h b (List.mem_cons_of_mem a hb)
      rw [List.flatMap_cons, List.flatMap_cons, ha, hl]

private theorem expand_slices (cap : Nat) (hcap : 0 < cap) (adj : List (Nat × List Nat)) :
    (slices cap adj).flatMap (fun x => x.snd.map fun v => (x.fst, v))
      = adj.flatMap (fun x => x.snd.map fun v => (x.fst, v)) := by
  unfold slices
  rw [List.flatMap_assoc]
  refine flatMap_congr' ?_
  intro a ha
  dsimp only
  rw [List.flatMap_map]
  dsimp only
  rw [← List.map_flatMap, chunks_flatten a.snd cap hcap]

theorem sound_EdgeMorsels : EdgeMorsels.Goal := by
  intro adj cap hcap
  dsimp only
  unfold slices
  refine ⟨?_, ?_, ?_⟩
  · exact expand_slices cap hcap adj
  · intro j hj
    rw [List.mem_flatMap] at hj
    obtain ⟨a, ha, hj⟩ := hj
    rw [List.mem_map] at hj
    obtain ⟨jj, hjjr, hjj⟩ := hj
    have hj2 : j.2 = (a.2.drop (jj*cap)).take cap := by
      have h2 := congrArg Prod.snd hjj
      simpa using h2.symm
    have hjjlt : jj < (a.2.length + cap - 1)/cap := by
      have h2 := List.mem_range.1 hjjr
      simpa using h2
    have hbound : jj * cap < a.2.length := by
      have h2 : (jj + 1) * cap ≤ ((a.2.length + cap - 1)/cap) * cap := by
        apply Nat.mul_le_mul_right cap
        omega
      rw [Nat.add_mul, Nat.one_mul] at h2
      have hdm := Nat.div_mul_le_self (a.2.length + cap - 1) cap
      omega
    have hclen : j.2.length = min cap (a.2.length - jj * cap) := by
      rw [hj2, List.length_take, List.length_drop]
    rw [hclen]
    constructor
    · exact Nat.lt_min.2 ⟨hcap, by omega⟩
    · exact Nat.min_le_left _ _
  · have hjobs_len : (adj.flatMap fun x => (List.range ((x.snd.length + cap - 1)/cap)).map
          fun j => (x.fst, (x.snd.drop (j*cap)).take cap)).length
        = (adj.map fun a => (a.2.length + cap - 1)/cap).sum := by
      rw [List.length_flatMap]
      congr 1
      apply List.map_congr_left
      intro a ha
      simp
    have hexp_len : (adj.flatMap fun x => x.snd.map fun v => (x.fst, v)).length
        = (adj.map fun a => a.2.length).sum := by
      rw [List.length_flatMap]
      congr 1
      apply List.map_congr_left
      intro a ha
      simp
    rw [hjobs_len, hexp_len]
    simp only [← sum_map_mul_right, ← sum_indicator, ← sum_map_mul_left, ← sum_map_add]
    apply sum_le_sum
    intro a ha
    obtain ⟨k, vs⟩ := a
    dsimp only
    by_cases hE : vs = []
    · subst hE
      have hc : (([] : List Nat).length + cap - 1)/cap = 0 := by
        rw [List.length_nil]
        have : (0:Nat) + cap - 1 < cap := by omega
        exact Nat.div_eq_of_lt this
      rw [hc]
      simp
    · have hne : (!vs.isEmpty) = true := by
        cases vs with
        | nil => exact absurd rfl hE
        | cons x xs => rfl
      have harg : vs.length + cap - 1 = vs.length + (cap - 1) := by omega
      have hdm := Nat.div_mul_le_self (vs.length + cap - 1) cap
      rw [harg] at hdm ⊢
      rw [hne]
      simp only [ite_true]
      omega

end TinylambOpt.ResearchProofs.AnalyticsWorkloads.PhysicalChoices
