import TinylambOpt.ResearchGoals.RestartRaces.JobJ3
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.JobJ3_PersonPages
open TinylambOpt.ResearchGoals.RestartRaces.JobJ3.PersonPages

theorem mem_demand (edges : List (Nat × Nat)) (titles page : Nat → Nat) (f : Nat) :
    f ∈ demand edges titles page ↔
      ∃ p m, (p, m) ∈ edges ∧ titles m ≠ 0 ∧ page p = f := by
  simp [demand, List.mem_filterMap]

theorem result_pos_at (edges : List (Nat × Nat)) (titles : Nat → Nat) (p m : Nat)
    (hp : (p, m) ∈ edges) (ht : titles m ≠ 0) :
    0 < result edges (fun k => if k == p then true else false) titles := by
  induction edges with
  | nil => simp at hp
  | cons e es ih =>
      rcases e with ⟨q, m'⟩
      dsimp [result]
      by_cases hqp : q = p
      · subst q
        by_cases ht' : titles m' = 0
        · have hp' : (p, m) ∈ es := by
            simp [ht'] at hp
            rcases hp with hmm | hIn
            · subst m'
              exact False.elim (ht ht')
            · exact hIn
          have htailpos :
              0 < (List.map (fun x : Nat × Nat =>
                if (if x.1 == p then true else false) then titles x.2 else 0) es).sum := by
            simpa [result] using ih hp'
          simpa [ht'] using htailpos
        · have hhead : 0 < titles m' := Nat.pos_of_ne_zero ht'
          simp
          omega
      · have hpq : p ≠ q := fun h => hqp h.symm
        have hp' : (p, m) ∈ es := by
          simpa [hpq] using hp
        have htailpos :
            0 < (List.map (fun x : Nat × Nat =>
              if (if x.1 == p then true else false) then titles x.2 else 0) es).sum := by
          simpa [result] using ih hp'
        simpa [hqp] using htailpos

theorem sum_zero (l : List (Nat × Nat)) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

theorem sound : Goal := by
  intro edges titles page loaded
  constructor
  · intro hload a b hab
    induction edges with
    | nil => simp [result]
    | cons e es ih =>
        rcases e with ⟨p, m⟩
        have htail : ∀ f ∈ demand es titles page, f ∈ loaded := by
          intro f hf
          apply hload f
          rw [mem_demand] at hf ⊢
          rcases hf with ⟨p', m', hp', ht', hpage'⟩
          exact ⟨p', m', by simp [hp'], ht', hpage'⟩
        change (if a p then titles m else 0) + result es a titles =
          (if b p then titles m else 0) + result es b titles
        rw [ih htail]
        by_cases ht : titles m == 0
        · have htz : titles m = 0 := by simpa using ht
          simp [htz]
        · have htn : titles m ≠ 0 := by simpa using ht
          have hmem : page p ∈ demand ((p, m) :: es) titles page := by
            rw [mem_demand]
            exact ⟨p, m, by simp, htn, rfl⟩
          have habp : a p = b p := hab p (hload (page p) hmem)
          simp [habp]
  · intro h f hf
    rw [mem_demand] at hf
    rcases hf with ⟨p, m, hp, ht, hpage⟩
    by_cases hfl : f ∈ loaded
    · exact hfl
    · exfalso
      let a : Nat → Bool := fun k => if k == p then true else false
      let b : Nat → Bool := fun _ => false
      have hab : ∀ k, page k ∈ loaded → a k = b k := by
        intro k hk
        by_cases hkp : k == p
        · have hkp' : k = p := by simpa using hkp
          subst k
          have hpk : page p ∈ loaded := hk
          have hfmem : f ∈ loaded := by simpa [hpage] using hpk
          exact False.elim (hfl hfmem)
        · have hkneq : k ≠ p := by
            intro heqk
            apply hkp
            simp [heqk]
          simp [a, b, hkneq]
      have heq := h a b hab
      have hpos := result_pos_at edges titles p m hp ht
      simp [result] at hpos
      simp [result, a, b] at heq
      rw [sum_zero] at heq
      omega
