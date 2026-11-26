import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_Decoder

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes

private abbrev hiCount (m : Machine) (n q : Nat) : Nat :=
  ((m.step q true).map (count m n)).getD 0

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + hiCount m n q := by
  simp [count, lowCount, hiCount]

/-! A rank is always strictly below the number of accepted words of that
length: the tail rank fits its own interval, and that interval is one of the
two disjoint intervals summed by `count`. -/
private theorem rank_bound (m : Machine) : ∀ (xs : List Bool) (q r : Nat),
    rank m q xs = some r → r < count m xs.length q
  | [], q, r, h => by
    unfold rank at h
    by_cases hacc : m.accept q = true
    · simp [hacc] at h
      subst h
      simp [count, hacc]
    · have hacc' : m.accept q = false := by cases h' : m.accept q <;> simp_all
      simp [hacc'] at h
  | b :: bs, q, r, h => by
    have hlen : (b :: bs).length = bs.length + 1 := by simp
    rw [hlen, count_succ]
    unfold rank at h
    cases hs : m.step q b with
    | none => simp [hs] at h
    | some q' =>
      simp [hs] at h
      cases hr : rank m q' bs with
      | none => simp [hr] at h
      | some tail =>
        simp [hr, Option.bind_some] at h
        subst h
        have ih := rank_bound m bs q' tail hr
        cases b with
        | false =>
          simp [lowCount, hs] at ih ⊢
          omega
        | true =>
          simp [lowCount, hiCount, hs] at ih ⊢
          omega

/-! Ranking then unranking recovers the bitmap: peeling one bit, the tail code
lies in the low interval when the bit is `false` and in the shifted high
interval when it is `true`. -/
private theorem decoder_all (m : Machine) : decoder m := by
  intro q xs r hrank
  induction xs generalizing q r with
  | nil =>
    unfold rank at hrank
    by_cases hacc : m.accept q = true
    · simp [hacc] at hrank
      subst hrank
      unfold unrank
      simp [hacc]
    · have hacc' : m.accept q = false := by cases h' : m.accept q <;> simp_all
      simp [hacc'] at hrank
  | cons b bs ih =>
    unfold rank at hrank
    cases hs : m.step q b with
    | none => simp [hs] at hrank
    | some q' =>
      simp [hs] at hrank
      cases ht : rank m q' bs with
      | none => simp [ht] at hrank
      | some tail =>
        simp [ht, Option.bind_some] at hrank
        subst hrank
        have ih' := ih q' tail ht
        have hbound := rank_bound m bs q' tail ht
        have hlen : (b :: bs).length = bs.length + 1 := by simp
        unfold unrank
        simp only [hlen]
        cases b with
        | false =>
          have hlt : ¬ (lowCount m bs.length q ≤ tail) := by
            simp [lowCount, hs] at hbound ⊢
            omega
          have hdec : decide (lowCount m bs.length q ≤ tail) = false := by simp [hlt]
          simp [hdec, hs, ih', Option.bind_some]
        | true =>
          have hle : lowCount m bs.length q ≤ tail + lowCount m bs.length q := by omega
          have hdec : decide (lowCount m bs.length q ≤ tail + lowCount m bs.length q) = true := by simp [hle]
          simp [hdec, hs, Option.bind_some]
          have : tail + lowCount m bs.length q - lowCount m bs.length q = tail := by omega
          simp [this, ih']

theorem sound_Decoder : PrefixOnes.Decoder.Goal := by
  exact decoder_all model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_Decoder
