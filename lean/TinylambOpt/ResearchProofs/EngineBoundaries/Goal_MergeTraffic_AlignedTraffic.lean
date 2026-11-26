import TinylambOpt.ResearchGoals.EngineBoundaries.MergeTraffic

/-!
A merge or compaction can reorganize its input streams subject to the modeled ordering,
traffic, and retention constraints.

Proved

- `AlignedTraffic` — If every run occupies whole pages, multiplying page traffic
  recovers byte traffic.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_MergeTraffic_AlignedTraffic

open TinylambOpt.ResearchGoals.EngineBoundaries.MergeTraffic

private theorem pages_mul (p n : Nat) (hp : 0 < p) (hd : p ∣ n) : pages p n * p = n := by
  rcases hd with ⟨k, rfl⟩
  unfold pages
  have hquot : (p * k + p - 1) / p = k := by
    have h1 : k*p ≤ p*k + p - 1 := by
      rw [Nat.mul_comm k p]
      omega
    have h2 : p*k + p - 1 < (k+1)*p := by
      rw [Nat.add_mul, Nat.one_mul]
      rw [Nat.mul_comm k p]
      omega
    exact Nat.div_eq_of_lt_le h1 h2
  rw [hquot]
  rw [Nat.mul_comm k p]

private theorem mass_dvd_of_aligned (p : Nat) : ∀ t : Tree,
    AlignedTraffic.Goal.aligned p t → p ∣ mass t := by
  intro t
  induction t with
  | leaf n =>
      intro h
      exact h
  | merge a b iha ihb =>
      intro h
      exact Nat.dvd_add (iha h.1) (ihb h.2)


theorem sound : AlignedTraffic.Goal := by
  intro p t hp halt
  induction t with
  | leaf n =>
      simpa [pageIO, io] using pages_mul p n hp halt
  | merge a b iha ihb =>
      rcases halt with ⟨ha, hb⟩
      have hma : p ∣ mass a := mass_dvd_of_aligned p a ha
      have hmb : p ∣ mass b := mass_dvd_of_aligned p b hb
      have hab : p ∣ mass a + mass b := Nat.dvd_add hma hmb
      simp only [pageIO, io]
      have hpa := pages_mul p (mass a) hp hma
      have hpb := pages_mul p (mass b) hp hmb
      have hpab := pages_mul p (mass a + mass b) hp hab
      simp only [Nat.add_mul]
      have hiA := iha ha
      have hiB := ihb hb
      omega

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_MergeTraffic_AlignedTraffic
