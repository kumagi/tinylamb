import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
import TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_Multiplicity

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RisePairs_MergeSorted

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

private def riseNat (x : Series) : Nat := (pairs x).countP rising
private def tieNat (x : Series) : Nat := (pairs x).countP (fun p => p.1 == p.2)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  simp only [pairs, List.zipIdx_cons', List.flatMap_cons, List.flatMap_map,
    List.drop_succ_cons, List.drop_zero]
  rfl

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [h, ih]

private theorem riseNat_cons (a : Int) (xs : Series) :
    riseNat (a :: xs) = riseNat xs + xs.countP (fun b => a < b) := by
  unfold riseNat
  rw [pairs_cons, List.countP_append, countP_map_comp]
  simp only [rising]
  omega

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  simpa only [beq_iff_eq] using eq_comm

private theorem tieNat_cons (a : Int) (xs : Series) :
    tieNat (a :: xs) = tieNat xs + xs.count a := by
  unfold tieNat
  rw [pairs_cons, List.countP_append, countP_map_comp]
  have hc : xs.countP (fun b => a == b) = xs.count a := by
    rw [List.count_eq_countP]
    congr 1
    funext b
    exact beq_comm_int a b
  rw [hc]
  omega

private theorem trichotomy_count (a : Int) (xs : Series) :
    xs.countP (fun b => a < b) + xs.countP (fun b => b < a) + xs.count a = xs.length := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.countP_cons, List.count_cons, List.length_cons]
      by_cases hab : a < b
      · have hba : ¬ b < a := by omega
        have hne : b ≠ a := by omega
        simp [hab, hba, hne]
        omega
      · by_cases hba : b < a
        · have hne : b ≠ a := by omega
          simp [hab, hba, hne]
          omega
        · have heq : b = a := by omega
          subst b
          simp
          omega

private theorem sorted_no_worse (a : Int) (xs : Series) (h : ∀ b ∈ xs, a ≤ b) :
    xs.countP (fun b => b < a) = 0 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      have hb : a ≤ b := h b (by simp)
      have hnlt : ¬ b < a := by omega
      have htail : ∀ c ∈ bs, a ≤ c := by
        intro c hc
        exact h c (by simp [hc])
      simp [hnlt, ih htail]

private theorem choose2_twice (n : Nat) : 2 * choose2 n = n * (n - 1) := by
  unfold choose2
  rcases Nat.mod_two_eq_zero_or_one n with h0 | h1
  · have he : n = 2 * (n / 2) := by omega
    rw [he]
    have hd : 2 ∣ (2 * (n / 2)) * (2 * (n / 2) - 1) := by
      refine ⟨(n / 2) * (2 * (n / 2) - 1), ?_⟩
      simp [Nat.mul_assoc]
    have hc := Nat.mul_div_cancel' hd
    omega
  · have he : n = 2 * (n / 2) + 1 := by omega
    rw [he]
    have hsub : 2 * (n / 2) + 1 - 1 = 2 * (n / 2) := by omega
    rw [hsub]
    have hd : 2 ∣ (2 * (n / 2) + 1) * (2 * (n / 2)) := by
      refine ⟨(2 * (n / 2) + 1) * (n / 2), ?_⟩
      simp [Nat.mul_assoc, Nat.mul_comm]
    have hc := Nat.mul_div_cancel' hd
    omega

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  have h1 := choose2_twice n
  have h2 := choose2_twice (n + 1)
  have hsub : n + 1 - 1 = n := by omega
  simp only [hsub] at h2
  have hp : n * (n - 1) + 2 * n = (n + 1) * n := by
    cases n with
    | zero => rfl
    | succ m =>
        simp only [Nat.add_sub_cancel, Nat.succ_mul, Nat.mul_succ]
        omega
  omega

private theorem main_nat (x : Series) (hs : x.Pairwise (· ≤ ·)) :
    riseNat x + tieNat x = choose2 x.length := by
  induction x with
  | nil => simp [riseNat, tieNat, pairs, choose2]
  | cons a xs ih =>
      cases xs with
      | nil => simp [riseNat, tieNat, pairs, choose2]
      | cons b bs =>
          have htail : (b :: bs).Pairwise (· ≤ ·) := (List.pairwise_cons.mp hs).2
          have hall : ∀ c ∈ b :: bs, a ≤ c := by
            intro c hc
            exact (List.pairwise_cons.mp hs).1 c hc
          have hih := ih htail
          rw [riseNat_cons, tieNat_cons, List.length_cons, choose2_succ]
          have hz := sorted_no_worse a (b :: bs) hall
          have htri := trichotomy_count a (b :: bs)
          omega

theorem sound_RisePairs_MergeSorted : RisePairs.MergeSorted.Goal := by
  intro x hs
  unfold RisePairs.measure
  have hsum : ((pairs x).countP rising : Int) +
      ((pairs x).countP (fun p => p.1 == p.2) : Int) = (choose2 x.length : Int) := by
    exact_mod_cast main_nat x hs
  have ht := TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_Multiplicity.sound_TiePairs_Multiplicity x
  unfold TiePairs.measure at ht
  have hpred : (fun p : Int × Int => match p with | (a, b) => a == b) =
      (fun p : Int × Int => p.1 == p.2) := by
    funext p
    cases p
    rfl
  rw [hpred] at ht
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_RisePairs_MergeSorted
