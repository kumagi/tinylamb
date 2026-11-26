import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# RollupTagSeparatesNullRankPartitions

Grouping tags distinguish rollup-generated NULLs from data NULLs when assigning rank
partitions. The executor can route both kinds of rows to the appropriate sorts.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RollupTagSeparatesNullRankPartitions
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem pairwise_reverse {α : Type} {R : α → α → Prop} : ∀ (l : List α),
    l.Pairwise R → l.reverse.Pairwise (fun a b => R b a) := by
  intro l h
  rw [List.pairwise_iff_getElem] at h ⊢
  intro i j hi hj hij
  have hi' : l.length - 1 - j < l.length := by
    have hlen : l.reverse.length = l.length := by simp
    rw [hlen] at hi hj
    omega
  have hj' : l.length - 1 - i < l.length := by
    have hlen : l.reverse.length = l.length := by simp
    rw [hlen] at hi hj
    omega
  have hlt : l.length - 1 - j < l.length - 1 - i := by
    have hlen : l.reverse.length = l.length := by simp
    rw [hlen] at hi hj
    omega
  have hh := h (l.length - 1 - j) (l.length - 1 - i) hi' hj' hlt
  simpa [List.getElem_reverse hi, List.getElem_reverse hj] using hh

theorem desc : ∀ (l : List Int) (sales : Int),
    l.Pairwise (fun a b => decide (b ≤ a) = true) →
    (l.takeWhile (fun x => decide (sales < x))).length =
      List.countP (fun x => decide (sales < x)) l := by
  intro l
  induction l with
  | nil => intro sales hp; simp [List.countP_eq_length_filter]
  | cons a t ih =>
      intro sales hp
      have hp' := List.pairwise_cons.mp hp
      by_cases hlt : sales < a
      · have ha : decide (sales < a) = true := by simp [hlt]
        simp only [List.takeWhile_cons]
        rw [if_pos ha]
        simp only [List.length_cons]
        rw [ih sales hp'.2]
        rw [List.countP_cons]
        simp [ha]
      · have ha : decide (sales < a) = false := by simp [hlt]
        have hnot : ∀ b ∈ t, ¬ decide (sales < b) = true := by
          intro b hb hbs
          have hba : decide (b ≤ a) = true := hp'.1 b hb
          have hba' : b ≤ a := by simpa using hba
          have hb' : sales < b := by simpa using hbs
          omega
        have hz : List.countP (fun x : Int => decide (sales < x)) t = 0 :=
          List.countP_eq_zero.mpr hnot
        simp only [List.takeWhile_cons]
        rw [if_neg (by simpa using ha : ¬ decide (sales < a) = true)]
        rw [List.countP_cons]
        simp [ha]
        simp [hz]

theorem count_partition : ∀ (xs : List ((Nat × Option Nat) × Int)) (tag : Nat) (parent : Option Nat) (sales : Int),
    List.countP (fun x : Int => decide (sales < x))
        ((xs.filter (fun r => r.1 == (tag, parent))).map Prod.snd) =
      countWhere xs (fun r => r.1 == (tag, parent) && sales < r.2) := by
  intro xs tag parent sales
  rw [List.countP_eq_length_filter]
  simp only [countWhere]
  induction xs with
  | nil => simp
  | cons r rs ih =>
      by_cases hk : r.1 == (tag, parent)
      · by_cases hs : sales < r.2 <;> simp [hk, hs, ih]
      · simp [hk, ih]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.RollupTagSeparatesNullRankPartitions.Goal := by
  intro xs tag parent sales
  dsimp only
  let partition := (xs.filter (fun r => r.1 == (tag, parent))).map Prod.snd
  let le : Int → Int → Bool := fun a b => decide (a ≤ b)
  change 1 + ((partition.mergeSort le).reverse.takeWhile (fun x : Int => decide (sales < x))).length =
    1 + countWhere xs (fun r => r.1 == (tag, parent) && sales < r.2)
  have ht : ∀ a b c : Int, decide (a ≤ b) = true → decide (b ≤ c) = true → decide (a ≤ c) = true := by
    intro a b c hab hbc
    simp at hab hbc ⊢
    omega
  have hto : ∀ a b : Int, (decide (a ≤ b) || decide (b ≤ a)) = true := by
    intro a b
    by_cases h : a ≤ b
    · simp [h]
    · have h' : b ≤ a := by omega
      simp [h, h']
  have hpAsc : (partition.mergeSort le).Pairwise (fun a b => decide (a ≤ b) = true) :=
    List.pairwise_mergeSort (le := le) ht hto partition
  have hpDesc : ((partition.mergeSort le).reverse).Pairwise (fun a b => decide (b ≤ a) = true) :=
    pairwise_reverse (partition.mergeSort le) hpAsc
  have hdesc := desc ((partition.mergeSort le).reverse) sales hpDesc
  rw [List.countP_reverse] at hdesc
  rw [(List.mergeSort_perm partition le).countP_eq (fun x : Int => decide (sales < x))] at hdesc
  rw [hdesc]
  exact congrArg (fun n => 1 + n) (count_partition xs tag parent sales)

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RollupTagSeparatesNullRankPartitions

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.RollupTagSeparatesNullRankPartitions.sound
