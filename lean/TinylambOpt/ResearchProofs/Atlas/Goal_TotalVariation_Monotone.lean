import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TotalVariation_Monotone

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

local notation "tv" => TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure

/-! Brute-force sanity check: every nondecreasing series of length <= 5
    over [-3, 3]. `false` below means a counterexample was found. -/
#eval
  let vals : List Int := [-3, -2, -1, 0, 1, 2, 3]
  let rec lists : Nat → List (List Int)
    | 0 => [[]]
    | n + 1 => lists n ++ vals.flatMap (fun a => (lists n).map (fun xs => a :: xs))
  let ls := (lists 5).filter (fun x => x.Pairwise (· ≤ ·))
  (ls.filter (fun x => tv x != spread x)).length == 0

private theorem tv_cons₂ (a b : Int) (xs : Series) :
    tv (a :: b :: xs) = distance a b + tv (b :: xs) := by
  simp [TotalVariation.measure, delta, adj, distance]

/-! If the head is no larger than every element of the tail, it is the minimum. -/
private theorem min?_cons_of_le {a : Int} {xs : Series} (h : ∀ v ∈ xs, a ≤ v) :
    (a :: xs).min? = some a := by
  rw [List.min?_cons]
  cases hm : xs.min? with
  | none => simp
  | some m =>
      have hmspec := (List.min?_eq_some_iff).mp hm
      have ham : a ≤ m := h m hmspec.1
      simp
      omega

/-! A nonempty list has a maximum. -/
private theorem max?_some_of_nonempty (a : Int) (xs : Series) :
    ∃ m, (a :: xs).max? = some m := by
  induction xs with
  | nil => simp
  | cons c cs ih =>
      rw [List.max?_cons]
      simp

/-! Dropping a head that is no larger than the new head leaves the spread
    larger by exactly the first jump. -/
private theorem spread_cons₂ (a b : Int) (bs : Series)
    (hab : a ≤ b) (hamin : ∀ v ∈ bs, a ≤ v) (hbmin : ∀ v ∈ bs, b ≤ v) :
    spread (a :: b :: bs) = (b - a) + spread (b :: bs) := by
  have htail : ∀ v ∈ b :: bs, a ≤ v := by
    intro v hv
    rcases List.mem_cons.mp hv with rfl | hv
    · exact hab
    · exact hamin v hv
  have hmin₁ : (a :: b :: bs).min? = some a := by
    rw [List.min?_cons]
    rw [min?_cons_of_le hbmin]
    simp
    omega
  have hmax : (a :: b :: bs).max?.getD 0 = (b :: bs).max?.getD 0 := by
    rw [List.max?_cons]
    obtain ⟨m, hm⟩ := max?_some_of_nonempty b bs
    have hmspec := (List.max?_eq_some_iff).mp hm
    have ham : a ≤ m := htail m hmspec.1
    simp [hm]
    omega
  unfold spread
  rw [hmin₁]
  simp only [Option.getD_some]
  rw [hmax]
  rw [min?_cons_of_le hbmin]
  simp only [Option.getD_some]
  omega

theorem sound_TotalVariation_Monotone : TotalVariation.Monotone.Goal := by
  intro x
  induction x with
  | nil =>
      intro hx
      simp [TotalVariation.measure, delta, adj, spread]
  | cons a xs ih =>
      intro hx
      cases xs with
      | nil =>
          simp [TotalVariation.measure, delta, adj, spread]
      | cons b bs =>
          have h := List.pairwise_cons.mp hx
          have hhead : ∀ v ∈ b :: bs, a ≤ v := h.1
          have htailmono : (b :: bs).Pairwise (· ≤ ·) := h.2
          have hb : ∀ v ∈ bs, b ≤ v := (List.pairwise_cons.mp htailmono).1
          have hab : a ≤ b := hhead b (by simp)
          have hamin : ∀ v ∈ bs, a ≤ v := by
            intro v hv
            exact hhead v (by simp [hv])
          have htv : tv (a :: b :: bs) = (b - a) + tv (b :: bs) := by
            rw [tv_cons₂]
            have hnonneg : 0 ≤ b - a := by omega
            simp [distance, Int.natAbs_of_nonneg hnonneg]
          rw [htv, ih htailmono, spread_cons₂ a b bs hab hamin hb]

end TinylambOpt.ResearchProofs.Atlas.Goal_TotalVariation_Monotone
