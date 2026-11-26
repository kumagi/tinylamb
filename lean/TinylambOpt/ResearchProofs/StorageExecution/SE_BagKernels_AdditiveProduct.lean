import TinylambOpt.ResearchProofs.StorageExecution.SE_BagKernels_Common

open TinylambOpt.ResearchProofs.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels

namespace AdditiveProductProofs

open AdditiveProduct

theorem hexp : ∀ (a : List Nat) (b : List Nat),
    bag.expand (a, b) = a.flatMap (fun x => b.map (x+·)) := fun _ _ => rfl

theorem hkeys : ∀ (a : List Nat) (b : List Nat),
    bag.keys (a, b) = a.eraseDups.flatMap (fun x => b.eraseDups.map (x+·)) := fun _ _ => rfl

theorem count_flatMap : ∀ (s : List Nat) (f : Nat → List Nat) (k : Nat),
    (s.flatMap f).count k = (s.map (fun x => (f x).count k)).sum := by
  intro s f
  induction s with
  | nil => intro k; rfl
  | cons a r ih =>
      intro k
      rw [List.flatMap_cons, List.count_append, List.map_cons, List.sum_cons, ih]

theorem count_map_add (x : Nat) : ∀ (b : List Nat) (k : Nat),
    (b.map (x+·)).count k = if x ≤ k then b.count (k - x) else 0 := by
  intro b
  induction b with
  | nil =>
      intro k
      rw [List.map_nil, List.count_nil]
      by_cases hx : x ≤ k
      · rw [if_pos hx, List.count_nil]
      · rw [if_neg hx]
  | cons y r ih =>
      intro k
      rw [List.map_cons, List.count_cons, ih, List.count_cons]
      by_cases hx : x ≤ k
      · simp [hx]
        by_cases h : x + y = k <;> by_cases h2 : y = k - x <;> simp [h, h2] <;> omega
      · simp [hx]
        omega

theorem mass_eq_ap : ∀ (a b : List Nat) (k : Nat),
    (a.flatMap (fun x => b.map (x+·))).count k =
      (a.eraseDups.map (fun x => if x ≤ k then a.count x * b.count (k - x) else 0)).sum := by
  intro a b k
  rw [count_flatMap]
  rw [show (a.map (fun x => (b.map (x+·)).count k)) =
      a.map (fun x => if x ≤ k then b.count (k - x) else 0) from by
        congr
        funext x
        rw [count_map_add]]
  rw [(sum_count_mul a (fun x => if x ≤ k then b.count (k - x) else 0)).symm]
  refine sum_map_congr_of_mem a.eraseDups
    (fun x => a.count x * if x ≤ k then b.count (k - x) else 0)
    (fun x => if x ≤ k then a.count x * b.count (k - x) else 0) ?_
  intro z _
  by_cases hx : z ≤ k
  · rw [if_pos hx, if_pos hx]
  · rw [if_neg hx, if_neg hx, Nat.mul_zero]

theorem wf : WellFormed bag := by
  constructor
  · intro p k
    obtain ⟨a, b⟩ := p
    show (a.flatMap (fun x => b.map (x+·))).count k = _
    exact mass_eq_ap a b k
  · intro p k hk
    obtain ⟨a, b⟩ := p
    rw [hexp] at hk
    rw [hkeys]
    rw [List.mem_flatMap] at hk ⊢
    obtain ⟨x, hxa, hxk⟩ := hk
    obtain ⟨y, hyb, hxy⟩ := List.mem_map.mp hxk
    refine ⟨x, List.mem_eraseDups.mpr hxa, ?_⟩
    rw [List.mem_map]
    exact ⟨y, List.mem_eraseDups.mpr hyb, hxy⟩

theorem sound_DistinctOutput : DistinctOutput.Goal := wf_distinctOutput bag wf
theorem sound_FilteredMoment : FilteredMoment.Goal := wf_filteredMoment bag wf
theorem sound_OrderStatistic : OrderStatistic.Goal := wf_orderStatistic bag wf
theorem sound_JoinFanout : JoinFanout.Goal := wf_joinFanout bag wf
theorem sound_AntiRevenue : AntiRevenue.Goal := wf_antiRevenue bag wf
theorem sound_SelfPairs : SelfPairs.Goal := wf_selfPairs bag wf
theorem sound_IntersectAll : IntersectAll.Goal := wf_intersectAll bag wf
theorem sound_OuterWeight : OuterWeight.Goal := wf_outerWeight bag wf

theorem sum_map_id : ∀ (l : List Nat), (l.map id).sum = l.sum := by
  intro l
  induction l with
  | nil => rfl
  | cons a r ih => rw [List.map_cons, List.sum_cons, List.sum_cons, ih]; simp

theorem sum_map_add_const : ∀ (b : List Nat) (x : Nat),
    (b.map (x+·)).sum = x * b.length + b.sum := by
  intro b
  induction b with
  | nil => intro x; simp
  | cons y r ih =>
      intro x
      rw [List.map_cons, List.sum_cons, ih, List.length_cons, List.sum_cons,
        Nat.mul_add, Nat.mul_one]
      omega

theorem sum_map_mul_left : ∀ (b : List Nat) (c : Nat),
    (b.map (fun y => c * y)).sum = c * b.sum := by
  intro b
  induction b with
  | nil => intro c; rfl
  | cons y r ih =>
      intro c
      rw [List.map_cons, List.sum_cons, ih, List.sum_cons, ← Nat.mul_add]

theorem sum_map_add : ∀ (l : List Nat) (f g : Nat → Nat),
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  intro l f g
  induction l with
  | nil => rfl
  | cons a r ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

theorem two_mul' : ∀ (n : Nat), 2 * n = n + n := by
  intro n
  omega

theorem sq_add (x y : Nat) : (x + y) * (x + y) = x * x + ((2 * x) * y + y * y) := by
  rw [Nat.add_mul, Nat.mul_add, Nat.mul_add, two_mul' x, Nat.add_mul, Nat.mul_comm y x]
  omega

theorem pr_sum : ∀ (a b : List Nat),
    ((a.flatMap (fun x => b.map (x+·))).map id).sum = a.sum * b.length + b.sum * a.length := by
  intro a
  induction a with
  | nil => intro b; simp
  | cons x r ih =>
      intro b
      rw [List.flatMap_cons, List.map_append, List.sum_append, ih]
      rw [show ((b.map (x+·)).map id).sum = x * b.length + b.sum from by
        rw [sum_map_id, sum_map_add_const]]
      rw [List.sum_cons, List.length_cons, Nat.add_mul, Nat.mul_add, Nat.mul_one]
      omega

theorem sm_sum : ∀ (a b : List Nat),
    ((a.flatMap (fun x => b.map (x+·))).map (fun z => z * z)).sum =
      b.length * (a.map (fun x => x * x)).sum + 2 * a.sum * b.sum +
        a.length * (b.map (fun y => y * y)).sum := by
  intro a
  induction a with
  | nil => intro b; simp
  | cons x r ih =>
      intro b
      rw [List.flatMap_cons, List.map_append, List.sum_append]
      have hblk : ((b.map (x+·)).map (fun z => z * z)).sum =
          (b.map (fun y => y * y)).sum + ((2 * x) * b.sum + b.length * (x * x)) := by
        rw [show ((b.map (x+·)).map (fun z => z * z)) = b.map (fun y => (x + y) * (x + y))
            from by
              induction b with
              | nil => rfl
              | cons c t ih => rw [List.map_cons, List.map_cons, List.map_cons, ih]]
        rw [show (b.map (fun y => (x + y) * (x + y))) =
            b.map (fun y => x * x + ((2 * x) * y + y * y)) from by
              congr
              funext y
              rw [sq_add]]
        rw [sum_map_add, sum_map_add, sum_map_const, sum_map_mul_left]
        omega
      rw [hblk, ih]
      simp only [List.map_cons, List.sum_cons, List.length_cons, Nat.add_mul, Nat.mul_add,
        Nat.one_mul]
      omega

theorem sound_ProductRevenue : ProductRevenue.Goal := by
  intro a b
  rw [weighted_eq bag wf]
  show ((AdditiveProduct.bag.expand (a, b)).map id).sum = _
  rw [hexp]
  exact pr_sum a b

theorem sound_SecondMoment : SecondMoment.Goal := by
  intro a b
  rw [weighted_eq bag wf]
  show ((AdditiveProduct.bag.expand (a, b)).map (fun z => z * z)).sum = _
  rw [hexp]
  exact sm_sum a b

end AdditiveProductProofs
