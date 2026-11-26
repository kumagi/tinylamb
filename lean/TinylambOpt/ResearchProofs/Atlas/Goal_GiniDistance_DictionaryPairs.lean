import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `GiniDistance.DictionaryPairs` — `∀ x, 2*measure x = ((cart x.eraseDups
  x.eraseDups).map (fun (a,b) => distance a b * x.count a * x.count b)).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GiniDistance_DictionaryPairs

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.GiniDistance

/-! ### Exhaustive sanity check on small inputs -/

private def rowsOf : Nat → List Int → List (List Int)
  | 0, _ => [[]]
  | n + 1, pool => pool.flatMap (fun r => (rowsOf n pool).map (r :: ·))

private def smallPool : List Int := [-2, -1, 0, 1, 2]

#eval (rowsOf 5 smallPool).all (fun x =>
  decide (2 * measure x =
    ((cart x.eraseDups x.eraseDups).map (fun (a,b) => distance a b * x.count a * x.count b)).sum))

/-! ### Counting by distinct value -/

private theorem beq_comm {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have heq : a = b := beq_iff_eq.mp h
    simp [heq]
  · intro h
    have heq : b = a := beq_iff_eq.mp h
    simp [heq]

private theorem sum_map_eq_count_head (k : Int) (ks : List Int) (f : Int → Int) :
    (ks.map f).sum = (ks.count k : Int) * f k + ((ks.filter (fun b => !(b == k))).map f).sum := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hxeq : x = k := beq_iff_eq.mp hx
        subst x
        simp [ih, Int.natCast_add, Int.add_mul]
        omega
      · simp [List.count_cons, hx, ih]
        omega

private theorem dedup_weight_sum (f : Int → Int) (keys : List Int) :
    ((keys.eraseDups.map fun k => (keys.count k : Int) * f k).sum) = (keys.map f).sum := by
  let motive : Nat → Prop := fun n => ∀ keys : List Int, keys.length = n →
      ((keys.eraseDups.map fun k => (keys.count k : Int) * f k).sum) = (keys.map f).sum
  have H : motive keys.length := by
    apply Nat.strongRecOn
    intro n ih keys hlen
    cases keys with
    | nil => simp
    | cons k ks =>
        let rest := ks.filter (fun b => !(b == k))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          have hle := List.length_filter_le (fun b : Int => !(b == k)) ks
          omega
        have ihrest := ih rest.length hrestlen rest rfl
        rw [List.eraseDups_cons]
        simp only [List.map_cons, List.sum_cons]
        have hhead : (((List.count k (k :: ks)) : Nat) : Int) * f k = f k + (ks.count k : Int) * f k := by
          rw [show List.count k (k :: ks) = ks.count k + 1 by simp]
          rw [Int.natCast_add, Int.natCast_one, Int.add_mul]
          simp
          omega
        rw [hhead]
        have hrest_count : ∀ a ∈ rest.eraseDups, (List.count a (k :: ks)) = rest.count a := by
          intro a ha
          have hamem : a ∈ rest := List.mem_eraseDups.mp ha
          have hamem' : a ∈ ks.filter (fun b => !(b == k)) := by simpa [rest] using hamem
          have haf : (!(a == k)) = true := (List.mem_filter.mp hamem').2
          have hane : (a == k) = false := by simpa using haf
          have hka : (k == a) = false := by simpa [beq_comm k a] using hane
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          rw [List.count_cons, hka, hrestc]
          simp
        rw [show ((rest.eraseDups.map fun a => ((List.count a (k :: ks) : Nat) : Int) * f a).sum) =
             ((rest.eraseDups.map fun a => (rest.count a : Int) * f a).sum) by
             apply congrArg List.sum
             apply List.map_congr_left
             intro a ha
             rw [hrest_count a ha]]
        rw [ihrest]
        have hks : (ks.map f).sum = (ks.count k : Int) * f k + (rest.map f).sum := by
          simpa [rest] using sum_map_eq_count_head k ks f
        rw [hks]
        omega
  exact H keys rfl

/-! ### Absolute deviation -/

private theorem distance_self (a : Int) : distance a a = 0 := by
  unfold distance
  simp

private theorem distance_symm (a b : Int) : distance a b = distance b a := by
  unfold distance
  have h : a - b = -(b - a) := by omega
  rw [h, Int.natAbs_neg]

private theorem zipIdx_succ (ys : Series) (i : Nat) :
    ys.zipIdx (i + 1) = (ys.zipIdx i).map (fun (v, j) => (v, j + 1)) := by
  induction ys generalizing i with
  | nil => simp
  | cons a ys ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons, ih (i + 1)]
      simp

private theorem pairs_cons (a : Int) (ys : Series) :
    pairs (a :: ys) = ys.map (fun b => (a, b)) ++ pairs ys := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, Nat.zero_add, List.drop_one]
  congr 1
  rw [zipIdx_succ ys 0]
  simp only [List.flatMap_map]
  simp

private theorem measure_cons (a : Int) (ys : Series) :
    measure (a :: ys) = (ys.map (fun b => distance a b)).sum + measure ys := by
  unfold GiniDistance.measure
  rw [pairs_cons, List.map_append, List.sum_append]
  congr 1
  rw [List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro b hb
  rfl

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_map_const_mul {α : Type} (xs : List α) (c : Int) (f : α → Int) :
    (xs.map (fun x => c * f x)).sum = c * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih, Int.mul_add]

/-! ### Cartesian double sum equals twice the pairwise sum -/

private theorem cart_sum (f : Int → Int → Int) (xs ys : Series) :
    ((cart xs ys).map (fun p => f p.1 p.2)).sum =
      (xs.map (fun a => (ys.map (fun b => f a b)).sum)).sum := by
  unfold cart
  induction xs with
  | nil => simp
  | cons a xs ih =>
      simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons]
      rw [ih]
      rw [List.map_map]
      have hmap : (List.map ((fun p => f p.1 p.2) ∘ fun b => (a, b)) ys).sum =
          (List.map (fun b => f a b) ys).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro b hb
        rfl
      rw [hmap]

private theorem cart_self_sum (x : Series) :
    (x.map (fun a => (x.map (fun b => distance a b)).sum)).sum = 2 * measure x := by
  induction x with
  | nil => simp [GiniDistance.measure, pairs]
  | cons a ys ih =>
      rw [measure_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [distance_self]
      simp
      rw [sum_map_add, ih]
      have hsym : (ys.map (fun u => distance u a)).sum =
          (ys.map (fun b => distance a b)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro u hu
        rw [distance_symm]
      rw [hsym]
      omega

/-! ### Compressing the two Cartesian coordinates to distinct values -/

private theorem compressed_cart (x : Series) :
    ((cart x.eraseDups x.eraseDups).map
      (fun (a, b) => distance a b * (x.count a : Int) * (x.count b : Int))).sum =
    (x.map (fun a => (x.map (fun b => distance a b)).sum)).sum := by
  have hcs := cart_sum (fun a b : Int =>
    distance a b * (x.count a : Int) * (x.count b : Int)) x.eraseDups x.eraseDups
  rw [hcs]
  calc
    (x.eraseDups.map (fun a =>
        (x.eraseDups.map (fun b =>
          distance a b * (x.count a : Int) * (x.count b : Int))).sum)).sum
        = (x.eraseDups.map (fun a =>
            (x.count a : Int) *
              (x.eraseDups.map (fun b => (x.count b : Int) * distance a b)).sum)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro a ha
            rw [show (x.eraseDups.map (fun b =>
                  distance a b * (x.count a : Int) * (x.count b : Int))).sum =
                  (x.eraseDups.map (fun b => (x.count a : Int) *
                    ((x.count b : Int) * distance a b))).sum by
                  apply congrArg List.sum
                  apply List.map_congr_left
                  intro b hb
                  ac_rfl]
            rw [sum_map_const_mul]
    _ = (x.eraseDups.map (fun a =>
            (x.count a : Int) * (x.map (fun b => distance a b)).sum)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro a ha
            rw [dedup_weight_sum (fun b => distance a b) x]
    _ = (x.map (fun a => (x.map (fun b => distance a b)).sum)).sum := by
            rw [dedup_weight_sum (fun a => (x.map (fun b => distance a b)).sum) x]

/-! ### Main theorem -/

theorem sound_GiniDistance_DictionaryPairs : GiniDistance.DictionaryPairs.Goal := by
  intro x
  exact (cart_self_sum x).symm.trans (compressed_cart x).symm

end TinylambOpt.ResearchProofs.Atlas.Goal_GiniDistance_DictionaryPairs
