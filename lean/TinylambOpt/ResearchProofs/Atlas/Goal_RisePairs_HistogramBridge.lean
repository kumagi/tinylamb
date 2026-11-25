import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RisePairs_HistogramBridge

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs

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

private theorem cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem pairs_countP_append (P : Int × Int → Bool) (x y : Series) :
    (pairs (x ++ y)).countP P =
      (pairs x).countP P + (pairs y).countP P + (cart x y).countP P := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      simp only [List.countP_append, List.map_append]
      rw [countP_map_comp, countP_map_comp]
      rw [ih]
      omega

private theorem cart_countP (P : Int × Int → Bool) (x y : Series) :
    (cart x y).countP P = (x.map (fun a => y.countP (fun b => P (a, b)))).sum := by
  induction x with
  | nil => simp [cart]
  | cons a xs ih =>
      rw [cart_cons]
      simp only [List.countP_append, List.map_cons, List.sum_cons]
      rw [countP_map_comp, ih]

private theorem beq_comm {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have heq : a = b := beq_iff_eq.mp h
    simp [heq]
  · intro h
    have heq : b = a := beq_iff_eq.mp h
    simp [heq]

private theorem sum_map_eq_count_head (k : Int) (ks : List Int) (f : Int → Nat) :
    (ks.map f).sum = ks.count k * f k + ((ks.filter (fun b => !(b == k))).map f).sum := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hxeq : x = k := beq_iff_eq.mp hx
        subst x
        simp [ih, Nat.succ_mul, Nat.add_assoc, Nat.add_comm]
      · have hkx : (k == x) = false := by simpa [beq_comm k x] using hx
        simp [List.count_cons, hx, ih, Nat.add_left_comm]

private theorem dedup_weight_sum (f : Int → Nat) (keys : List Int) :
    ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum := by
  let motive : Nat → Prop := fun n => ∀ keys : List Int, keys.length = n →
      ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum
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
        have hhead : (List.count k (k :: ks)) * f k = f k + ks.count k * f k := by
          rw [List.count_cons]
          simp [Nat.add_mul, Nat.add_comm]
        rw [hhead]
        have hrest_count : ∀ a ∈ rest.eraseDups, (List.count a (k :: ks)) = rest.count a := by
          intro a ha
          have hane : (a == k) = false := by
            have hamem : a ∈ rest := List.mem_eraseDups.mp ha
            have hamem' : a ∈ ks.filter (fun b => !(b == k)) := by simpa [rest] using hamem
            have haf : (!(a == k)) = true := (List.mem_filter.mp hamem').2
            simpa using haf
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          have hka : (k == a) = false := by simpa [beq_comm k a] using hane
          rw [List.count_cons, hka, hrestc]
          simp
        rw [show ((rest.eraseDups.map fun a => List.count a (k :: ks) * f a).sum) =
             ((rest.eraseDups.map fun a => rest.count a * f a).sum) by
             apply congrArg List.sum
             apply List.map_congr_left
             intro a ha
             rw [hrest_count a ha]]
        rw [ihrest]
        have hks : (ks.map f).sum = ks.count k * f k + (rest.map f).sum := by
          simpa [rest] using sum_map_eq_count_head k ks f
        rw [hks]
        omega
  exact H keys rfl

theorem sound_RisePairs_HistogramBridge : RisePairs.HistogramBridge.Goal := by
  intro x y
  unfold RisePairs.measure
  rw [pairs_countP_append rising x y]
  have hcross : (cart x y).countP rising =
      (x.eraseDups.map (fun a => x.count a * y.countP (a < ·))).sum := by
    rw [cart_countP rising x y]
    rw [show (x.map (fun a => y.countP (fun b => rising (a, b)))).sum =
           (x.map (fun a => y.countP (a < ·))).sum by rfl]
    exact (dedup_weight_sum (fun a => y.countP (a < ·)) x).symm
  rw [hcross]
  simp only [Int.natCast_add]

end TinylambOpt.ResearchProofs.Atlas.Goal_RisePairs_HistogramBridge
