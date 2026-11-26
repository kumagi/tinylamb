import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_CutWeight

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.EdgeDegrees

private theorem beq_comm {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have heq : a = b := beq_iff_eq.mp h
    simp [heq]
  · intro h
    have heq : b = a := beq_iff_eq.mp h
    simp [heq]

private theorem sum_map_eq_count_head {α : Type} [BEq α] [LawfulBEq α]
    (k : α) (ks : List α) (f : α → Nat) :
    (ks.map f).sum = ks.count k * f k + ((ks.filter (fun b => !(b == k))).map f).sum := by
  induction ks with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == k
      · have hxeq : x = k := beq_iff_eq.mp hx
        subst x
        simp [ih, Nat.succ_mul, Nat.add_assoc, Nat.add_comm]
      · simp [List.count_cons, hx, ih, Nat.add_left_comm]

-- For any list, the distinct-key histogram weighted by `f` is the plain key sum.
private theorem dedup_weight_sum {α : Type} [BEq α] [LawfulBEq α]
    (f : α → Nat) (keys : List α) :
    ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum := by
  let motive : Nat → Prop := fun n => ∀ keys : List α, keys.length = n →
      ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum
  have H : motive keys.length := by
    apply Nat.strongRecOn
    intro n ih keys hlen
    cases keys with
    | nil => simp
    | cons k ks =>
        let rest := ks.filter (fun b => ! (b == k))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          have hle := List.length_filter_le (fun b : α => ! (b == k)) ks
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
            have hamem' : a ∈ ks.filter (fun b => ! (b == k)) := by simpa [rest] using hamem
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

private theorem sum_map_fst_snd {α β : Type} (f : α → Nat) (g : β → Nat) (es : List (α × β)) :
    (es.map (fun e => f e.1)).sum + (es.map (fun e => g e.2)).sum =
      (es.map (fun e => f e.1 + g e.2)).sum := by
  induction es with
  | nil => simp
  | cons e es ih =>
      obtain ⟨a, b⟩ := e
      simp [ih, Nat.add_assoc, Nat.add_left_comm]

-- The histogram form of `weighted` sums the kernel over all endpoint occurrences.
private theorem weighted_eq_sum_endpoints (es : List (Nat × Nat)) (f : Nat → Nat) :
    weighted bag es f = (es.map (fun (a, b) => f a + f b)).sum := by
  have hmass : ∀ k : Nat, bag.mass es k = (bag.keys es).count k := by
    intro k
    simp [bag, List.count_append]
  unfold weighted
  calc
    ((bag.keys es).eraseDups.map (fun k => bag.mass es k * f k)).sum
        = ((bag.keys es).eraseDups.map (fun k => (bag.keys es).count k * f k)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro k hk
            rw [hmass k]
    _ = ((bag.keys es).map f).sum := dedup_weight_sum f (bag.keys es)
    _ = ((es.map Prod.fst).map f).sum + ((es.map Prod.snd).map f).sum := by
            simp [bag, List.map_append, List.sum_append]
    _ = (es.map (fun e => f e.1)).sum + (es.map (fun e => f e.2)).sum := by
            simp [Function.comp_def]
    _ = (es.map (fun e => f e.1 + f e.2)).sum := sum_map_fst_snd f f es

private theorem length_filter_eq_sum {α : Type} (p : α → Bool) (l : List α) :
    (l.filter p).length = (l.map (fun x => (p x).toNat)).sum := by
  induction l with
  | nil => simp
  | cons a as ih => cases h : p a <;> simp [h, ih, Nat.add_comm]

private theorem sum_map_add_nat {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_map_mul_const {α : Type} (l : List α) (g : α → Nat) (c : Nat) :
    (l.map (fun x => c * g x)).sum = c * (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih, Nat.mul_add]

theorem sound_CutWeight : EdgeDegrees.CutWeight.Goal := by
  intro es inside
  rw [weighted_eq_sum_endpoints es (fun v => (inside v).toNat)]
  have hcountP (p : Nat × Nat → Bool) :
      es.countP p = (es.map (fun e => (p e).toNat)).sum := by
    rw [List.countP_eq_length_filter]
    exact length_filter_eq_sum p es
  rw [hcountP (fun e : Nat × Nat => inside e.1 && inside e.2)]
  rw [hcountP (fun e : Nat × Nat => xor (inside e.1) (inside e.2))]
  have hedge : ∀ a b : Nat,
      (inside a).toNat + (inside b).toNat =
        2 * (decide (inside a && inside b)).toNat +
          (decide (xor (inside a) (inside b))).toNat := by
    intro a b
    cases ha : inside a <;> cases hb : inside b <;> simp [xor]
  calc
    (es.map (fun e : Nat × Nat => (inside e.1).toNat + (inside e.2).toNat)).sum
        = (es.map (fun e : Nat × Nat =>
            2 * (decide (inside e.1 && inside e.2)).toNat +
              (decide (xor (inside e.1) (inside e.2))).toNat)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro e he
            obtain ⟨a, b⟩ := e
            exact hedge a b
    _ = 2 * (es.map (fun e : Nat × Nat => (decide (inside e.1 && inside e.2)).toNat)).sum +
          (es.map (fun e : Nat × Nat => (decide (xor (inside e.1) (inside e.2))).toNat)).sum := by
            rw [sum_map_add_nat, sum_map_mul_const]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_EdgeDegrees_CutWeight
