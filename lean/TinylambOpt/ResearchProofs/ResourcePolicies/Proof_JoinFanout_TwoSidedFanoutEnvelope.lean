import TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

/-!
Proof contribution for ResourcePolicies_JoinFanout_TwoSidedFanoutEnvelope. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout.Goal_TwoSidedFanoutEnvelope

open TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem sum_map_filter_ne (f : Nat → Nat) (a : Nat) (as : List Nat) :
    (as.map f).sum = as.count a * f a + ((as.filter (fun b => !(b == a))).map f).sum := by
  induction as with
  | nil => simp
  | cons b t ih =>
      by_cases hba : (b == a) = true
      · have hbeq : b = a := by simpa using hba
        subst b
        simp only [List.map_cons, List.sum_cons, List.count_cons_self]
        have hfilt : (a :: t).filter (fun b => !(b == a)) = t.filter (fun b => !(b == a)) := by simp
        rw [hfilt, ih, Nat.add_mul]
        omega
      · have hbne : b ≠ a := by simpa using hba
        simp only [List.map_cons, List.sum_cons, List.count_cons_of_ne hbne]
        have hfilt : (b :: t).filter (fun b => !(b == a)) = b :: t.filter (fun b => !(b == a)) := by simp [hba]
        rw [hfilt]
        simp only [List.map_cons, List.sum_cons]
        rw [ih]
        omega

private theorem count_cons_filter (a x : Nat) (as : List Nat)
    (hx : x ∈ as.filter (fun b => !(b == a))) :
    (a :: as).count x = (as.filter (fun b => !(b == a))).count x := by
  simp only [List.mem_filter] at hx
  have hxa : (x == a) = false := by simpa using hx.2
  have hax : a ≠ x := by intro h; subst x; simp at hxa
  rw [List.count_cons_of_ne hax, List.count_filter]
  simp [hxa]

private theorem map_count_eq (a : Nat) (as : List Nat) : ∀ (l : List Nat),
    (∀ x ∈ l, x ∈ as.filter (fun b => !(b == a))) →
    l.map (fun x => (x, (a :: as).count x)) =
      l.map (fun x => (x, (as.filter (fun b => !(b == a))).count x))
  | [], _ => by simp
  | x :: t, h => by
      have hx := h x (by simp)
      simp only [List.map_cons]
      rw [count_cons_filter a x as hx]
      rw [map_count_eq a as t (fun y hy => h y (by simp [hy]))]

private theorem sum_group (f : Nat → Nat) : ∀ (xs : List Nat),
    ((frequency xs).map (fun (k, n) => n * f k)).sum = (xs.map f).sum
  | [] => by simp [frequency]
  | a :: as => by
      let r := as.filter (fun b => !(b == a))
      have ih_r := sum_group f r
      have hcons : (a :: as).count a = as.count a + 1 := by simp
      have hfreq : frequency (a :: as) = (a, (a :: as).count a) :: frequency r := by
        dsimp [r]
        unfold frequency
        rw [List.eraseDups_cons]
        simp only [List.map_cons]
        rw [map_count_eq a as (List.filter (fun b => !(b == a)) as).eraseDups
          (fun x hx => (List.mem_eraseDups).1 hx)]
      rw [hfreq]
      simp only [List.map_cons, List.sum_cons]
      rw [hcons, ih_r, sum_map_filter_ne f a as]
      change (as.count a + 1) * f a + ((as.filter (fun b => !(b == a))).map f).sum =
        f a + (as.count a * f a + ((as.filter (fun b => !(b == a))).map f).sum)
      rw [Nat.add_mul]
      omega
termination_by xs => xs.length
decreasing_by
  exact Nat.lt_of_le_of_lt (by simpa [r] using List.length_filter_le (fun b => !(b == a)) as) (by simp)

private theorem product_eq_sum_count (a b : Keys) :
    product a b = ((a.filterMap id).map (fun k => b.count (some k))).sum := by
  unfold product hist
  exact sum_group (fun k => b.count (some k)) (a.filterMap id)

private theorem count_some_eq_filterMap_count (b : Keys) (k : Nat) :
    b.count (some k) = (b.filterMap id).count k := by
  induction b with
  | nil => simp
  | cons x xs ih =>
      cases x with
      | none => simp [ih]
      | some x =>
          by_cases hx : x = k
          · subst x; simp [ih]
          · simp [ih, hx]

private theorem count_cons_eq_add (a k : Nat) (A : List Nat) :
    (a :: A).count k = A.count k + (if (a == k) = true then 1 else 0) := by
  by_cases h : (a == k) = true <;> simp [List.count_cons, h]

private theorem indicator_sum (a : Nat) (B : List Nat) :
    (B.map (fun k => if (a == k) = true then 1 else 0)).sum = B.count a := by
  induction B with
  | nil => simp
  | cons b B ih =>
      simp only [List.map_cons, List.sum_cons, List.count_cons]
      by_cases h : (b == a) = true
      · have hab : (a == b) = true := by simpa [Bool.beq_comm] using h
        rw [if_pos h, if_pos hab, ih]
        omega
      · have hab : (a == b) = false := by simpa [Bool.beq_comm] using h
        have h' : ¬ (b == a) = true := h
        have hab' : ¬ (a == b) = true := by simpa using hab
        rw [if_neg h', if_neg hab', ih]
        omega

private theorem sum_map_add (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      omega

private theorem sum_zero (B : List Nat) : (B.map (fun _ => 0)).sum = 0 := by
  induction B with
  | nil => simp
  | cons b B ih => simp [ih]

private theorem sum_count_eq_sum_count (A B : List Nat) :
    (A.map (fun k => B.count k)).sum = (B.map (fun k => A.count k)).sum := by
  induction A with
  | nil =>
      simp [sum_zero]
  | cons a A ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      have hmap : (B.map (fun k => (a :: A).count k)).sum =
          (B.map (fun k => A.count k + (if (a == k) = true then 1 else 0))).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro k hk
        exact count_cons_eq_add a k A
      rw [hmap, sum_map_add, indicator_sum]
      omega

private theorem product_comm (a b : Keys) : product a b = product b a := by
  rw [product_eq_sum_count, product_eq_sum_count]
  simp [count_some_eq_filterMap_count]
  exact sum_count_eq_sum_count (a.filterMap id) (b.filterMap id)

private theorem entry_le_maxFreq (b : Keys) (k n : Nat) (h : (k, n) ∈ hist b) :
    n ≤ ((hist b).map Prod.snd).max?.getD 0 := by
  let ns := (hist b).map Prod.snd
  have hnmem : n ∈ ns := List.mem_map.mpr ⟨(k, n), h, rfl⟩
  have hne : ns ≠ [] := List.ne_nil_of_mem hnmem
  obtain ⟨m, hm⟩ : ∃ m, ns.max? = some m := by
    cases hm0 : ns.max? with
    | none => have : ns = [] := (List.max?_eq_none_iff).mp hm0; contradiction
    | some m => exact ⟨m, rfl⟩
  have hle : n ≤ m := (List.max?_eq_some_iff.mp hm).2 n hnmem
  simp [ns, hm]
  omega

private theorem count_le_maxFreq (b : Keys) (k : Nat) :
    b.count (some k) ≤ ((hist b).map Prod.snd).max?.getD 0 := by
  by_cases hk : k ∈ b.filterMap id
  · have hentry : (k, (b.filterMap id).count k) ∈ hist b := by
      unfold hist frequency
      exact List.mem_map.mpr ⟨k, (List.mem_eraseDups).mpr hk, rfl⟩
    have hb := entry_le_maxFreq b k ((b.filterMap id).count k) hentry
    rw [count_some_eq_filterMap_count]
    exact hb
  · rw [count_some_eq_filterMap_count]
    rw [List.count_eq_countP, List.countP_eq_length_filter]
    have hf : List.filter (fun x => x == k) (b.filterMap id) = [] := by
      apply (List.filter_eq_nil_iff).mpr
      intro x hx hxk
      apply hk
      have hxeq : x = k := by simpa using hxk
      simpa [hxeq] using hx
    rw [hf]
    simp

private theorem sum_le_card_mul_max (l : List Nat) (c : Nat) (h : ∀ x ∈ l, x ≤ c) :
    l.sum ≤ l.length * c := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.length_cons, List.sum_cons, Nat.succ_mul]
      have hx := h x (by simp)
      have htail := ih (fun y hy => h y (by simp [hy]))
      omega

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.JoinFanout.TwoSidedFanoutEnvelope.Goal := by
  intro a b
  let maxA := ((hist a).map Prod.snd).max?.getD 0
  let maxB := ((hist b).map Prod.snd).max?.getD 0
  change product a b ≤ min ((a.filterMap id).length * maxB) ((b.filterMap id).length * maxA)
  apply Nat.le_min.mpr
  constructor
  · rw [product_eq_sum_count]
    simpa using (sum_le_card_mul_max ((a.filterMap id).map (fun k => b.count (some k))) maxB (by
      intro v hv
      rcases List.mem_map.mp hv with ⟨key, hkey, rfl⟩
      exact count_le_maxFreq b key))
  · rw [product_comm, product_eq_sum_count]
    simpa using (sum_le_card_mul_max ((b.filterMap id).map (fun k => a.count (some k))) maxA (by
      intro v hv
      rcases List.mem_map.mp hv with ⟨key, hkey, rfl⟩
      exact count_le_maxFreq a key))

end TinylambOpt.ResearchProofs.ResourcePolicies.JoinFanout.Goal_TwoSidedFanoutEnvelope
