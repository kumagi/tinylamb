import TinylambOpt.ResearchProofs.StorageExecution.SE_BagKernels_Common

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved (10 claims, including the following)

- `ExceptionOverlay.AntiRevenue` — Compute revenue of all left rows whose key is absent
  on the right.
- `ExceptionOverlay.DistinctOutput` — Output live grouping keys once even if physical
  codes alias.
- `ExceptionOverlay.FilteredMoment` — Use a histogram for a filtered sum of squared
  values.
-/

set_option maxHeartbeats 0
set_option linter.unusedVariables false

open TinylambOpt.ResearchProofs.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels

namespace ExceptionOverlayProofs

open ExceptionOverlay

theorem beq_self (n : Nat) : (n == n) = true := by
  rw [Bool.beq_eq_decide_eq]
  exact decide_eq_true rfl

theorem toNat_eq_ite (b : Bool) : b.toNat = if b then (1 : Nat) else 0 := by
  cases b <;> rfl

theorem not_not_beq_false {b : Bool} (h : (!b) = true) : b = false := by
  cases b <;> simp_all

theorem not_contains {l : List Nat} {a : Nat} (h : l.contains a = false) : ¬ a ∈ l := by
  rw [contains_decide] at h
  exact decide_eq_false_iff_not.mp h

theorem not_contains_of_not {l : List Nat} {a : Nat} (h : ¬ l.contains a = true) : ¬ a ∈ l :=
  not_contains (by rw [← Bool.not_eq_true]; exact h)

theorem contains_true {l : List Nat} {a : Nat} (h : l.contains a = true) : a ∈ l := by
  rw [contains_decide] at h
  exact of_decide_eq_true h

theorem eo_some : ∀ (es : List (Nat × Nat)) (i : Nat),
    i ∈ es.map Prod.fst → ∃ v, es.lookup i = some v := by
  intro es; induction es with
  | nil => intro i h; cases h
  | cons z t ih =>
    intro i h
    rw [List.map_cons, List.mem_cons] at h
    rcases h with (h | hi)
    · refine ⟨z.2, ?_⟩
      rw [h, List.lookup_cons, beq_self]
    · cases hbz : (i == z.1) with
      | true => exact ⟨z.2, by rw [List.lookup_cons, hbz]⟩
      | false =>
        obtain ⟨v, hv⟩ := ih i hi
        exact ⟨v, by rw [List.lookup_cons, hbz]; exact hv⟩

theorem eo_none {es : List (Nat × Nat)} {i : Nat} (h : ¬ i ∈ es.map Prod.fst) :
    es.lookup i = none := by
  induction es with
  | nil => rfl
  | cons z t ih =>
    rw [List.map_cons] at h
    rw [List.lookup_cons]
    cases hbz : (i == z.1) with
    | true =>
      exact absurd (Nat.eq_of_beq_eq_true hbz)
        (fun heq => h (by rw [heq]; exact List.mem_cons_self))
    | false => exact ih (fun hx => h (List.mem_cons_of_mem z.1 hx))

theorem eo_lookup_snd : ∀ (es : List (Nat × Nat)) (i v : Nat),
    es.lookup i = some v → v ∈ es.map Prod.snd := by
  intro es; induction es with
  | nil => intro i v h; cases h
  | cons z t ih =>
    intro i v h
    rw [List.map_cons]
    rw [List.lookup_cons] at h
    cases hbz : (i == z.1) with
    | true =>
      rw [hbz] at h
      have hEq : z.snd = v := Option.some.inj (show some z.snd = some v from h)
      rw [← hEq]
      exact List.mem_cons_self
    | false =>
      rw [hbz] at h
      exact List.mem_cons_of_mem z.2 (ih i v h)

theorem lookup_app_ne : ∀ (es : List (Nat × Nat)) (i v j : Nat), j ≠ i →
    (es ++ [(i,v)]).lookup j = es.lookup j := by
  intro es; induction es with
  | nil =>
    intro i v j hj
    rw [List.nil_append, List.lookup_cons]
    cases h : (j == i) with
    | true => exact absurd (Nat.eq_of_beq_eq_true h) hj
    | false => exact rfl
  | cons z t ih =>
    intro i v j hj
    rw [List.cons_append, List.lookup_cons, List.lookup_cons]
    cases hz : (j == z.1) with
    | true => rfl
    | false => rw [ih i v j hj]

theorem countP_partition (p q : Nat → Bool) : ∀ (l : List Nat),
    l.countP p = (l.filter q).countP p + (l.filter (fun i => !q i)).countP p := by
  intro l; induction l with
  | nil => rfl
  | cons x t ih =>
    rw [List.filter_cons, List.filter_cons]
    cases hq : q x with
    | true =>
      rw [Bool.not_true, if_pos (rfl : (true : Bool) = true),
        if_neg (by decide : ¬((false : Bool) = true)), List.countP_cons, ih, List.countP_cons]
      omega
    | false =>
      rw [Bool.not_false, if_neg (by decide : ¬((false : Bool) = true)),
        if_pos (rfl : (true : Bool) = true), List.countP_cons, ih, List.countP_cons]
      omega

theorem countP_constBool (b : Bool) : ∀ (l : List Nat),
    l.countP (fun _ => b) = if b then l.length else 0 := by
  intro l; induction l with
  | nil => cases b <;> rfl
  | cons x t ih =>
    rw [List.countP_cons, ih]
    cases b <;> simp [List.length_cons] <;> try omega

theorem length_filter_add (q : Nat → Bool) : ∀ (l : List Nat),
    (l.filter q).length + (l.filter (fun i => !q i)).length = l.length := by
  intro l; induction l with
  | nil => rfl
  | cons x t ih =>
    rw [List.filter_cons, List.filter_cons, List.length_cons]
    cases hq : q x with
    | true =>
      rw [Bool.not_true, if_pos (rfl : (true : Bool) = true),
        if_neg (by decide : ¬((false : Bool) = true)), List.length_cons]
      omega
    | false =>
      rw [Bool.not_false, if_neg (by decide : ¬((false : Bool) = true)),
        if_pos (rfl : (true : Bool) = true), List.length_cons]
      omega

theorem mass_eq_EO : ∀ (n d : Nat) (es : List (Nat × Nat)) (k : Nat),
    (bag.expand (n,d,es)).count k = bag.mass (n,d,es) k := by
  intro n d es k
  have hexp : bag.expand (n,d,es) = (List.range n).map (fun i => (es.lookup i).getD d) := rfl
  have hmass : bag.mass (n,d,es) k
      = (if k == d then n - ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).length else 0)
        + ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).countP
            (fun i => es.lookup i == some k) := rfl
  rw [hexp, hmass]
  rw [count_eq_length_filter, ← List.countP_eq_length_filter, List.countP_map]
  rw [show ((fun x => k == x) ∘ fun i => (es.lookup i).getD d)
      = (fun i => (es.lookup i).getD d == k) from by
        funext i; exact beq_comm k ((es.lookup i).getD d)]
  rw [countP_partition (fun i => (es.lookup i).getD d == k) (fun i => (es.map Prod.fst).contains i)]
  have hperm : List.Perm ((List.range n).filter (fun i => (es.map Prod.fst).contains i))
      (((es.map Prod.fst).eraseDups).filter (fun i => i < n)) := by
    refine (List.perm_ext_iff_of_nodup ?_ ?_).mpr (fun x => ?_)
    · exact nodup_filter _ List.nodup_range
    · exact nodup_filter _ (nodup_eraseDups _)
    · simp only [List.mem_filter, List.mem_range, List.mem_eraseDups, contains_decide,
        decide_eq_true_eq]
      exact ⟨fun ⟨h1, h2⟩ => ⟨h2, h1⟩, fun ⟨h1, h2⟩ => ⟨h2, h1⟩⟩
  have hA : ((List.range n).filter (fun i => (es.map Prod.fst).contains i)).countP
        (fun i => (es.lookup i).getD d == k)
      = ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).countP
          (fun i => es.lookup i == some k) := by
    rw [List.Perm.countP_eq _ hperm]
    refine List.countP_congr (fun i hi => ?_)
    rw [List.mem_filter, List.mem_eraseDups] at hi
    obtain ⟨v, hv⟩ := eo_some es i hi.1
    rw [hv]
    exact Iff.rfl
  have hlenA : ((List.range n).filter (fun i => (es.map Prod.fst).contains i)).length
      = ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).length := hperm.length_eq
  have hsum : ((List.range n).filter (fun i => (es.map Prod.fst).contains i)).length
      + ((List.range n).filter (fun i => !(es.map Prod.fst).contains i)).length = n := by
    rw [length_filter_add, List.length_range]
  have hB0 : ((List.range n).filter (fun i => !(es.map Prod.fst).contains i)).countP
        (fun i => (es.lookup i).getD d == k)
      = ((List.range n).filter (fun i => !(es.map Prod.fst).contains i)).countP
          (fun _ => d == k) := by
    refine List.countP_congr (fun i hi => ?_)
    simp only [List.mem_filter, List.mem_range] at hi
    have hln : es.lookup i = none :=
      eo_none (not_contains (not_not_beq_false hi.2))
    rw [hln]
    exact Iff.rfl
  have hB1 : ((List.range n).filter (fun i => !(es.map Prod.fst).contains i)).countP
        (fun _ => d == k)
      = (if k == d then n - ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).length else 0) := by
    rw [countP_constBool]
    rw [beq_comm d k]
    by_cases hb : (k == d) = true
    · rw [if_pos hb, if_pos hb]
      rw [← hlenA]
      omega
    · rw [if_neg hb, if_neg hb]
  rw [hA, hB0, hB1]
  omega

theorem cover_EO : ∀ (p : Nat × Nat × List (Nat × Nat)) (k : Nat),
    k ∈ bag.expand p → k ∈ bag.keys p := by
  intro p k hk
  obtain ⟨n, d, es⟩ := p
  rw [show bag.keys (n,d,es) = d :: es.map Prod.snd from rfl]
  rw [show bag.expand (n,d,es) = (List.range n).map (fun i => (es.lookup i).getD d) from rfl] at hk
  obtain ⟨i, hi, hki⟩ := List.mem_map.mp hk
  cases h : es.lookup i with
  | none =>
    rw [h] at hki
    rw [show ((none : Option Nat).getD d) = d from rfl] at hki
    rw [← hki]
    exact List.mem_cons_self
  | some v =>
    rw [h] at hki
    rw [show ((some v : Option Nat).getD d) = v from rfl] at hki
    rw [← hki]
    exact List.mem_cons_of_mem d (eo_lookup_snd es i v h)

theorem wf : WellFormed bag :=
  ⟨fun p k => by obtain ⟨n, d, es⟩ := p; exact mass_eq_EO n d es k, cover_EO⟩

theorem sound_DistinctOutput : DistinctOutput.Goal := wf_distinctOutput bag wf
theorem sound_FilteredMoment : FilteredMoment.Goal := wf_filteredMoment bag wf
theorem sound_OrderStatistic : OrderStatistic.Goal := wf_orderStatistic bag wf
theorem sound_JoinFanout : JoinFanout.Goal := wf_joinFanout bag wf
theorem sound_AntiRevenue : AntiRevenue.Goal := wf_antiRevenue bag wf
theorem sound_SelfPairs : SelfPairs.Goal := wf_selfPairs bag wf
theorem sound_IntersectAll : IntersectAll.Goal := wf_intersectAll bag wf
theorem sound_OuterWeight : OuterWeight.Goal := wf_outerWeight bag wf

theorem sound_IgnoreTail : IgnoreTail.Goal := by
  intro n d es i v hle
  have hlookup : ∀ j, j ∈ List.range n → ((es ++ [(i,v)]).lookup j).getD d = (es.lookup j).getD d := by
    intro j hj
    have hj' : j < n := List.mem_range.mp hj
    have hne : j ≠ i := by omega
    exact congrArg (fun o => o.getD d) (lookup_app_ne es i v j hne)
  rw [weighted_eq bag wf (n,d,es ++ [(i,v)]) id, weighted_eq bag wf (n,d,es) id]
  rw [show bag.expand (n,d,es ++ [(i,v)])
      = (List.range n).map (fun j => ((es ++ [(i,v)]).lookup j).getD d) from rfl,
    show bag.expand (n,d,es) = (List.range n).map (fun j => (es.lookup j).getD d) from rfl]
  rw [List.map_congr_left hlookup]

theorem hbeq_decide_ite (a k : Nat) :
    (if (a == k) then (1 : Nat) else 0) = (decide (a = k)).toNat := by
  rw [← Bool.beq_eq_decide_eq]
  rw [toNat_eq_ite]

theorem nodupP0_le {es : List (Nat × Nat)} {i n : Nat} (hlt : i < n)
    (hni : ¬ i ∈ es.map Prod.fst) :
    (((es.map Prod.fst).eraseDups).filter (fun y => y < n)).length + 1 ≤ n := by
  have hsub : (i :: ((es.map Prod.fst).eraseDups).filter (fun y => y < n))
      ⊆ List.range n := by
    intro y hy
    rw [List.mem_cons] at hy
    rcases hy with rfl | hy
    · exact List.mem_range.mpr hlt
    · rw [List.mem_filter, List.mem_eraseDups] at hy
      exact List.mem_range.mpr (of_decide_eq_true hy.2)
  have hnd : (i :: ((es.map Prod.fst).eraseDups).filter (fun y => y < n)).Nodup :=
    List.nodup_cons.mpr ⟨fun hh => hni (List.mem_eraseDups.1 (List.mem_filter.1 hh).1),
      nodup_filter _ (nodup_eraseDups _)⟩
  have := hnd.length_le_of_subset hsub
  rw [List.length_cons, List.length_range] at this
  exact this

theorem sound_OverrideHead : OverrideHead.Goal := by
  intro n d es i x k hlt
  dsimp only
  have h1 : bag.mass (n,d,(i,x)::es) k
      = (if k == d
          then n - (((i :: es.map Prod.fst).eraseDups).filter (fun y => y < n)).length
          else 0)
        + (((i :: es.map Prod.fst).eraseDups).filter (fun y => y < n)).countP
            (fun y => ((i,x)::es).lookup y == some k) := rfl
  have h0 : bag.mass (n,d,es) k
      = (if k == d
          then n - (((es.map Prod.fst).eraseDups).filter (fun y => y < n)).length
          else 0)
        + (((es.map Prod.fst).eraseDups).filter (fun y => y < n)).countP
            (fun y => es.lookup y == some k) := rfl
  rw [h1, h0]
  have hed : ((i :: es.map Prod.fst).eraseDups).filter (fun y => y < n)
      = i :: ((((es.map Prod.fst).filter (fun y => !(y == i))).eraseDups).filter (fun y => y < n)) := by
    rw [List.eraseDups_cons, List.filter_cons, if_pos (decide_eq_true hlt)]
  by_cases hi : (es.map Prod.fst).contains i = true
  · -- i already present in the alias list: the patched-position multiset is unchanged
    have hmem : i ∈ es.map Prod.fst := contains_true hi
    have hcommon : List.Perm ((((es.map Prod.fst).filter (fun y => !(y == i))).eraseDups).filter (fun y => y < n)) (((((es.map Prod.fst).eraseDups).filter (fun y => !(y == i))).filter (fun y => y < n))) := by
      refine (List.perm_ext_iff_of_nodup ?_ ?_).mpr (fun y => ?_)
      · exact nodup_filter _ (nodup_eraseDups _)
      · exact nodup_filter _ (nodup_filter _ (nodup_eraseDups _))
      · simp only [List.mem_filter, List.mem_eraseDups]
    have hYni : ∀ z ∈ ((((es.map Prod.fst).eraseDups).filter (fun y => !(y == i))).filter (fun y => y < n)), (z == i) = false := by
      intro z hz
      cases h : (z == i) with
      | true =>
        have hzi : z = i := Nat.eq_of_beq_eq_true h
        rw [hzi] at hz
        rw [List.mem_filter, List.mem_filter, List.mem_eraseDups] at hz
        rw [beq_self i] at hz
        exact absurd hz.1.2 (by decide)
      | false => exact rfl
    have hps0perm : List.Perm (((es.map Prod.fst).eraseDups).filter (fun y => y < n))
        (i :: (((es.map Prod.fst).eraseDups).filter (fun y => !(y == i))).filter (fun y => y < n)) := by
      refine (List.perm_ext_iff_of_nodup (nodup_filter _ (nodup_eraseDups _))
        (List.nodup_cons.mpr ⟨?_, nodup_filter _ (nodup_filter _ (nodup_eraseDups _))⟩)).mpr
        (fun y => ?_)
      · intro hh
        have := hYni i hh
        rw [beq_self i] at this
        exact absurd this (by decide)
      · rw [List.mem_filter, List.mem_eraseDups, List.mem_cons]
        refine ⟨fun ⟨hyl, hq⟩ => ?_, fun hor => ?_⟩
        · rw [List.mem_filter, List.mem_filter, List.mem_eraseDups]
          cases hbe : (y == i) with
          | true => exact Or.inl (Nat.eq_of_beq_eq_true hbe)
          | false => exact Or.inr ⟨⟨hyl, rfl⟩, hq⟩
        · rcases hor with rfl | hy
          · exact ⟨hmem, decide_eq_true hlt⟩
          · rw [List.mem_filter, List.mem_filter, List.mem_eraseDups] at hy
            exact ⟨hy.1.1, hy.2⟩
    have hpi : ((i,x)::es).lookup i = some x := by
      rw [List.lookup_cons, beq_self]
    obtain ⟨v, hv⟩ := eo_some es i hmem
    have hC1 : ((((es.map Prod.fst).filter (fun y => !(y == i))).eraseDups).filter (fun y => y < n)).countP (fun y => ((i,x)::es).lookup y == some k)
        = ((((es.map Prod.fst).eraseDups).filter (fun y => !(y == i))).filter (fun y => y < n)).countP (fun y => es.lookup y == some k) := by
      rw [List.Perm.countP_eq _ hcommon]
      refine List.countP_congr (fun y hy => ?_)
      rw [List.lookup_cons, hYni y hy]
    have hsplit : (((es.map Prod.fst).eraseDups).filter (fun y => y < n)).countP
          (fun y => es.lookup y == some k)
        = ((((es.map Prod.fst).eraseDups).filter (fun y => !(y == i))).filter (fun y => y < n)).countP (fun y => es.lookup y == some k)
          + (if (v == k) then (1 : Nat) else 0) := by
      rw [List.Perm.countP_eq _ hps0perm, List.countP_cons]
      rw [show (es.lookup i == some k) = (v == k) from by rw [hv] <;> rfl]
      try rw [hbeq_decide_ite v k]
    rw [hed, List.length_cons, List.countP_cons]
    rw [show (((i,x)::es).lookup i == some k) = (x == k) from by rw [hpi] <;> rfl]
    rw [hC1, hsplit]
    rw [hbeq_decide_ite x k]
    rw [show ((es.lookup i).getD d) = v from by rw [hv] <;> rfl]
    rw [hcommon.length_eq, hps0perm.length_eq, List.length_cons]
    rw [hbeq_decide_ite v k]
    omega
  · -- i absent: the head is a genuinely new patched position
    have hni : ¬ i ∈ es.map Prod.fst := not_contains_of_not hi
    have hqf : (es.map Prod.fst).filter (fun y => !(y == i)) = es.map Prod.fst := by
      refine (List.filter_eq_self).mpr (fun y hy => ?_)
      cases hbz : (y == i) with
      | true => exact False.elim (hni (Nat.eq_of_beq_eq_true hbz ▸ hy))
      | false => exact rfl
    have hps1 : ((i :: es.map Prod.fst).eraseDups).filter (fun y => y < n)
        = i :: ((es.map Prod.fst).eraseDups).filter (fun y => y < n) := by
      rw [hed, hqf]
    have hcongr : (((es.map Prod.fst).eraseDups).filter (fun y => y < n)).countP
          (fun y => ((i,x)::es).lookup y == some k)
        = (((es.map Prod.fst).eraseDups).filter (fun y => y < n)).countP
            (fun y => es.lookup y == some k) :=
      List.countP_congr (fun y hy => by
        rw [List.mem_filter, List.mem_eraseDups] at hy
        have hq : (y == i) = false := by
          cases h : (y == i) with
          | true => exact absurd (Nat.eq_of_beq_eq_true h) (fun hh => hni (hh ▸ hy.1))
          | false => exact rfl
        rw [List.lookup_cons, hq])
    have hlen : (((es.map Prod.fst).eraseDups).filter (fun y => y < n)).length + 1 ≤ n :=
      nodupP0_le hlt hni
    rw [hps1, List.length_cons, List.countP_cons]
    rw [show (((i,x)::es).lookup i == some k) = (x == k) from by rw [List.lookup_cons, beq_self] <;> rfl]
    rw [hcongr, hbeq_decide_ite x k]
    have hnone : es.lookup i = none := eo_none hni
    rw [show ((es.lookup i).getD d) = d from by rw [hnone] <;> rfl]
    rw [← hbeq_decide_ite d k]
    rw [beq_comm d k]
    cases hkd : (k == d) with
    | true => simp; omega
    | false => simp

end ExceptionOverlayProofs
