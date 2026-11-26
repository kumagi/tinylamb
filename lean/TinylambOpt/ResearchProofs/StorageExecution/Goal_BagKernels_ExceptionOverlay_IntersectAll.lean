import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `ExceptionOverlay.IntersectAll` — Intersect two bags by the minimum multiplicity at
  each key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_IntersectAll

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

private theorem lookup_none_of_not_mem_fst (es : List (Nat × Nat)) (i : Nat)
    (h : i ∉ es.map Prod.fst) : es.lookup i = none := by
  induction es with
  | nil => rfl
  | cons r es ih =>
      rcases r with ⟨j,v⟩
      simp only [List.map_cons, List.mem_cons] at h
      have hij : i ≠ j := fun heq => h (Or.inl heq)
      have hes : i ∉ es.map Prod.fst := fun hm => h (Or.inr hm)
      have hjbeq : (i == j) = false := by simp [hij]
      rw [List.lookup_cons]
      split
      · rw [hjbeq] at ‹(i == j) = true›
        contradiction
      · exact ih hes

private theorem lookup_some_of_mem_fst (es : List (Nat × Nat)) (i : Nat)
    (h : i ∈ es.map Prod.fst) : ∃ v, es.lookup i = some v := by
  induction es with
  | nil => simp at h
  | cons r es ih =>
      rcases r with ⟨j,v⟩
      simp only [List.map_cons, List.mem_cons] at h
      rcases h with heq | hm
      · subst j
        exact ⟨v, by simp⟩
      · rcases ih hm with ⟨w, hw⟩
        by_cases hieq : i == j
        · exact ⟨v, by simp [List.lookup_cons, hieq]⟩
        · exact ⟨w, by simp [List.lookup_cons, hieq, hw]⟩

private theorem nodup_eraseDups_self (l : List Nat) : l.eraseDups.Nodup := by
  suffices H : ∀ n (l : List Nat), l.length ≤ n → l.eraseDups.Nodup from H l.length l (Nat.le_refl _)
  intro n
  induction n with
  | zero =>
      intro l h
      cases l with
      | nil => simp
      | cons a as => simp at h
  | succ n ih =>
      intro l h
      cases l with
      | nil => simp [List.eraseDups]
      | cons a as =>
          have hlen : as.length ≤ n := by simpa using h
          rw [List.eraseDups_cons]
          have hf : (as.filter (fun b => !b == a)).length ≤ n :=
            Nat.le_trans (List.length_filter_le _ _) hlen
          have ht : ((as.filter (fun b => !b == a)).eraseDups).Nodup := ih _ hf
          rw [List.nodup_cons]
          constructor
          · intro hm
            rw [List.mem_eraseDups, List.mem_filter] at hm
            simp at hm
          · exact ht

private theorem filter_eq_perm_nodup (l : List Nat) (hn : l.Nodup) (n : Nat) :
    (l.filter (fun i => i == n)).Perm (if n ∈ l then [n] else []) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have hna : a ∉ l := (List.nodup_cons.mp hn).1
      have hln : l.Nodup := (List.nodup_cons.mp hn).2
      have ih' := ih hln
      by_cases han : a == n
      · have haeq : a = n := by simpa using han
        subst a
        have hfilter : l.filter (fun i => i == n) = [] := by
          apply List.eq_nil_iff_forall_not_mem.mpr
          intro x hx
          rw [List.mem_filter] at hx
          have hxeq : x = n := by simpa using hx.2
          exact hna (by simpa [hxeq] using hx.1)
        simp [hfilter]
      · have hnaeq : n ≠ a := by
          intro h; exact han (by simp [h])
        by_cases hnl : n ∈ l
        · simpa [han, hnaeq, hnl] using ih'
        · simpa [han, hnaeq, hnl] using ih'

private theorem countP_filter_lt_succ (l : List Nat) (n : Nat) (p : Nat → Bool) :
    (l.filter (fun i => i < n+1)).countP p =
      (l.filter (fun i => i < n)).countP p + (l.filter (fun i => i == n)).countP p := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.filter_cons]
      by_cases han : a < n
      · have hans : a < n+1 := by omega
        have hane : (a == n) = false := by simp; omega
        simp [hans, han, hane]
        rw [List.countP_cons, List.countP_cons, ih]
        omega
      · by_cases haeq : a = n
        · subst a
          have hans : n < n+1 := by omega
          simp [hans, han]
          rw [List.countP_cons, List.countP_cons, ih]
          omega
        · have hans : ¬ a < n+1 := by omega
          have hane : (a == n) = false := by simp [haeq]
          simp [hans, han, hane, ih]

private theorem length_filter_lt_succ (l : List Nat) (n : Nat) :
    (l.filter (fun i => i < n+1)).length =
      (l.filter (fun i => i < n)).length + (l.filter (fun i => i == n)).length := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.filter_cons]
      by_cases han : a < n
      · have hans : a < n+1 := by omega
        have hane : (a == n) = false := by simp; omega
        simp [hans, han, hane, ih]
        omega
      · by_cases haeq : a = n
        · subst a
          have hans : n < n+1 := by omega
          have haneq : (n == n) = true := by simp
          simp [hans, han, ih]
          omega
        · have hans : ¬ a < n+1 := by omega
          have hane : (a == n) = false := by simp [haeq]
          simp [hans, han, hane, ih]

private def M' (n d : Nat) (U : List Nat) (es : List (Nat × Nat)) (k : Nat) : Nat :=
  (if k == d then n - (U.filter (fun i => i < n)).length else 0) +
    (U.filter (fun i => i < n)).countP (fun i => es.lookup i == some k)

private theorem mass_eq_M' (n d : Nat) (es : List (Nat × Nat)) (k : Nat) :
    bag.mass (n,d,es) k = M' n d ((es.map Prod.fst).eraseDups) es k := by
  rfl

private theorem filter_false_nil (l : List Nat) : l.filter (fun _ => false) = [] := by
  apply List.eq_nil_iff_forall_not_mem.mpr
  intro x hx
  simp at hx

private theorem psLen_le (es : List (Nat × Nat)) (n : Nat) :
    ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).length ≤ n := by
  induction n with
  | zero => simp [filter_false_nil]
  | succ n ih =>
      let U := (es.map Prod.fst).eraseDups
      have ihU : (U.filter (fun i => i < n)).length ≤ n := by simpa [U] using ih
      have h := length_filter_lt_succ U n
      have hperm := filter_eq_perm_nodup U (nodup_eraseDups_self (es.map Prod.fst)) n
      have hlen : (U.filter (fun i => i == n)).length = if n ∈ U then 1 else 0 := by
        rw [hperm.length_eq]
        by_cases hnU : n ∈ U <;> simp [hnU]
      rw [hlen] at h
      have hres : (U.filter (fun i => i < n+1)).length ≤ n+1 := by
        by_cases hnU : n ∈ U <;> simp [hnU] at h <;> omega
      simpa [U] using hres

private theorem count_expand_eq_mass (n d : Nat) (es : List (Nat × Nat)) (k : Nat) :
    (bag.expand (n,d,es)).count k = bag.mass (n,d,es) k := by
  let U := (es.map Prod.fst).eraseDups
  induction n with
  | zero => simp [bag, filter_false_nil]
  | succ n ih =>
      have ih' : ((List.range n).map (fun i => (es.lookup i).getD d)).count k = M' n d U es k := by
        change (bag.expand (n,d,es)).count k = M' n d U es k
        rw [ih, mass_eq_M']
      have hUlen : U.Nodup := nodup_eraseDups_self (es.map Prod.fst)
      have hperm := filter_eq_perm_nodup U hUlen n
      have hlen : (U.filter (fun i => i == n)).length = if n ∈ U then 1 else 0 := by
        rw [hperm.length_eq]
        by_cases hnU : n ∈ U <;> simp [hnU]
      have hcnt (q : Nat → Bool) :
          (U.filter (fun i => i == n)).countP q = if n ∈ U then (if q n = true then 1 else 0) else 0 := by
        rw [hperm.countP_eq]
        by_cases hnU : n ∈ U <;> simp [hnU]
      have hlen_rec := length_filter_lt_succ U n
      have hcnt_rec := countP_filter_lt_succ U n (fun i => es.lookup i == some k)
      have hle : (U.filter (fun i => i < n)).length ≤ n := by
        simpa [U] using psLen_le es n
      change (List.count k ((List.range (n+1)).map (fun i => (es.lookup i).getD d)) = bag.mass (n+1,d,es) k)
      rw [List.range_succ, List.map_append, List.count_append]
      simp only [List.map_singleton, List.count_cons, List.count_nil]
      rw [ih', mass_eq_M']
      simp only [Nat.zero_add]
      change M' n d U es k + (if (es.lookup n).getD d == k then 1 else 0) = M' (n+1) d U es k
      unfold M'
      by_cases hmem : n ∈ es.map Prod.fst
      · have hmemU : n ∈ U := by simpa [U] using hmem
        rcases lookup_some_of_mem_fst es n hmem with ⟨v, hlk⟩
        have hlenS : (U.filter (fun i => i < n+1)).length =
            (U.filter (fun i => i < n)).length + 1 := by
          rw [hlen_rec, hlen]
          simp [hmemU]
        have hcntS : (U.filter (fun i => i < n+1)).countP (fun i => es.lookup i == some k) =
            (U.filter (fun i => i < n)).countP (fun i => es.lookup i == some k) +
              (if es.lookup n = some k then 1 else 0) := by
          rw [hcnt_rec]
          have hz := hcnt (fun i => es.lookup i == some k)
          simp [hmemU, beq_iff_eq] at hz
          omega
        have hind : (if (es.lookup n).getD d == k then 1 else 0) =
            (if es.lookup n = some k then 1 else 0) := by
          rw [hlk]
          simp [beq_iff_eq]
        rw [hlenS, hcntS, hind]
        by_cases hkd : (k == d) = true
        · simp [hkd, Nat.add_assoc]
        · simp [hkd]
      · have hmemU : n ∉ U := by simpa [U] using hmem
        have hlk : es.lookup n = none := lookup_none_of_not_mem_fst es n hmem
        have hlenS : (U.filter (fun i => i < n+1)).length = (U.filter (fun i => i < n)).length := by
          rw [hlen_rec, hlen]
          simp [hmemU]
        have hcntS : (U.filter (fun i => i < n+1)).countP (fun i => es.lookup i == some k) =
            (U.filter (fun i => i < n)).countP (fun i => es.lookup i == some k) := by
          rw [hcnt_rec]
          have hz0 : (U.filter (fun i => i == n)).countP (fun i => es.lookup i == some k) = 0 := by
            have hz := hcnt (fun i => es.lookup i == some k)
            have hz' : (if n ∈ U then (if (es.lookup n == some k) = true then 1 else 0) else 0) = 0 := by
              simp [hmemU]
            rw [hz'] at hz
            exact hz
          rw [hz0]
          simp
        have hind : (if (es.lookup n).getD d == k then 1 else 0) =
            (if k == d then 1 else 0) := by
          rw [hlk]
          by_cases hdk : d = k
          · simp [hdk]
          · have hkd' : k ≠ d := fun h => hdk h.symm
            simp [hdk, hkd']
        rw [hlenS, hcntS, hind]
        by_cases hkd : (k == d) = true
        · simp [hkd, Nat.add_assoc]
          omega
        · by_cases hdk : d = k
          · have hkdeq : (k == d) = true := by simp [hdk]
            exact (hkd hkdeq).elim
          · simp [hkd]

private theorem count_flatMap_replicate (xs : List Nat) (f : Nat → Nat) (k : Nat) :
    (xs.flatMap (fun x => List.replicate (f x) x)).count k =
      (xs.map (fun x => if x = k then f x else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, List.count_replicate, beq_iff_eq]

private theorem nodup_filter {α : Type} [DecidableEq α] (l : List α) (hn : l.Nodup) (p : α → Bool) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hn
      simp only [List.filter_cons]
      by_cases hp : p a = true
      · simp [hp]
        constructor
        · exact hn.1
        · exact ih hn.2
      · simp [hp]
        exact ih hn.2

private theorem sum_eq_single_of_nodup (xs : List Nat) (hn : xs.Nodup) (k : Nat) (f : Nat → Nat) :
    (xs.map (fun x => if x = k then f x else 0)).sum = if k ∈ xs then f k else 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.map_cons, List.sum_cons]
      by_cases hxk : x = k
      · subst x
        have hnot : k ∉ xs := hn.1
        simpa [hnot] using (ih hn.2)
      · have hi := ih hn.2
        have hkx : k ≠ x := fun h => hxk h.symm
        by_cases hk : k ∈ xs
        · simp [hxk, hkx, hk, hi]
        · simp [hxk, hkx, hk, hi]

private theorem lookup_snd_mem (es : List (Nat × Nat)) (i v : Nat)
    (hlk : es.lookup i = some v) : v ∈ es.map Prod.snd := by
  induction es with
  | nil => simp at hlk
  | cons r es ih =>
      rcases r with ⟨j,w⟩
      rw [List.lookup_cons] at hlk
      by_cases hij : (i == j) = true
      · simp [hij] at hlk
        subst v
        simp
      · simp [hij] at hlk
        have hm := ih hlk
        simp [hm]

private theorem mass_eq_zero_of_not_mem_keys (a : Nat × Nat × List (Nat × Nat)) (k : Nat)
    (hkin : k ∉ bag.keys a) : bag.mass a k = 0 := by
  rcases a with ⟨n,d,es⟩
  rw [mass_eq_M']
  unfold M'
  have hkd : k ≠ d := by
    intro heq
    apply hkin
    simp [bag, heq]
  have hk_snd : k ∉ es.map Prod.snd := by
    intro hm
    apply hkin
    simp [bag, hm]
  have hif : (k == d) = false := by simp [hkd]
  have hcnt0 : (((es.map Prod.fst).eraseDups.filter (fun i => i < n)).countP
      (fun i => es.lookup i == some k)) = 0 := by
    rw [List.countP_eq_zero]
    intro i hi hlk
    have hlkeq : es.lookup i = some k := by simpa [beq_iff_eq] using hlk
    exact hk_snd (lookup_snd_mem es i k hlkeq)
  simp [hif, hcnt0]

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  rw [count_flatMap_replicate]
  have hnodup : (live bag a).Nodup := by
    change ((bag.keys a).eraseDups.filter (fun x => 0 < bag.mass a x)).Nodup
    exact nodup_filter _ (nodup_eraseDups_self _) _
  rw [sum_eq_single_of_nodup (live bag a) hnodup]
  rw [count_expand_eq_mass, count_expand_eq_mass]
  by_cases hk : k ∈ live bag a
  · simp [hk]
  · simp [hk]
    by_cases hkin : k ∈ bag.keys a
    · have hm0 : bag.mass a k = 0 := by
        have hnotpos : ¬ 0 < bag.mass a k := by
          intro hp
          apply hk
          change k ∈ (bag.keys a).eraseDups.filter (fun x => 0 < bag.mass a x)
          exact List.mem_filter.mpr ⟨List.mem_eraseDups.mpr hkin, by simp [hp]⟩
        omega
      simp [hm0]
    · have hm0 : bag.mass a k = 0 := mass_eq_zero_of_not_mem_keys a k hkin
      simp [hm0]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_IntersectAll
