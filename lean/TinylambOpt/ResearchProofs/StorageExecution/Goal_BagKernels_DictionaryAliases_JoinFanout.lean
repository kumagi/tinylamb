import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `DictionaryAliases.JoinFanout` — Evaluate band join count from the two weighted
  supports.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_JoinFanout

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

/-! ## Generic list lemmas -/

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
      · have hkx : (k == x) = false := Bool.eq_false_of_ne_true (by
          intro h
          exact hx (beq_iff_eq.mpr (beq_iff_eq.mp h).symm))
        simp [List.count_cons, hx, ih, Nat.add_left_comm]

private theorem dedup_weight_sum (f : Nat → Nat) (keys : List Nat) :
    ((keys.eraseDups.map fun k => keys.count k * f k).sum) = (keys.map f).sum := by
  let motive : Nat → Prop := fun n => ∀ keys : List Nat, keys.length = n →
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
          exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : Nat => !(b == k)) ks)
            (Nat.lt_succ_self _)
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
            apply Bool.eq_false_of_ne_true
            intro h
            have hamem : a ∈ rest := List.mem_eraseDups.mp ha
            have hamem' : a ∈ ks.filter (fun b : Nat => !(b == k)) := by simpa [rest] using hamem
            have hff := (List.mem_filter.mp hamem').2
            simp [h] at hff
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          have hkane : k ≠ a := by
            intro h
            subst k
            simp at hane
          have hka : (k == a) = false := Bool.eq_false_of_ne_true (by
            intro h
            exact hkane (beq_iff_eq.mp h))
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

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have H : motive l.length := by
    apply Nat.strongRecOn
    intro n ih l hlen
    cases l with
    | nil => simp
    | cons a as =>
        let rest := as.filter (fun b => !(b == a))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          exact Nat.lt_of_le_of_lt (List.length_filter_le (fun b : α => !(b == a)) as)
            (Nat.lt_succ_self _)
        have ihrest := ih rest.length hrestlen rest rfl
        rw [List.eraseDups_cons]
        apply List.nodup_cons.mpr
        constructor
        · intro h
          have hm : a ∈ rest.eraseDups := h
          rw [List.mem_eraseDups] at hm
          have hm' := (List.mem_filter.mp hm).2
          have hself : (a == a) = true := beq_iff_eq.mpr rfl
          simp [hself] at hm'
        · exact ihrest
  exact H l rfl

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} {a : α} (hn : l.Nodup) (ha : a ∈ l) : l.count a = 1 := by
  have hle : l.count a ≤ 1 := List.nodup_iff_count.mp hn a
  have hpos : 0 < l.count a := List.count_pos_iff.mpr ha
  omega

private theorem sum_eq_of_nodup_mem (l₁ l₂ : List Nat) (f : Nat → Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup) (hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂) :
    (l₁.map f).sum = (l₂.map f).sum := by
  have hperm : l₁.Perm l₂ := List.perm_iff_count.mpr (by
    intro d
    by_cases hd : d ∈ l₁
    · have hd2 : d ∈ l₂ := (hmem d).mp hd
      rw [count_eq_one_of_mem_nodup hnd₁ hd, count_eq_one_of_mem_nodup hnd₂ hd2]
    · have hd2 : d ∉ l₂ := by intro h; exact hd ((hmem d).mpr h)
      rw [List.count_eq_zero.mpr hd, List.count_eq_zero.mpr hd2])
  exact List.Perm.sum_nat (hperm.map f)

private theorem sum_map_filter_eq {α : Type} (l : List α) (f : α → Nat) (p : α → Bool)
    (hz : ∀ x ∈ l, p x = false → f x = 0) :
    (l.map f).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      by_cases h : p x = true
      · simp [h]
        exact ih (by intro y hy hpy; exact hz y (by simp [hy]) hpy)
      · have hf : p x = false := Bool.eq_false_of_ne_true h
        have hx := hz x (by simp) hf
        simp [h, hx]
        exact ih (by intro y hy hpy; exact hz y (by simp [hy]) hpy)

private theorem sum_eq_of_mem_or_zero (l₁ l₂ : List Nat) (f : Nat → Nat)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup)
    (h₁ : ∀ d ∈ l₁, d ∉ l₂ → f d = 0)
    (h₂ : ∀ d ∈ l₂, d ∉ l₁ → f d = 0) :
    (l₁.map f).sum = (l₂.map f).sum := by
  let p : Nat → Bool := fun d => f d ≠ 0
  have hz₁ : ∀ d ∈ l₁, p d = false → f d = 0 := by
    intro d hd hp
    by_cases hne : f d = 0
    · exact hne
    · have ht : p d = true := by simp [p, hne]
      simp [ht] at hp
  have hz₂ : ∀ d ∈ l₂, p d = false → f d = 0 := by
    intro d hd hp
    by_cases hne : f d = 0
    · exact hne
    · have ht : p d = true := by simp [p, hne]
      simp [ht] at hp
  have e₁ : (l₁.map f).sum = ((l₁.filter p).map f).sum := sum_map_filter_eq l₁ f p hz₁
  have e₂ : (l₂.map f).sum = ((l₂.filter p).map f).sum := sum_map_filter_eq l₂ f p hz₂
  have hmem : ∀ d, d ∈ l₁.filter p ↔ d ∈ l₂.filter p := by
    intro d
    simp only [List.mem_filter]
    constructor
    · rintro ⟨hd1, hp1⟩
      by_cases hd2 : d ∈ l₂
      · exact ⟨hd2, hp1⟩
      · have hf0 := h₁ d hd1 hd2
        simp [p, hf0] at hp1
    · rintro ⟨hd2, hp2⟩
      by_cases hd1 : d ∈ l₁
      · exact ⟨hd1, hp2⟩
      · have hf0 := h₂ d hd2 hd1
        simp [p, hf0] at hp2
  have hnd₁' : (l₁.filter p).Nodup := List.Sublist.nodup List.filter_sublist hnd₁
  have hnd₂' : (l₂.filter p).Nodup := List.Sublist.nodup List.filter_sublist hnd₂
  have heq : ((l₁.filter p).map f).sum = ((l₂.filter p).map f).sum :=
    sum_eq_of_nodup_mem _ _ f hnd₁' hnd₂' hmem
  omega

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem decide_le_toNat (x y slack : Nat) :
    (decide (x ≤ y ∧ y ≤ x + slack)).toNat =
      (if x ≤ y && y ≤ x + slack then 1 else 0) := by
  by_cases hxy : x ≤ y <;> by_cases hyx : y ≤ x + slack <;> simp [hxy, hyx]

private theorem sum_ite_bool_eq_filter_length (l : List Nat) (p : Nat → Bool) :
    (l.map fun y => if p y then 1 else 0).sum = (l.filter p).length := by
  induction l with
  | nil => simp
  | cons y ys ih => by_cases h : p y <;> simp [h, ih, Nat.add_comm]

private theorem sum_indicator_eq_filter_length (l : List Nat) (x slack : Nat) :
    (l.map (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat)).sum =
      (l.filter (fun y => x ≤ y && y ≤ x + slack)).length := by
  let p : Nat → Bool := fun y => x ≤ y && y ≤ x + slack
  have hmap : (l.map (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat)) =
      (l.map fun y => if p y then 1 else 0) := by
    apply List.map_congr_left
    intro y hy
    simpa [p] using decide_le_toNat x y slack
  rw [hmap]
  exact sum_ite_bool_eq_filter_length l p

private theorem pairs_length_eq_sum (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = (xs.map fun x => (ys.filter (pred x)).length).sum := by
  induction xs with
  | nil => simp [pairs]
  | cons x xs ih => simp [pairs, ih]

/-! ## DictionaryAliases bag facts -/

private theorem zipIdx_count_below (d : List Nat) (n i k : Nat) (hi : i < n) :
    (((d.zipIdx n).filter (fun e => e.1 == k)).map (fun e => if i = e.2 then 1 else 0)).sum = 0 := by
  induction d generalizing n with
  | nil => simp
  | cons v ds ih =>
      rw [show (v :: ds).zipIdx n = (v, n) :: ds.zipIdx (n + 1) by simp [List.zipIdx]]
      simp only [List.filter_cons, List.map_cons, List.sum_cons]
      have hne : i ≠ n := Nat.ne_of_lt hi
      by_cases hv : v == k
      · simp [hv, hne]
        simpa using ih (n + 1) (by omega)
      · simp [hv]
        simpa using ih (n + 1) (by omega)

private theorem zipIdx_count_from (d : List Nat) (n r k : Nat) :
    (((d.zipIdx n).filter (fun e => e.1 == k)).map (fun e => if n + r = e.2 then 1 else 0)).sum =
      (if d[r]? = some k then 1 else 0) := by
  induction d generalizing n r with
  | nil => simp
  | cons v ds ih =>
      rw [show (v :: ds).zipIdx n = (v, n) :: ds.zipIdx (n + 1) by simp [List.zipIdx]]
      simp only [List.filter_cons, List.map_cons, List.sum_cons]
      by_cases hv : v == k
      · simp [hv]
        cases r with
        | zero =>
            have hv' : v = k := beq_iff_eq.mp hv
            simp only [Nat.add_zero]
            have htail : (((ds.zipIdx (n + 1)).filter (fun e => e.1 == k)).map
                (fun e => if n = e.2 then 1 else 0)).sum = 0 :=
              zipIdx_count_below ds (n + 1) n k (by omega)
            rw [htail]
            simp [hv']
        | succ r' =>
            have hidx : n + (r' + 1) = (n + 1) + r' := by omega
            rw [hidx, ih (n + 1) r']
            simp
      · simp [hv]
        cases r with
        | zero =>
            have hvne : v ≠ k := by intro h; exact hv (beq_iff_eq.mpr h)
            simp only [Nat.add_zero]
            have htail : (((ds.zipIdx (n + 1)).filter (fun e => e.1 == k)).map
                (fun e => if n = e.2 then 1 else 0)).sum = 0 :=
              zipIdx_count_below ds (n + 1) n k (by omega)
            rw [htail]
            simp [hvne]
        | succ r' =>
            have hidx : n + (r' + 1) = (n + 1) + r' := by omega
            rw [hidx, ih (n + 1) r']
            simp

private theorem mass_eq_indicator_sum (d ids : List Nat) (k : Nat) :
    bag.mass (d, ids) k = (ids.map (fun i => if d[i]? = some k then 1 else 0)).sum := by
  change ((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => ids.count e.2)).sum =
    (ids.map (fun i => if d[i]? = some k then 1 else 0)).sum
  induction ids with
  | nil =>
      simpa using sum_map_zero (d.zipIdx.filter (fun e => e.1 == k))
  | cons i ids ih =>
      rw [List.map_cons, List.sum_cons]
      rw [show ((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => (i :: ids).count e.2)) =
          ((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => ids.count e.2 + (if i = e.2 then 1 else 0))) by
          apply List.map_congr_left
          intro e he
          simp [List.count_cons, Nat.add_comm]]
      rw [sum_map_add, ih]
      rw [show (((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => if i = e.2 then 1 else 0)).sum) =
          (if d[i]? = some k then 1 else 0) by
          simpa using zipIdx_count_from d 0 i k]
      omega

private theorem count_filterMap_eq_indicator_sum (d ids : List Nat) (k : Nat) :
    (bag.expand (d, ids)).count k = (ids.map (fun i => if d[i]? = some k then 1 else 0)).sum := by
  change (ids.filterMap (fun i => d[i]?)).count k = _
  induction ids with
  | nil => simp
  | cons i ids ih =>
      cases hi : d[i]? with
      | none =>
          simpa [hi] using ih
      | some v =>
          rw [List.filterMap_cons, hi]
          rw [List.count_cons]
          by_cases hvk : v = k
          · have hopt : d[i]? = some k := by rw [hi]; simp [hvk]
            simp [hvk, hopt, ih, Nat.add_comm]
          · have hopt : d[i]? ≠ some k := by
              intro hcontra
              rw [hi] at hcontra
              cases hcontra
              exact hvk rfl
            simp [hvk, hopt, ih]

private theorem mass_eq_count (d ids : List Nat) (k : Nat) :
    bag.mass (d, ids) k = (bag.expand (d, ids)).count k := by
  exact (mass_eq_indicator_sum d ids k).trans (count_filterMap_eq_indicator_sum d ids k).symm

private theorem mem_keys_of_mem_expand (d ids : List Nat) (k : Nat) :
    k ∈ bag.expand (d, ids) → k ∈ bag.keys (d, ids) := by
  intro h
  change k ∈ ids.filterMap (fun i => d[i]?) at h
  change k ∈ d
  rw [List.mem_filterMap] at h
  rcases h with ⟨i, hi, hval⟩
  rw [List.getElem?_eq_some_iff] at hval
  rcases hval with ⟨hlt, hget⟩
  rw [← hget]
  exact List.getElem_mem hlt

private theorem mass_zero_of_not_mem_expand (d ids : List Nat) (k : Nat)
    (h : k ∉ bag.expand (d, ids)) : bag.mass (d, ids) k = 0 := by
  rw [mass_eq_count]
  exact List.count_eq_zero.mpr h

private theorem weighted_eq_expand_sum (p : List Nat × List Nat) (f : Nat → Nat) :
    weighted bag p f = ((bag.expand p).map f).sum := by
  rcases p with ⟨d, ids⟩
  let m : Nat → Nat := fun k => bag.mass (d, ids) k
  change ((bag.keys (d, ids)).eraseDups.map fun k => m k * f k).sum =
    ((bag.expand (d, ids)).map f).sum
  have hL : ((bag.keys (d, ids)).eraseDups.map fun k => m k * f k).sum =
      ((bag.expand (d, ids)).eraseDups.map fun k => m k * f k).sum := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro x hx hxnot
      have hxnot' : x ∉ bag.expand (d, ids) := by
        intro hxE
        apply hxnot
        rw [List.mem_eraseDups]
        exact hxE
      simp [m, mass_zero_of_not_mem_expand d ids x hxnot']
    · intro x hx hxnot
      exfalso
      apply hxnot
      rw [List.mem_eraseDups]
      rw [List.mem_eraseDups] at hx
      exact mem_keys_of_mem_expand d ids x hx
  have hR : ((bag.expand (d, ids)).eraseDups.map fun k => m k * f k).sum =
      ((bag.expand (d, ids)).map f).sum := by
    rw [← dedup_weight_sum f (bag.expand (d, ids))]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    dsimp [m]
    rw [← mass_eq_count]
  exact hL.trans hR

/-! ## Goal -/

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c slack
  calc
    weighted bag a (fun x => weighted bag c
        (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat))
        = ((bag.expand a).map (fun x => weighted bag c
            (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat))).sum := by
              rw [weighted_eq_expand_sum]
    _ = ((bag.expand a).map (fun x =>
          ((bag.expand c).filter (fun y => x ≤ y && y ≤ x + slack)).length)).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro x hx
            rw [weighted_eq_expand_sum]
            exact sum_indicator_eq_filter_length (bag.expand c) x slack
    _ = (pairs (bag.expand a) (bag.expand c) (fun x y => x ≤ y && y ≤ x + slack)).length := by
            rw [pairs_length_eq_sum]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DictionaryAliases_JoinFanout
