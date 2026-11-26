import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `ExceptionOverlay.OuterWeight` — LEFT JOIN contributes one unmatched right
  placeholder or the complete right multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_OuterWeight

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

private def lhs (a c : Nat × Nat × List (Nat × Nat)) : Nat :=
  weighted bag a (fun x => max 1 (bag.mass c x))

private def rhs (a c : Nat × Nat × List (Nat × Nat)) : Nat :=
  (pairs (bag.expand a) (bag.expand c) (· == ·)).length +
  ((bag.expand a).filter (fun x => !(bag.expand c).contains x)).length

#eval lhs (5,3,[(1,7),(1,9),(12,4)]) (4,7,[(0,7),(2,3)])
#eval rhs (5,3,[(1,7),(1,9),(12,4)]) (4,7,[(0,7),(2,3)])
#eval lhs (3,1,[(0,5),(0,6),(2,1)]) (2,5,[(1,5)])
#eval rhs (3,1,[(0,5),(0,6),(2,1)]) (2,5,[(1,5)])

private def smallPairs : List (Nat × Nat) :=
  (List.range 3).flatMap fun x => (List.range 3).map fun y => (x, y)

private def smallES : List (List (Nat × Nat)) :=
  [[]] ++ (smallPairs.map fun p => [p]) ++
  (smallPairs.flatMap fun p => smallPairs.map fun q => [p,q])

private def smallBags : List (Nat × Nat × List (Nat × Nat)) :=
  (List.range 4).flatMap fun n =>
  (List.range 3).flatMap fun d =>
  smallES.map fun es => (n,d,es)

#eval (smallBags.flatMap fun a =>
  smallBags.flatMap fun c =>
    if lhs a c == rhs a c then [] else [(a,c)]).length

/-! ## Generic list lemmas -/

private theorem split_count (l : List Nat) (x : Nat) (f : Nat → Nat) :
    (l.map f).sum = l.count x * f x + ((l.filter fun y => !(y == x)).map f).sum := by
  induction l with
  | nil => simp
  | cons y l ih =>
      by_cases hy : y = x
      · subst y
        simp [ih, Nat.add_mul, Nat.add_assoc, Nat.add_comm]
      · have hbeq : (y == x) = false := by simp [hy]
        simp [hbeq, hy, ih, Nat.add_left_comm]

private theorem sum_count_of_sub_aux : ∀ n (ks : List Nat), ks.length = n →
    ∀ (xs : List Nat) (f : Nat → Nat), (∀ x ∈ xs, x ∈ ks) →
    (ks.eraseDups.map (fun k => xs.count k * f k)).sum = (xs.map f).sum := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro ks hlen xs f hsub
      cases ks with
      | nil =>
          have hxnil : xs = [] := List.eq_nil_iff_forall_not_mem.mpr (by
            intro x hx
            simpa using hsub x hx)
          subst xs
          simp
      | cons k ks =>
          rw [List.eraseDups_cons]
          simp only [List.map_cons, List.sum_cons]
          by_cases hk : k ∈ xs
          · let xs' := xs.filter (fun y => !(y == k))
            have hnotk : ∀ y ∈ xs', y ≠ k := by
              intro y hy
              have hp : (!(y == k)) = true := (List.mem_filter.mp hy).2
              intro hyk
              subst y
              simp at hp
            have hsub' : ∀ y ∈ xs', y ∈ ks.filter (fun b => !(b == k)) := by
              intro y hy
              have hyks : y ∈ ks := by
                have hyall := hsub y (List.mem_filter.mp hy).1
                rcases List.mem_cons.mp hyall with hEq | hTail
                · exact False.elim (hnotk y hy hEq)
                · exact hTail
              exact List.mem_filter.mpr ⟨hyks, by simp [hnotk y hy]⟩
            have hweights :
                ((ks.filter fun b => !(b == k)).eraseDups.map (fun y => xs.count y * f y)) =
                  ((ks.filter fun b => !(b == k)).eraseDups.map (fun y => xs'.count y * f y)) := by
              apply List.map_congr_left
              intro y hy
              have hyks : y ∈ ks.filter (fun b => !(b == k)) := List.mem_eraseDups.mp hy
              have hp : (!(y == k)) = true := (List.mem_filter.mp hyks).2
              have hcf : List.count y (List.filter (fun b : Nat => !(b == k)) xs) = xs.count y :=
                List.count_filter hp
              exact congrArg (fun t => t * f y) hcf.symm
            have hn : ks.length + 1 = n := by simpa using hlen
            have hlt : (ks.filter fun b => !(b == k)).length < n := by
              have hle := List.length_filter_le (fun b : Nat => !(b == k)) ks
              omega
            have hih := ih ((ks.filter fun b => !(b == k)).length) hlt
              (ks.filter fun b => !(b == k)) rfl xs' f hsub'
            rw [hweights, hih]
            exact (split_count xs k f).symm
          · have hzero : xs.count k = 0 := List.count_eq_zero.mpr hk
            have hsub' : ∀ y ∈ xs, y ∈ ks.filter (fun b => !(b == k)) := by
              intro y hy
              have hyks : y ∈ ks := by
                have hyall := hsub y hy
                rcases List.mem_cons.mp hyall with hEq | hTail
                · exact False.elim (hk (by simpa [hEq] using hy))
                · exact hTail
              have hyne : y ≠ k := by
                intro hEq
                apply hk
                simpa [hEq] using hy
              exact List.mem_filter.mpr ⟨hyks, by simp [hyne]⟩
            have hn : ks.length + 1 = n := by simpa using hlen
            have hlt : (ks.filter fun b => !(b == k)).length < n := by
              have hle := List.length_filter_le (fun b : Nat => !(b == k)) ks
              omega
            have hih := ih ((ks.filter fun b => !(b == k)).length) hlt
              (ks.filter fun b => !(b == k)) rfl xs f hsub'
            rw [hzero]
            simp
            exact hih

private theorem sum_count_of_sub (xs ks : List Nat) (f : Nat → Nat)
    (hsub : ∀ x ∈ xs, x ∈ ks) :
    (ks.eraseDups.map (fun k => xs.count k * f k)).sum = (xs.map f).sum := by
  exact sum_count_of_sub_aux ks.length ks rfl xs f hsub

private theorem length_filter_eq_sum (l : List Nat) (p : Nat → Bool) :
    (l.filter p).length = (l.map fun x => if p x then 1 else 0).sum := by
  induction l with
  | nil => simp
  | cons x l ih => by_cases hp : p x <;> simp [hp, ih, Nat.add_comm]

private theorem sum_const_nat (l : List Nat) (c : Nat) :
    (l.map fun _ => c).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.succ_mul, Nat.add_comm]

private theorem sum_map_add (l : List Nat) (g h : Nat → Nat) :
    (l.map fun x => g x + h x).sum = (l.map g).sum + (l.map h).sum := by
  induction l with
  | nil => simp
  | cons x l ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem max_one_eq (n : Nat) : max 1 n = n + if n = 0 then 1 else 0 := by
  by_cases h : n = 0
  · simp [h]
  · have hle : 1 ≤ n := by omega
    rw [Nat.max_eq_right hle]
    simp [h]

private theorem filter_partition_length (l : List Nat) (p : Nat → Bool) :
    (l.filter p).length + (l.filter fun x => !(p x)).length = l.length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x <;> simp [hp] <;> omega

private theorem filter_split_len (l : List Nat) (p f : Nat → Bool) :
    (l.filter f).length =
      (l.filter (fun x => p x && f x)).length +
      (l.filter (fun x => !(p x) && f x)).length := by
  have h := filter_partition_length (l.filter f) p
  rw [List.filter_filter, List.filter_filter] at h
  omega

private theorem sum_split_filter (l : List Nat) (p : Nat → Bool) (f : Nat → Nat) :
    (l.map f).sum = ((l.filter p).map f).sum + ((l.filter fun x => !(p x)).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x <;> simp [hp, ih, Nat.add_assoc, Nat.add_left_comm]

/-! ## ExceptionOverlay specific lemmas -/

private theorem nodup_eraseDups_aux : ∀ n (l : List Nat), l.length = n → l.eraseDups.Nodup := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro l hlen
      cases l with
      | nil => simp
      | cons a l =>
          rw [List.eraseDups_cons]
          apply List.nodup_cons.mpr
          constructor
          · intro h
            have h' : a ∈ l.filter (fun b => !(b == a)) := List.mem_eraseDups.mp h
            have hp : (!(a == a)) = true := (List.mem_filter.mp h').2
            have haa : (a == a) = true := by simp
            rw [haa] at hp
            simp at hp
          · apply ih (l.filter (fun b => !(b == a))).length
            · have hle := List.length_filter_le (fun b : Nat => !(b == a)) l
              have hlen' : l.length + 1 = n := by simpa using hlen
              omega
            · rfl

private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup :=
  nodup_eraseDups_aux l.length l rfl

private theorem mem_fst_iff_lookup_some (es : List (Nat × Nat)) (i : Nat) :
    i ∈ es.map Prod.fst ↔ ∃ v, es.lookup i = some v := by
  constructor
  · intro h
    induction es with
    | nil => simp at h
    | cons p es ih =>
        rcases p with ⟨j, v⟩
        simp only [List.lookup_cons]
        by_cases hij : i == j
        · exact ⟨v, by simp [hij]⟩
        · have hmem : i ∈ es.map Prod.fst := by
            simp only [List.map_cons, List.mem_cons] at h
            rcases h with hhead | htail
            · have hij' : i == j := by simpa [beq_iff_eq] using hhead
              simp [hij'] at hij
            · exact htail
          obtain ⟨w, hw⟩ := ih hmem
          exact ⟨w, by simp [hij, hw]⟩
  · rintro ⟨v, hv⟩
    induction es generalizing v with
    | nil => simp at hv
    | cons p es ih =>
        rcases p with ⟨j, w⟩
        simp only [List.lookup_cons] at hv
        by_cases hij : i == j
        · have hij' : i = j := by simpa [beq_iff_eq] using hij
          subst j
          simp
        · simp only [List.map_cons, List.mem_cons]
          right
          exact ih v (by simpa [hij] using hv)

private theorem mem_of_lookup_eq_some (es : List (Nat × Nat)) (i v : Nat)
    (hv : es.lookup i = some v) : (i, v) ∈ es := by
  induction es with
  | nil => simp at hv
  | cons p es ih =>
      rcases p with ⟨j, w⟩
      simp only [List.lookup_cons] at hv
      by_cases hij : i == j
      · have hij' : i = j := by simpa [beq_iff_eq] using hij
        subst j
        have hvw : v = w := by simpa using hv.symm
        subst v
        simp
      · simp only [List.mem_cons]
        right
        exact ih (by simpa [hij] using hv)

private theorem ps_perm (n : Nat) (es : List (Nat × Nat)) :
    List.Perm ((es.map Prod.fst).eraseDups.filter (fun i => i < n))
      ((List.range n).filter (fun i => (es.lookup i).isSome)) := by
  have hnd1 : ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).Nodup :=
    (nodup_eraseDups (es.map Prod.fst)).filter (fun i => i < n)
  have hnd2 : ((List.range n).filter (fun i => (es.lookup i).isSome)).Nodup :=
    List.nodup_range.filter (fun i => (es.lookup i).isSome)
  exact (List.perm_ext_iff_of_nodup hnd1 hnd2).2 (by
    intro i
    constructor
    · intro hi
      have hi_u : i ∈ (es.map Prod.fst).eraseDups := (List.mem_filter.mp hi).1
      have hlt : i < n := by simpa using (List.mem_filter.mp hi).2
      have hmemfst : i ∈ es.map Prod.fst := List.mem_eraseDups.mp hi_u
      obtain ⟨v, hv⟩ := (mem_fst_iff_lookup_some es i).mp hmemfst
      exact List.mem_filter.mpr ⟨List.mem_range.mpr hlt, by simp [hv]⟩
    · intro hi
      have hlt : i < n := List.mem_range.mp (List.mem_filter.mp hi).1
      have hsome : (es.lookup i).isSome = true := (List.mem_filter.mp hi).2
      obtain ⟨v, hv⟩ := Option.isSome_iff_exists.mp hsome
      have hmemfst : i ∈ es.map Prod.fst := (mem_fst_iff_lookup_some es i).mpr ⟨v, hv⟩
      exact List.mem_filter.mpr ⟨List.mem_eraseDups.mpr hmemfst, by simp [hlt]⟩)

private theorem mass_formula (n d k : Nat) (es : List (Nat × Nat)) :
    bag.mass (n,d,es) k =
      (if k == d then n - ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).length else 0) +
      (((es.map Prod.fst).eraseDups.filter (fun i => i < n)).countP
        (fun i => es.lookup i == some k)) := by
  rfl

private theorem val_some (es : List (Nat × Nat)) (d i k : Nat) (v : Nat)
    (hv : es.lookup i = some v) :
    (decide (((es.lookup i).getD d) == k)).toNat =
      (decide (es.lookup i == some k)).toNat := by
  rw [hv]
  simp

private theorem val_none (es : List (Nat × Nat)) (d i k : Nat)
    (hv : es.lookup i = none) :
    (decide (((es.lookup i).getD d) == k)).toNat = (if k == d then 1 else 0) := by
  rw [hv]
  by_cases hdk : d = k
  · subst k
    simp
  · have hkd : k ≠ d := by intro h; exact hdk h.symm
    simp [hdk, hkd]

private theorem mass_eq_count (n d k : Nat) (es : List (Nat × Nat)) :
    bag.mass (n,d,es) k = (bag.expand (n,d,es)).count k := by
  let ps := (es.map Prod.fst).eraseDups.filter (fun i => i < n)
  let S := (List.range n).filter (fun i => (es.lookup i).isSome)
  let Sn := (List.range n).filter (fun i => !(es.lookup i).isSome)
  let q := fun i => es.lookup i == some k
  let val := fun i => (es.lookup i).getD d
  have hperm : List.Perm ps S := by simpa [ps, S] using ps_perm n es
  have hlen : ps.length = S.length := hperm.length_eq
  have hq : ps.countP q = S.countP q := List.Perm.countP_eq q hperm
  have hpart : S.length + Sn.length = n := by
    simpa [S, Sn] using filter_partition_length (List.range n) (fun i => (es.lookup i).isSome)
  have hsub : n - ps.length = Sn.length := by omega
  have hdefault : (if k == d then n - ps.length else 0) = Sn.length * (if k == d then 1 else 0) := by
    by_cases hkd : k == d <;> simp [hkd, hsub]
  have hpoint : ∀ i ∈ List.range n,
      (if (es.lookup i).isSome then
        (if (es.lookup i == some k) then 1 else 0)
      else (if k == d then 1 else 0)) =
      (if val i == k then 1 else 0) := by
    intro i hi
    by_cases hs : (es.lookup i).isSome = true
    · obtain ⟨v, hv⟩ := Option.isSome_iff_exists.mp hs
      have hval : val i = v := by simp [val, hv]
      simp only [ite_eq_left hs]
      have hbeq : (es.lookup i == some k) = (v == k) := by simp [hv]
      rw [hbeq, hval]
    · have hn : es.lookup i = none := Option.not_isSome_iff_eq_none.mp hs
      have hval : val i = d := by simp [val, hn]
      simp only [ite_eq_right hs]
      rw [hval]
      by_cases hdk : d = k
      · subst k
        simp
      · have hkd : k ≠ d := by intro h; exact hdk h.symm
        simp [hdk, hkd]
  have hsum : ((List.range n).map fun i =>
        if (es.lookup i).isSome then (if q i then 1 else 0)
        else (if k == d then 1 else 0)).sum =
      ((List.range n).map (fun i => if val i == k then 1 else 0)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro i hi
    exact hpoint i hi
  have hsum_split : ((List.range n).map fun i =>
        if (es.lookup i).isSome then (if q i then 1 else 0)
        else (if k == d then 1 else 0)).sum =
      S.countP q + Sn.length * (if k == d then 1 else 0) := by
    rw [sum_split_filter]
    · have hSmap : ((S.map fun i => if (es.lookup i).isSome then (if q i then 1 else 0)
          else (if k == d then 1 else 0)).sum) = S.countP q := by
        rw [show (S.map fun i => if (es.lookup i).isSome then (if q i then 1 else 0)
              else (if k == d then 1 else 0)) = (S.map fun i => if q i then 1 else 0) by
          apply List.map_congr_left
          intro i hi
          have : (es.lookup i).isSome = true := (List.mem_filter.mp hi).2
          simp [this]]
        exact (length_filter_eq_sum S q).symm.trans List.countP_eq_length_filter.symm
      have hSnmap : ((Sn.map fun i => if (es.lookup i).isSome then (if q i then 1 else 0)
          else (if k == d then 1 else 0)).sum) = Sn.length * (if k == d then 1 else 0) := by
        rw [show (Sn.map fun i => if (es.lookup i).isSome then (if q i then 1 else 0)
              else (if k == d then 1 else 0)) = (Sn.map fun _ => (if k == d then 1 else 0)) by
          apply List.map_congr_left
          intro i hi
          have : (!(es.lookup i).isSome) = true := (List.mem_filter.mp hi).2
          have hf : (es.lookup i).isSome = false := by cases h : (es.lookup i).isSome <;> simp_all
          simp [hf]]
        exact sum_const_nat Sn (if k == d then 1 else 0)
      rw [hSmap, hSnmap]
  have hcount : (bag.expand (n,d,es)).count k =
      ((List.range n).map (fun i => if val i == k then 1 else 0)).sum := by
    change ((List.range n).map val).count k = _
    rw [List.count_eq_length_filter, List.filter_map, List.length_map]
    exact length_filter_eq_sum (List.range n) (fun i => val i == k)
  have hgoal : (if k == d then n - ps.length else 0) + ps.countP q =
      S.countP q + Sn.length * (if k == d then 1 else 0) := by
    rw [hdefault, hq]
    omega
  rw [mass_formula, hgoal, ← hsum_split, hsum, ← hcount]

private theorem mem_expand_mem_keys {p : Nat × Nat × List (Nat × Nat)} {x : Nat} :
    x ∈ bag.expand p → x ∈ bag.keys p := by
  rcases p with ⟨n, d, es⟩
  intro h
  change x ∈ (List.range n).map (fun i => (es.lookup i).getD d) at h
  rw [List.mem_map] at h
  rcases h with ⟨i, hi, hval⟩
  change x ∈ d :: es.map Prod.snd
  by_cases hs : (es.lookup i).isSome = true
  · obtain ⟨v, hv⟩ := Option.isSome_iff_exists.mp hs
    have hxv : x = v := by simpa [hv] using hval.symm
    have hmem : (i, v) ∈ es := mem_of_lookup_eq_some es i v hv
    have hvin : v ∈ es.map Prod.snd := List.mem_map.mpr ⟨(i,v), hmem, rfl⟩
    rw [hxv]
    exact List.mem_cons_of_mem d hvin
  · have hn : es.lookup i = none := Option.not_isSome_iff_eq_none.mp hs
    have hxd : x = d := by simpa [hn] using hval.symm
    rw [hxd]
    simp

private theorem weighted_eq_sum_expand (p : Nat × Nat × List (Nat × Nat)) (f : Nat → Nat) :
    weighted bag p f = ((bag.expand p).map f).sum := by
  unfold weighted
  change (((bag.keys p).eraseDups.map fun k => bag.mass p k * f k).sum) =
    ((bag.expand p).map f).sum
  have hmass :
      ((bag.keys p).eraseDups.map fun k => bag.mass p k * f k) =
        ((bag.keys p).eraseDups.map fun k => (bag.expand p).count k * f k) := by
    apply List.map_congr_left
    intro k hk
    rcases p with ⟨n, d, es⟩
    rw [mass_eq_count]
  rw [hmass]
  exact sum_count_of_sub (bag.expand p) (bag.keys p) f (by
    intro x hx
    exact mem_expand_mem_keys hx)

/-! ## Goal -/

private theorem pairs_len_eq_sum_count (xs ys : List Nat) :
    (pairs xs ys (· == ·)).length = (xs.map fun x => ys.count x).sum := by
  unfold pairs
  rw [List.length_flatMap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  rw [List.length_map]
  have hbeq : (ys.filter fun y => x == y) = ys.filter (fun y => y == x) := by
    apply List.filter_congr
    intro y hy
    exact Bool.beq_comm
  rw [hbeq]
  exact List.count_eq_length_filter.symm

private theorem filter_notcontains_eq (xs ys : List Nat) :
    (xs.filter fun x => !ys.contains x) = xs.filter (fun x => decide (¬ x ∈ ys)) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x ∈ ys <;> simp [hx]

private theorem count_zero_iff_notmem (ys : List Nat) (x : Nat) :
    (ys.count x == 0) = true ↔ x ∉ ys := by
  constructor
  · intro h hmem
    have hn : ys.count x ≠ 0 := by
      intro hz
      exact ((List.count_eq_zero).mp hz) hmem
    simp [hn] at h
  · intro h
    have hz : ys.count x = 0 := List.count_eq_zero.mpr h
    simp [hz]

private theorem filter_notcontains_length (xs ys : List Nat) :
    (xs.filter fun x => !ys.contains x).length =
      (xs.map fun x => if ys.count x = 0 then 1 else 0).sum := by
  rw [filter_notcontains_eq]
  rw [length_filter_eq_sum]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  by_cases hz : ys.count x = 0
  · have hnmem : x ∉ ys := (count_zero_iff_notmem ys x).mp (by simp [hz])
    simp [hz, hnmem]
  · have hmem : x ∈ ys := by
      by_cases hm : x ∈ ys
      · exact hm
      · exact False.elim (hz (List.count_eq_zero.mpr hm))
    have hpred : (decide (¬ x ∈ ys)) = false := by simp [hmem]
    simp [hz, hpred]

private theorem sum_count_plus_filter (xs ys : List Nat) :
    (xs.map fun x => ys.count x).sum + (xs.filter fun x => !ys.contains x).length =
      (xs.map fun x => max 1 (ys.count x)).sum := by
  rw [filter_notcontains_length]
  rw [← sum_map_add]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  exact (max_one_eq (ys.count x)).symm

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a c
  let xs := bag.expand a
  let ys := bag.expand c
  have hleft : weighted bag a (fun x => max 1 (bag.mass c x)) =
      ((xs.map fun x => max 1 (ys.count x)).sum) := by
    rw [weighted_eq_sum_expand]
    change ((xs.map fun x => max 1 (bag.mass c x)).sum) =
      ((xs.map fun x => max 1 (ys.count x)).sum)
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    rcases c with ⟨n, d, es⟩
    rw [mass_eq_count]
  have hpairs : (pairs xs ys (· == ·)).length = (xs.map fun x => ys.count x).sum :=
    pairs_len_eq_sum_count xs ys
  rw [hleft, hpairs]
  exact (sum_count_plus_filter xs ys).symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_OuterWeight
