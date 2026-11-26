import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `ExceptionOverlay.OrderStatistic` — Find an order statistic with ties by cumulative
  multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_OrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

private abbrev leNat (a b : Nat) : Bool := decide (a ≤ b)

private theorem leNat_trans : ∀ a b c : Nat, leNat a b = true → leNat b c = true → leNat a c = true := by
  intro a b c hab hbc
  simp [leNat] at hab hbc ⊢
  omega

private theorem leNat_total : ∀ a b : Nat, (leNat a b || leNat b a) = true := by
  intro a b
  simp [leNat]
  omega

private theorem sort_pairwise (l : List Nat) : (l.mergeSort).Pairwise (fun a b => a ≤ b) := by
  exact (List.pairwise_mergeSort leNat_trans leNat_total l).imp (by
    intro a b h
    simpa [leNat] using h)

private theorem countP_le_of_imp {p q : Nat → Bool} (h : ∀ x, p x = true → q x = true) :
    ∀ l : List Nat, l.countP p ≤ l.countP q
  | [] => by simp
  | a :: l => by
      simp only [List.countP_cons]
      have hp : (if p a = true then 1 else 0) ≤ (if q a = true then 1 else 0) := by
        by_cases hpa : p a = true
        · have hqa : q a = true := h a hpa
          simp [hpa, hqa]
        · simp [hpa]
      have hi := countP_le_of_imp h l
      omega

private theorem rank_bounds {zs : List Nat} (hs : zs.Pairwise (fun a b => a ≤ b)) {i : Nat}
    (hi : i < zs.length) :
    zs.countP (fun y => y < zs[i]) ≤ i ∧ i < zs.countP (fun y => y ≤ zs[i]) := by
  induction zs generalizing i with
  | nil => simp at hi
  | cons a tl ih =>
      cases hs with
      | cons hforall htail =>
          by_cases hi0 : i = 0
          · subst i
            have hzero : tl.countP (fun y => y < a) = 0 := by
              apply List.countP_eq_zero.mpr
              intro y hy
              have hay : a ≤ y := hforall y hy
              simp [Nat.not_lt.mpr hay]
            constructor
            · simp [hzero]
            · simp
          · have hilen : i < tl.length + 1 := by simpa [List.length_cons] using hi
            have hitl : i - 1 < tl.length := by omega
            have ihres := ih htail hitl
            have hsome : some ((a :: tl)[i]) = some (tl[i - 1]) := by
              rw [← List.getElem?_eq_getElem hi]
              simp [List.getElem?_cons, hi0]
            have hget : (a :: tl)[i] = tl[i - 1] := Option.some.inj hsome
            have hmem : tl[i - 1] ∈ tl := List.getElem_mem hitl
            have hav : a ≤ tl[i - 1] := hforall _ hmem
            constructor
            · rw [hget, List.countP_cons]
              by_cases hv : a < tl[i - 1] <;> simp [hv] <;> omega
            · rw [hget, List.countP_cons]
              simp [hav]
              omega

private theorem find?_sorted {d : List Nat} (hs : d.Pairwise (fun a b => a ≤ b)) (pred : Nat → Bool)
    {v : Nat} (hv : v ∈ d) (hp : pred v = true)
    (hb : ∀ x, x ∈ d → x < v → pred x = false) :
    d.find? pred = some v := by
  induction d with
  | nil => simp at hv
  | cons a tl ih =>
      cases hs with
      | cons hforall htail =>
          by_cases hva : a = v
          · subst a
            simp [hp]
          · have hv' : v ∈ tl := by
              cases List.mem_cons.mp hv with
              | inl h => exact False.elim (hva h.symm)
              | inr h => exact h
            have hav : a < v := by
              have hle : a ≤ v := hforall v hv'
              omega
            have hpa : pred a = false := hb a (by simp) hav
            simp [hpa]
            exact ih htail hv' (by
              intro x hx hxv
              exact hb x (by simp [hx]) hxv)

private theorem quantile_aux (xs d : List Nat) (i : Nat)
    (hds : d.Pairwise (fun a b => a ≤ b))
    (hsub : ∀ x, x ∈ xs → x ∈ d) :
    d.find? (fun x => decide (i < (xs.filter (fun y => y ≤ x)).length)) = xs.mergeSort[i]? := by
  let zs := xs.mergeSort
  have hlen : zs.length = xs.length := by simp [zs]
  have hperm : zs.Perm xs := by simpa [zs] using List.mergeSort_perm xs leNat
  have hsp : zs.Pairwise (fun a b => a ≤ b) := by simpa [zs] using sort_pairwise xs
  by_cases hi : i < xs.length
  · have hizs : i < zs.length := by omega
    let v := zs[i]
    have hv_zs : v ∈ zs := by
      dsimp [v]
      exact List.getElem_mem hizs
    have hv_xs : v ∈ xs := (hperm.mem_iff).mp hv_zs
    have hv_d : v ∈ d := hsub v hv_xs
    have hrank := rank_bounds hsp hizs
    have hrank' : zs.countP (fun y => y < v) ≤ i ∧ i < zs.countP (fun y => y ≤ v) := by
      simpa [v] using hrank
    have hp : decide (i < (xs.filter (fun y => y ≤ v)).length) = true := by
      have hcp : xs.countP (fun y => y ≤ v) = zs.countP (fun y => y ≤ v) :=
        (List.Perm.countP_eq (fun y => y ≤ v) hperm).symm
      have hcnt : i < (xs.filter (fun y => y ≤ v)).length := by
        rw [← List.countP_eq_length_filter]
        rw [hcp]
        exact hrank'.2
      simp [hcnt]
    have hbefore : ∀ x, x ∈ d → x < v →
        decide (i < (xs.filter (fun y => y ≤ x)).length) = false := by
      intro x hx hxv
      have hcp : xs.countP (fun y => y ≤ x) = zs.countP (fun y => y ≤ x) :=
        (List.Perm.countP_eq (fun y => y ≤ x) hperm).symm
      have hmono : zs.countP (fun y => y ≤ x) ≤ zs.countP (fun y => y < v) := by
        apply countP_le_of_imp
        intro y hy
        simp at hy ⊢
        omega
      have hcnt : (xs.filter (fun y => y ≤ x)).length ≤ i := by
        rw [← List.countP_eq_length_filter]
        rw [hcp]
        omega
      have hnot : ¬ i < (xs.filter (fun y => y ≤ x)).length := by omega
      simp [hnot]
    have hf : d.find? (fun x => decide (i < (xs.filter (fun y => y ≤ x)).length)) = some v :=
      find?_sorted hds _ hv_d hp hbefore
    have hg' : xs.mergeSort[i]? = some v := by
      have hg : zs[i]? = some v := by
        dsimp [v]
        exact List.getElem?_eq_getElem hizs
      simpa [zs] using hg
    rw [hg']
    exact hf
  · have hg : zs.length ≤ i := by omega
    have hnone : zs[i]? = none := List.getElem?_eq_none hg
    have hnone' : xs.mergeSort[i]? = none := by simpa [zs] using hnone
    rw [hnone']
    apply List.find?_eq_none.mpr
    intro x hx
    have hbnd : (xs.filter (fun y => y ≤ x)).length ≤ xs.length := List.length_filter_le _ _
    have hnot : ¬ i < (xs.filter (fun y => y ≤ x)).length := by omega
    simp [hnot]

private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  induction hl : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
    cases l with
    | nil => simp
    | cons a as =>
      have hlen : as.length + 1 = n := by simpa using hl
      rw [List.eraseDups_cons]
      apply List.Pairwise.cons
      · intro b hb
        have hbmem : b ∈ as.filter (fun c => ! c == a) := by simpa using hb
        have hne : !(b == a) = true := (List.mem_filter.mp hbmem).2
        have hba : b ≠ a := by
          intro heq
          subst heq
          simp at hne
        exact hba.symm
      · apply ih (as.filter (fun c => ! c == a)).length
        · have hle := List.length_filter_le (fun c => ! c == a) as
          omega
        · rfl

private theorem count_eraseDups (l : List Nat) (k : Nat) :
    l.eraseDups.count k = if k ∈ l then 1 else 0 := by
  rw [List.nodup_iff_count_eq_ite.mp (nodup_eraseDups l)]
  simp [List.mem_eraseDups]

private theorem perm_filter_lt_succ (l : List Nat) (n : Nat) :
    (l.eraseDups.filter (fun i => i < n + 1)).Perm
      (l.eraseDups.filter (fun i => i < n) ++ (if n ∈ l then [n] else [])) := by
  rw [List.perm_iff_count]
  intro k
  have h₁ : (l.eraseDups.filter (fun i => i < n + 1)).Nodup := (nodup_eraseDups l).filter _
  have h₂ : (l.eraseDups.filter (fun i => i < n) ++ (if n ∈ l then [n] else [])).Nodup := by
    change List.Pairwise (fun a b : Nat => a ≠ b)
      (l.eraseDups.filter (fun i => i < n) ++ (if n ∈ l then [n] else []))
    rw [List.pairwise_append]
    constructor
    · exact List.Pairwise.filter _ (nodup_eraseDups l)
    constructor
    · by_cases h : n ∈ l <;> simp [h]
    · intro a ha b hb
      have ha' : a < n := by
        have htmp := (List.mem_filter.mp ha).2
        simpa using htmp
      have hbn : b = n := by
        by_cases hnl : n ∈ l
        · simp [hnl] at hb
          exact hb
        · simp [hnl] at hb
      subst b
      omega
  rw [List.nodup_iff_count_eq_ite.mp h₁, List.nodup_iff_count_eq_ite.mp h₂]
  have hmem : (k ∈ l.eraseDups.filter (fun i => i < n + 1)) ↔
      (k ∈ l.eraseDups.filter (fun i => i < n) ++ (if n ∈ l then [n] else [])) := by
    constructor
    · intro h
      have hklt : k < n + 1 := by
        have htmp := (List.mem_filter.mp h).2
        simpa using htmp
      have hkE : k ∈ l.eraseDups := (List.mem_filter.mp h).1
      by_cases hkn : k < n
      · exact List.mem_append.mpr (Or.inl (List.mem_filter.mpr ⟨hkE, by simpa using hkn⟩))
      · have hkeq : k = n := by omega
        subst k
        apply List.mem_append.mpr
        right
        by_cases hnl : n ∈ l
        · simp [hnl]
        · have : n ∈ l := by simpa [List.mem_eraseDups] using hkE
          contradiction
    · intro h
      rcases List.mem_append.mp h with h | h
      · have hklt' : k < n := by
          have htmp := (List.mem_filter.mp h).2
          simpa using htmp
        exact List.mem_filter.mpr ⟨(List.mem_filter.mp h).1, by
          have htmp : k < n + 1 := Nat.lt_succ_of_lt hklt'
          simpa using htmp⟩
      · by_cases hnl : n ∈ l
        · simp [hnl] at h
          subst k
          exact List.mem_filter.mpr ⟨by simpa [List.mem_eraseDups] using hnl, by simp⟩
        · simp [hnl] at h
  simp [hmem]

private theorem filter_lt_length_le (l : List Nat) (n : Nat) :
    (l.eraseDups.filter (fun i => i < n)).length ≤ n := by
  induction n with
  | zero => simp
  | succ n ih =>
      have hp := perm_filter_lt_succ l n
      rw [hp.length_eq]
      by_cases h : n ∈ l <;> simp [h] <;> omega

private theorem lookup_some_of_mem_fst (es : List (Nat × Nat)) {n : Nat}
    (h : n ∈ es.map Prod.fst) : ∃ v, es.lookup n = some v := by
  have hne : es.lookup n ≠ none := by
    intro hnone
    rw [List.lookup_eq_none_iff] at hnone
    rcases List.mem_map.mp h with ⟨p, hp, hpf⟩
    have hneq := hnone p hp
    rw [hpf] at hneq
    simp at hneq
  cases h' : es.lookup n with
  | none => exact False.elim (hne h')
  | some v => exact ⟨v, rfl⟩

private theorem count_singleton (k x : Nat) : List.count k [x] = if x = k then 1 else 0 := by
  rw [List.count_cons]
  by_cases h : x = k
  · subst x
    simp
  · have hk : k ≠ x := by intro h'; exact h h'.symm
    simp [h]

private theorem mass_count (n d : Nat) (es : List (Nat × Nat)) (k : Nat) :
    bag.mass (n, d, es) k = (bag.expand (n, d, es)).count k := by
  induction n with
  | zero => simp [bag]
  | succ n ih =>
      dsimp [bag] at ih ⊢
      have h_expand : (List.range (n + 1)).map (fun i => (es.lookup i).getD d) =
          (List.range n).map (fun i => (es.lookup i).getD d) ++ [(es.lookup n).getD d] := by
        rw [List.range_succ, List.map_append]
        simp
      rw [h_expand, List.count_append, ← ih]
      have hps_bound : ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).length ≤ n :=
        filter_lt_length_le (es.map Prod.fst) n
      have hpperm := perm_filter_lt_succ (es.map Prod.fst) n
      by_cases hnmem : n ∈ es.map Prod.fst
      · rcases lookup_some_of_mem_fst es hnmem with ⟨v, hlookup⟩
        have hpperm' :
            ((es.map Prod.fst).eraseDups.filter (fun i => i < n + 1)).Perm
              ((es.map Prod.fst).eraseDups.filter (fun i => i < n) ++ [n]) := by
          simpa [hnmem] using hpperm
        have hlen : ((es.map Prod.fst).eraseDups.filter (fun i => i < n + 1)).length =
            ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).length + 1 := by
          rw [hpperm'.length_eq]
          simp
        have hcountP : ((es.map Prod.fst).eraseDups.filter (fun i => i < n + 1)).countP
              (fun i => es.lookup i == some k) =
            ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).countP
              (fun i => es.lookup i == some k) + (if es.lookup n == some k then 1 else 0) := by
          rw [hpperm'.countP_eq (fun i => es.lookup i == some k), List.countP_append]
          simp
        have hcntSingle : List.count k [(es.lookup n).getD d] = (if v = k then 1 else 0) := by
          simp [hlookup, count_singleton]
        rw [hlen, hcountP, hcntSingle, hlookup]
        simp_all <;> omega
      · have hlookup : es.lookup n = none := by
          rw [List.lookup_eq_none_iff]
          intro p hp
          have hmem : p.fst ∈ es.map Prod.fst := List.mem_map.mpr ⟨p, hp, rfl⟩
          by_cases heq : n = p.fst
          · subst n
            exact False.elim (hnmem hmem)
          · simp [heq]
        have hpperm' :
            ((es.map Prod.fst).eraseDups.filter (fun i => i < n + 1)).Perm
              ((es.map Prod.fst).eraseDups.filter (fun i => i < n)) := by
          simpa [hnmem] using hpperm
        have hlen : ((es.map Prod.fst).eraseDups.filter (fun i => i < n + 1)).length =
            ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).length := hpperm'.length_eq
        have hcountP : ((es.map Prod.fst).eraseDups.filter (fun i => i < n + 1)).countP
              (fun i => es.lookup i == some k) =
            ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).countP
              (fun i => es.lookup i == some k) := hpperm'.countP_eq _
        have hcntSingle : List.count k [(es.lookup n).getD d] = (if k = d then 1 else 0) := by
          rw [hlookup]
          simp [count_singleton]
          by_cases hkd : k = d
          · subst k
            simp
          · have hdk : d ≠ k := by intro h; exact hkd h.symm
            simp [hkd, hdk]
        rw [hlen, hcountP, hcntSingle]
        by_cases hkd : k = d
        · subst k
          simp_all <;> omega
        · simp_all

private theorem expand_subset_keys (n d : Nat) (es : List (Nat × Nat)) {x : Nat}
    (hx : x ∈ bag.expand (n, d, es)) : x ∈ bag.keys (n, d, es) := by
  dsimp [bag] at hx ⊢
  rcases List.mem_map.mp hx with ⟨i, hi, hix⟩
  cases hlook : es.lookup i with
  | none =>
      simp only [List.mem_cons, List.mem_map]
      left
      simpa [hlook] using hix.symm
  | some v =>
      right
      have hx' : x = v := by simpa [hlook] using hix.symm
      rcases List.lookup_eq_some_iff.mp hlook with ⟨l₁, l₂, hes, hfirst⟩
      apply List.mem_map.mpr
      refine ⟨(i, v), ?_, ?_⟩
      · rw [hes]
        simp
      · exact hx'.symm

private theorem sum_eq_zero_of_not_mem (l : List Nat) (a : Nat) (h : a ∉ l) (f : Nat → Nat) :
    (l.map (fun k => (if k = a then 1 else 0) * f k)).sum = 0 := by
  induction l with
  | nil => simp
  | cons b tl ih =>
      have hb : b ≠ a := by intro heq; exact h (by simp [heq])
      have htl : a ∉ tl := by intro ha; exact h (by simp [ha])
      simp [hb, ih htl]

private theorem sum_indicator_nodup (l : List Nat) (hnd : l.Nodup) (a : Nat) (ha : a ∈ l) (f : Nat → Nat) :
    (l.map (fun k => (if k = a then 1 else 0) * f k)).sum = f a := by
  induction l with
  | nil => simp at ha
  | cons b tl ih =>
      cases hnd with
      | cons hnot htlnod =>
          by_cases hba : b = a
          · subst b
            have hnot_a : a ∉ tl := by
              intro hmem
              exact hnot a hmem rfl
            rw [List.map_cons, List.sum_cons]
            simp
            rw [sum_eq_zero_of_not_mem tl a hnot_a f]
          · have hatl : a ∈ tl := by
              have hab : a ≠ b := by intro h; exact hba h.symm
              simpa [hab] using ha
            rw [List.map_cons, List.sum_cons]
            simp [hba]
            exact ih htlnod hatl

private theorem sum_map_add_nat (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a tl ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_map_zero (l : List Nat) : (l.map (fun _ : Nat => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons a tl ih => simp [ih]

private theorem sum_count_filter (ks xs : List Nat) (hsub : ∀ x ∈ xs, x ∈ ks) (f : Nat → Bool) :
    (ks.eraseDups.map (fun k => xs.count k * (f k).toNat)).sum = (xs.filter f).length := by
  induction xs with
  | nil =>
      simpa using sum_map_zero ks.eraseDups
  | cons a tl ih =>
      have hsub_tl : ∀ x ∈ tl, x ∈ ks := by
        intro x hx
        exact hsub x (by simp [hx])
      have ih' := ih hsub_tl
      have hmap : ks.eraseDups.map (fun k => (a :: tl).count k * (f k).toNat) =
          ks.eraseDups.map (fun k => tl.count k * (f k).toNat + (if k = a then 1 else 0) * (f k).toNat) := by
        apply List.map_congr_left
        intro k hk
        rw [List.count_cons]
        by_cases hka : k = a
        · subst k
          simp [Nat.add_mul]
        · have hak : a ≠ k := by intro h; exact hka h.symm
          have hbeq : (a == k) = false := by simp [hak]
          simp [hka, hbeq]
      rw [hmap, sum_map_add_nat, ih']
      have ha_ks : a ∈ ks.eraseDups := by simpa [List.mem_eraseDups] using hsub a (by simp)
      have hsingle : (ks.eraseDups.map (fun k => (if k = a then 1 else 0) * (f k).toNat)).sum = (f a).toNat :=
        sum_indicator_nodup ks.eraseDups (nodup_eraseDups ks) a ha_ks _
      rw [hsingle]
      by_cases hfa : f a = true <;> simp [hfa]

private theorem expand_subset_live (n d : Nat) (es : List (Nat × Nat)) {x : Nat}
    (hx : x ∈ bag.expand (n, d, es)) : x ∈ live bag (n, d, es) := by
  unfold live
  rw [List.mem_filter]
  constructor
  · simpa [List.mem_eraseDups] using expand_subset_keys n d es hx
  · rw [mass_count]
    simpa using (List.count_pos_iff.mpr hx)

private theorem below_eq_countP (n d : Nat) (es : List (Nat × Nat)) (x : Nat) :
    below bag (n, d, es) x = ((bag.expand (n, d, es)).filter (fun y => y ≤ x)).length := by
  unfold below weighted
  change ((bag.keys (n, d, es)).eraseDups.map
    (fun k => bag.mass (n, d, es) k * (decide (k ≤ x)).toNat)).sum = _
  simp only [mass_count]
  exact sum_count_filter (bag.keys (n, d, es)) (bag.expand (n, d, es))
    (by intro y hy; exact expand_subset_keys n d es hy) (fun y => decide (y ≤ x))

theorem sound_OrderStatistic : OrderStatistic.Goal := by
  intro p i
  rcases p with ⟨n, d, es⟩
  unfold quantile
  simpa [below_eq_countP] using
    (quantile_aux (bag.expand (n, d, es)) ((live bag (n, d, es)).mergeSort) i
      (sort_pairwise (live bag (n, d, es)))
      (by
        intro x hx
        exact List.mem_mergeSort.mpr (expand_subset_live n d es hx)))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_OrderStatistic
