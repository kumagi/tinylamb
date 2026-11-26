import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `ExceptionOverlay.AntiRevenue` — Compute revenue of all left rows whose key is absent
  on the right.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_AntiRevenue

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

set_option linter.unusedSimpArgs false

private theorem mem_fst_of_lookup_some {es : List (Nat × Nat)} {i v : Nat}
    (h : es.lookup i = some v) : i ∈ es.map Prod.fst := by
  induction es with
  | nil => simp at h
  | cons p es ih =>
      rcases p with ⟨j, w⟩
      by_cases hij : i == j
      · have hij' : i = j := by simpa using hij
        simp [hij']
      · have h' : es.lookup i = some v := by simpa [List.lookup_cons, hij] using h
        have hm := ih h'
        simp [hij, hm]

private theorem mem_snd_of_lookup_some {es : List (Nat × Nat)} {i v : Nat}
    (h : es.lookup i = some v) : v ∈ es.map Prod.snd := by
  induction es with
  | nil => simp at h
  | cons p es ih =>
      rcases p with ⟨j, w⟩
      by_cases hij : i == j
      · have hij' : i = j := by simpa using hij
        subst j
        simp at h
        simp [h.symm]
      · have h' : es.lookup i = some v := by simpa [List.lookup_cons, hij] using h
        have hm := ih h'
        simp [hij, hm]

private theorem lookup_none_iff_not_mem_fst (es : List (Nat × Nat)) (i : Nat) :
    es.lookup i = none ↔ i ∉ es.map Prod.fst := by
  constructor
  · intro h hm
    rcases List.mem_map.mp hm with ⟨⟨j, v⟩, hj, hv⟩
    have hn := (List.lookup_eq_none_iff.mp h) (j, v) hj
    have hneq : i ≠ j := by
      simp [beq_iff_eq] at hn
      exact hn
    exact hneq hv.symm
  · intro h
    rw [List.lookup_eq_none_iff]
    intro p hp
    by_cases hpi : i == p.1
    · have hi_eq : i = p.1 := by simpa using hpi
      exfalso
      apply h
      rw [hi_eq]
      exact List.mem_map_of_mem hp
    · have hneq : i ≠ p.1 := by
        simp [beq_iff_eq] at hpi
        exact hpi
      simp [hneq]

private theorem countP_getD_aux (l : List Nat) (es : List (Nat × Nat)) (d k : Nat) :
    l.countP (fun i => ((es.lookup i).getD d) == k) =
      (if d == k then l.countP (fun i => es.lookup i == none) else 0) +
        l.countP (fun i => es.lookup i == some k) := by
  induction l with
  | nil => simp
  | cons i is ih =>
      by_cases h : es.lookup i = none
      · by_cases hdk : d == k
        · simp [h, hdk, ih, Nat.add_assoc, Nat.add_comm]
        · simp [h, hdk, ih, Nat.add_assoc, Nat.add_comm]
      · rcases ho : es.lookup i with _ | v
        · contradiction
        · by_cases hvk : v == k
          · simp [ho, hvk, ih]
            omega
          · simp [ho, hvk, ih]

private theorem countP_compl (p : Nat → Bool) (l : List Nat) :
    l.countP p + l.countP (fun i => !p i) = l.length := by
  induction l with
  | nil => simp
  | cons i is ih =>
      cases hpi : p i <;> simp [hpi, ih] <;> omega

private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | x :: xs =>
      rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro hx
        simp at hx
      · exact nodup_eraseDups (xs.filter (fun b => !b == x))
termination_by l.length
decreasing_by
  have hle := List.length_filter_le (fun b : Nat => !b == x) xs
  simp at *
  omega

private theorem countP_none_range (n : Nat) (es : List (Nat × Nat)) :
    (List.range n).countP (fun i => es.lookup i == none) =
      n - ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).length := by
  have hperm : ((List.range n).filter (fun i => es.lookup i != none)).Perm
      ((es.map Prod.fst).eraseDups.filter (fun j => j < n)) := by
    apply (List.perm_ext_iff_of_nodup (List.nodup_range.filter _)
      ((nodup_eraseDups (es.map Prod.fst)).filter _)).2
    intro i
    constructor
    · intro hi
      rcases List.mem_filter.mp hi with ⟨hir, hne⟩
      have hir' : i < n := by simpa using hir
      have hmem : i ∈ es.map Prod.fst := by
        by_cases hm : i ∈ es.map Prod.fst
        · exact hm
        · have hn : es.lookup i = none := (lookup_none_iff_not_mem_fst es i).2 hm
          exfalso
          simp [hn] at hne
      exact List.mem_filter.mpr ⟨List.mem_eraseDups.mpr hmem, by simp [hir']⟩
    · intro hi
      rcases List.mem_filter.mp hi with ⟨hie, hlt⟩
      have hir' : i < n := by simpa using hlt
      have hmem : i ∈ es.map Prod.fst := List.mem_eraseDups.mp hie
      have hne : (es.lookup i != none) = true := by
        have hn : es.lookup i ≠ none := by
          intro hnone
          exact (lookup_none_iff_not_mem_fst es i).1 hnone hmem
        cases hopt : es.lookup i with
        | none => exact (hn hopt).elim
        | some v => simp
      exact List.mem_filter.mpr ⟨by simpa using hir', hne⟩
  have hc : (List.range n).countP (fun i => es.lookup i != none) =
      ((es.map Prod.fst).eraseDups.filter (fun j => j < n)).length := by
    simpa [List.countP_eq_length_filter] using hperm.length_eq
  have hsum := countP_compl (fun i => es.lookup i == none) (List.range n)
  have hc' : (List.range n).countP (fun i => es.lookup i != none) =
      (List.range n).countP (fun i => !(es.lookup i == none)) := by
    apply List.countP_congr
    intro i hi
    cases hopt : es.lookup i <;> simp [hopt]
  rw [hc'] at hc
  simp [List.length_range] at hsum hc ⊢
  omega

private theorem countP_some_range (n k : Nat) (es : List (Nat × Nat)) :
    (List.range n).countP (fun i => es.lookup i == some k) =
      ((es.map Prod.fst).eraseDups.filter (fun i => i < n)).countP
        (fun i => es.lookup i == some k) := by
  have hA : ((List.range n).filter (fun i => es.lookup i == some k)).Nodup :=
    List.nodup_range.filter _
  have hB : (((es.map Prod.fst).eraseDups.filter (fun i => i < n)).filter
      (fun i => es.lookup i == some k)).Nodup :=
    ((nodup_eraseDups (es.map Prod.fst)).filter _).filter _
  have hperm : ((List.range n).filter (fun i => es.lookup i == some k)).Perm
      (((es.map Prod.fst).eraseDups.filter (fun i => i < n)).filter
        (fun i => es.lookup i == some k)) := by
    apply (List.perm_ext_iff_of_nodup hA hB).2
    intro i
    constructor
    · intro hi
      rcases List.mem_filter.mp hi with ⟨hir, hsk⟩
      have hir' : i < n := by simpa using hir
      have hl : es.lookup i = some k := by
        cases hopt : es.lookup i with
        | none => simp [hopt] at hsk
        | some v =>
            have hvk : v = k := by simpa [hopt] using hsk
            subst k
            rfl
      have hmem : i ∈ es.map Prod.fst := mem_fst_of_lookup_some hl
      exact List.mem_filter.mpr ⟨List.mem_filter.mpr
        ⟨List.mem_eraseDups.mpr hmem, by simp [hir']⟩, hsk⟩
    · intro hi
      rcases List.mem_filter.mp hi with ⟨hie, hsk⟩
      rcases List.mem_filter.mp hie with ⟨hed, hlt⟩
      have hir' : i < n := by simpa using hlt
      exact List.mem_filter.mpr ⟨by simpa using hir', hsk⟩
  simpa [List.countP_eq_length_filter] using hperm.length_eq

private theorem count_expand_eq_mass (n d : Nat) (es : List (Nat × Nat)) (k : Nat) :
    (bag.expand (n, d, es)).count k = bag.mass (n, d, es) k := by
  unfold bag
  simp only
  rw [List.count_eq_countP, List.countP_map]
  change (List.range n).countP (fun i => ((es.lookup i).getD d) == k) = _
  rw [countP_getD_aux, countP_none_range, countP_some_range]
  by_cases hdk : d == k
  · have heqdk : d = k := by simpa [beq_iff_eq] using hdk
    have hkdeq : k == d := by simpa [beq_iff_eq] using heqdk.symm
    simp [hdk, hkdeq]
  · have hkdeq : (k == d) = false := by
      have hkne : k ≠ d := by
        intro h
        apply hdk
        simp [beq_iff_eq, h]
      simpa [beq_iff_eq] using hkne
    simp [hdk, hkdeq]

private theorem mem_expand_mem_keys (n d : Nat) (es : List (Nat × Nat)) {k : Nat}
    (hk : k ∈ bag.expand (n, d, es)) : k ∈ bag.keys (n, d, es) := by
  unfold bag at hk ⊢
  simp only [List.mem_map] at hk
  rcases hk with ⟨i, hi, hval⟩
  by_cases h : es.lookup i = none
  · have hk_eq : k = d := by simpa [h] using hval.symm
    rw [hk_eq]
    simp
  · rcases ho : es.lookup i with _ | v
    · contradiction
    · have hk_eq : k = v := by simpa [ho] using hval.symm
      rw [hk_eq]
      have hvmem := mem_snd_of_lookup_some ho
      simp [hvmem]

private theorem sum_eraseDups_filter_aux (x : Nat) (h : Nat → Nat) :
    ∀ n, ∀ l : List Nat, l.length = n → (x ∈ l ∨ h x = 0) →
      (l.eraseDups.map h).sum = ((l.filter (fun b => !b == x)).eraseDups.map h).sum + h x := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro l hlen hc
      cases l with
      | nil =>
          simp at hc ⊢
          exact hc.symm
      | cons y ys =>
          by_cases hyx : y == x
          · have hyx' : y = x := by simpa using hyx
            subst y
            simp [List.eraseDups_cons, Nat.add_comm]
          · have hlen_l' : (ys.filter (fun b => !b == y)).length < n := by
              have hle := List.length_filter_le (fun b : Nat => !b == y) ys
              simp at hlen
              omega
            have hcond : x ∈ ys.filter (fun b => !b == y) ∨ h x = 0 := by
              rcases hc with hcin | hx0
              · left
                have hxne : x ≠ y := by
                  intro heq
                  subst heq
                  simp at hyx
                simp only [List.mem_cons] at hcin
                rcases hcin with hxy | hxys
                · subst hxy
                  contradiction
                · simp [hxne, hxys]
              · right
                exact hx0
            have ih' := ih (ys.filter (fun b => !b == y)).length hlen_l'
              (ys.filter (fun b => !b == y)) rfl hcond
            calc
              ((y :: ys).eraseDups.map h).sum =
                  h y + ((ys.filter (fun b => !b == y)).eraseDups.map h).sum := by
                simp [List.eraseDups_cons]
              _ = h y + ((((ys.filter (fun b => !b == y)).filter (fun b => !b == x)).eraseDups.map h).sum + h x) := by
                rw [ih']
              _ = (((y :: ys).filter (fun b => !b == x)).eraseDups.map h).sum + h x := by
                simp [hyx, List.eraseDups_cons, List.filter_filter, Bool.and_comm, Nat.add_assoc]

private theorem sum_eraseDups_filter (x : Nat) (h : Nat → Nat) (l : List Nat)
    (hc : x ∈ l ∨ h x = 0) :
    (l.eraseDups.map h).sum = ((l.filter (fun b => !b == x)).eraseDups.map h).sum + h x :=
  sum_eraseDups_filter_aux x h l.length l rfl hc

private theorem sum_count_cons_ne (x : Nat) (f : Nat → Nat) (xs l : List Nat)
    (hl : ∀ k ∈ l, (k == x) = false) :
    (l.map (fun k => (x :: xs).count k * f k)).sum =
    (l.map (fun k => xs.count k * f k)).sum := by
  induction l with
  | nil => simp
  | cons k ks ih =>
      have hkne : k ≠ x := by simpa using hl k (by simp)
      have hks : ∀ a ∈ ks, (a == x) = false := by
        intro a ha
        exact hl a (by simp [ha])
      have ih' := ih hks
      simp only [List.map_cons, List.sum_cons]
      rw [ih']
      simp [hkne.symm]

private theorem sum_eq_sum_count (f : Nat → Nat) : ∀ l : List Nat,
    (l.map f).sum = (l.eraseDups.map (fun k => l.count k * f k)).sum := by
  intro l
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hcond : x ∈ xs ∨ (fun k => xs.count k * f k) x = 0 := by
        by_cases hx : x ∈ xs
        · exact Or.inl hx
        · right
          simp [List.count_eq_zero_of_not_mem hx]
      have hB := sum_eraseDups_filter x (fun k => xs.count k * f k) xs hcond
      have hne : ((xs.filter (fun b => !b == x)).eraseDups.map
          (fun k => (x :: xs).count k * f k)).sum =
          ((xs.filter (fun b => !b == x)).eraseDups.map
          (fun k => xs.count k * f k)).sum := by
        apply sum_count_cons_ne
        intro k hk
        have hk' : k ∈ xs.filter (fun b => !b == x) := by simpa using hk
        have hkne : k ≠ x := by
          simp at hk'
          exact hk'.2
        simpa using hkne
      calc
        ((x :: xs).map f).sum
            = f x + (xs.map f).sum := by simp
        _ = f x + (xs.eraseDups.map (fun k => xs.count k * f k)).sum := by rw [ih]
        _ = f x + (((xs.filter (fun b => !b == x)).eraseDups.map
              (fun k => xs.count k * f k)).sum + xs.count x * f x) := by rw [hB]
        _ = (xs.count x + 1) * f x + ((xs.filter (fun b => !b == x)).eraseDups.map
              (fun k => xs.count k * f k)).sum := by
            simp [Nat.add_mul, Nat.add_assoc, Nat.add_comm]
        _ = ((x :: xs).eraseDups.map (fun k => (x :: xs).count k * f k)).sum := by
            simp only [List.eraseDups_cons, List.map_cons, List.sum_cons]
            rw [hne]
            simp

private theorem sum_eraseDups_subset {l1 l2 : List Nat} {h : Nat → Nat}
    (hsub : ∀ k ∈ l1, k ∈ l2) (hz : ∀ k ∈ l2, k ∉ l1 → h k = 0) :
    (l1.eraseDups.map h).sum = (l2.eraseDups.map h).sum := by
  match l2 with
  | [] =>
      have hl1 : l1 = [] := by
        cases l1 with
        | nil => rfl
        | cons x xs =>
            have hx := hsub x (by simp)
            simp at hx
      simp [hl1]
  | x :: xs =>
      by_cases hx1 : x ∈ l1
      · have hB := sum_eraseDups_filter x h l1 (Or.inl hx1)
        have hsub' : ∀ k ∈ l1.filter (fun b => !b == x),
            k ∈ xs.filter (fun b => !b == x) := by
          intro k hk
          rcases List.mem_filter.mp hk with ⟨hk1, hk2⟩
          have hkne : k ≠ x := by
            simpa [beq_iff_eq] using hk2
          have hkx : k ∈ x :: xs := hsub k hk1
          simp only [List.mem_cons] at hkx
          rcases hkx with rfl | hkxs
          · contradiction
          · exact List.mem_filter.mpr ⟨hkxs, by simp [hkne]⟩
        have hz' : ∀ k ∈ xs.filter (fun b => !b == x),
            k ∉ l1.filter (fun b => !b == x) → h k = 0 := by
          intro k hk hknot
          rcases List.mem_filter.mp hk with ⟨hk1, hk2⟩
          apply hz k (by simp [hk1])
          intro hkl1
          apply hknot
          exact List.mem_filter.mpr ⟨hkl1, hk2⟩
        have ih' := sum_eraseDups_subset (l1 := l1.filter (fun b => !b == x))
          (l2 := xs.filter (fun b => !b == x)) hsub' hz'
        calc
          (l1.eraseDups.map h).sum =
              ((l1.filter (fun b => !b == x)).eraseDups.map h).sum + h x := hB
          _ = ((xs.filter (fun b => !b == x)).eraseDups.map h).sum + h x := by rw [ih']
          _ = ((x :: xs).eraseDups.map h).sum := by
              simp [List.eraseDups_cons, Nat.add_comm]
      · have hx0 : h x = 0 := hz x (by simp) hx1
        have hsub' : ∀ k ∈ l1, k ∈ xs.filter (fun b => !b == x) := by
          intro k hk
          have hkx : k ∈ x :: xs := hsub k hk
          simp only [List.mem_cons] at hkx
          rcases hkx with rfl | hkxs
          · contradiction
          · have hkne : k ≠ x := by
              intro hkeq
              apply hx1
              simpa [hkeq] using hk
            exact List.mem_filter.mpr ⟨hkxs, by simp [hkne]⟩
        have hz' : ∀ k ∈ xs.filter (fun b => !b == x), k ∉ l1 → h k = 0 := by
          intro k hk hknot
          rcases List.mem_filter.mp hk with ⟨hk1, hk2⟩
          exact hz k (by simp [hk1]) hknot
        have ih' := sum_eraseDups_subset (l1 := l1)
          (l2 := xs.filter (fun b => !b == x)) hsub' hz'
        calc
          (l1.eraseDups.map h).sum =
              ((xs.filter (fun b => !b == x)).eraseDups.map h).sum := ih'
          _ = ((x :: xs).eraseDups.map h).sum := by
              simp [List.eraseDups_cons, hx0]
termination_by l2.length
decreasing_by
  all_goals
    have hle := List.length_filter_le (fun b : Nat => !b == x) xs
    simp
    omega

private theorem weighted_eq_sum_map {α : Type} (b : Bag α) (p : α) (g : Nat → Nat)
    (hmass : ∀ k, b.mass p k = (b.expand p).count k)
    (hkeys : ∀ k ∈ b.expand p, k ∈ b.keys p) :
    weighted b p g = ((b.expand p).map g).sum := by
  unfold weighted
  calc
    ((b.keys p).eraseDups.map (fun k => b.mass p k * g k)).sum =
        ((b.keys p).eraseDups.map (fun k => (b.expand p).count k * g k)).sum := by
      apply congrArg (fun l : List Nat => l.sum)
      apply List.map_congr_left
      intro k hk
      rw [hmass k]
    _ = ((b.expand p).eraseDups.map (fun k => (b.expand p).count k * g k)).sum := by
      symm
      apply sum_eraseDups_subset
      · intro k hk
        exact hkeys k hk
      · intro k hk hknot
        have hz : (b.expand p).count k = 0 := List.count_eq_zero_of_not_mem hknot
        simp [hz]
    _ = ((b.expand p).map g).sum := (sum_eq_sum_count g (b.expand p)).symm

private theorem sum_filter_eq_sum_map_ite (P : Nat → Bool) (l : List Nat) :
    (l.filter P).sum = (l.map (fun k => if P k then k else 0)).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : P x = true
      · simp [hx, ih]
      · have hx' : P x = false := by cases hpx : P x <;> simp_all
        simp [hx', ih]

example : weighted bag (5, 3, [(1, 7), (1, 9), (12, 4)])
    (fun x => if bag.mass (4, 7, [(0, 3), (2, 9), (6, 3)]) x == 0 then x else 0) =
  ((bag.expand (5, 3, [(1, 7), (1, 9), (12, 4)])).filter
    (fun x => !(bag.expand (4, 7, [(0, 3), (2, 9), (6, 3)])).contains x)).sum := by
  decide

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  let P : Nat → Bool := fun x => !(bag.expand c).contains x
  let g : Nat → Nat := fun k => if P k then k else 0
  have hmass : ∀ (p : Nat × Nat × List (Nat × Nat)) k,
      bag.mass p k = (bag.expand p).count k := by
    intro p k
    rcases p with ⟨n, d, es⟩
    exact (count_expand_eq_mass n d es k).symm
  have hkeys : ∀ (p : Nat × Nat × List (Nat × Nat)) k,
      k ∈ bag.expand p → k ∈ bag.keys p := by
    intro p k hk
    rcases p with ⟨n, d, es⟩
    exact mem_expand_mem_keys n d es hk
  have hcond : ∀ k, (bag.mass c k == 0) = P k := by
    intro k
    by_cases hmem : k ∈ bag.expand c
    · have hcpos : 0 < bag.mass c k := by
        rw [hmass]
        exact List.count_pos_iff.mpr hmem
      have hz : (bag.mass c k == 0) = false := by simp [Nat.ne_of_gt hcpos]
      have hcont : (bag.expand c).contains k = true := by simpa using hmem
      simpa [hz, hcont, P]
    · have hczero : bag.mass c k = 0 := by
        rw [hmass]
        exact List.count_eq_zero_of_not_mem hmem
      have hz : (bag.mass c k == 0) = true := by simp [hczero]
      have hcont : (bag.expand c).contains k = false := by simpa using hmem
      simpa [hz, hcont, P]
  have hg : (fun x => if bag.mass c x == 0 then x else 0) = g := by
    funext k
    dsimp [g]
    rw [hcond k]
  calc
    weighted bag a (fun x => if bag.mass c x == 0 then x else 0)
        = weighted bag a g := by rw [hg]
    _ = ((bag.expand a).map g).sum := weighted_eq_sum_map bag a g (hmass a) (hkeys a)
    _ = ((bag.expand a).filter P).sum := by
      simpa [g] using (sum_filter_eq_sum_map_ite P (bag.expand a)).symm
    _ = ((bag.expand a).filter (fun x => !(bag.expand c).contains x)).sum := by rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_AntiRevenue
