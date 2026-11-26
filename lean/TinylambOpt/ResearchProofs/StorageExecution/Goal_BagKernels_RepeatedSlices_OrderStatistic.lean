import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `RepeatedSlices.OrderStatistic` — Find an order statistic with ties by cumulative
  multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_OrderStatistic

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RepeatedSlices

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
  · have hgez : zs.length ≤ i := by omega
    have hnone : zs[i]? = none := List.getElem?_eq_none hgez
    have hnone' : xs.mergeSort[i]? = none := by simpa [zs] using hnone
    rw [hnone']
    apply List.find?_eq_none.mpr
    intro x hx
    have hbnd : (xs.filter (fun y => y ≤ x)).length ≤ xs.length := List.length_filter_le _ _
    have hnot : ¬ i < (xs.filter (fun y => y ≤ x)).length := by omega
    simp [hnot]

private theorem countP_eq_sum_toNat (l : List Nat) (p : Nat → Bool) :
    l.countP p = (l.map (fun x => (p x).toNat)).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
      cases h : p a <;> simp [h, ih, Nat.add_comm]

private theorem sum_filter_partition (l : List Nat) (p : Nat → Bool) (f : Nat → Nat) :
    (l.map f).sum = ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum := by
  induction l with
  | nil => simp
  | cons a t ih =>
      by_cases h : p a
      · simp [h, ih]
        omega
      · simp [h, ih]
        omega

private theorem sum_filter_eq_count (l : List Nat) (x : Nat) (f : Nat → Nat) :
    ((l.filter (fun y => y == x)).map f).sum = l.count x * f x := by
  induction l with
  | nil => simp
  | cons a t ih =>
      by_cases h : a == x
      · have hax : a = x := by simpa using h
        simp [hax, ih]
        rw [Nat.succ_mul]
        omega
      · have hbeq : (a == x) = false := by simpa using h
        simp [h, ih, List.count_cons]

private theorem sum_map_eraseDups_count (l : List Nat) (f : Nat → Nat) :
    (l.map f).sum = (l.eraseDups.map (fun x => l.count x * f x)).sum := by
  induction hl : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons x xs =>
          let u : List Nat := xs.filter (fun y : Nat => !y == x)
          have hulen : u.length < n := by
            rw [← hl]
            have hh := List.length_filter_le (fun y : Nat => !y == x) xs
            dsimp [u]
            change (xs.filter (fun y : Nat => !y == x)).length < xs.length + 1
            omega
          have ihu := ih u.length hulen u rfl
          have hpart := sum_filter_partition xs (fun y : Nat => y == x) f
          have hcount_part := sum_filter_eq_count xs x f
          have hxs : (xs.map f).sum = xs.count x * f x + (u.map f).sum := by
            rw [hpart, hcount_part]
          have hxcount : (x :: xs).count x = xs.count x + 1 := by
            rw [List.count_cons]
            simp
          have hycount : ∀ y ∈ u.eraseDups, (x :: xs).count y = u.count y := by
            intro y hy
            have hyu : y ∈ u := List.mem_eraseDups.mp hy
            have hbad : (!y == x) = true := (List.mem_filter.mp hyu).2
            have hxne : (x == y) = false := by
              by_cases h : x == y
              · have : y == x := by simpa [Bool.beq_comm] using h
                simp [this] at hbad
              · simpa using h
            have hu_count : u.count y = xs.count y := by
              dsimp [u]
              exact List.count_filter hbad
            rw [List.count_cons]
            simp [hxne, hu_count]
          have hsumy : (u.eraseDups.map (fun y => (x :: xs).count y * f y)).sum =
              (u.eraseDups.map (fun y => u.count y * f y)).sum := by
            congr 1
            apply List.map_congr_left
            intro y hy
            rw [hycount y hy]
          simp only [List.map_cons, List.sum_cons, List.eraseDups_cons]
          rw [hxs, hxcount, hsumy, ← ihu]
          rw [Nat.succ_mul]
          omega

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
            have hbmem : b ∈ as.filter (fun c => ! c == a) := by
              simpa using hb
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

private theorem eraseDups_perm_of_same_mem {l₁ l₂ : List Nat}
    (h : ∀ k, k ∈ l₁ ↔ k ∈ l₂) : l₁.eraseDups.Perm l₂.eraseDups := by
  rw [List.perm_iff_count]
  intro k
  rw [count_eraseDups, count_eraseDups]
  simp [h k]

private theorem perm_sum {l₁ l₂ : List Nat} (h : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction h with
  | nil => rfl
  | cons x h ih => simpa using ih
  | swap x y l => simp [Nat.add_left_comm]
  | trans h₁ h₂ ih₁ ih₂ => exact ih₁.trans ih₂

private abbrev B := bag

private theorem range_succ_eq_cons_map (n : Nat) :
    List.range (n+1) = 0 :: (List.range n).map (fun i => i+1) := by
  induction n with
  | zero => simp
  | succ n ih =>
      calc
        List.range (n+2) = List.range (n+1) ++ [n+1] := by rw [List.range_succ]
        _ = (0 :: (List.range n).map (fun i => i+1)) ++ [n+1] := by rw [ih]
        _ = 0 :: ((List.range n).map (fun i => i+1) ++ [n+1]) := rfl
        _ = 0 :: (List.range (n+1)).map (fun i => i+1) := by simp [List.range_succ]

private theorem flatMap_map_succ (l : List Nat) (xs : List Nat) (a : Nat) :
    (l.map (fun i => i+1)).flatMap (fun i => (a::xs).drop i) =
      l.flatMap (fun i => xs.drop i) := by
  induction l with
  | nil => simp
  | cons i is ih =>
      simp only [List.map_cons, List.flatMap_cons, List.drop_succ_cons, ih]

private theorem expand_cons (a : Nat) (xs : List Nat) :
    B.expand (a::xs) = (a::xs) ++ B.expand xs := by
  unfold B bag
  simp only
  rw [List.length_cons, range_succ_eq_cons_map]
  simp only [List.flatMap_cons, List.drop_zero]
  rw [flatMap_map_succ]

private theorem zipIdx_succ (xs : List Nat) (n : Nat) :
    xs.zipIdx (n+1) = (xs.zipIdx n).map (fun e => (e.1, e.2+1)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      simp only [List.map_cons]
      congr 1
      exact ih (n+1)

private theorem zipIdx_filter_count (xs : List Nat) (n : Nat) (k : Nat) :
    ((xs.zipIdx n).filter (fun e => e.1 == k)).length = xs.count k := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.filter_cons, List.count_cons]
      by_cases h : a == k <;> simp [h, ih]

private theorem sum_map_add_const {α : Type} (l : List α) (f : α → Nat) (c : Nat) :
    (l.map (fun x => f x + c)).sum = (l.map f).sum + l.length * c := by
  induction l with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih, Nat.succ_mul]
      omega

private theorem filter_map_snd_add (l : List (Nat × Nat)) (k c d : Nat) :
    ((l.map (fun e => (e.1, e.2 + c))).filter (fun e => e.1 == k)).map (fun e => e.2 + d) =
      ((l.filter (fun e => e.1 == k)).map (fun e => e.2 + (c+d))) := by
  induction l with
  | nil => simp
  | cons e es ih =>
      simp only [List.map_cons, List.filter_cons]
      by_cases h : e.1 == k
      · simp [h, ih, Nat.add_assoc]
      · simp [h, ih]

private theorem mass_cons (a : Nat) (xs : List Nat) (k : Nat) :
    B.mass (a::xs) k = (if a == k then 1 else 0) + xs.count k + B.mass xs k := by
  unfold B bag
  simp only
  rw [List.zipIdx_cons, zipIdx_succ xs 0]
  by_cases h : a == k
  · simp [h]
    rw [filter_map_snd_add]
    have hs := sum_map_add_const (xs.zipIdx.filter (fun e => e.1 == k)) (fun e => e.2 + 1) 1
    have hc := zipIdx_filter_count xs 0 k
    simp [Nat.add_assoc] at hs ⊢
    rw [hs, hc]
    omega
  · simp [h]
    rw [filter_map_snd_add]
    have hs := sum_map_add_const (xs.zipIdx.filter (fun e => e.1 == k)) (fun e => e.2 + 1) 1
    have hc := zipIdx_filter_count xs 0 k
    simp [Nat.add_assoc] at hs ⊢
    rw [hs, hc]
    omega

private theorem count_expand_eq_mass (xs : List Nat) (k : Nat) :
    (B.expand xs).count k = B.mass xs k := by
  induction xs with
  | nil => simp [B, bag]
  | cons a as ih =>
      rw [expand_cons, List.count_append, mass_cons, ih]
      simp [List.count_cons, Nat.add_comm]

private theorem expand_subset_xs (xs : List Nat) {x : Nat} (hx : x ∈ B.expand xs) : x ∈ xs := by
  induction xs with
  | nil => simp [B, bag] at hx
  | cons a as ih =>
      rw [expand_cons] at hx
      simp only [List.mem_append, List.mem_cons] at hx ⊢
      cases hx with
      | inl h => exact h
      | inr h => exact Or.inr (ih h)

private theorem xs_subset_expand (xs : List Nat) {x : Nat} (hx : x ∈ xs) : x ∈ B.expand xs := by
  induction xs with
  | nil => simp at hx
  | cons a as ih =>
      rw [expand_cons]
      simp only [List.mem_append, List.mem_cons] at hx ⊢
      cases hx with
      | inl h => exact Or.inl (Or.inl h)
      | inr h => exact Or.inr (ih h)

private theorem count_pos_of_mem {l : List Nat} {x : Nat} (h : x ∈ l) : 0 < l.count x := by
  induction l with
  | nil => simp at h
  | cons a as ih =>
      simp only [List.count_cons, List.mem_cons] at h ⊢
      cases h with
      | inl hx =>
          subst hx
          simp
      | inr hx =>
          have hi := ih hx
          omega

private theorem live_eq_eraseDups (xs : List Nat) : live B xs = xs.eraseDups := by
  unfold live
  simp only [bag]
  apply List.filter_eq_self.mpr
  intro k hk
  have hkx : k ∈ xs := List.mem_eraseDups.mp hk
  have hcnt : 0 < (B.expand xs).count k := count_pos_of_mem (xs_subset_expand xs hkx)
  rw [count_expand_eq_mass] at hcnt
  simpa [B, bag] using hcnt

private theorem expand_subset_live (xs : List Nat) {x : Nat} (hx : x ∈ B.expand xs) :
    x ∈ live B xs := by
  rw [live_eq_eraseDups]
  exact List.mem_eraseDups.mpr (expand_subset_xs xs hx)

private theorem below_eq (xs : List Nat) (v : Nat) :
    below B xs v = (B.expand xs).countP (fun y => decide (y ≤ v)) := by
  unfold below weighted
  change (xs.eraseDups.map (fun k => B.mass xs k * (decide (k ≤ v)).toNat)).sum =
    (B.expand xs).countP (fun y => decide (y ≤ v))
  rw [countP_eq_sum_toNat]
  rw [sum_map_eraseDups_count (B.expand xs) (fun k => (decide (k ≤ v)).toNat)]
  have hperm : xs.eraseDups.Perm (B.expand xs).eraseDups := by
    apply eraseDups_perm_of_same_mem
    intro k
    constructor
    · intro hk
      exact xs_subset_expand xs hk
    · intro hk
      exact expand_subset_xs xs hk
  calc
    (xs.eraseDups.map (fun k => B.mass xs k * (decide (k ≤ v)).toNat)).sum =
        (xs.eraseDups.map (fun k => (B.expand xs).count k * (decide (k ≤ v)).toNat)).sum := by
          congr 1
          apply List.map_congr_left
          intro k hk
          rw [count_expand_eq_mass]
    _ = ((B.expand xs).eraseDups.map
          (fun k => (B.expand xs).count k * (decide (k ≤ v)).toNat)).sum := by
          exact perm_sum (hperm.map _)

theorem sound_OrderStatistic : OrderStatistic.Goal := by
  intro xs i
  unfold quantile
  simpa [below_eq, List.countP_eq_length_filter] using
    (quantile_aux (B.expand xs) ((live B xs).mergeSort) i
      (sort_pairwise (live B xs)) (by
        intro x hx
        exact List.mem_mergeSort.mpr (expand_subset_live xs hx)))

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_OrderStatistic
