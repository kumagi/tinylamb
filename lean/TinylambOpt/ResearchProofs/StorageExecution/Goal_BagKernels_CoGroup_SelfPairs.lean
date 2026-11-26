import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_SelfPairs

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.CoGroup

private theorem comb_even (k : Nat) : comb (2*k) = k*(2*k-1) := by
  unfold comb
  rw [show 2*k*(2*k-1) = 2*(k*(2*k-1)) by ac_rfl]
  rw [Nat.mul_div_right _ (by decide : 0 < 2)]

private theorem comb_odd (k : Nat) : comb (2*k+1) = k*(2*k+1) := by
  unfold comb
  have h : 2*k+1-1 = 2*k := by omega
  rw [h]
  rw [show (2*k+1)*(2*k) = 2*(k*(2*k+1)) by ac_rfl]
  rw [Nat.mul_div_right _ (by decide : 0 < 2)]

private theorem key_even (k : Nat) : k*(2*k+1) = k*(2*k-1) + 2*k := by
  cases k with
  | zero => simp
  | succ k =>
      have h : 2*(k+1)+1 = (2*(k+1)-1)+2 := by omega
      rw [h, Nat.mul_add]
      rw [show k.succ * 2 = 2 * k.succ by ac_rfl]

private theorem key_odd (k : Nat) : (k+1)*(2*k+1) = k*(2*k+1) + (2*k+1) := by
  rw [Nat.add_mul]
  simp

private theorem comb_succ (n : Nat) : comb (n+1) = comb n + n := by
  rcases Nat.mod_two_eq_zero_or_one n with h | h
  · have hn : n = 2 * (n / 2) := by
      have hdiv := Nat.mod_add_div n 2
      omega
    rw [hn]
    rw [comb_odd, comb_even]
    exact key_even (n/2)
  · have hn : n = 2 * (n / 2) + 1 := by
      have hdiv := Nat.mod_add_div n 2
      omega
    rw [hn]
    have hshift : 2*(n/2)+1+1 = 2*((n/2)+1) := by omega
    rw [hshift, comb_even, comb_odd]
    exact key_odd (n/2)

private def pairCount (l : List Nat) : Nat :=
  (l.zipIdx.map (fun (x,i) => (l.drop (i+1)).count x)).sum

private theorem pairCount_cons (a : Nat) (l : List Nat) :
    pairCount (a::l) = l.count a + pairCount l := by
  unfold pairCount
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons]
  have hhead : (a :: l).drop (0 + 1) = l := by simp
  rw [hhead]
  have hmap :
      (List.map (fun x => List.count x.fst (List.drop (x.snd + 1) (a :: l))) (l.zipIdx (0+1))) =
      (List.map (fun x => List.count x.fst (List.drop (x.snd + 1) l)) (l.zipIdx 0)) := by
    rw [show l.zipIdx (0+1) = (l.zipIdx 0).map (fun (x,i) => (x,i+1)) by
      rw [List.zipIdx_succ]]
    simp [List.map_map, List.drop_succ_cons]
  rw [hmap]

private theorem count_eraseDups (k : Nat) (l : List Nat) :
    (l.eraseDups).count k = if k ∈ l then 1 else 0 := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons, List.count_cons]
      have hrec := count_eraseDups k (as.filter (fun b => ! b == a))
      by_cases h : a = k
      · subst k
        rw [hrec]
        simp
      · have hka : k ≠ a := by
          intro hk
          exact h hk.symm
        rw [hrec]
        have hmem : k ∈ as.filter (fun b => ! b == a) ↔ k ∈ as := by
          simp [hka]
        simp only [hmem]
        simp [hka, h]
termination_by l.length
decreasing_by
  have hle := List.length_filter_le (fun b : Nat => ! b == a) as
  simp
  omega

private theorem perm_eraseDups {l₁ l₂ : List Nat} (p : l₁.Perm l₂) : l₁.eraseDups.Perm l₂.eraseDups := by
  rw [List.perm_iff_count]
  intro k
  rw [count_eraseDups, count_eraseDups]
  simp [p.mem_iff]

private theorem sum_eq_foldr (l : List Nat) (f : Nat → Nat) :
    (l.map f).sum = l.foldr (fun a acc => f a + acc) 0 := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem perm_sum {l₁ l₂ : List Nat} (p : l₁.Perm l₂) (f : Nat → Nat) :
    (l₁.map f).sum = (l₂.map f).sum := by
  rw [sum_eq_foldr, sum_eq_foldr]
  apply p.foldr_eq'
  intro x hx y hy z
  omega

private theorem sum_map_congr {l : List Nat} {f g : Nat → Nat}
    (h : ∀ k ∈ l, f k = g k) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons k ks ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h k (by simp)]
      rw [ih (fun x hx => h x (by simp [hx]))]

private theorem eraseDups_insert_perm (a : Nat) (l : List Nat) (ha : a ∈ l) :
    l.eraseDups.Perm (a :: (l.filter (fun b => ! b == a)).eraseDups) := by
  rw [List.perm_iff_count]
  intro k
  rw [count_eraseDups, List.count_cons, count_eraseDups]
  by_cases h : a = k
  · subst k
    simp [ha]
  · have hka : k ≠ a := by
      intro hk
      exact h hk.symm
    have hmem : k ∈ l.filter (fun b => ! b == a) ↔ k ∈ l := by
      simp [hka]
    simp only [hmem]
    simp [hka, h]

private def S (l : List Nat) : Nat := (l.eraseDups.map (fun k => comb (l.count k))).sum

private theorem S_cons (a : Nat) (l : List Nat) : S (a::l) = l.count a + S l := by
  unfold S
  rw [List.eraseDups_cons]
  simp only [List.map_cons, List.sum_cons]
  let flt := l.filter (fun b => ! b == a)
  have hfg : ∀ k ∈ flt.eraseDups, comb ((a::l).count k) = comb (l.count k) := by
    intro k hk
    have hkmem : k ∈ flt := List.mem_eraseDups.mp hk
    have hkne : k ≠ a := by
      intro heq
      subst k
      simp [flt] at hkmem
    rw [List.count_cons]
    have hbeq : (a == k) = false := by
      simpa [show a ≠ k from hkne.symm]
    simp [hbeq]
  have hsumfg : (flt.eraseDups.map (fun k => comb ((a::l).count k))).sum =
      (flt.eraseDups.map (fun k => comb (l.count k))).sum := by
    apply sum_map_congr
    exact hfg
  rw [hsumfg]
  dsimp [flt]
  by_cases ha : a ∈ l
  · have hperm := perm_sum (eraseDups_insert_perm a l ha) (fun k => comb (l.count k))
    simp only [List.map_cons, List.sum_cons] at hperm
    have hcnt : (a :: l).count a = l.count a + 1 := by simp [List.count_cons]
    rw [hcnt, comb_succ]
    rw [hperm]
    omega
  · have hfilter : l.filter (fun b => ! b == a) = l := by
      rw [List.filter_eq_self]
      intro k hk
      have hkne : k ≠ a := by
        intro heq
        apply ha
        simpa [heq] using hk
      simp [hkne]
    rw [hfilter]
    have h1 : comb 1 = 0 := by decide
    have hca : l.count a = 0 := by exact (List.count_eq_zero).2 ha
    simp [List.count_cons, hca, h1]

private theorem pairCount_eq (l : List Nat) : pairCount l = S l := by
  induction l with
  | nil => simp [pairCount, S]
  | cons a l ih =>
      rw [pairCount_cons, ih, S_cons]


namespace Extra

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.CoGroup

theorem count_filter_eraseDups (P : Nat → Bool) (k : Nat) (l : List Nat) :
    ((l.eraseDups).filter P).count k = if (P k == true) && (k ∈ l) then 1 else 0 := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons, List.filter_cons]
      have hrec := count_filter_eraseDups P k (as.filter (fun b => ! b == a))
      by_cases hPa : P a = true
      · simp only [hPa, ↓reduceIte, List.count_cons]
        rw [hrec]
        by_cases h : a = k
        · subst k
          simp [hPa]
        · have hka : k ≠ a := by intro hk; exact h hk.symm
          have hmem : k ∈ as.filter (fun b => ! b == a) ↔ k ∈ as := by simp [hka]
          simp only [hmem]
          simp [hka, h, hPa]
      · simp only [hPa, Bool.false_eq_true, ↓reduceIte, List.count_cons]
        rw [hrec]
        by_cases h : a = k
        · subst k
          simp [hPa]
        · have hka : k ≠ a := by intro hk; exact h hk.symm
          have hmem : k ∈ as.filter (fun b => ! b == a) ↔ k ∈ as := by simp [hka]
          simp only [hmem]
          simp [hka, h, hPa]
termination_by l.length
decreasing_by
  have hle := List.length_filter_le (fun b : Nat => ! b == a) as
  simp
  omega

theorem mem_expand_cogroup (k : Nat) (xs ys : List Nat) :
    k ∈ bag.expand (xs,ys) ↔ k ∈ xs ∧ k ∈ ys := by
  change k ∈ xs.flatMap (fun x => ys.filter (fun y => y == x)) ↔ k ∈ xs ∧ k ∈ ys
  simp only [List.mem_flatMap, List.mem_filter]
  constructor
  · rintro ⟨x, hx, hky, hkx⟩
    have hkx' : k = x := LawfulBEq.eq_of_beq hkx
    subst x
    exact ⟨hx, hky⟩
  · rintro ⟨hkx, hky⟩
    exact ⟨k, hkx, hky, by simp⟩

theorem count_filter_eq (k x : Nat) (ys : List Nat) :
    (ys.filter (fun y => y == x)).count k = if k == x then ys.count k else 0 := by
  by_cases h : k == x
  · have hk : k = x := LawfulBEq.eq_of_beq h
    subst x
    simp [h]
  · have hne : k ≠ x := by
      intro heq
      subst x
      simp at h
    rw [List.count_eq_zero.mpr]
    · simp [h]
    · intro hm
      simp only [List.mem_filter] at hm
      exact hne (LawfulBEq.eq_of_beq hm.2)

theorem count_expand_cogroup (k : Nat) (xs ys : List Nat) :
    (bag.expand (xs,ys)).count k = xs.count k * ys.count k := by
  change (xs.flatMap (fun x => ys.filter (fun y => y == x))).count k = xs.count k * ys.count k
  rw [List.count_flatMap]
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.count_cons, Function.comp_apply]
      rw [count_filter_eq]
      by_cases h : k == x
      · have hk : k = x := LawfulBEq.eq_of_beq h
        subst x
        rw [ih]
        simp [Nat.add_mul]
        omega
      · have hxne : x ≠ k := by
          intro heq
          subst x
          simp at h
        have hkne : k ≠ x := by intro hk; exact hxne hk.symm
        rw [ih]
        simp [hxne, hkne]

end Extra


theorem sound_SelfPairs : CoGroup.SelfPairs.Goal := by
  intro p
  rcases p with ⟨xs, ys⟩
  let e := bag.expand (xs, ys)
  have hcount : ∀ k, e.count k = bag.mass (xs, ys) k := by
    intro k
    change (xs.flatMap (fun x => ys.filter (fun y => y == x))).count k = xs.count k * ys.count k
    exact Extra.count_expand_cogroup k xs ys
  have hlive_perm : (live bag (xs, ys)).Perm e.eraseDups := by
    rw [List.perm_iff_count]
    intro k
    unfold live
    dsimp [bag]
    rw [Extra.count_filter_eraseDups]
    rw [count_eraseDups]
    have hmem : k ∈ e ↔ k ∈ xs ∧ k ∈ ys := by
      change k ∈ xs.flatMap (fun x => ys.filter (fun y => y == x)) ↔ k ∈ xs ∧ k ∈ ys
      exact Extra.mem_expand_cogroup k xs ys
    by_cases hxe : k ∈ xs <;> by_cases hye : k ∈ ys
    · have hxpos : 0 < xs.count k := List.count_pos_iff.mpr hxe
      have hypos : 0 < ys.count k := List.count_pos_iff.mpr hye
      have hmpos : 0 < bag.mass (xs, ys) k := by
        change 0 < xs.count k * ys.count k
        exact Nat.mul_pos hxpos hypos
      have hke : k ∈ e := hmem.mpr ⟨hxe, hye⟩
      simp [hxe, hye, hmpos, hke]
    · have hxpos : 0 < xs.count k := List.count_pos_iff.mpr hxe
      have hyzero : ys.count k = 0 := by
        exact (List.count_eq_zero).2 hye
      have hmzero : bag.mass (xs, ys) k = 0 := by
        change xs.count k * ys.count k = 0
        simp [hyzero]
      have hkne : k ∉ e := by
        intro hk
        exact hye (hmem.mp hk).2
      simp [hxe, hye, hmzero, hkne]
    · have hxzero : xs.count k = 0 := by
        exact (List.count_eq_zero).2 hxe
      have hmzero : bag.mass (xs, ys) k = 0 := by
        change xs.count k * ys.count k = 0
        simp [hxzero]
      have hkne : k ∉ e := by
        intro hk
        exact hxe (hmem.mp hk).1
      simp [hxe, hye, hmzero, hkne]
    · have hmzero : bag.mass (xs, ys) k = 0 := by
        change xs.count k * ys.count k = 0
        have hxzero : xs.count k = 0 := (List.count_eq_zero).2 hxe
        simp [hxzero]
      have hkne : k ∉ e := by
        intro hk
        exact hxe (hmem.mp hk).1
      simp [hxe, hye, hmzero, hkne]
  change ((live bag (xs, ys)).map (fun k => comb (bag.mass (xs, ys) k))).sum = pairCount e
  rw [pairCount_eq]
  unfold S
  have hf : (e.eraseDups.map (fun k => comb (e.count k))).sum =
      (e.eraseDups.map (fun k => comb (bag.mass (xs, ys) k))).sum := by
    apply sum_map_congr
    intro k hk
    rw [hcount k]
  rw [hf]
  exact perm_sum hlive_perm (fun k => comb (bag.mass (xs, ys) k))
end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_CoGroup_SelfPairs
