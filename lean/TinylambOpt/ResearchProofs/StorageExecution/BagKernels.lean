import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

namespace TinylambOpt.ResearchProofs.StorageExecution.BagKernels

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ## Common lemmas -/

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  match l with
  | [] => simp [List.eraseDups]
  | a :: as =>
    rw [List.eraseDups_cons, List.nodup_cons]
    refine ⟨?_, nodup_eraseDups (List.filter (fun b => !(b == a)) as)⟩
    intro hm
    rw [List.mem_eraseDups, List.mem_filter] at hm
    have h := hm.2
    simp at h
termination_by l.length
decreasing_by
  simp_wf
  exact Nat.lt_succ_of_le (List.length_filter_le _ _)

private theorem count_eq_one_of_mem_nodup {α : Type} [DecidableEq α] {l : List α} {a : α}
    (hn : l.Nodup) (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
    rw [List.nodup_cons] at hn
    rw [List.mem_cons] at ha
    rw [List.count_cons]
    by_cases hab : b = a
    · subst hab
      simp [List.count_eq_zero_of_not_mem hn.1]
    · have ha' : a ∈ bs := by cases ha with | inl h => exact absurd h.symm hab | inr h => exact h
      have hba : (b == a) = false := by simpa [beq_eq_false_iff_ne] using hab
      simp [hba, ih hn.2 ha']

private theorem perm_sum_eq {l1 l2 : List Nat} (h : l1.Perm l2) : l1.sum = l2.sum := by
  induction h with
  | nil => rfl
  | cons x _ ih => simp [ih]
  | swap x y l => simp; omega
  | trans _ _ ih1 ih2 => exact ih1.trans ih2

private theorem list_sum_eq_zero {l : List Nat} (h : ∀ x ∈ l, x = 0) : l.sum = 0 := by
  induction l with
  | nil => simp
  | cons a as ih => simp [h a List.mem_cons_self, ih (fun x hx => h x (List.mem_cons_of_mem a hx))]

private theorem sum_filter_split (l : List Nat) (f : Nat → Nat) (p : Nat → Bool) :
    (l.map f).sum = ((l.filter p).map f).sum + ((l.filter (fun x => !p x)).map f).sum := by
  induction l with
  | nil => simp
  | cons a as ih =>
    simp only [List.map_cons, List.sum_cons, List.filter_cons]
    by_cases hp : p a = true
    · simp [hp, ih]; omega
    · have hp' : p a = false := by cases h : p a <;> simp_all
      simp [hp', ih]; omega

private theorem count_eq_count_filter_ne (a : Nat) (as : List Nat) :
    ∀ k, k ≠ a → (List.filter (fun b => !(b == a)) as).count k = as.count k := by
  intro k hne
  induction as with
  | nil => simp
  | cons b bs ih =>
    simp only [List.filter, List.count_cons]
    by_cases hb : !(b == a) = true
    · simp only [hb, ite_true, List.count_cons, ih]
    · have hb' : (!(b == a)) = false := by cases h : (!(b == a)) <;> simp_all
      simp only [hb', ite_false]
      have hba : b = a := by simpa using hb'
      rw [hba]
      have hak : (a == k) = false := by simpa [beq_eq_false_iff_ne] using (Ne.symm hne)
      simp [hak, ih]

private theorem sum_map_const_nat (l : List Nat) (c : Nat) :
    (l.map (fun _ => c)).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_mul, Nat.one_mul]; omega

private theorem sum_filter_eq_count_mul (as : List Nat) (a : Nat) (f : Nat → Nat) :
    ((as.filter (fun b => b == a)).map f).sum = as.count a * f a := by
  have h1 : ((as.filter (fun b => b == a)).map f).sum =
      ((as.filter (fun b => b == a)).map (fun _ => f a)).sum := by
    congr 1
    apply List.map_congr_left
    intro b hb
    rw [List.mem_filter] at hb
    have : b = a := eq_of_beq hb.2
    simp [this]
  rw [h1, sum_map_const_nat]
  have h2 : (List.filter (fun b => b == a) as).length = as.count a := by
    rw [List.count_eq_length_filter]
  rw [h2, Nat.mul_comm]

private theorem sum_eraseDups_count_mul (xs : List Nat) (f : Nat → Nat) :
    ((xs.eraseDups.map (fun k => xs.count k * f k)).sum) = (xs.map f).sum := by
  suffices H : ∀ n (xs : List Nat), xs.length ≤ n →
      ((xs.eraseDups.map (fun k => xs.count k * f k)).sum) = (xs.map f).sum from
    H xs.length xs (Nat.le_refl _)
  intro n
  induction n with
  | zero =>
    intro xs hle
    match xs with
    | [] => simp
    | _ :: _ => simp at hle
  | succ n ih =>
    intro xs hle
    match xs with
    | [] => simp
    | a :: as =>
      have hle' : as.length ≤ n := by simp at hle; omega
      rw [List.eraseDups_cons]
      simp only [List.map_cons, List.sum_cons, List.count_cons, beq_self_eq_true, ite_true]
      have hhead : (as.count a + 1) * f a = f a + as.count a * f a := by
        rw [Nat.add_mul, Nat.one_mul, Nat.add_comm]
      rw [hhead]
      have hcount_k : ∀ k, k ∈ (List.filter (fun b => !(b == a)) as).eraseDups →
          (if (a == k) = true then 1 else 0) + as.count k = as.count k := by
        intro k hk
        rw [List.mem_eraseDups, List.mem_filter] at hk
        have hne : (a == k) = false := by
          have : (!(k == a)) = true := hk.2
          simp at this
          rw [beq_eq_false_iff_ne]
          intro heq; subst heq; simp at this
        simp [hne]
      have hmap_eq : ((List.filter (fun b => !(b == a)) as).eraseDups.map
          (fun k => (as.count k + if (a == k) = true then 1 else 0) * f k)).sum =
          ((List.filter (fun b => !(b == a)) as).eraseDups.map
          (fun k => as.count k * f k)).sum := by
        congr 1; apply List.map_congr_left; intro k hk
        rw [Nat.add_comm (List.count k as) (if (a == k) = true then 1 else 0), hcount_k k hk]
      rw [hmap_eq]
      have hcount_filter : ∀ k, k ∈ (List.filter (fun b => !(b == a)) as).eraseDups →
          as.count k = (List.filter (fun b => !(b == a)) as).count k := by
        intro k hk
        rw [List.mem_eraseDups, List.mem_filter] at hk
        have hne : k ≠ a := by
          have : (!(k == a)) = true := hk.2
          simp at this
          intro heq; subst heq; simp at this
        exact (count_eq_count_filter_ne a as k hne).symm
      have hmap_eq2 : ((List.filter (fun b => !(b == a)) as).eraseDups.map
          (fun k => as.count k * f k)).sum =
          ((List.filter (fun b => !(b == a)) as).eraseDups.map
          (fun k => (List.filter (fun b => !(b == a)) as).count k * f k)).sum := by
        congr 1; apply List.map_congr_left; intro k hk
        rw [hcount_filter k hk]
      rw [hmap_eq2]
      have hlen_f : (List.filter (fun b => !(b == a)) as).length ≤ n :=
        Nat.le_trans (List.length_filter_le _ _) hle'
      have ihf := ih _ hlen_f
      rw [ihf]
      have hsplit := sum_filter_split as f (fun b => b == a)
      have hfeq := sum_filter_eq_count_mul as a f
      have hnot_a : (List.filter (fun b => !(b == a)) as) = (List.filter (fun b => !(b == a)) as) := by
        congr 1; ext b; simp
      rw [hnot_a] at hsplit
      rw [hsplit, hfeq]
      omega

private theorem sum_map_if_one (xs : List Nat) (p : Nat → Bool) :
    ((xs.map (fun x => if p x then 1 else 0)).sum) = (xs.filter p).length := by
  induction xs with
  | nil => simp
  | cons a as ih =>
    simp only [List.map_cons, List.sum_cons, List.filter_cons]
    by_cases hp : p a = true <;> simp [hp, ih] <;> omega

private theorem map_if_sum (xs : List Nat) (p : Nat → Bool) (g : Nat → Nat) :
    ((xs.map (fun x => if p x then g x else 0)).sum) = (((xs.filter p).map g).sum) := by
  induction xs with
  | nil => simp
  | cons a as ih =>
    simp only [List.map_cons, List.sum_cons, List.filter_cons]
    by_cases hp : p a = true <;> simp [hp, ih]

private theorem sum_eraseDups_zero_filter (xs ks : List Nat) (f : Nat → Nat)
    (hcover : ∀ k, 0 < xs.count k → k ∈ ks) :
    ((ks.eraseDups.map (fun k => xs.count k * f k)).sum) =
    ((xs.eraseDups.map (fun k => xs.count k * f k)).sum) := by
  have hsum_ks : ((ks.eraseDups.map (fun k => xs.count k * f k)).sum) =
      (((ks.eraseDups.filter (fun k => decide (0 < xs.count k))).map (fun k => xs.count k * f k)).sum) := by
    have hsplit := sum_filter_split ks.eraseDups (fun k => xs.count k * f k) (fun k => decide (0 < xs.count k))
    have hzero : (((ks.eraseDups.filter (fun k => !decide (0 < xs.count k))).map (fun k => xs.count k * f k)).sum) = 0 := by
      apply list_sum_eq_zero
      intro x hx
      rw [List.mem_map] at hx
      rcases hx with ⟨k, hk, rfl⟩
      rw [List.mem_filter] at hk
      have hk0 : xs.count k = 0 := by
        have : ¬ 0 < xs.count k := by intro hp; have := hk.2; simp [hp] at this
        omega
      simp [hk0]
    rw [hsplit, hzero, Nat.add_zero]
  rw [hsum_ks]
  have hperm : (ks.eraseDups.filter (fun k => decide (0 < xs.count k))).Perm xs.eraseDups := by
    rw [List.perm_iff_count]
    intro k
    by_cases hk : 0 < xs.count k
    · have hk1 : k ∈ ks.eraseDups.filter (fun k => decide (0 < xs.count k)) := by
        rw [List.mem_filter, List.mem_eraseDups]
        simp [hcover k hk, hk]
      have hk2 : k ∈ xs.eraseDups := by rw [List.mem_eraseDups]; exact List.count_pos_iff.mp hk
      rw [count_eq_one_of_mem_nodup ((nodup_eraseDups ks).filter _) hk1,
          count_eq_one_of_mem_nodup (nodup_eraseDups xs) hk2]
    · have hk0 : xs.count k = 0 := by omega
      have hk1 : k ∉ ks.eraseDups.filter (fun k => decide (0 < xs.count k)) := by
        rw [List.mem_filter]; simp [hk0]
      have hk2 : k ∉ xs.eraseDups := by
        rw [List.mem_eraseDups]
        intro hk'; exact hk (List.count_pos_iff.mpr hk')
      rw [List.count_eq_zero_of_not_mem hk1, List.count_eq_zero_of_not_mem hk2]
  exact perm_sum_eq (hperm.map (fun k => xs.count k * f k))

private theorem weighted_eq_map_of_count {α : Type} (b : Bag α) (p : α) (f : Nat → Nat)
    (hcount : ∀ k, b.mass p k = (b.expand p).count k)
    (hcover : ∀ k, 0 < (b.expand p).count k → k ∈ b.keys p) :
    weighted b p f = ((b.expand p).map f).sum := by
  unfold weighted
  have h1 : ((b.keys p).eraseDups.map (fun k => b.mass p k * f k)).sum =
      ((b.keys p).eraseDups.map (fun k => (b.expand p).count k * f k)).sum := by
    congr 1; apply List.map_congr_left; intro k hk; rw [hcount k]
  rw [h1, sum_eraseDups_zero_filter (b.expand p) (b.keys p) f hcover]
  exact sum_eraseDups_count_mul (b.expand p) f

private theorem filter_length_perm {ys zs : List Nat} (h : ys.Perm zs) (p : Nat → Bool) :
    (ys.filter p).length = (zs.filter p).length := by
  induction h with
  | nil => rfl
  | cons x h ih => simp only [List.filter_cons]; by_cases hp : p x <;> simp [hp, ih]
  | swap x y l => simp only [List.filter_cons]; by_cases hx : p x <;> by_cases hy : p y <;> simp [hx, hy]
  | trans _ _ ih1 ih2 => exact ih1.trans ih2

private theorem below_eq_filter_length {α : Type} (b : Bag α) (p : α) (v : Nat)
    (hcount : ∀ k, b.mass p k = (b.expand p).count k)
    (hcover : ∀ k, 0 < (b.expand p).count k → k ∈ b.keys p) :
    below b p v = ((b.expand p).filter (fun x => decide (x ≤ v))).length := by
  unfold below
  rw [weighted_eq_map_of_count b p _ hcount hcover]
  have h1 : ((b.expand p).map (fun x => (decide (x ≤ v)).toNat)).sum =
      ((b.expand p).map (fun x => if decide (x ≤ v) then 1 else 0)).sum := by
    congr 1; apply List.map_congr_left; intro x hx
    by_cases h : decide (x ≤ v) = true <;> simp [h]
  rw [h1, sum_map_if_one]

private theorem below_eq_filter_length_mergeSort {α : Type} (b : Bag α) (p : α) (v : Nat)
    (hcount : ∀ k, b.mass p k = (b.expand p).count k)
    (hcover : ∀ k, 0 < (b.expand p).count k → k ∈ b.keys p) :
    below b p v = (((b.expand p).mergeSort (fun a b => decide (a ≤ b))).filter (fun x => decide (x ≤ v))).length := by
  rw [below_eq_filter_length b p v hcount hcover]
  exact filter_length_perm (List.mergeSort_perm _ _).symm _

private theorem comb_succ (n : Nat) : comb (n + 1) = comb n + n := by
  unfold comb
  cases n with
  | zero => simp
  | succ m =>
    -- (m+2)*(m+1)/2 = (m+1)*m/2 + m
    have h1 : (m + 1 + 1) * (m + 1) = (m + 1) * m + (m + 1) * 2 := by
      rw [Nat.add_mul, Nat.one_mul, Nat.mul_add, Nat.mul_one]; omega
    rw [h1, Nat.add_mul_div_right ((m + 1) * m) (m + 1) (by decide)]
    omega

private theorem nat_le_trans_bool (a b c : Nat) (hab : decide (a ≤ b) = true) (hbc : decide (b ≤ c) = true) : decide (a ≤ c) = true := by
  simp at hab hbc ⊢; omega

private theorem nat_le_total_bool (a b : Nat) : (decide (a ≤ b) || decide (b ≤ a)) = true := by
  by_cases h : a ≤ b <;> simp [h]; omega

private theorem sorted_mergeSort_nat (l : List Nat) :
    List.Pairwise (fun a b => decide (a ≤ b) = true) (l.mergeSort (fun a b => decide (a ≤ b))) :=
  List.pairwise_mergeSort nat_le_trans_bool nat_le_total_bool l

private theorem le_of_pairwise_sorted {ys : List Nat}
    (hpw : List.Pairwise (fun a b => decide (a ≤ b) = true) ys)
    {i j : Nat} (hi : i < ys.length) (hj : j < ys.length) (hij : i ≤ j) :
    ys[i] ≤ ys[j] := by
  by_cases h : i = j
  · subst h; exact Nat.le_refl _
  · have hlt : i < j := Nat.lt_of_le_of_ne hij h
    have := List.pairwise_iff_getElem.mp hpw i j hi hj hlt
    simpa using this

private theorem filter_take_len {ys : List Nat} (i : Nat) (hi : i < ys.length)
    (hpw : List.Pairwise (fun a b => decide (a ≤ b) = true) ys) :
    i < (ys.filter (fun x => decide (x ≤ ys[i]))).length := by
  have hall : ∀ j, (hj : j < ys.length) → j ≤ i → decide (ys[j] ≤ ys[i]) = true := by
    intro j hj hjle
    by_cases hji : j = i
    · subst hji; simp
    · have hlt : j < i := Nat.lt_of_le_of_ne hjle hji
      have h := List.pairwise_iff_getElem.mp hpw j i hj hi hlt
      simpa using h
  have hsub : ys.take (i + 1) <+ (ys.filter (fun x => decide (x ≤ ys[i]))) := by
    have h1 : ys.take (i + 1) <+ ys := List.take_sublist _ _
    have h2 : ys.take (i + 1) = (ys.take (i + 1)).filter (fun x => decide (x ≤ ys[i])) := by
      rw [List.filter_eq_self.mpr]
      intro x hx
      obtain ⟨j, hj, rfl⟩ := List.mem_take_iff_getElem.mp hx
      have hj' : j < ys.length := by omega
      have hjle : j ≤ i := by omega
      exact hall j hj' hjle
    rw [h2]
    exact List.Sublist.filter _ h1
  have htake_len : (ys.take (i + 1)).length = i + 1 := by rw [List.length_take]; omega
  calc i < i + 1 := by omega
    _ = (ys.take (i + 1)).length := htake_len.symm
    _ ≤ (ys.filter (fun x => decide (x ≤ ys[i]))).length := hsub.length_le

private theorem filter_le_length_le {ys : List Nat} (i : Nat) (hi : i < ys.length)
    (hpw : List.Pairwise (fun a b => decide (a ≤ b) = true) ys) {k : Nat}
    (hk : k < ys[i]) :
    (ys.filter (fun x => decide (x ≤ k))).length ≤ i := by
  have hdrop : ∀ j, (hj : j < ys.length) → i ≤ j → ¬ (ys[j] ≤ k) := by
    intro j hj hij
    by_cases hji : j = i
    · subst hji; intro hle; omega
    · have hlt : i < j := Nat.lt_of_le_of_ne hij (Ne.symm hji)
      have h := List.pairwise_iff_getElem.mp hpw i j hi hj hlt
      simp at h
      intro hle; omega
  have hsub : ys.drop i <+ ys.filter (fun x => !(decide (x ≤ k))) := by
    have h1 : ys.drop i <+ ys := List.drop_sublist i ys
    have h2 : ys.drop i = (ys.drop i).filter (fun x => !(decide (x ≤ k))) := by
      rw [List.filter_eq_self.mpr]
      intro x hx
      obtain ⟨j, hj, rfl⟩ := List.mem_drop_iff_getElem.mp hx
      have hj' : i + j < ys.length := by omega
      have hij : i ≤ i + j := by omega
      have hnk := hdrop (i+j) hj' hij
      simp; omega
    rw [h2]
    exact List.Sublist.filter _ h1
  have hpart := sum_filter_split ys (fun _ => 1) (fun x => decide (x ≤ k))
  have hsum_ys : (ys.map (fun _ => 1)).sum = ys.length := by simp [sum_map_const_nat]
  have hsum_le : ((ys.filter (fun x => decide (x ≤ k))).map (fun _ => 1)).sum =
      (ys.filter (fun x => decide (x ≤ k))).length := by
    simp [sum_map_const_nat]
  have hsum_gt : ((ys.filter (fun x => !decide (x ≤ k))).map (fun _ => 1)).sum =
      (ys.filter (fun x => !(decide (x ≤ k)))).length := by
    simp [sum_map_const_nat]
  have htotal : (ys.filter (fun x => decide (x ≤ k))).length + (ys.filter (fun x => !(decide (x ≤ k)))).length = ys.length := by
    have := hpart; rw [hsum_ys, hsum_le, hsum_gt] at this; omega
  have hge : ys.length - i ≤ (ys.filter (fun x => !(decide (x ≤ k)))).length := by
    calc ys.length - i = (ys.drop i).length := (List.length_drop).symm
      _ ≤ (ys.filter (fun x => !(decide (x ≤ k)))).length := hsub.length_le
  omega

private theorem sum_eraseDups_comb (xs : List Nat) :
    ((xs.eraseDups.map (fun k => comb (xs.count k))).sum) =
    (((xs.zipIdx.map (fun (x, i) => ((xs.drop (i + 1)).count x))).sum)) := by
  symm
  suffices H : ∀ n (xs : List Nat), xs.length ≤ n →
      (((xs.zipIdx.map (fun (x, i) => ((xs.drop (i + 1)).count x))).sum)) =
      ((xs.eraseDups.map (fun k => comb (xs.count k))).sum) from
    H xs.length xs (Nat.le_refl _)
  intro n
  induction n with
  | zero =>
    intro xs hle
    match xs with
    | [] => simp [List.zipIdx]
    | _ :: _ => simp at hle
  | succ n ih =>
    intro xs hle
    match xs with
    | [] => simp [List.zipIdx]
    | a :: as =>
      have hle' : as.length ≤ n := by simp at hle; omega
      have hzip : ((a :: as).zipIdx.map (fun (x, i) => (((a :: as).drop (i + 1)).count x))).sum =
          as.count a + ((as.zipIdx.map (fun (x, i) => ((as.drop (i + 1)).count x))).sum) := by
        simp only [List.zipIdx_cons, List.map_cons, List.sum_cons, Nat.zero_add, Nat.add_zero, List.drop_succ_cons, List.drop_zero]
        congr 1
        have h1 : (as.zipIdx 1).map (fun (x, i) => (((a :: as).drop (i + 1)).count x)) =
            as.zipIdx.map (fun (x, i) => ((as.drop (i + 1)).count x)) := by
          apply List.ext_getElem
          · simp [List.length_map, List.length_zipIdx]
          · intro i hi1 hi2
            simp only [List.getElem_map, List.getElem_zipIdx, List.length_map, List.length_zipIdx] at hi1 hi2 ⊢
            simp [List.drop_succ_cons, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
        rw [h1]
      rw [hzip, ih as hle']
      rw [List.eraseDups_cons]
      simp only [List.map_cons, List.sum_cons, List.count_cons, beq_self_eq_true, ite_true]
      have hhead : comb (as.count a + 1) = comb (as.count a) + as.count a := by rw [comb_succ]
      rw [hhead]
      have hcount_k : ∀ k, k ∈ (List.filter (fun b => !(b == a)) as).eraseDups →
          (if (a == k) = true then 1 else 0) + as.count k = as.count k := by
        intro k hk
        rw [List.mem_eraseDups, List.mem_filter] at hk
        have hne : (a == k) = false := by
          have : (!(k == a)) = true := hk.2
          simp at this
          rw [beq_eq_false_iff_ne]
          intro heq; subst heq; simp at this
        simp [hne]
      have hmap_eq : ((List.filter (fun b => !(b == a)) as).eraseDups.map
          (fun k => comb ((as.count k + if (a == k) = true then 1 else 0)))).sum =
          ((List.filter (fun b => !(b == a)) as).eraseDups.map
          (fun k => comb (as.count k))).sum := by
        congr 1; apply List.map_congr_left; intro k hk
        rw [Nat.add_comm (List.count k as) (if (a == k) = true then 1 else 0), hcount_k k hk]
      rw [hmap_eq]
      have hcount_filter : ∀ k, k ∈ (List.filter (fun b => !(b == a)) as).eraseDups →
          as.count k = (List.filter (fun b => !(b == a)) as).count k := by
        intro k hk
        rw [List.mem_eraseDups, List.mem_filter] at hk
        have hne : k ≠ a := by
          have : (!(k == a)) = true := hk.2
          simp at this
          intro heq; subst heq; simp at this
        exact (count_eq_count_filter_ne a as k hne).symm
      have hmap_eq2 : ((List.filter (fun b => !(b == a)) as).eraseDups.map
          (fun k => comb (as.count k))).sum =
          ((List.filter (fun b => !(b == a)) as).eraseDups.map
          (fun k => comb ((List.filter (fun b => !(b == a)) as).count k))).sum := by
        congr 1; apply List.map_congr_left; intro k hk
        rw [hcount_filter k hk]
      rw [hmap_eq2]
      have hlen_f : (List.filter (fun b => !(b == a)) as).length ≤ n :=
        Nat.le_trans (List.length_filter_le _ _) hle'
      have ih_f := ih (List.filter (fun b => !(b == a)) as) hlen_f
      rw [ih_f]
      have hsplit_as := sum_filter_split as.eraseDups (fun k => comb (as.count k)) (fun k => k == a)
      have hsum_eq_a : ((as.eraseDups.filter (fun k => k == a)).map (fun k => comb (as.count k))).sum =
          if a ∈ as then comb (as.count a) else 0 := by
        by_cases ha : a ∈ as
        · simp only [ha, ite_true]
          have hmem : a ∈ as.eraseDups := List.mem_eraseDups.mpr ha
          have hnodup := nodup_eraseDups as
          have hfilter : as.eraseDups.filter (fun k => k == a) = [a] := by
            apply List.ext_getElem
            · rw [List.length_filter]
              have hle1 : (as.eraseDups.filter (fun k => k == a)).length ≤ 1 := by
                have hnod := (nodup_eraseDups as).filter (fun k => k == a)
                have hall : ∀ x ∈ as.eraseDups.filter (fun k => k == a), x = a := by
                  intro x hx; rw [List.mem_filter] at hx; exact eq_of_beq hx.2
                match hf : as.eraseDups.filter (fun k => k == a) with
                | [] => simp
                | [_] => simp
                | x :: y :: zs =>
                  have hxv := hall x (by rw [hf]; simp)
                  have hyv := hall y (by rw [hf]; simp [List.mem_cons])
                  subst hxv; subst hyv
                  rw [hf] at hnod
                  simp [List.nodup_cons] at hnod
              have hge1 : 1 ≤ (as.eraseDups.filter (fun k => k == a)).length :=
                List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hmem, by simp⟩)
              omega
            · intro i hi1 hi2
              have hi0 : i = 0 := by simp at hi2; omega
              subst hi0
              have hget := List.getElem_mem hi1
              rw [List.mem_filter] at hget
              simp [eq_of_beq hget.2]
          rw [hfilter]; simp
        · simp only [ha, ite_false]
          have hfilter : as.eraseDups.filter (fun k => k == a) = [] := by
            rw [List.filter_eq_nil_iff]
            intro k hk hk2
            have hk3 : k = a := eq_of_beq hk2
            subst hk3
            rw [List.mem_eraseDups] at hk
            exact ha hk
          rw [hfilter]; simp
      have hsum_ne_a : ((as.eraseDups.filter (fun k => !(k == a))).map (fun k => comb (as.count k))).sum =
          ((List.filter (fun b => !(b == a)) as).eraseDups.map (fun k => comb ((List.filter (fun b => !(b == a)) as).count k))).sum := by
        have hperm : (as.eraseDups.filter (fun k => !(k == a))).Perm (List.filter (fun b => !(b == a)) as).eraseDups := by
          rw [List.perm_iff_count]
          intro k
          by_cases hka : k = a
          · subst hka
            have h1 : k ∉ as.eraseDups.filter (fun j => !(j == k)) := by
              rw [List.mem_filter]; intro ⟨_, h⟩; simp at h
            have h2 : k ∉ (List.filter (fun b => !(b == k)) as).eraseDups := by
              rw [List.mem_eraseDups, List.mem_filter]; intro ⟨_, h⟩; simp at h
            rw [List.count_eq_zero_of_not_mem h1, List.count_eq_zero_of_not_mem h2]
          · have hka' : (!(k == a)) = true := by
              have : (k == a) = false := beq_eq_false_iff_ne.mpr hka
              simp [this]
            by_cases hkmem : k ∈ as
            · have h1 : k ∈ as.eraseDups.filter (fun j => !(j == a)) := by
                rw [List.mem_filter, List.mem_eraseDups]; exact ⟨hkmem, hka'⟩
              have h2 : k ∈ (List.filter (fun b => !(b == a)) as).eraseDups := by
                rw [List.mem_eraseDups, List.mem_filter]; exact ⟨hkmem, hka'⟩
              rw [count_eq_one_of_mem_nodup ((nodup_eraseDups as).filter _) h1,
                  count_eq_one_of_mem_nodup (nodup_eraseDups _) h2]
            · have h1 : k ∉ as.eraseDups.filter (fun j => !(j == a)) := by
                rw [List.mem_filter, List.mem_eraseDups]; intro ⟨h, _⟩; exact hkmem h
              have h2 : k ∉ (List.filter (fun b => !(b == a)) as).eraseDups := by
                rw [List.mem_eraseDups, List.mem_filter]; intro ⟨h, _⟩; exact hkmem h
              rw [List.count_eq_zero_of_not_mem h1, List.count_eq_zero_of_not_mem h2]
        rw [perm_sum_eq (hperm.map (fun k => comb (as.count k)))]
        congr 1; apply List.map_congr_left; intro k hk
        rw [List.mem_eraseDups, List.mem_filter] at hk
        have hne : k ≠ a := by
          have : (!(k == a)) = true := hk.2; simp at this; intro heq; subst heq; simp at this
        rw [count_eq_count_filter_ne a as k hne]
      rw [hsplit_as, hsum_eq_a, hsum_ne_a]
      by_cases ha : a ∈ as
      · simp [ha]; omega
      · have hca : as.count a = 0 := List.count_eq_zero_of_not_mem ha
        simp [ha, hca, comb]

private theorem orderStatistic_of_count {α : Type} (b : Bag α)
    (hcount : ∀ p k, b.mass p k = (b.expand p).count k)
    (hcover : ∀ p k, 0 < (b.expand p).count k → k ∈ b.keys p) :
    orderStatistic b := by
  intro p i
  let ys := (b.expand p).mergeSort (fun a b => decide (a ≤ b))
  let ks := (live b p).mergeSort (fun a b => decide (a ≤ b))
  have hys_pw : List.Pairwise (fun a b => decide (a ≤ b) = true) ys := sorted_mergeSort_nat _
  have hks_pw : List.Pairwise (fun a b => decide (a ≤ b) = true) ks := sorted_mergeSort_nat _
  have hcover_ks : ∀ x ∈ ys, x ∈ ks := by
    intro x hx
    rw [List.mem_mergeSort] at hx
    have hpos : 0 < (b.expand p).count x := List.count_pos_iff.mpr hx
    rw [List.mem_mergeSort]
    unfold live
    rw [List.mem_filter]
    refine ⟨?_, ?_⟩
    · rw [List.mem_eraseDups]; exact hcover p x hpos
    · simpa [hcount p x] using hpos
  have hbelow : ∀ k, below b p k = (ys.filter (fun x => decide (x ≤ k))).length := by
    intro k
    exact below_eq_filter_length_mergeSort b p k (hcount p) (hcover p)
  unfold quantile
  simp_rw [hbelow]
  by_cases hi : i < ys.length
  · have htarget : decide (i < (ys.filter (fun x => decide (x ≤ ys[i]))).length) = true := by
      simp [filter_take_len i hi hys_pw]
    have hmem : ys[i] ∈ ks := hcover_ks _ (List.getElem_mem hi)
    obtain ⟨left, right, hks, hnin⟩ := List.append_of_mem hmem
    rw [hks, List.find?_append]
    have hleft_none : left.find? (fun k => decide (i < (ys.filter (fun x => decide (x ≤ k))).length)) = none := by
      rw [List.find?_eq_none]
      intro k hk
      have hk_le : k ≤ ys[i] := by
        have hpair := (List.pairwise_append.mp (by rw [← hks]; exact hks_pw)).2.2
        exact hpair k hk (ys[i]) (by simp)
      have hk_ne : k ≠ ys[i] := by intro heq; subst heq; exact hnin hk
      have hk_lt : k < ys[i] := Nat.lt_of_le_of_ne hk_le hk_ne
      have hle := filter_le_length_le i hi hys_pw hk_lt
      simp; omega
    rw [hleft_none]
    simp [htarget, List.getElem?_eq_getElem hi]
  · push_neg at hi
    rw [List.getElem?_eq_none hi, List.find?_eq_none]
    intro k hk
    simp only [decide_eq_true_eq, not_lt]
    calc (ys.filter (fun x => decide (x ≤ k))).length ≤ ys.length := List.length_filter_le _ _
      _ ≤ i := hi

/-! ## MultiplicityRuns -/

namespace MultiplicityRuns
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns

private abbrev B : Bag (List (Nat × Nat)) := bag

private theorem mass_eq_count (rs : List (Nat × Nat)) (k : Nat) :
    B.mass rs k = (B.expand rs).count k := by
  simp only [B, bag]
  induction rs with
  | nil => simp
  | cons r rs ih =>
    obtain ⟨x, n⟩ := r
    simp only [List.flatMap_cons, List.count_append, List.count_replicate, List.filter_cons,
      List.map_cons, List.sum_cons]
    by_cases hx : x == k
    · simp [hx]; omega
    · simp [hx, ih]

private theorem cover (rs : List (Nat × Nat)) (k : Nat) (hk : 0 < (B.expand rs).count k) :
    k ∈ B.keys rs := by
  simp only [B, bag] at hk ⊢
  induction rs with
  | nil => simp at hk
  | cons r rs ih =>
    obtain ⟨x, n⟩ := r
    simp only [List.flatMap_cons, List.count_append, List.count_replicate] at hk
    simp only [List.map_cons, List.mem_cons]
    by_cases hx : x == k
    · left; exact (eq_of_beq hx).symm
    · right
      have hzero : (List.replicate n x).count k = 0 := by
        rw [List.count_replicate]; simp [hx]
      have : 0 < (B.expand rs).count k := by
        simp only [B, bag] at hk ⊢
        rw [List.count_append, hzero] at hk
        omega
      exact ih this

private theorem mem_live_iff (rs : List (Nat × Nat)) (k : Nat) :
    k ∈ live B rs ↔ 0 < B.mass rs k := by
  unfold live
  constructor
  · intro h; rw [List.mem_filter] at h; exact by simpa using h.2
  · intro h
    rw [List.mem_filter]
    exact ⟨by rw [List.mem_eraseDups]; exact cover rs k (by rw [← mass_eq_count]; exact h), by simpa⟩

private theorem live_nodup (rs : List (Nat × Nat)) : (live B rs).Nodup :=
  (nodup_eraseDups _).filter _

theorem sound_DistinctOutput : DistinctOutput.Goal := by
  intro rs
  rw [List.perm_iff_count]
  intro k
  by_cases hk : k ∈ live B rs
  · have hk2 : k ∈ (B.expand rs).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢
      exact hk
    rw [count_eq_one_of_mem_nodup (live_nodup rs) hk,
        count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
  · have hk2 : k ∉ (B.expand rs).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff]
      intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
    rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro rs lo hi
  rw [weighted_eq_map_of_count B rs _ (mass_eq_count rs) (cover rs)]
  have h : (fun x => if lo ≤ x && x < hi then x * x else 0) =
      (fun x => if decide (lo ≤ x ∧ x < hi) then x * x else 0) := by
    funext x; by_cases h1 : lo ≤ x <;> by_cases h2 : x < hi <;> simp [h1, h2]
  rw [h, map_if_sum]
  congr 1
  apply List.filter_congr
  intro x hx; simp

theorem sound_OrderStatistic : OrderStatistic.Goal :=
  orderStatistic_of_count B (fun rs k => mass_eq_count rs k) (fun rs k hk => cover rs k hk)

private theorem pairs_length (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = ((xs.map (fun x => (ys.filter (pred x)).length)).sum) := by
  unfold pairs
  induction xs with
  | nil => simp
  | cons a as ih =>
    simp only [List.flatMap_cons, List.length_append, ih, List.map_cons, List.sum_cons]
    rw [List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c slack
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hinner : ∀ x, weighted B c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat) =
      ((B.expand c).map (fun y => if x ≤ y && y ≤ x + slack then 1 else 0)).sum := by
    intro x
    rw [weighted_eq_map_of_count B c _ (mass_eq_count c) (cover c)]
    congr 1; funext y
    by_cases h : x ≤ y ∧ y ≤ x + slack <;> simp [h]
  simp_rw [hinner]
  rw [pairs_length]
  congr 1; apply List.map_congr_left; intro x hx
  rw [map_if_sum]; simp

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => if B.mass c x == 0 then x else 0)).sum =
      ((B.expand a).map (fun x => if !(B.expand c).contains x then x else 0)).sum := by
    congr 1; apply List.map_congr_left; intro x hx
    simp only [mass_eq_count, List.contains_iff_mem, beq_iff_eq]
    by_cases h : x ∈ B.expand c <;> simp [h, List.count_eq_zero_of_not_mem]
  rw [hL, map_if_sum]; simp

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro rs
  have hperm : (live B rs).Perm (B.expand rs).eraseDups := by
    rw [List.perm_iff_count]; intro k
    by_cases hk : k ∈ live B rs
    · have hk2 : k ∈ (B.expand rs).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
      rw [count_eq_one_of_mem_nodup (live_nodup rs) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
    · have hk2 : k ∉ (B.expand rs).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
      rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]
  rw [perm_sum_eq (hperm.map (fun k => comb (B.mass rs k)))]
  have hmass : ((B.expand rs).eraseDups.map (fun k => comb (B.mass rs k))).sum =
      ((B.expand rs).eraseDups.map (fun k => comb ((B.expand rs).count k))).sum := by
    congr 1; apply List.map_congr_left; intro k hk; rw [mass_eq_count]
  rw [hmass]
  exact sum_eraseDups_comb (B.expand rs)

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  have hL : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k =
      min (B.mass a k) (B.mass c k) := by
    by_cases hk : k ∈ live B a
    · have hsplit : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) =
          (((live B a).filter (· == k)).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) ++
          (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) := by
        rw [← List.flatMap_append]; congr 1
        conv_lhs => rw [← List.filter_append (fun x => x == k) (fun x => !(x == k)) (live B a)]
        congr 1; ext x; by_cases hx : x == k <;> simp [hx]
      rw [hsplit, List.count_append]
      have hzero : (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k = 0 := by
        rw [List.count_eq_zero_of_not_mem]
        intro hm; rw [List.mem_flatMap] at hm
        rcases hm with ⟨x, hx, hxk⟩
        rw [List.mem_filter] at hx; rw [List.mem_replicate] at hxk
        have hxne : x ≠ k := by intro heq; subst heq; simp at hx
        exact hxne hxk.2
      rw [hzero, Nat.add_zero]
      have hfilter : (live B a).filter (· == k) = [k] := by
        apply List.ext_getElem
        · rw [List.length_filter]
          have hle1 : ((live B a).filter (· == k)).length ≤ 1 := by
            have hnod := (live_nodup a).filter (· == k)
            have hall : ∀ x ∈ (live B a).filter (· == k), x = k := by
              intro x hx; rw [List.mem_filter] at hx; exact eq_of_beq hx.2
            match hf : (live B a).filter (· == k) with
            | [] => simp
            | [_] => simp
            | x :: y :: zs =>
              have hxv := hall x (by rw [hf]; simp)
              have hyv := hall y (by rw [hf]; simp [List.mem_cons])
              subst hxv; subst hyv; rw [hf] at hnod; simp [List.nodup_cons] at hnod
          have hge1 : 1 ≤ ((live B a).filter (· == k)).length :=
            List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hk, by simp⟩)
          omega
        · intro i hi1 hi2
          have hi0 : i = 0 := by simp at hi2; omega
          subst hi0
          have hget := List.getElem_mem hi1
          rw [List.mem_filter] at hget
          simp [eq_of_beq hget.2]
      rw [hfilter]; simp [List.count_replicate_self]
    · have hma : B.mass a k = 0 := by
        by_contra h; exact hk ((mem_live_iff a k).mpr (Nat.pos_of_ne_zero h))
      have hmin : min (B.mass a k) (B.mass c k) = 0 := by simp [hma]
      rw [hmin, List.count_eq_zero_of_not_mem]
      intro hm; rw [List.mem_flatMap] at hm
      rcases hm with ⟨x, hx, hxk⟩
      rw [List.mem_replicate] at hxk; rcases hxk with ⟨_, rfl⟩
      exact hk hx
  simp only [B]; rw [hL, mass_eq_count, mass_eq_count]

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => max 1 (B.mass c x))).sum =
      (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum +
      (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun _ => 1)).sum := by
    have hsplit := sum_filter_split (B.expand a) (fun x => max 1 (B.mass c x)) (fun x => (B.expand c).contains x)
    rw [hsplit]; congr 1
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hmem : x ∈ B.expand c := List.contains_iff_mem.mp hx.2
      have hpos : 0 < B.mass c x := by rw [mass_eq_count]; exact List.count_pos_iff.mpr hmem
      omega
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hnmem : x ∉ B.expand c := by intro hm; have := List.contains_iff_mem.mpr hm; simp_all
      have h0 : B.mass c x = 0 := by rw [mass_eq_count, List.count_eq_zero_of_not_mem hnmem]
      simp [h0]
  rw [hL]; congr 1
  · rw [pairs_length]
    have hpair : ((B.expand a).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum =
        (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum := by
      have hsplit2 := sum_filter_split (B.expand a) (fun x => ((B.expand c).filter (fun y => x == y)).length) (fun x => (B.expand c).contains x)
      rw [hsplit2]
      have hz : (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum = 0 := by
        apply list_sum_eq_zero; intro v hv
        rw [List.mem_map] at hv; rcases hv with ⟨x, hx, rfl⟩
        rw [List.mem_filter] at hx
        have hnmem : x ∉ B.expand c := by intro hm; simp [List.contains_iff_mem.mpr hm] at hx
        rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
        intro y hy
        simp only [beq_iff_eq]
        intro heq; subst heq; exact hnmem hy
      rw [hz, Nat.add_zero]
      congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hmem : x ∈ B.expand c := List.contains_iff_mem.mp hx.2
      have hc : ((B.expand c).filter (fun y => x == y)).length = (B.expand c).count x := by
        rw [List.count_eq_length_filter]; congr 1; apply List.filter_congr; intro y hy; simp [Bool.beq_comm]
      rw [hc, ← mass_eq_count]
    rw [hpair]
  · rw [sum_map_const_nat]; simp

theorem sound_WeightedOrderStatistic : WeightedOrderStatistic.Goal := by
  intro x y a b i hxy hi
  have hxy_ne : (y == x) = false := by rw [beq_eq_false_iff_ne]; omega
  have hxx : (x == x) = true := by simp
  have hyy : (y == y) = true := by simp
  have hxy_beq : (x == y) = false := by rw [beq_eq_false_iff_ne]; omega
  have hbx : below B [(x,a),(y,b)] x = a := by
    unfold below weighted B bag
    simp [List.eraseDups_cons, List.mem_cons, hxy_ne, hyy, hxx, hxy_beq]
  have hby : below B [(x,a),(y,b)] y = a + b := by
    unfold below weighted B bag
    simp [List.eraseDups_cons, List.mem_cons, hxy_ne, hyy, hxx, hxy_beq, Nat.le_of_lt hxy, Nat.le_refl]
  unfold quantile
  by_cases ha : 0 < a
  · by_cases hb : 0 < b
    · have hlive : live B [(x,a),(y,b)] = [x,y] := by
        unfold live B bag
        simp [List.eraseDups_cons, List.mem_cons, hxy_ne, hyy, hxx, hxy_beq, ha, hb, List.filter]
      rw [hlive]; simp [List.mergeSort, hbx, hby]; by_cases hia : i < a <;> simp [hia]
    · have hb0 : b = 0 := by omega
      subst hb0
      have hlive : live B [(x,a),(y,0)] = [x] := by
        unfold live B bag
        simp [List.eraseDups_cons, List.mem_cons, hxy_ne, hyy, hxx, hxy_beq, ha, List.filter]
      rw [hlive]; simp [List.mergeSort, hbx]; have hia : i < a := by omega; simp [hia]
  · have ha0 : a = 0 := by omega
    subst ha0
    by_cases hb : 0 < b
    · have hlive : live B [(x,0),(y,b)] = [y] := by
        unfold live B bag
        simp [List.eraseDups_cons, List.mem_cons, hxy_ne, hyy, hxx, hxy_beq, hb, List.filter]
      rw [hlive]; simp [List.mergeSort, hby]; omega
    · omega

theorem sound_ZeroRuns : ZeroRuns.Goal := by
  intro rs
  rw [List.perm_iff_count]; intro k
  have hmass_f : B.mass (rs.filter (fun r => 0 < r.2)) k = B.mass rs k := by
    simp only [B, bag]
    induction rs with
    | nil => simp
    | cons r rs ih =>
      obtain ⟨v, n⟩ := r
      simp only [List.filter_cons]
      by_cases hn : 0 < n
      · simp only [show (0 < n) = true from by simpa using hn, ite_true, List.map_cons, List.sum_cons]
        by_cases hv : v == k <;> simp [hv, ih]
      · have hn0 : n = 0 := by omega
        subst hn0
        simp only [show (0 < 0) = false by decide, ite_false, List.map_cons, List.sum_cons]
        by_cases hv : v == k <;> simp [hv, ih]
  by_cases hk : k ∈ live B (rs.filter (fun r => 0 < r.2))
  · have hk2 : k ∈ live B rs := by rw [mem_live_iff] at hk ⊢; rw [← hmass_f]; exact hk
    rw [count_eq_one_of_mem_nodup (live_nodup _) hk, count_eq_one_of_mem_nodup (live_nodup _) hk2]
  · have hk2 : k ∉ live B rs := by intro hk2; apply hk; rw [mem_live_iff] at hk2 ⊢; rw [hmass_f]; exact hk2
    rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]

end MultiplicityRuns

/-! ## IntervalCover -/

namespace IntervalCover
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.IntervalCover

private abbrev B : Bag (List (Nat × Nat)) := bag

private theorem mass_eq_count (rs : List (Nat × Nat)) (k : Nat) :
    B.mass rs k = (B.expand rs).count k := by
  simp only [B, bag]
  induction rs with
  | nil => simp
  | cons r rs ih =>
    obtain ⟨s, n⟩ := r
    simp only [List.flatMap_cons, List.count_append, List.countP_cons]
    rw [ih]; congr 1
    by_cases h : s ≤ k ∧ k < s + n
    · have hmem : k ∈ (List.range n).map (s + ·) := by
        rw [List.mem_map]; exact ⟨k - s, by rw [List.mem_range]; omega, by omega⟩
      have hnodup : ((List.range n).map (s + ·)).Nodup := by
        apply List.Nodup.map
        · intro a b hab; omega
        · exact List.nodup_range n
      have hcount : ((List.range n).map (s + ·)).count k = 1 :=
        List.count_eq_one_of_mem hnodup hmem
      simp [h, hcount]
    · have hnotmem : k ∉ (List.range n).map (s + ·) := by
        rw [List.mem_map]; intro ⟨i, hi, hik⟩; rw [List.mem_range] at hi; apply h; omega
      simp [h, List.count_eq_zero_of_not_mem hnotmem]

private theorem cover (rs : List (Nat × Nat)) (k : Nat) (hk : 0 < (B.expand rs).count k) :
    k ∈ B.keys rs := by
  simp only [B, bag] at hk ⊢
  rw [List.mem_range]
  by_contra hle
  push_neg at hle
  have hnotmem : k ∉ (List.flatMap (fun (s, n) => (List.range n).map (s + ·)) rs) := by
    rw [List.mem_flatMap]
    intro ⟨r, hr, hk2⟩
    obtain ⟨s, n⟩ := r
    rw [List.mem_map] at hk2
    rcases hk2 with ⟨i, hi, rfl⟩
    rw [List.mem_range] at hi
    have hle2 : s + n ≤ peak (rs.map (fun (s, n) => s + n)) := by
      unfold peak
      have hmem : s + n ∈ rs.map (fun (s, n) => s + n) := List.mem_map.mpr ⟨(s,n), hr, rfl⟩
      exact List.le_max?_getD hmem
    omega
  exact (Nat.not_lt_zero _) (by simpa [List.count_eq_zero_of_not_mem hnotmem] using hk)

private theorem mem_live_iff (rs : List (Nat × Nat)) (k : Nat) :
    k ∈ live B rs ↔ 0 < B.mass rs k := by
  unfold live
  constructor
  · intro h; rw [List.mem_filter] at h; exact by simpa using h.2
  · intro h
    rw [List.mem_filter]
    exact ⟨by rw [List.mem_eraseDups]; exact cover rs k (by rw [← mass_eq_count]; exact h), by simpa⟩

private theorem live_nodup (rs : List (Nat × Nat)) : (live B rs).Nodup :=
  (nodup_eraseDups _).filter _

theorem sound_DistinctOutput : DistinctOutput.Goal := by
  intro p
  rw [List.perm_iff_count]; intro k
  by_cases hk : k ∈ live B p
  · have hk2 : k ∈ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
    rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
  · have hk2 : k ∉ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
    rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rw [weighted_eq_map_of_count B p _ (mass_eq_count p) (cover p)]
  have h : (fun x => if lo ≤ x && x < hi then x * x else 0) =
      (fun x => if decide (lo ≤ x ∧ x < hi) then x * x else 0) := by
    funext x; by_cases h1 : lo ≤ x <;> by_cases h2 : x < hi <;> simp [h1, h2]
  rw [h, map_if_sum]; congr 1; apply List.filter_congr; intro x hx; simp

theorem sound_OrderStatistic : OrderStatistic.Goal :=
  orderStatistic_of_count B (fun p k => mass_eq_count p k) (fun p k hk => cover p k hk)

private theorem pairs_length (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = ((xs.map (fun x => (ys.filter (pred x)).length)).sum) := by
  unfold pairs
  induction xs with
  | nil => simp
  | cons a as ih => simp only [List.flatMap_cons, List.length_append, ih, List.map_cons, List.sum_cons, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c slack
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hinner : ∀ x, weighted B c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat) =
      ((B.expand c).map (fun y => if x ≤ y && y ≤ x + slack then 1 else 0)).sum := by
    intro x; rw [weighted_eq_map_of_count B c _ (mass_eq_count c) (cover c)]
    congr 1; funext y; by_cases h : x ≤ y ∧ y ≤ x + slack <;> simp [h]
  simp_rw [hinner]; rw [pairs_length]
  congr 1; apply List.map_congr_left; intro x hx; rw [map_if_sum]; simp

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => if B.mass c x == 0 then x else 0)).sum =
      ((B.expand a).map (fun x => if !(B.expand c).contains x then x else 0)).sum := by
    congr 1; apply List.map_congr_left; intro x hx
    simp only [mass_eq_count, List.contains_iff_mem, beq_iff_eq]
    by_cases h : x ∈ B.expand c <;> simp [h, List.count_eq_zero_of_not_mem]
  rw [hL, map_if_sum]; simp

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro p
  have hperm : (live B p).Perm (B.expand p).eraseDups := by
    rw [List.perm_iff_count]; intro k
    by_cases hk : k ∈ live B p
    · have hk2 : k ∈ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
      rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
    · have hk2 : k ∉ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
      rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]
  rw [perm_sum_eq (hperm.map (fun k => comb (B.mass p k)))]
  have hmass : ((B.expand p).eraseDups.map (fun k => comb (B.mass p k))).sum =
      ((B.expand p).eraseDups.map (fun k => comb ((B.expand p).count k))).sum := by
    congr 1; apply List.map_congr_left; intro k hk; rw [mass_eq_count]
  rw [hmass]
  exact sum_eraseDups_comb (B.expand p)

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  have hL : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k =
      min (B.mass a k) (B.mass c k) := by
    by_cases hk : k ∈ live B a
    · have hsplit : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) =
          (((live B a).filter (· == k)).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) ++
          (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) := by
        rw [← List.flatMap_append]; congr 1
        conv_lhs => rw [← List.filter_append (fun x => x == k) (fun x => !(x == k)) (live B a)]
        congr 1; ext x; by_cases hx : x == k <;> simp [hx]
      rw [hsplit, List.count_append]
      have hzero : (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k = 0 := by
        rw [List.count_eq_zero_of_not_mem]
        intro hm; rw [List.mem_flatMap] at hm
        rcases hm with ⟨x, hx, hxk⟩
        rw [List.mem_filter] at hx; rw [List.mem_replicate] at hxk
        have hxne : x ≠ k := by intro heq; subst heq; simp at hx
        exact hxne hxk.2
      rw [hzero, Nat.add_zero]
      have hfilter : (live B a).filter (· == k) = [k] := by
        apply List.ext_getElem
        · rw [List.length_filter]
          have hle1 : ((live B a).filter (· == k)).length ≤ 1 := by
            have hnod := (live_nodup a).filter (· == k)
            have hall : ∀ x ∈ (live B a).filter (· == k), x = k := by
              intro x hx; rw [List.mem_filter] at hx; exact eq_of_beq hx.2
            match hf : (live B a).filter (· == k) with
            | [] => simp
            | [_] => simp
            | x :: y :: zs =>
              have hxv := hall x (by rw [hf]; simp)
              have hyv := hall y (by rw [hf]; simp [List.mem_cons])
              subst hxv; subst hyv; rw [hf] at hnod; simp [List.nodup_cons] at hnod
          have hge1 : 1 ≤ ((live B a).filter (· == k)).length :=
            List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hk, by simp⟩)
          omega
        · intro i hi1 hi2
          have hi0 : i = 0 := by simp at hi2; omega
          subst hi0
          have hget := List.getElem_mem hi1
          rw [List.mem_filter] at hget
          simp [eq_of_beq hget.2]
      rw [hfilter]; simp [List.count_replicate_self]
    · have hma : B.mass a k = 0 := by
        by_contra h; exact hk ((mem_live_iff a k).mpr (Nat.pos_of_ne_zero h))
      have hmin : min (B.mass a k) (B.mass c k) = 0 := by simp [hma]
      rw [hmin, List.count_eq_zero_of_not_mem]
      intro hm; rw [List.mem_flatMap] at hm
      rcases hm with ⟨x, hx, hxk⟩
      rw [List.mem_replicate] at hxk; rcases hxk with ⟨_, rfl⟩
      exact hk hx
  simp only [B]; rw [hL, mass_eq_count, mass_eq_count]

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => max 1 (B.mass c x))).sum =
      (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum +
      (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun _ => 1)).sum := by
    have hsplit := sum_filter_split (B.expand a) (fun x => max 1 (B.mass c x)) (fun x => (B.expand c).contains x)
    rw [hsplit]; congr 1
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hmem : x ∈ B.expand c := List.contains_iff_mem.mp hx.2
      have hpos : 0 < B.mass c x := by rw [mass_eq_count]; exact List.count_pos_iff.mpr hmem
      omega
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hnmem : x ∉ B.expand c := by intro hm; have := List.contains_iff_mem.mpr hm; simp_all
      have h0 : B.mass c x = 0 := by rw [mass_eq_count, List.count_eq_zero_of_not_mem hnmem]
      simp [h0]
  rw [hL]; congr 1
  · rw [pairs_length]
    have hpair : ((B.expand a).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum =
        (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum := by
      have hsplit2 := sum_filter_split (B.expand a) (fun x => ((B.expand c).filter (fun y => x == y)).length) (fun x => (B.expand c).contains x)
      rw [hsplit2]
      have hz : (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum = 0 := by
        apply list_sum_eq_zero; intro v hv
        rw [List.mem_map] at hv; rcases hv with ⟨x, hx, rfl⟩
        rw [List.mem_filter] at hx
        have hnmem : x ∉ B.expand c := by intro hm; simp [List.contains_iff_mem.mpr hm] at hx
        rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
        intro y hy; simp only [beq_iff_eq]; intro heq; subst heq; exact hnmem hy
      rw [hz, Nat.add_zero]
      congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hc : ((B.expand c).filter (fun y => x == y)).length = (B.expand c).count x := by
        rw [List.count_eq_length_filter]; congr 1; apply List.filter_congr; intro y hy; simp [Bool.beq_comm]
      rw [hc, ← mass_eq_count]
    rw [hpair]
  · rw [sum_map_const_nat]; simp

theorem sound_ClipRanges : ClipRanges.Goal := by
  intro rs lim
  rw [weighted_eq_map_of_count B rs _ (mass_eq_count rs) (cover rs)]
  have h1 : ((B.expand rs).map (fun x => (decide (x < lim)).toNat)).sum =
      ((B.expand rs).filter (fun x => x < lim)).length := by
    have h : ((B.expand rs).map (fun x => (decide (x < lim)).toNat)).sum =
        ((B.expand rs).map (fun x => if x < lim then 1 else 0)).sum := by
      congr 1; apply List.map_congr_left; intro x hx; by_cases h : x < lim <;> simp [h]
    rw [h, sum_map_if_one]; congr 1; apply List.filter_congr; intro x hx; simp
  rw [h1]; simp only [B, bag]
  induction rs with
  | nil => simp
  | cons r rs ih =>
    obtain ⟨s, n⟩ := r
    simp only [List.flatMap_cons, List.filter_append, List.length_append, List.map_cons, List.sum_cons]
    rw [ih]; congr 1
    rw [List.filter_map, List.length_map]
    have hcount : ∀ m, (List.filter (fun i => decide (s + i < lim)) (List.range m)).length = min m (lim - s) := by
      intro m
      induction m with
      | zero => simp
      | succ m ihm =>
        rw [List.range_succ, List.filter_append, List.length_append, ihm]
        simp only [List.filter]
        by_cases h : s + m < lim
        · have hd : decide (s + m < lim) = true := by simpa using h
          simp [hd]; omega
        · have hd : decide (s + m < lim) = false := by simpa using h
          simp [hd]; omega
    simpa [Function.comp] using hcount n

theorem sound_CoverageIntegral : CoverageIntegral.Goal := by
  intro rs
  rw [weighted_eq_map_of_count B rs _ (mass_eq_count rs) (cover rs)]
  simp only [B, bag]
  induction rs with
  | nil => simp
  | cons r rs ih =>
    obtain ⟨s, n⟩ := r
    simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons, ih]
    congr 1
    simp [List.map_map, sum_map_const_nat]

end IntervalCover

/-! ## AdditiveProduct -/

namespace AdditiveProduct
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct

private abbrev B : Bag (List Nat × List Nat) := bag

private theorem mass_eq_count (p : List Nat × List Nat) (k : Nat) :
    B.mass p k = (B.expand p).count k := by
  obtain ⟨a, b⟩ := p
  simp only [B, bag]
  -- count in flatMap (fun x => b.map (x+·)) a
  have hcount_expand : ((a.flatMap (fun x => b.map (x + ·))).count k) =
      (a.map (fun x => if x ≤ k then b.count (k - x) else 0)).sum := by
    induction a with
    | nil => simp
    | cons x xs ih =>
      simp only [List.flatMap_cons, List.count_append, List.map_cons, List.sum_cons]
      rw [ih]; congr 1
      -- count k in b.map (x+·)
      by_cases hx : x ≤ k
      · simp only [hx, ite_true]
        -- count k in map (x+·) b = count (k-x) in b
        have h1 : (List.map (fun y => x + y) b).count k = b.count (k - x) := by
          induction b with
          | nil => simp
          | cons y ys ihb =>
            simp only [List.map_cons, List.count_cons, ihb]
            by_cases hy : x + y == k
            · have heq : k - x = y := by have := eq_of_beq hy; omega
              have hbeq : (y == k - x) = true := by simp [heq]
              simp [hy, hbeq]; omega
            · have hne : (y == k - x) = false := by
                rw [beq_eq_false_iff_ne]; intro heq
                apply (beq_eq_false_iff_ne.mp hy); omega
              simp [hy, hne]
        exact h1
      · simp only [hx, ite_false]
        rw [List.count_eq_zero_of_not_mem]
        rw [List.mem_map]; intro ⟨y, hy, hyeq⟩
        push_neg at hx; omega
  rw [hcount_expand]
  -- now show (a.eraseDups.map (fun x => if x ≤ k then a.count x * b.count (k-x) else 0)).sum = (a.map ...).sum
  have hsum : ((a.eraseDups.map (fun x => if x ≤ k then a.count x * b.count (k - x) else 0)).sum) =
      (a.map (fun x => if x ≤ k then b.count (k - x) else 0)).sum := by
    have h1 := sum_eraseDups_count_mul a (fun x => if x ≤ k then b.count (k - x) else 0)
    -- h1 : ((a.eraseDups.map (fun k_1 => a.count k_1 * f k_1)).sum) = (a.map f).sum
    rw [← h1]
    congr 1; apply List.map_congr_left; intro x hx
    by_cases hxk : x ≤ k <;> simp [hxk, Nat.mul_assoc]
  exact hsum

private theorem cover (p : List Nat × List Nat) (k : Nat) (hk : 0 < (B.expand p).count k) :
    k ∈ B.keys p := by
  obtain ⟨a, b⟩ := p
  simp only [B, bag] at hk ⊢
  rw [List.mem_flatMap]
  -- k is in expand, so k = x+y for some x in a, y in b
  have hmem : k ∈ a.flatMap (fun x => b.map (x + ·)) := List.count_pos_iff.mp hk
  rw [List.mem_flatMap] at hmem
  rcases hmem with ⟨x, hx, hkx⟩
  rw [List.mem_map] at hkx
  rcases hkx with ⟨y, hy, rfl⟩
  refine ⟨x, List.mem_eraseDups.mpr hx, ?_⟩
  rw [List.mem_map]
  exact ⟨y, List.mem_eraseDups.mpr hy, rfl⟩

private theorem mem_live_iff (p : List Nat × List Nat) (k : Nat) :
    k ∈ live B p ↔ 0 < B.mass p k := by
  unfold live; constructor
  · intro h; rw [List.mem_filter] at h; exact by simpa using h.2
  · intro h; rw [List.mem_filter]
    exact ⟨by rw [List.mem_eraseDups]; exact cover p k (by rw [← mass_eq_count]; exact h), by simpa⟩

private theorem live_nodup (p : List Nat × List Nat) : (live B p).Nodup := (nodup_eraseDups _).filter _

theorem sound_DistinctOutput : DistinctOutput.Goal := by
  intro p
  rw [List.perm_iff_count]; intro k
  by_cases hk : k ∈ live B p
  · have hk2 : k ∈ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
    rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
  · have hk2 : k ∉ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
    rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rw [weighted_eq_map_of_count B p _ (mass_eq_count p) (cover p)]
  have h : (fun x => if lo ≤ x && x < hi then x * x else 0) =
      (fun x => if decide (lo ≤ x ∧ x < hi) then x * x else 0) := by
    funext x; by_cases h1 : lo ≤ x <;> by_cases h2 : x < hi <;> simp [h1, h2]
  rw [h, map_if_sum]; congr 1; apply List.filter_congr; intro x hx; simp

theorem sound_OrderStatistic : OrderStatistic.Goal :=
  orderStatistic_of_count B (fun p k => mass_eq_count p k) (fun p k hk => cover p k hk)

private theorem pairs_length (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = ((xs.map (fun x => (ys.filter (pred x)).length)).sum) := by
  unfold pairs
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.flatMap_cons, List.length_append, ih, List.map_cons, List.sum_cons, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a1 c1 slack
  rw [weighted_eq_map_of_count B a1 _ (mass_eq_count a1) (cover a1)]
  have hinner : ∀ x, weighted B c1 (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat) =
      ((B.expand c1).map (fun y => if x ≤ y && y ≤ x + slack then 1 else 0)).sum := by
    intro x; rw [weighted_eq_map_of_count B c1 _ (mass_eq_count c1) (cover c1)]
    congr 1; funext y; by_cases h : x ≤ y ∧ y ≤ x + slack <;> simp [h]
  simp_rw [hinner]; rw [pairs_length]
  congr 1; apply List.map_congr_left; intro x hx; rw [map_if_sum]; simp

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a1 c1
  rw [weighted_eq_map_of_count B a1 _ (mass_eq_count a1) (cover a1)]
  have hL : ((B.expand a1).map (fun x => if B.mass c1 x == 0 then x else 0)).sum =
      ((B.expand a1).map (fun x => if !(B.expand c1).contains x then x else 0)).sum := by
    congr 1; apply List.map_congr_left; intro x hx
    simp only [mass_eq_count, List.contains_iff_mem, beq_iff_eq]
    by_cases h : x ∈ B.expand c1 <;> simp [h, List.count_eq_zero_of_not_mem]
  rw [hL, map_if_sum]; simp

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro p
  have hperm : (live B p).Perm (B.expand p).eraseDups := by
    rw [List.perm_iff_count]; intro k
    by_cases hk : k ∈ live B p
    · have hk2 : k ∈ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
      rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
    · have hk2 : k ∉ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
      rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]
  rw [perm_sum_eq (hperm.map (fun k => comb (B.mass p k)))]
  have hmass : ((B.expand p).eraseDups.map (fun k => comb (B.mass p k))).sum =
      ((B.expand p).eraseDups.map (fun k => comb ((B.expand p).count k))).sum := by
    congr 1; apply List.map_congr_left; intro k hk; rw [mass_eq_count]
  rw [hmass]
  exact sum_eraseDups_comb (B.expand p)

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a1 c1 k
  have hL : ((live B a1).flatMap (fun x => List.replicate (min (B.mass a1 x) (B.mass c1 x)) x)).count k =
      min (B.mass a1 k) (B.mass c1 k) := by
    by_cases hk : k ∈ live B a1
    · have hsplit : ((live B a1).flatMap (fun x => List.replicate (min (B.mass a1 x) (B.mass c1 x)) x)) =
          (((live B a1).filter (· == k)).flatMap (fun x => List.replicate (min (B.mass a1 x) (B.mass c1 x)) x)) ++
          (((live B a1).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a1 x) (B.mass c1 x)) x)) := by
        rw [← List.flatMap_append]; congr 1
        conv_lhs => rw [← List.filter_append (fun x => x == k) (fun x => !(x == k)) (live B a1)]
        congr 1; ext x; by_cases hx : x == k <;> simp [hx]
      rw [hsplit, List.count_append]
      have hzero : (((live B a1).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a1 x) (B.mass c1 x)) x)).count k = 0 := by
        rw [List.count_eq_zero_of_not_mem]
        intro hm; rw [List.mem_flatMap] at hm
        rcases hm with ⟨x, hx, hxk⟩
        rw [List.mem_filter] at hx; rw [List.mem_replicate] at hxk
        have hxne : x ≠ k := by intro heq; subst heq; simp at hx
        exact hxne hxk.2
      rw [hzero, Nat.add_zero]
      have hfilter : (live B a1).filter (· == k) = [k] := by
        apply List.ext_getElem
        · rw [List.length_filter]
          have hle1 : ((live B a1).filter (· == k)).length ≤ 1 := by
            have hnod := (live_nodup a1).filter (· == k)
            have hall : ∀ x ∈ (live B a1).filter (· == k), x = k := by
              intro x hx; rw [List.mem_filter] at hx; exact eq_of_beq hx.2
            match hf : (live B a1).filter (· == k) with
            | [] => simp
            | [_] => simp
            | x :: y :: zs =>
              have hxv := hall x (by rw [hf]; simp)
              have hyv := hall y (by rw [hf]; simp [List.mem_cons])
              subst hxv; subst hyv; rw [hf] at hnod; simp [List.nodup_cons] at hnod
          have hge1 : 1 ≤ ((live B a1).filter (· == k)).length :=
            List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hk, by simp⟩)
          omega
        · intro i hi1 hi2
          have hi0 : i = 0 := by simp at hi2; omega
          subst hi0
          have hget := List.getElem_mem hi1
          rw [List.mem_filter] at hget
          simp [eq_of_beq hget.2]
      rw [hfilter]; simp [List.count_replicate_self]
    · have hma : B.mass a1 k = 0 := by
        by_contra h; exact hk ((mem_live_iff a1 k).mpr (Nat.pos_of_ne_zero h))
      have hmin : min (B.mass a1 k) (B.mass c1 k) = 0 := by simp [hma]
      rw [hmin, List.count_eq_zero_of_not_mem]
      intro hm; rw [List.mem_flatMap] at hm
      rcases hm with ⟨x, hx, hxk⟩
      rw [List.mem_replicate] at hxk; rcases hxk with ⟨_, rfl⟩
      exact hk hx
  simp only [B]; rw [hL, mass_eq_count, mass_eq_count]

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a1 c1
  rw [weighted_eq_map_of_count B a1 _ (mass_eq_count a1) (cover a1)]
  have hL : ((B.expand a1).map (fun x => max 1 (B.mass c1 x))).sum =
      (((B.expand a1).filter (fun x => (B.expand c1).contains x)).map (fun x => B.mass c1 x)).sum +
      (((B.expand a1).filter (fun x => !(B.expand c1).contains x)).map (fun _ => 1)).sum := by
    have hsplit := sum_filter_split (B.expand a1) (fun x => max 1 (B.mass c1 x)) (fun x => (B.expand c1).contains x)
    rw [hsplit]; congr 1
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hmem : x ∈ B.expand c1 := List.contains_iff_mem.mp hx.2
      have hpos : 0 < B.mass c1 x := by rw [mass_eq_count]; exact List.count_pos_iff.mpr hmem
      omega
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hnmem : x ∉ B.expand c1 := by intro hm; have := List.contains_iff_mem.mpr hm; simp_all
      have h0 : B.mass c1 x = 0 := by rw [mass_eq_count, List.count_eq_zero_of_not_mem hnmem]
      simp [h0]
  rw [hL]; congr 1
  · rw [pairs_length]
    have hpair : ((B.expand a1).map (fun x => ((B.expand c1).filter (fun y => x == y)).length)).sum =
        (((B.expand a1).filter (fun x => (B.expand c1).contains x)).map (fun x => B.mass c1 x)).sum := by
      have hsplit2 := sum_filter_split (B.expand a1) (fun x => ((B.expand c1).filter (fun y => x == y)).length) (fun x => (B.expand c1).contains x)
      rw [hsplit2]
      have hz : (((B.expand a1).filter (fun x => !(B.expand c1).contains x)).map (fun x => ((B.expand c1).filter (fun y => x == y)).length)).sum = 0 := by
        apply list_sum_eq_zero; intro v hv
        rw [List.mem_map] at hv; rcases hv with ⟨x, hx, rfl⟩
        rw [List.mem_filter] at hx
        have hnmem : x ∉ B.expand c1 := by intro hm; simp [List.contains_iff_mem.mpr hm] at hx
        rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
        intro y hy; simp only [beq_iff_eq]; intro heq; subst heq; exact hnmem hy
      rw [hz, Nat.add_zero]
      congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hc : ((B.expand c1).filter (fun y => x == y)).length = (B.expand c1).count x := by
        rw [List.count_eq_length_filter]; congr 1; apply List.filter_congr; intro y hy; simp [Bool.beq_comm]
      rw [hc, ← mass_eq_count]
    rw [hpair]
  · rw [sum_map_const_nat]; simp

theorem sound_SecondMoment : SecondMoment.Goal := by
  intro a b
  -- (weighted bag (a,b) (fun x => x*x)) = ...
  -- Use weighted_eq_map_of_count? Need mass_eq_count/cover for pair.
  -- Easier to compute directly via expand.
  rw [weighted_eq_map_of_count B (a,b) _ (mass_eq_count (a,b)) (cover (a,b))]
  simp only [B, bag]
  -- sum of (x+y)^2 over x in a, y in b
  induction a with
  | nil => simp
  | cons x xs ih =>
    simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons,
      List.length_cons, Nat.succ_mul, Nat.add_mul, Nat.one_mul]
    rw [ih]
    -- (map (fun z => z*z) (b.map (x+·))).sum = b.length*x*x + 2*x*b.sum + (b.map (fun y => y*y)).sum
    have hb : ((b.map (fun y => x + y)).map (fun z => z * z)).sum =
        b.length * (x * x) + 2 * x * b.sum + (b.map (fun y => y * y)).sum := by
      have h1 : ((b.map (fun y => x + y)).map (fun z => z * z)).sum =
          (b.map (fun y => (x + y) * (x + y))).sum := by
        rw [List.map_map]; simp [Function.comp]
      rw [h1]
      have h2 : (b.map (fun y => (x + y) * (x + y))).sum =
          (b.map (fun y => x*x + 2*x*y + y*y)).sum := by
        congr 1; apply List.map_congr_left; intro y hy; omega
      rw [h2]
      -- split sum
      have h3 : ∀ (l : List Nat), (l.map (fun y => x*x + 2*x*y + y*y)).sum =
          (l.map (fun _ => x*x)).sum + (l.map (fun y => 2*x*y)).sum + (l.map (fun y => y*y)).sum := by
        intro l; induction l with
        | nil => simp
        | cons z zs ihz => simp [ihz]; omega
      rw [h3]
      have hs1 : (b.map (fun _ => x*x)).sum = b.length * (x*x) := by simp [sum_map_const_nat]
      have hs2 : (b.map (fun y => 2*x*y)).sum = 2*x*b.sum := by
        have hfac : (b.map (fun y => 2*x*y)).sum = (2*x) * b.sum := by
          induction b with
          | nil => simp
          | cons z zs ihz => simp [ihz]; omega
        rw [hfac]; omega
      rw [hs1, hs2]; omega
    rw [hb]; omega

theorem sound_ProductRevenue : ProductRevenue.Goal := by
  intro a b
  rw [weighted_eq_map_of_count B (a,b) _ (mass_eq_count (a,b)) (cover (a,b))]
  simp only [B, bag]
  induction a with
  | nil => simp
  | cons x xs ih =>
    simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons, id_eq]
    rw [ih]
    have hb : ((b.map (x + ·)).map id).sum = x * b.length + b.sum := by
      simp only [List.map_map, Function.comp, id_eq]
      induction b with
      | nil => simp
      | cons y ys ihb => simp [ihb]; omega
    rw [hb]; omega

end AdditiveProduct

/-! ## DictionaryAliases -/

namespace DictionaryAliases
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DictionaryAliases

private abbrev B : Bag (List Nat × List Nat) := bag

private theorem mass_eq_count (p : List Nat × List Nat) (k : Nat) :
    B.mass p k = (B.expand p).count k := by
  obtain ⟨d, ids⟩ := p
  simp only [B, bag]
  -- count k in filterMap (fun i => d[i]?) ids
  -- = sum over zipIdx d of if e.1==k then ids.count e.2 else 0
  induction ids generalizing d k with
  | nil => simp
  | cons i is ih =>
    simp only [List.filterMap_cons, List.count_cons]
    rw [ih d k]
    cases hget : d[i]? with
    | none =>
      simp
    | some v =>
      simp only [hget]
      by_cases hv : v == k
      · have hvk : v = k := eq_of_beq hv
        subst hvk
        simp only [hv, ite_true]
        -- need: 1 + ... = (zipIdx d filter ...).map ... sum
        -- add 1 for this occurrence of index i giving k
        have hinc : (((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => (i :: is).count e.2)).sum) =
            (((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => is.count e.2)).sum) +
            (if (d.zipIdx.filter (fun e => e.1 == k)).any (fun e => e.2 == i) then 1 else 0) := by
          -- easier to show directly by rewriting count of i::is
          have h2 : (((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => (i :: is).count e.2)).sum) =
              (((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => is.count e.2 + if e.2 == i then 1 else 0)).sum) := by
            congr 1; apply List.map_congr_left; intro e he; simp [List.count_cons]
          rw [h2]
          -- split sum
          have hsplit : ∀ (l : List (Nat × Nat)),
              (l.map (fun e => is.count e.2 + if e.2 == i then 1 else 0)).sum =
              (l.map (fun e => is.count e.2)).sum + (l.map (fun e => if e.2 == i then 1 else 0)).sum := by
            intro l; induction l with
            | nil => simp
            | cons e es ihs => simp [ihs]; omega
          rw [hsplit]
          congr 1
          -- (map (fun e => if e.2==i then 1 else 0) ...).sum = if any then 1 else 0
          have hany : ((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => if e.2 == i then 1 else 0)).sum =
              if (d.zipIdx.filter (fun e => e.1 == k)).any (fun e => e.2 == i) then 1 else 0 := by
            have hsum01 : ((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => if e.2 == i then 1 else 0)).sum =
                ((d.zipIdx.filter (fun e => e.1 == k)).filter (fun e => e.2 == i)).length := by
              rw [sum_map_if_one]
            rw [hsum01]
            by_cases hany : (d.zipIdx.filter (fun e => e.1 == k)).any (fun e => e.2 == i) = true
            · rw [hany]; simp
              rw [List.any_eq_true] at hany
              rcases hany with ⟨e, he, hee⟩
              have hmemf : e ∈ (d.zipIdx.filter (fun e => e.1 == k)).filter (fun e => e.2 == i) := by
                rw [List.mem_filter]; exact ⟨he, hee⟩
              have hnodup : (d.zipIdx.filter (fun e => e.1 == k)).Nodup := by
                apply List.Nodup.filter
                -- zipIdx is nodup in indices? zipIdx produces (elem, idx) with unique idx
                have hnd : d.zipIdx.Nodup := by
                  apply List.Nodup.of_map Prod.snd
                  simp [List.map_zipIdx_snd]
                exact hnd
              have hnodup2 := hnodup.filter (fun e => e.2 == i)
              -- any element in filtered has snd == i and fst == k, and index is i, so unique
              have hall : ∀ x ∈ (d.zipIdx.filter (fun e => e.1 == k)).filter (fun e => e.2 == i), x = e := by
                intro x hx
                rw [List.mem_filter, List.mem_filter] at hx
                have hx2 : x.2 = i := eq_of_beq hx.2
                have he2 : e.2 = i := eq_of_beq hee
                have hxidx : x.2 < d.length := by
                  have := List.mem_zipIdx hx.1.1; omega
                have heidx : e.2 < d.length := by
                  have := List.mem_zipIdx he; omega
                -- same index in zipIdx means same element
                have hx_get := List.getElem?_zipIdx (l := d) (i := x.2)
                have he_get := List.getElem?_zipIdx (l := d) (i := e.2)
                have hxi : x.2 < d.zipIdx.length := by simp [List.length_zipIdx]; omega
                have hei : e.2 < d.zipIdx.length := by simp [List.length_zipIdx]; omega
                -- use getElem of zipIdx
                have hxval : d.zipIdx[x.2]'(by simp [List.length_zipIdx]; omega) = (d[x.2], x.2) := List.getElem_zipIdx _ _
                have heval : d.zipIdx[e.2]'(by simp [List.length_zipIdx]; omega) = (d[e.2], e.2) := List.getElem_zipIdx _ _
                -- x is in zipIdx and has snd = x.2
                have hxmem : x ∈ d.zipIdx := hx.1.1
                have hemem : e ∈ d.zipIdx := he
                -- find x in zipIdx: x.1 = d[x.2]
                have hx1 : x = (d[x.2], x.2) := by
                  have := List.mem_zipIdx hxmem
                  ext <;> simp_all
                have he1 : e = (d[e.2], e.2) := by
                  have := List.mem_zipIdx hemem
                  ext <;> simp_all
                rw [hx1, he1, hx2, he2]
              match hf : (d.zipIdx.filter (fun e => e.1 == k)).filter (fun e => e.2 == i) with
              | [] => simp at hmemf
              | [x] => simp
              | x :: y :: zs =>
                have hxv := hall x (by rw [hf]; simp)
                have hyv := hall y (by rw [hf]; simp [List.mem_cons])
                subst hxv; subst hyv; rw [hf] at hnodup2; simp [List.nodup_cons] at hnodup2
            · simp at hany
              rw [hany]; simp
              rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
              intro e he
              have := hany e he
              simpa using this
          rw [hany]
          -- show any is true because (k, i) is in zipIdx when d[i]? = some k
          have hmem_ki : (k, i) ∈ d.zipIdx.filter (fun e => e.1 == k) := by
            rw [List.mem_filter]
            constructor
            · rw [List.mem_iff_getElem]
              refine ⟨i, ?_, ?_⟩
              · simp [List.length_zipIdx]; exact (List.getElem?_eq_some.mp hget).1
              · simp [List.getElem_zipIdx, (List.getElem?_eq_some.mp hget).2]
            · simp
          have hany_true : (d.zipIdx.filter (fun e => e.1 == k)).any (fun e => e.2 == i) = true := by
            rw [List.any_eq_true]; exact ⟨(k,i), hmem_ki, by simp⟩
          simp [hany_true]; omega
        rw [hinc]
        have hany_true : (d.zipIdx.filter (fun e => e.1 == k)).any (fun e => e.2 == i) = true := by
          rw [List.any_eq_true]
          refine ⟨(k, i), ?_, by simp⟩
          rw [List.mem_filter]
          constructor
          · rw [List.mem_iff_getElem]
            refine ⟨i, ?_, ?_⟩
            · simp [List.length_zipIdx]; exact (List.getElem?_eq_some.mp hget).1
            · simp [List.getElem_zipIdx, (List.getElem?_eq_some.mp hget).2]
          · simp
        simp [hany_true]
      · have hvne : v ≠ k := by intro heq; subst heq; simp at hv
        simp only [hv, ite_false]
        -- count of k in v::filterMap... is same as in filterMap...
        have hcount_v : (v :: List.filterMap (fun i => d[i]?) is).count k = (List.filterMap (fun i => d[i]?) is).count k := by
          rw [List.count_cons]; simp [hvne]
        rw [hcount_v]
        congr 1
        congr 1; apply List.map_congr_left; intro e he; simp [List.count_cons]

private theorem cover (p : List Nat × List Nat) (k : Nat) (hk : 0 < (B.expand p).count k) :
    k ∈ B.keys p := by
  obtain ⟨d, ids⟩ := p
  simp only [B, bag] at hk ⊢
  have hmem : k ∈ (ids.filterMap (fun i => d[i]?)) := List.count_pos_iff.mp hk
  rw [List.mem_filterMap] at hmem
  rcases hmem with ⟨i, hi, hget⟩
  -- k = d[i] so k in d
  exact List.mem_of_getElem? hget

private theorem mem_live_iff (p : List Nat × List Nat) (k : Nat) :
    k ∈ live B p ↔ 0 < B.mass p k := by
  unfold live; constructor
  · intro h; rw [List.mem_filter] at h; exact by simpa using h.2
  · intro h; rw [List.mem_filter]
    exact ⟨by rw [List.mem_eraseDups]; exact cover p k (by rw [← mass_eq_count]; exact h), by simpa⟩

private theorem live_nodup (p : List Nat × List Nat) : (live B p).Nodup := (nodup_eraseDups _).filter _

theorem sound_DistinctOutput : DistinctOutput.Goal := by
  intro p
  rw [List.perm_iff_count]; intro k
  by_cases hk : k ∈ live B p
  · have hk2 : k ∈ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
    rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
  · have hk2 : k ∉ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
    rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rw [weighted_eq_map_of_count B p _ (mass_eq_count p) (cover p)]
  have h : (fun x => if lo ≤ x && x < hi then x * x else 0) =
      (fun x => if decide (lo ≤ x ∧ x < hi) then x * x else 0) := by
    funext x; by_cases h1 : lo ≤ x <;> by_cases h2 : x < hi <;> simp [h1, h2]
  rw [h, map_if_sum]; congr 1; apply List.filter_congr; intro x hx; simp

theorem sound_OrderStatistic : OrderStatistic.Goal :=
  orderStatistic_of_count B (fun p k => mass_eq_count p k) (fun p k hk => cover p k hk)

private theorem pairs_length (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = ((xs.map (fun x => (ys.filter (pred x)).length)).sum) := by
  unfold pairs
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.flatMap_cons, List.length_append, ih, List.map_cons, List.sum_cons, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c slack
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hinner : ∀ x, weighted B c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat) =
      ((B.expand c).map (fun y => if x ≤ y && y ≤ x + slack then 1 else 0)).sum := by
    intro x; rw [weighted_eq_map_of_count B c _ (mass_eq_count c) (cover c)]
    congr 1; funext y; by_cases h : x ≤ y ∧ y ≤ x + slack <;> simp [h]
  simp_rw [hinner]; rw [pairs_length]
  congr 1; apply List.map_congr_left; intro x hx; rw [map_if_sum]; simp

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => if B.mass c x == 0 then x else 0)).sum =
      ((B.expand a).map (fun x => if !(B.expand c).contains x then x else 0)).sum := by
    congr 1; apply List.map_congr_left; intro x hx
    simp only [mass_eq_count, List.contains_iff_mem, beq_iff_eq]
    by_cases h : x ∈ B.expand c <;> simp [h, List.count_eq_zero_of_not_mem]
  rw [hL, map_if_sum]; simp

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro p
  have hperm : (live B p).Perm (B.expand p).eraseDups := by
    rw [List.perm_iff_count]; intro k
    by_cases hk : k ∈ live B p
    · have hk2 : k ∈ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
      rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
    · have hk2 : k ∉ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
      rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]
  rw [perm_sum_eq (hperm.map (fun k => comb (B.mass p k)))]
  have hmass : ((B.expand p).eraseDups.map (fun k => comb (B.mass p k))).sum =
      ((B.expand p).eraseDups.map (fun k => comb ((B.expand p).count k))).sum := by
    congr 1; apply List.map_congr_left; intro k hk; rw [mass_eq_count]
  rw [hmass]
  exact sum_eraseDups_comb (B.expand p)

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  have hL : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k =
      min (B.mass a k) (B.mass c k) := by
    by_cases hk : k ∈ live B a
    · have hsplit : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) =
          (((live B a).filter (· == k)).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) ++
          (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) := by
        rw [← List.flatMap_append]; congr 1
        conv_lhs => rw [← List.filter_append (fun x => x == k) (fun x => !(x == k)) (live B a)]
        congr 1; ext x; by_cases hx : x == k <;> simp [hx]
      rw [hsplit, List.count_append]
      have hzero : (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k = 0 := by
        rw [List.count_eq_zero_of_not_mem]
        intro hm; rw [List.mem_flatMap] at hm
        rcases hm with ⟨x, hx, hxk⟩
        rw [List.mem_filter] at hx; rw [List.mem_replicate] at hxk
        have hxne : x ≠ k := by intro heq; subst heq; simp at hx
        exact hxne hxk.2
      rw [hzero, Nat.add_zero]
      have hfilter : (live B a).filter (· == k) = [k] := by
        apply List.ext_getElem
        · rw [List.length_filter]
          have hle1 : ((live B a).filter (· == k)).length ≤ 1 := by
            have hnod := (live_nodup a).filter (· == k)
            have hall : ∀ x ∈ (live B a).filter (· == k), x = k := by
              intro x hx; rw [List.mem_filter] at hx; exact eq_of_beq hx.2
            match hf : (live B a).filter (· == k) with
            | [] => simp
            | [_] => simp
            | x :: y :: zs =>
              have hxv := hall x (by rw [hf]; simp)
              have hyv := hall y (by rw [hf]; simp [List.mem_cons])
              subst hxv; subst hyv; rw [hf] at hnod; simp [List.nodup_cons] at hnod
          have hge1 : 1 ≤ ((live B a).filter (· == k)).length :=
            List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hk, by simp⟩)
          omega
        · intro i hi1 hi2
          have hi0 : i = 0 := by simp at hi2; omega
          subst hi0
          have hget := List.getElem_mem hi1
          rw [List.mem_filter] at hget
          simp [eq_of_beq hget.2]
      rw [hfilter]; simp [List.count_replicate_self]
    · have hma : B.mass a k = 0 := by
        by_contra h; exact hk ((mem_live_iff a k).mpr (Nat.pos_of_ne_zero h))
      have hmin : min (B.mass a k) (B.mass c k) = 0 := by simp [hma]
      rw [hmin, List.count_eq_zero_of_not_mem]
      intro hm; rw [List.mem_flatMap] at hm
      rcases hm with ⟨x, hx, hxk⟩
      rw [List.mem_replicate] at hxk; rcases hxk with ⟨_, rfl⟩
      exact hk hx
  simp only [B]; rw [hL, mass_eq_count, mass_eq_count]

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => max 1 (B.mass c x))).sum =
      (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum +
      (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun _ => 1)).sum := by
    have hsplit := sum_filter_split (B.expand a) (fun x => max 1 (B.mass c x)) (fun x => (B.expand c).contains x)
    rw [hsplit]; congr 1
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hmem : x ∈ B.expand c := List.contains_iff_mem.mp hx.2
      have hpos : 0 < B.mass c x := by rw [mass_eq_count]; exact List.count_pos_iff.mpr hmem
      omega
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hnmem : x ∉ B.expand c := by intro hm; have := List.contains_iff_mem.mpr hm; simp_all
      have h0 : B.mass c x = 0 := by rw [mass_eq_count, List.count_eq_zero_of_not_mem hnmem]
      simp [h0]
  rw [hL]; congr 1
  · rw [pairs_length]
    have hpair : ((B.expand a).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum =
        (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum := by
      have hsplit2 := sum_filter_split (B.expand a) (fun x => ((B.expand c).filter (fun y => x == y)).length) (fun x => (B.expand c).contains x)
      rw [hsplit2]
      have hz : (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum = 0 := by
        apply list_sum_eq_zero; intro v hv
        rw [List.mem_map] at hv; rcases hv with ⟨x, hx, rfl⟩
        rw [List.mem_filter] at hx
        have hnmem : x ∉ B.expand c := by intro hm; simp [List.contains_iff_mem.mpr hm] at hx
        rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
        intro y hy; simp only [beq_iff_eq]; intro heq; subst heq; exact hnmem hy
      rw [hz, Nat.add_zero]
      congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hc : ((B.expand c).filter (fun y => x == y)).length = (B.expand c).count x := by
        rw [List.count_eq_length_filter]; congr 1; apply List.filter_congr; intro y hy; simp [Bool.beq_comm]
      rw [hc, ← mass_eq_count]
    rw [hpair]
  · rw [sum_map_const_nat]; simp

theorem sound_ManyToOne : ManyToOne.Goal := by
  intro d ids v
  -- bag.mass (d++[v], ids) v = bag.mass (d,ids) v + ids.count d.length
  simp only [B, bag]
  -- mass = ((zipIdx (d++[v])).filter (fun e => e.1==v)).map (fun e => ids.count e.2) sum
  -- zipIdx (d++[v]) = d.zipIdx ++ [(v, d.length)]
  rw [List.zipIdx_append]
  simp only [List.filter_append, List.map_append, List.sum_append]
  -- filter on the appended [(v, d.length)] gives [(v,d.length)] if v==v
  have htail : (List.filter (fun e => e.1 == v) ([(v, d.length)] : List (Nat × Nat))).map (fun e => ids.count e.2) =
      [ids.count d.length] := by simp
  have htailzip : (List.filter (fun e => e.1 == v) (([v] : List Nat).zipIdx d.length)).map (fun e => ids.count e.2) = [ids.count d.length] := by
    simp [List.zipIdx_cons]
  rw [htailzip]
  simp
  -- main part: filter/map over d.zipIdx same as before
  congr 1
  congr 1
  apply List.filter_congr
  intro e he
  rfl

theorem sound_UnusedSuffix : UnusedSuffix.Goal := by
  intro d ids ext hids
  rw [weighted_eq_map_of_count B (d ++ ext, ids) _ (mass_eq_count (d ++ ext, ids)) (cover (d ++ ext, ids))]
  rw [weighted_eq_map_of_count B (d, ids) _ (mass_eq_count (d, ids)) (cover (d, ids))]
  simp only [B, bag]
  -- expand same: filterMap (fun i => (d++ext)[i]?) ids = filterMap (fun i => d[i]?) ids
  congr 1
  congr 1
  induction ids with
  | nil => simp
  | cons i is ih =>
    simp only [List.filterMap_cons]
    have hi : i < d.length := hids i (by simp)
    rw [List.getElem?_append_left hi]
    rw [ih (fun j hj => hids j (List.mem_cons_of_mem i hj))]

end DictionaryAliases

/-! ## ExceptionOverlay -/

namespace ExceptionOverlay
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

private abbrev B : Bag (Nat × Nat × List (Nat × Nat)) := bag

private theorem mass_eq_count (p : Nat × Nat × List (Nat × Nat)) (k : Nat) :
    B.mass p k = (B.expand p).count k := by
  obtain ⟨n, d, es⟩ := p
  simp only [B, bag]
  -- expand = (range n).map (fun i => (es.lookup i).getD d)
  -- mass = (if k==d then n - ps.length else 0) + ps.countP (fun i => es.lookup i == some k)
  -- count k in expand
  have hcount : ((List.range n).map (fun i => (es.lookup i).getD d)).count k =
      (List.range n).countP (fun i => (es.lookup i).getD d == k) := by
    induction (List.range n) with
    | nil => simp
    | cons i is ih => simp [List.count_cons, List.countP_cons, ih]
  rw [hcount]
  -- split range n into positions where lookup is some _ vs none/default
  let ps := (es.map Prod.fst).eraseDups.filter (· < n)
  -- countP over range n = countP over ps (overridden) + countP over rest (default)
  have hsplit : (List.range n).countP (fun i => (es.lookup i).getD d == k) =
      ps.countP (fun i => (es.lookup i).getD d == k) +
      ((List.range n).filter (fun i => !(ps.contains i))).countP (fun i => (es.lookup i).getD d == k) := by
    have hpart : (List.range n).countP (fun i => (es.lookup i).getD d == k) =
        ((List.range n).filter (fun i => ps.contains i)).countP (fun i => (es.lookup i).getD d == k) +
        ((List.range n).filter (fun i => !(ps.contains i))).countP (fun i => (es.lookup i).getD d == k) := by
      have h := List.length_filter_add_length_filter_not (p := fun i => ps.contains i) (l := List.range n)
      -- countP p l = countP p (filter q l) + countP p (filter !q l)
      induction (List.range n) with
      | nil => simp
      | cons i is ih =>
        simp only [List.filter_cons, List.countP_cons]
        by_cases hc : ps.contains i <;> simp [hc, ih] <;> omega
    -- filter by ps.contains on range n gives ps (as set) but need to be careful
    -- easier: show (range n).filter (contains ps) = ps (up to perm/countP)
    -- Actually ps is subset of range n by definition
    have hps_sub : ∀ i ∈ ps, i ∈ List.range n := by
      intro i hi; rw [List.mem_filter] at hi; rw [List.mem_range]; exact hi.2
    have hperm : ((List.range n).filter (fun i => ps.contains i)).Perm ps := by
      rw [List.perm_iff_count]; intro i
      by_cases hip : i ∈ ps
      · have hi1 : i ∈ (List.range n).filter (fun i => ps.contains i) := by
          rw [List.mem_filter]; exact ⟨hps_sub i hip, List.contains_iff_mem.mpr hip⟩
        have hi2 : i ∈ ps := hip
        -- nodup of ps
        have hnodup_ps : ps.Nodup := (nodup_eraseDups _).filter _
        have hnodup_f : ((List.range n).filter (fun i => ps.contains i)).Nodup := (List.nodup_range n).filter _
        rw [count_eq_one_of_mem_nodup hnodup_f hi1, count_eq_one_of_mem_nodup hnodup_ps hi2]
      · have hi1 : i ∉ (List.range n).filter (fun i => ps.contains i) := by
          rw [List.mem_filter]; intro ⟨_, hc⟩; exact hip (List.contains_iff_mem.mp hc)
        rw [List.count_eq_zero_of_not_mem hi1, List.count_eq_zero_of_not_mem hip]
    rw [List.countP_eq_length_filter]
    rw [List.countP_eq_length_filter]
    have hperm2 := filter_length_perm hperm (fun i => (es.lookup i).getD d == k)
    -- need countP form
    have hc1 : ((List.range n).filter (fun i => ps.contains i)).countP (fun i => (es.lookup i).getD d == k) =
        (((List.range n).filter (fun i => ps.contains i)).filter (fun i => (es.lookup i).getD d == k)).length := by
      rw [List.countP_eq_length_filter]
    have hc2 : ps.countP (fun i => (es.lookup i).getD d == k) =
        (ps.filter (fun i => (es.lookup i).getD d == k)).length := by
      rw [List.countP_eq_length_filter]
    rw [hc1, hc2, hperm2]
    -- the rest
    have hc3 : ((List.range n).filter (fun i => !(ps.contains i))).countP (fun i => (es.lookup i).getD d == k) =
        (((List.range n).filter (fun i => !(ps.contains i))).filter (fun i => (es.lookup i).getD d == k)).length := by
      rw [List.countP_eq_length_filter]
    rw [hc3]
  rw [hsplit]
  -- for i in ps (overridden positions), lookup returns some v
  -- for i not in ps, lookup returns none so getD d = d
  have hps_count : ps.countP (fun i => (es.lookup i).getD d == k) =
      ps.countP (fun i => es.lookup i == some k) := by
    congr 1; ext i
    by_cases hlook : es.lookup i = none
    · simp [hlook]
      -- i in ps means lookup i is some
      have : i ∈ ps → False := by
        intro hip
        rw [List.mem_filter, List.mem_eraseDups, List.mem_map] at hip
        rcases hip.1 with ⟨⟨j, v⟩, hj, rfl⟩
        simp [List.lookup_eq_some] at hlook
        have := hlook v hj
        exact this
      -- but we are inside ext/countP, just show equality of bool
      simp [hlook]
    · cases hl : es.lookup i with
      | none => exact absurd hl hlook
      | some v => simp [hl]
  have hrest_count : ((List.range n).filter (fun i => !(ps.contains i))).countP (fun i => (es.lookup i).getD d == k) =
      if k == d then ((List.range n).filter (fun i => !(ps.contains i))).length else 0 := by
    have hall : ∀ i ∈ (List.range n).filter (fun i => !(ps.contains i)), (es.lookup i).getD d = d := by
      intro i hi
      rw [List.mem_filter] at hi
      have hnc : ps.contains i = false := by simpa using hi.2
      have hnotin : i ∉ ps := by intro hp; have := List.contains_iff_mem.mpr hp; simp_all
      -- i not in ps means either i >= n or no entry in es has fst i
      have hlook : es.lookup i = none := by
        cases h : es.lookup i with
        | none => rfl
        | some v =>
          exfalso
          apply hnotin
          rw [List.mem_filter, List.mem_eraseDups, List.mem_map]
          constructor
          · exact ⟨(i,v), List.mem_of_lookup_eq_some h, rfl⟩
          · rw [List.mem_range] at hi; exact hi.1
      simp [hlook]
    by_cases hkd : k == d
    · have hkd' : k = d := eq_of_beq hkd
      subst hkd'
      simp only [beq_self_eq_true, ite_true]
      rw [List.countP_eq_length_filter]
      congr 1
      rw [List.filter_eq_self]
      intro i hi
      simp [hall i hi]
    · have hkd' : k ≠ d := by intro heq; subst heq; simp at hkd
      simp only [hkd, ite_false]
      rw [List.countP_eq_length_filter, List.length_eq_zero_iff, List.filter_eq_nil_iff]
      intro i hi
      simp [hall i hi, hkd']
  rw [hps_count, hrest_count]
  -- length of rest = n - ps.length
  have hrest_len : ((List.range n).filter (fun i => !(ps.contains i))).length = n - ps.length := by
    have htotal := List.length_filter_add_length_filter_not (p := fun i => ps.contains i) (l := List.range n)
    rw [List.length_range] at htotal
    -- filter (contains ps) on range n has same length as ps
    have hperm : ((List.range n).filter (fun i => ps.contains i)).Perm ps := by
      rw [List.perm_iff_count]; intro i
      by_cases hip : i ∈ ps
      · have hi1 : i ∈ (List.range n).filter (fun i => ps.contains i) := by
          rw [List.mem_filter]
          exact ⟨by rw [List.mem_range]; exact (List.mem_filter.mp hip).2, List.contains_iff_mem.mpr hip⟩
        have hnodup_ps : ps.Nodup := (nodup_eraseDups _).filter _
        have hnodup_f : ((List.range n).filter (fun i => ps.contains i)).Nodup := (List.nodup_range n).filter _
        rw [count_eq_one_of_mem_nodup hnodup_f hi1, count_eq_one_of_mem_nodup hnodup_ps hip]
      · have hi1 : i ∉ (List.range n).filter (fun i => ps.contains i) := by
          rw [List.mem_filter]; intro ⟨_, hc⟩; exact hip (List.contains_iff_mem.mp hc)
        rw [List.count_eq_zero_of_not_mem hi1, List.count_eq_zero_of_not_mem hip]
    have hlen := hperm.length_eq
    omega
  rw [hrest_len]

private theorem cover (p : Nat × Nat × List (Nat × Nat)) (k : Nat) (hk : 0 < (B.expand p).count k) :
    k ∈ B.keys p := by
  obtain ⟨n, d, es⟩ := p
  simp only [B, bag] at hk ⊢
  have hmem : k ∈ (List.range n).map (fun i => (es.lookup i).getD d) := List.count_pos_iff.mp hk
  rw [List.mem_map] at hmem
  rcases hmem with ⟨i, hi, rfl⟩
  cases hlook : es.lookup i with
  | none => simp [List.mem_cons]
  | some v =>
    simp only [Option.getD_some, List.mem_cons, List.mem_map]
    right
    exact ⟨(i, v), List.mem_of_lookup_eq_some hlook, rfl⟩

private theorem mem_live_iff (p : Nat × Nat × List (Nat × Nat)) (k : Nat) :
    k ∈ live B p ↔ 0 < B.mass p k := by
  unfold live; constructor
  · intro h; rw [List.mem_filter] at h; exact by simpa using h.2
  · intro h; rw [List.mem_filter]
    exact ⟨by rw [List.mem_eraseDups]; exact cover p k (by rw [← mass_eq_count]; exact h), by simpa⟩

private theorem live_nodup (p : Nat × Nat × List (Nat × Nat)) : (live B p).Nodup := (nodup_eraseDups _).filter _

theorem sound_DistinctOutput : DistinctOutput.Goal := by
  intro p
  rw [List.perm_iff_count]; intro k
  by_cases hk : k ∈ live B p
  · have hk2 : k ∈ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
    rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
  · have hk2 : k ∉ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
    rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rw [weighted_eq_map_of_count B p _ (mass_eq_count p) (cover p)]
  have h : (fun x => if lo ≤ x && x < hi then x * x else 0) =
      (fun x => if decide (lo ≤ x ∧ x < hi) then x * x else 0) := by
    funext x; by_cases h1 : lo ≤ x <;> by_cases h2 : x < hi <;> simp [h1, h2]
  rw [h, map_if_sum]; congr 1; apply List.filter_congr; intro x hx; simp

theorem sound_OrderStatistic : OrderStatistic.Goal :=
  orderStatistic_of_count B (fun p k => mass_eq_count p k) (fun p k hk => cover p k hk)

private theorem pairs_length (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = ((xs.map (fun x => (ys.filter (pred x)).length)).sum) := by
  unfold pairs
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.flatMap_cons, List.length_append, ih, List.map_cons, List.sum_cons, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c slack
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hinner : ∀ x, weighted B c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat) =
      ((B.expand c).map (fun y => if x ≤ y && y ≤ x + slack then 1 else 0)).sum := by
    intro x; rw [weighted_eq_map_of_count B c _ (mass_eq_count c) (cover c)]
    congr 1; funext y; by_cases h : x ≤ y ∧ y ≤ x + slack <;> simp [h]
  simp_rw [hinner]; rw [pairs_length]
  congr 1; apply List.map_congr_left; intro x hx; rw [map_if_sum]; simp

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => if B.mass c x == 0 then x else 0)).sum =
      ((B.expand a).map (fun x => if !(B.expand c).contains x then x else 0)).sum := by
    congr 1; apply List.map_congr_left; intro x hx
    simp only [mass_eq_count, List.contains_iff_mem, beq_iff_eq]
    by_cases h : x ∈ B.expand c <;> simp [h, List.count_eq_zero_of_not_mem]
  rw [hL, map_if_sum]; simp

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro p
  have hperm : (live B p).Perm (B.expand p).eraseDups := by
    rw [List.perm_iff_count]; intro k
    by_cases hk : k ∈ live B p
    · have hk2 : k ∈ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
      rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
    · have hk2 : k ∉ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
      rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]
  rw [perm_sum_eq (hperm.map (fun k => comb (B.mass p k)))]
  have hmass : ((B.expand p).eraseDups.map (fun k => comb (B.mass p k))).sum =
      ((B.expand p).eraseDups.map (fun k => comb ((B.expand p).count k))).sum := by
    congr 1; apply List.map_congr_left; intro k hk; rw [mass_eq_count]
  rw [hmass]
  exact sum_eraseDups_comb (B.expand p)

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  have hL : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k =
      min (B.mass a k) (B.mass c k) := by
    by_cases hk : k ∈ live B a
    · have hsplit : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) =
          (((live B a).filter (· == k)).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) ++
          (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) := by
        rw [← List.flatMap_append]; congr 1
        conv_lhs => rw [← List.filter_append (fun x => x == k) (fun x => !(x == k)) (live B a)]
        congr 1; ext x; by_cases hx : x == k <;> simp [hx]
      rw [hsplit, List.count_append]
      have hzero : (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k = 0 := by
        rw [List.count_eq_zero_of_not_mem]
        intro hm; rw [List.mem_flatMap] at hm
        rcases hm with ⟨x, hx, hxk⟩
        rw [List.mem_filter] at hx; rw [List.mem_replicate] at hxk
        have hxne : x ≠ k := by intro heq; subst heq; simp at hx
        exact hxne hxk.2
      rw [hzero, Nat.add_zero]
      have hfilter : (live B a).filter (· == k) = [k] := by
        apply List.ext_getElem
        · rw [List.length_filter]
          have hle1 : ((live B a).filter (· == k)).length ≤ 1 := by
            have hnod := (live_nodup a).filter (· == k)
            have hall : ∀ x ∈ (live B a).filter (· == k), x = k := by
              intro x hx; rw [List.mem_filter] at hx; exact eq_of_beq hx.2
            match hf : (live B a).filter (· == k) with
            | [] => simp
            | [_] => simp
            | x :: y :: zs =>
              have hxv := hall x (by rw [hf]; simp)
              have hyv := hall y (by rw [hf]; simp [List.mem_cons])
              subst hxv; subst hyv; rw [hf] at hnod; simp [List.nodup_cons] at hnod
          have hge1 : 1 ≤ ((live B a).filter (· == k)).length :=
            List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hk, by simp⟩)
          omega
        · intro i hi1 hi2
          have hi0 : i = 0 := by simp at hi2; omega
          subst hi0
          have hget := List.getElem_mem hi1
          rw [List.mem_filter] at hget
          simp [eq_of_beq hget.2]
      rw [hfilter]; simp [List.count_replicate_self]
    · have hma : B.mass a k = 0 := by
        by_contra h; exact hk ((mem_live_iff a k).mpr (Nat.pos_of_ne_zero h))
      have hmin : min (B.mass a k) (B.mass c k) = 0 := by simp [hma]
      rw [hmin, List.count_eq_zero_of_not_mem]
      intro hm; rw [List.mem_flatMap] at hm
      rcases hm with ⟨x, hx, hxk⟩
      rw [List.mem_replicate] at hxk; rcases hxk with ⟨_, rfl⟩
      exact hk hx
  simp only [B]; rw [hL, mass_eq_count, mass_eq_count]

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => max 1 (B.mass c x))).sum =
      (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum +
      (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun _ => 1)).sum := by
    have hsplit := sum_filter_split (B.expand a) (fun x => max 1 (B.mass c x)) (fun x => (B.expand c).contains x)
    rw [hsplit]; congr 1
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hmem : x ∈ B.expand c := List.contains_iff_mem.mp hx.2
      have hpos : 0 < B.mass c x := by rw [mass_eq_count]; exact List.count_pos_iff.mpr hmem
      omega
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hnmem : x ∉ B.expand c := by intro hm; have := List.contains_iff_mem.mpr hm; simp_all
      have h0 : B.mass c x = 0 := by rw [mass_eq_count, List.count_eq_zero_of_not_mem hnmem]
      simp [h0]
  rw [hL]; congr 1
  · rw [pairs_length]
    have hpair : ((B.expand a).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum =
        (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum := by
      have hsplit2 := sum_filter_split (B.expand a) (fun x => ((B.expand c).filter (fun y => x == y)).length) (fun x => (B.expand c).contains x)
      rw [hsplit2]
      have hz : (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum = 0 := by
        apply list_sum_eq_zero; intro v hv
        rw [List.mem_map] at hv; rcases hv with ⟨x, hx, rfl⟩
        rw [List.mem_filter] at hx
        have hnmem : x ∉ B.expand c := by intro hm; simp [List.contains_iff_mem.mpr hm] at hx
        rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
        intro y hy; simp only [beq_iff_eq]; intro heq; subst heq; exact hnmem hy
      rw [hz, Nat.add_zero]
      congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hc : ((B.expand c).filter (fun y => x == y)).length = (B.expand c).count x := by
        rw [List.count_eq_length_filter]; congr 1; apply List.filter_congr; intro y hy; simp [Bool.beq_comm]
      rw [hc, ← mass_eq_count]
    rw [hpair]
  · rw [sum_map_const_nat]; simp

theorem sound_OverrideHead : OverrideHead.Goal := by
  intro n d es i x k hi
  simp only [B, bag]
  let old := (es.lookup i).getD d
  -- helper: mass via countP over range n
  have hmass : ∀ (es' : List (Nat × Nat)),
      (let ps := (es'.map Prod.fst).eraseDups.filter (· < n);
       (if k == d then n - ps.length else 0) + ps.countP (fun j => es'.lookup j == some k)) =
      (List.range n).countP (fun j => (es'.lookup j).getD d == k) := by
    intro es'
    -- reuse the argument from mass_eq_count: both equal count in expand
    have h1 : ((List.range n).map (fun j => (es'.lookup j).getD d)).count k =
        (List.range n).countP (fun j => (es'.lookup j).getD d == k) := by
      induction (List.range n) with
      | nil => simp
      | cons j js ih => simp [List.count_cons, List.countP_cons, ih]
    have h2 := mass_eq_count (n, d, es') k
    simp only [B, bag] at h2
    -- h2 : (let ps := ...; ...) = ((range n).map ...).count k
    rw [h1] at h2
    exact h2
  rw [hmass es, hmass ((i,x)::es)]
  -- countP difference over range n: only index i changes
  have hsplit_i : (List.range n).countP (fun j => (((i,x)::es).lookup j).getD d == k) =
      (List.range n).countP (fun j => if j == i then x == k else (es.lookup j).getD d == k) := by
    congr 1; ext j
    by_cases hji : j == i
    · have hji' : j = i := eq_of_beq hji
      subst hji'; simp [List.lookup_cons]
    · have hji' : j ≠ i := by intro heq; subst heq; simp at hji
      have hbeq : (j == i) = false := beq_eq_false_iff_ne.mpr hji'
      simp [List.lookup_cons, hbeq]
  have hsplit_old : (List.range n).countP (fun j => (es.lookup j).getD d == k) =
      (List.range n).countP (fun j => if j == i then old == k else (es.lookup j).getD d == k) := by
    congr 1; ext j
    by_cases hji : j == i
    · have hji' : j = i := eq_of_beq hji
      subst hji'; simp [old]
    · have hbeq : (j == i) = false := beq_eq_false_iff_ne.mpr (by intro heq; subst heq; simp at hji)
      simp [hbeq]
  rw [hsplit_i, hsplit_old]
  -- now both are countP over same base with one index different
  have hcount_diff : ∀ (v1 v2 : Nat),
      (List.range n).countP (fun j => if j == i then v1 == k else (es.lookup j).getD d == k) +
      (decide (v2 == k)).toNat =
      (List.range n).countP (fun j => if j == i then v2 == k else (es.lookup j).getD d == k) +
      (decide (v1 == k)).toNat := by
    intro v1 v2
    -- split range n into {i} and rest
    have hi_mem : i ∈ List.range n := List.mem_range.mpr hi
    have hperm : (List.range n).Perm (i :: (List.range n).filter (· != i)) := by
      have hsplit := (List.filter_cons_perm (fun j => j != i) (List.range n)).symm
      -- simpler: use perm_iff_count
      rw [List.perm_iff_count]; intro j
      by_cases hji : j = i
      · subst hji
        have h1 : (i :: (List.range n).filter (· != i)).count i = 1 := by
          rw [List.count_cons_self]
          have : (List.filter (fun j => j != i) (List.range n)).count i = 0 := by
            rw [List.count_eq_zero_of_not_mem]
            rw [List.mem_filter]; intro ⟨_, h⟩; simp at h
          omega
        rw [h1, count_eq_one_of_mem_nodup (List.nodup_range n) hi_mem]
      · have h1 : (i :: (List.range n).filter (· != i)).count j = (List.range n).count j := by
          rw [List.count_cons]
          have hne : (i == j) = false := beq_eq_false_iff_ne.mpr hji
          simp [hne]
          by_cases hjm : j ∈ List.range n
          · have hjf : j ∈ (List.range n).filter (· != i) := by
              rw [List.mem_filter]; simp [hjm, bne_iff_ne, Ne.symm hji]
            rw [count_eq_one_of_mem_nodup ((List.nodup_range n).filter _) hjf,
                count_eq_one_of_mem_nodup (List.nodup_range n) hjm]
          · rw [List.count_eq_zero_of_not_mem hjm, List.count_eq_zero_of_not_mem]
            rw [List.mem_filter]; intro ⟨h, _⟩; exact hjm h
        exact h1
    have hc1 : (List.range n).countP (fun j => if j == i then v1 == k else (es.lookup j).getD d == k) =
        (i :: (List.range n).filter (· != i)).countP (fun j => if j == i then v1 == k else (es.lookup j).getD d == k) := by
      rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
      exact filter_length_perm hperm _
    have hc2 : (List.range n).countP (fun j => if j == i then v2 == k else (es.lookup j).getD d == k) =
        (i :: (List.range n).filter (· != i)).countP (fun j => if j == i then v2 == k else (es.lookup j).getD d == k) := by
      rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
      exact filter_length_perm hperm _
    rw [hc1, hc2]
    simp only [List.countP_cons, beq_self_eq_true, ite_true]
    -- rest: for j != i, both sides same
    have hrest : ((List.range n).filter (· != i)).countP (fun j => if j == i then v1 == k else (es.lookup j).getD d == k) =
        ((List.range n).filter (· != i)).countP (fun j => if j == i then v2 == k else (es.lookup j).getD d == k) := by
      congr 1; ext j
      by_cases hji : j == i
      · subst hji
        -- j can't be in filter (· != i)
        simp
      · have hbeq : (j == i) = false := beq_eq_false_iff_ne.mpr hji
        simp [hbeq]
    rw [hrest]
    -- (if v1==k then 1 else 0) + rest + (decide (v2==k)).toNat = (if v2==k then 1 else 0) + rest + (decide (v1==k)).toNat
    simp only [decide_eq_true_eq]
    have hv1 : (if (v1 == k) = true then 1 else 0) = (if v1 == k then 1 else 0) := by rfl
    have hv2 : (if (v2 == k) = true then 1 else 0) = (if v2 == k then 1 else 0) := by rfl
    -- normalize decide to Bool
    simp only [Bool.toNat_true, Bool.toNat_false]
    by_cases hk1 : v1 == k <;> by_cases hk2 : v2 == k <;> simp [hk1, hk2] <;> omega
  have := hcount_diff x old
  -- goal: countP_new + decide(old==k).toNat = countP_old + decide(x==k).toNat
  -- hcount_diff gives: countP (if j==i then x else ...) + decide(old==k).toNat = countP (if j==i then old else ...) + decide(x==k).toNat
  -- but wait, our hsplit_i gave countP with if j==i then x==k else ..., and hsplit_old with old==k. Yes.
  -- hcount_diff v1 v2 : countP(if j==i then v1==k else ...) + decide(v2==k).toNat = countP(if j==i then v2==k else ...) + decide(v1==k).toNat
  -- we want: countP(x) + decide(old==k).toNat = countP(old) + decide(x==k).toNat => v1=x, v2=old gives it.
  exact this

theorem sound_IgnoreTail : IgnoreTail.Goal := by
  intro n d es i v hni
  rw [weighted_eq_map_of_count B (n,d,es++[(i,v)]) _ (mass_eq_count (n,d,es++[(i,v)])) (cover (n,d,es++[(i,v)]))]
  rw [weighted_eq_map_of_count B (n,d,es) _ (mass_eq_count (n,d,es)) (cover (n,d,es))]
  simp only [B, bag]
  congr 1
  apply List.map_congr_left
  intro j hj
  rw [List.mem_range] at hj
  -- lookup in es++[(i,v)] same as es for j < n <= i
  have hlook : (es ++ [(i,v)]).lookup j = es.lookup j := by
    apply List.lookup_append_left
    intro hmem
    rw [List.mem_singleton] at hmem
    have : j = i := hmem.1
    omega
  simp [hlook]

end ExceptionOverlay

/-! ## RotationCycle -/

namespace RotationCycle
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

private abbrev B : Bag (Nat × Nat × Nat) := bag

private theorem mass_eq_count (p : Nat × Nat × Nat) (k : Nat) :
    B.mass p k = (B.expand p).count k := by
  obtain ⟨n, m, s⟩ := p
  simp only [B, bag]
  -- expand = (range n).map (fun i => (s+i)%(m+1))
  -- mass = if k<=m then n/(m+1) + decide(...) else 0
  by_cases hkm : k ≤ m
  · simp only [hkm, ite_true]
    -- count how many i in range n have (s+i)%(m+1) == k
    have hcount : ((List.range n).map (fun i => (s + i) % (m + 1))).count k =
        (List.range n).countP (fun i => (s + i) % (m + 1) == k) := by
      induction (List.range n) with
      | nil => simp
      | cons i is ih => simp [List.count_cons, List.countP_cons, ih]
    rw [hcount]
    -- This is a modular counting argument. Prove by showing each full cycle gives 1.
    -- Use induction on n / quotient-remainder by (m+1).
    have hmod : m + 1 > 0 := Nat.succ_pos m
    -- characterize when (s+i)%(m+1) == k
    have hcond : ∀ i, ((s + i) % (m + 1) == k) = decide ((k + (m+1) - s % (m+1)) % (m+1) == i % (m+1)) := by
      intro i
      -- both are equivalent to i ≡ (k - s) mod (m+1)
      by_cases h : (s + i) % (m + 1) == k
      · have heq : (s + i) % (m + 1) = k := eq_of_beq h
        symm; simp only [decide_eq_true_eq]
        -- show (k + (m+1) - s%(m+1))%(m+1) == i%(m+1)
        have h1 : (s + i) % (m + 1) = k := heq
        -- derive i % (m+1) = (k + (m+1) - s%(m+1)) % (m+1)
        omega
      · symm; simp only [decide_eq_false_iff_not]
        intro heq
        apply h
        rw [beq_iff_eq]
        have : (s + i) % (m + 1) = k := by omega
        exact this
    simp_rw [hcond]
    -- count i in range n where i%(m+1) == target
    let t := (k + (m + 1) - s % (m + 1)) % (m + 1)
    have ht : t < m + 1 := Nat.mod_lt _ hmod
    -- countP (fun i => decide (t == i%(m+1))) (range n) = n/(m+1) + if t < n%(m+1) then 1 else 0
    have hcount_t : (List.range n).countP (fun i => decide (t == i % (m + 1))) =
        n / (m + 1) + if t < n % (m + 1) then 1 else 0 := by
      -- prove by induction on n
      suffices H : ∀ q r, r < m + 1 → (List.range (q * (m+1) + r)).countP (fun i => decide (t == i % (m+1))) =
          q + if t < r then 1 else 0 by
        have := H (n / (m+1)) (n % (m+1)) (Nat.mod_lt n hmod)
        have hn : n / (m+1) * (m+1) + n % (m+1) = n := Nat.div_add_mod n (m+1)
        rw [hn] at this; exact this
      intro q r hr
      induction q with
      | zero => simp
        -- countP in range r
        have : (List.range r).countP (fun i => decide (t == i % (m + 1))) = if t < r then 1 else 0 := by
          -- t < m+1, r < m+1, and for i < r < m+1, i%(m+1)=i
          by_cases htr : t < r
          · have hmem : t ∈ List.range r := List.mem_range.mpr htr
            have hcount1 : (List.range r).countP (fun i => decide (t == i % (m + 1))) = 1 := by
              rw [List.countP_eq_length_filter]
              have hfilter : (List.range r).filter (fun i => decide (t == i % (m + 1))) = [t] := by
                apply List.ext_getElem
                · rw [List.length_filter]
                  have hle1 : ((List.range r).filter (fun i => decide (t == i % (m + 1)))).length ≤ 1 := by
                    have hall : ∀ x ∈ (List.range r).filter (fun i => decide (t == i % (m + 1))), x = t := by
                      intro x hx
                      rw [List.mem_filter, List.mem_range] at hx
                      have heq := of_decide_eq_true hx.2
                      have hxmod : x % (m+1) = x := Nat.mod_eq_of_lt (by omega)
                      omega
                    match hf : (List.range r).filter (fun i => decide (t == i % (m + 1))) with
                    | [] => simp
                    | [_] => simp
                    | x :: y :: zs =>
                      have := hall x (by rw [hf]; simp); have := hall y (by rw [hf]; simp [List.mem_cons])
                      subst_vars; rw [hf]; simp [List.nodup_cons] at *; have hnd := (List.nodup_range r).filter (fun i => decide (t == i % (m + 1))); rw [hf] at hnd; simp [List.nodup_cons] at hnd
                  have hge1 : 1 ≤ ((List.range r).filter (fun i => decide (t == i % (m + 1)))).length :=
                    List.length_pos_of_mem (by rw [List.mem_filter, List.mem_range]; simp [htr, Nat.mod_eq_of_lt ht])
                  omega
                · intro i hi1 hi2
                  have hi0 : i = 0 := by simp at hi2; omega
                  subst hi0
                  have hget := List.getElem_mem hi1
                  rw [List.mem_filter] at hget
                  have heq := of_decide_eq_true hget.2
                  rw [List.mem_range] at hget
                  have hmodx : (List.filter (fun i => decide (t == i % (m + 1))) (List.range r))[0]'hi1 % (m+1) = (List.filter (fun i => decide (t == i % (m + 1))) (List.range r))[0]'hi1 := Nat.mod_eq_of_lt (by omega)
                  simp; omega
              rw [hfilter]; simp
            simp [htr, hcount1]
          · push_neg at htr
            have hcount0 : (List.range r).countP (fun i => decide (t == i % (m + 1))) = 0 := by
              rw [List.countP_eq_length_filter, List.length_eq_zero_iff, List.filter_eq_nil_iff]
              intro i hi
              rw [List.mem_range] at hi
              simp only [decide_eq_true_eq]
              intro heq
              have hmodi : i % (m+1) = i := Nat.mod_eq_of_lt (by omega)
              omega
            simp [htr, hcount0]
        simp [this]
      | succ q ih =>
        -- range ((q+1)*(m+1)+r) = range (q*(m+1)+r) ++ range' (q*(m+1)+r) (m+1)? easier: split as range(q*(m+1)+r) ++ map (+ (q*(m+1)+r)) (range (m+1))? No, better split differently.
        -- (q+1)*(m+1)+r = q*(m+1)+r + (m+1)
        rw [show (q + 1) * (m + 1) + r = (q * (m + 1) + r) + (m + 1) by omega]
        rw [List.range_add, List.countP_append, ih]
        -- countP on the shifted range (m+1) gives 1
        have hsecond : (List.map (· + (q * (m + 1) + r)) (List.range (m + 1))).countP (fun i => decide (t == i % (m + 1))) = 1 := by
          -- same as range (m+1) shifted by multiple of (m+1)? no, shifted by q*(m+1)+r
          -- i % (m+1) cycles over 0..m, so exactly one match
          have hmap : (List.map (· + (q * (m + 1) + r)) (List.range (m + 1))).countP (fun i => decide (t == i % (m + 1))) =
              (List.range (m + 1)).countP (fun j => decide (t == (j + (q * (m + 1) + r)) % (m + 1))) := by
            induction (List.range (m+1)) with
            | nil => simp
            | cons j js ihj => simp [List.countP_cons, ihj]
          rw [hmap]
          -- (j + offset) % (m+1) cycles through all residues
          have hcycle : (List.range (m + 1)).countP (fun j => decide (t == (j + (q * (m + 1) + r)) % (m + 1))) = 1 := by
            -- bijection on residues
            have hmem : ∃ j ∈ List.range (m+1), t == (j + (q * (m + 1) + r)) % (m + 1) := by
              -- find j with (j + (q*(m+1)+r)) % (m+1) = t
              let j := (t + (m+1) - (r % (m+1))) % (m+1)
              refine ⟨j, List.mem_range.mpr (Nat.mod_lt _ hmod), ?_⟩
              simp only [beq_iff_eq]
              omega
            rcases hmem with ⟨j, hj, hjt⟩
            rw [List.countP_eq_length_filter]
            have hfilter : (List.range (m+1)).filter (fun j => decide (t == (j + (q * (m + 1) + r)) % (m + 1))) = [j] := by
              apply List.ext_getElem
              · rw [List.length_filter]
                have hle1 : ((List.range (m+1)).filter (fun j => decide (t == (j + (q * (m + 1) + r)) % (m + 1)))).length ≤ 1 := by
                  have hall : ∀ x ∈ (List.range (m+1)).filter (fun j => decide (t == (j + (q * (m + 1) + r)) % (m + 1))), x = j := by
                    intro x hx
                    rw [List.mem_filter, List.mem_range] at hx
                    have hxeq := of_decide_eq_true hx.2
                    have hjeq := eq_of_beq hjt
                    omega
                  match hf : (List.range (m+1)).filter (fun j => decide (t == (j + (q * (m + 1) + r)) % (m + 1))) with
                  | [] => simp
                  | [_] => simp
                  | a :: b :: cs =>
                    have := hall a (by rw [hf]; simp); have := hall b (by rw [hf]; simp [List.mem_cons])
                    subst_vars; rw [hf]; have hnd := (List.nodup_range (m+1)).filter (fun j => decide (t == (j + (q * (m + 1) + r)) % (m + 1))); rw [hf] at hnd; simp [List.nodup_cons] at hnd
                have hge1 : 1 ≤ ((List.range (m+1)).filter (fun j => decide (t == (j + (q * (m + 1) + r)) % (m + 1)))).length :=
                  List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hj, hjt⟩)
                omega
              · intro i hi1 hi2
                have hi0 : i = 0 := by simp at hi2; omega
                subst hi0
                have hget := List.getElem_mem hi1
                rw [List.mem_filter] at hget
                have heq := of_decide_eq_true hget.2
                have hjeq := eq_of_beq hjt
                rw [List.mem_range] at hget
                simp; omega
            rw [hfilter]; simp
          exact hcycle
        rw [hsecond]; omega
    rw [hcount_t]
    simp [t]
  · simp only [hkm, ite_false]
    rw [List.count_eq_zero_of_not_mem]
    rw [List.mem_map]
    intro ⟨i, hi, heq⟩
    have hmod : (s + i) % (m + 1) ≤ m := Nat.lt_succ_iff.mp (Nat.mod_lt _ (Nat.succ_pos m))
    omega

private theorem cover (p : Nat × Nat × Nat) (k : Nat) (hk : 0 < (B.expand p).count k) :
    k ∈ B.keys p := by
  obtain ⟨n, m, s⟩ := p
  simp only [B, bag] at hk ⊢
  have hmem : k ∈ (List.range n).map (fun i => (s + i) % (m + 1)) := List.count_pos_iff.mp hk
  rw [List.mem_map] at hmem
  rcases hmem with ⟨i, hi, rfl⟩
  rw [List.mem_range]
  exact Nat.lt_succ_iff.mpr (Nat.lt_succ_iff.mp (Nat.mod_lt _ (Nat.succ_pos m)))

private theorem mem_live_iff (p : Nat × Nat × Nat) (k : Nat) :
    k ∈ live B p ↔ 0 < B.mass p k := by
  unfold live; constructor
  · intro h; rw [List.mem_filter] at h; exact by simpa using h.2
  · intro h; rw [List.mem_filter]
    exact ⟨by rw [List.mem_eraseDups]; exact cover p k (by rw [← mass_eq_count]; exact h), by simpa⟩

private theorem live_nodup (p : Nat × Nat × Nat) : (live B p).Nodup := (nodup_eraseDups _).filter _

theorem sound_DistinctOutput : DistinctOutput.Goal := by
  intro p
  rw [List.perm_iff_count]; intro k
  by_cases hk : k ∈ live B p
  · have hk2 : k ∈ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
    rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
  · have hk2 : k ∉ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
    rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rw [weighted_eq_map_of_count B p _ (mass_eq_count p) (cover p)]
  have h : (fun x => if lo ≤ x && x < hi then x * x else 0) =
      (fun x => if decide (lo ≤ x ∧ x < hi) then x * x else 0) := by
    funext x; by_cases h1 : lo ≤ x <;> by_cases h2 : x < hi <;> simp [h1, h2]
  rw [h, map_if_sum]; congr 1; apply List.filter_congr; intro x hx; simp

theorem sound_OrderStatistic : OrderStatistic.Goal :=
  orderStatistic_of_count B (fun p k => mass_eq_count p k) (fun p k hk => cover p k hk)

private theorem pairs_length (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = ((xs.map (fun x => (ys.filter (pred x)).length)).sum) := by
  unfold pairs
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.flatMap_cons, List.length_append, ih, List.map_cons, List.sum_cons, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c slack
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hinner : ∀ x, weighted B c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat) =
      ((B.expand c).map (fun y => if x ≤ y && y ≤ x + slack then 1 else 0)).sum := by
    intro x; rw [weighted_eq_map_of_count B c _ (mass_eq_count c) (cover c)]
    congr 1; funext y; by_cases h : x ≤ y ∧ y ≤ x + slack <;> simp [h]
  simp_rw [hinner]; rw [pairs_length]
  congr 1; apply List.map_congr_left; intro x hx; rw [map_if_sum]; simp

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => if B.mass c x == 0 then x else 0)).sum =
      ((B.expand a).map (fun x => if !(B.expand c).contains x then x else 0)).sum := by
    congr 1; apply List.map_congr_left; intro x hx
    simp only [mass_eq_count, List.contains_iff_mem, beq_iff_eq]
    by_cases h : x ∈ B.expand c <;> simp [h, List.count_eq_zero_of_not_mem]
  rw [hL, map_if_sum]; simp

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro p
  have hperm : (live B p).Perm (B.expand p).eraseDups := by
    rw [List.perm_iff_count]; intro k
    by_cases hk : k ∈ live B p
    · have hk2 : k ∈ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
      rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
    · have hk2 : k ∉ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
      rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]
  rw [perm_sum_eq (hperm.map (fun k => comb (B.mass p k)))]
  have hmass : ((B.expand p).eraseDups.map (fun k => comb (B.mass p k))).sum =
      ((B.expand p).eraseDups.map (fun k => comb ((B.expand p).count k))).sum := by
    congr 1; apply List.map_congr_left; intro k hk; rw [mass_eq_count]
  rw [hmass]
  exact sum_eraseDups_comb (B.expand p)

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  have hL : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k =
      min (B.mass a k) (B.mass c k) := by
    by_cases hk : k ∈ live B a
    · have hsplit : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) =
          (((live B a).filter (· == k)).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) ++
          (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) := by
        rw [← List.flatMap_append]; congr 1
        conv_lhs => rw [← List.filter_append (fun x => x == k) (fun x => !(x == k)) (live B a)]
        congr 1; ext x; by_cases hx : x == k <;> simp [hx]
      rw [hsplit, List.count_append]
      have hzero : (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k = 0 := by
        rw [List.count_eq_zero_of_not_mem]
        intro hm; rw [List.mem_flatMap] at hm
        rcases hm with ⟨x, hx, hxk⟩
        rw [List.mem_filter] at hx; rw [List.mem_replicate] at hxk
        have hxne : x ≠ k := by intro heq; subst heq; simp at hx
        exact hxne hxk.2
      rw [hzero, Nat.add_zero]
      have hfilter : (live B a).filter (· == k) = [k] := by
        apply List.ext_getElem
        · rw [List.length_filter]
          have hle1 : ((live B a).filter (· == k)).length ≤ 1 := by
            have hnod := (live_nodup a).filter (· == k)
            have hall : ∀ x ∈ (live B a).filter (· == k), x = k := by
              intro x hx; rw [List.mem_filter] at hx; exact eq_of_beq hx.2
            match hf : (live B a).filter (· == k) with
            | [] => simp
            | [_] => simp
            | x :: y :: zs =>
              have hxv := hall x (by rw [hf]; simp)
              have hyv := hall y (by rw [hf]; simp [List.mem_cons])
              subst hxv; subst hyv; rw [hf] at hnod; simp [List.nodup_cons] at hnod
          have hge1 : 1 ≤ ((live B a).filter (· == k)).length :=
            List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hk, by simp⟩)
          omega
        · intro i hi1 hi2
          have hi0 : i = 0 := by simp at hi2; omega
          subst hi0
          have hget := List.getElem_mem hi1
          rw [List.mem_filter] at hget
          simp [eq_of_beq hget.2]
      rw [hfilter]; simp [List.count_replicate_self]
    · have hma : B.mass a k = 0 := by
        by_contra h; exact hk ((mem_live_iff a k).mpr (Nat.pos_of_ne_zero h))
      have hmin : min (B.mass a k) (B.mass c k) = 0 := by simp [hma]
      rw [hmin, List.count_eq_zero_of_not_mem]
      intro hm; rw [List.mem_flatMap] at hm
      rcases hm with ⟨x, hx, hxk⟩
      rw [List.mem_replicate] at hxk; rcases hxk with ⟨_, rfl⟩
      exact hk hx
  simp only [B]; rw [hL, mass_eq_count, mass_eq_count]

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => max 1 (B.mass c x))).sum =
      (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum +
      (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun _ => 1)).sum := by
    have hsplit := sum_filter_split (B.expand a) (fun x => max 1 (B.mass c x)) (fun x => (B.expand c).contains x)
    rw [hsplit]; congr 1
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hmem : x ∈ B.expand c := List.contains_iff_mem.mp hx.2
      have hpos : 0 < B.mass c x := by rw [mass_eq_count]; exact List.count_pos_iff.mpr hmem
      omega
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hnmem : x ∉ B.expand c := by intro hm; have := List.contains_iff_mem.mpr hm; simp_all
      have h0 : B.mass c x = 0 := by rw [mass_eq_count, List.count_eq_zero_of_not_mem hnmem]
      simp [h0]
  rw [hL]; congr 1
  · rw [pairs_length]
    have hpair : ((B.expand a).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum =
        (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum := by
      have hsplit2 := sum_filter_split (B.expand a) (fun x => ((B.expand c).filter (fun y => x == y)).length) (fun x => (B.expand c).contains x)
      rw [hsplit2]
      have hz : (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum = 0 := by
        apply list_sum_eq_zero; intro v hv
        rw [List.mem_map] at hv; rcases hv with ⟨x, hx, rfl⟩
        rw [List.mem_filter] at hx
        have hnmem : x ∉ B.expand c := by intro hm; simp [List.contains_iff_mem.mpr hm] at hx
        rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
        intro y hy; simp only [beq_iff_eq]; intro heq; subst heq; exact hnmem hy
      rw [hz, Nat.add_zero]
      congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hc : ((B.expand c).filter (fun y => x == y)).length = (B.expand c).count x := by
        rw [List.count_eq_length_filter]; congr 1; apply List.filter_congr; intro y hy; simp [Bool.beq_comm]
      rw [hc, ← mass_eq_count]
    rw [hpair]
  · rw [sum_map_const_nat]; simp

theorem sound_RangePerCycle : RangePerCycle.Goal := by
  intro m k s lo hi hlo hhi
  rw [weighted_eq_map_of_count B (k*(m+1),m,s) _ (mass_eq_count (k*(m+1),m,s)) (cover (k*(m+1),m,s))]
  simp only [B, bag]
  have hmod : m + 1 > 0 := Nat.succ_pos m
  -- show mass of each v is k when in full cycles
  have hmass_full : ∀ v, v ≤ m → B.mass (k*(m+1), m, s) v = k := by
    intro v hv
    rw [mass_eq_count]
    simp only [B, bag]
    -- count v in expand = countP
    have hcount : ((List.range (k * (m + 1))).map (fun i => (s + i) % (m + 1))).count v =
        (List.range (k * (m + 1))).countP (fun i => (s + i) % (m + 1) == v) := by
      induction (List.range (k * (m + 1))) with
      | nil => simp
      | cons i is ih => simp [List.count_cons, List.countP_cons, ih]
    rw [hcount]
    -- each cycle of m+1 gives exactly 1
    have hcycle : ∀ q, (List.range (q * (m + 1))).countP (fun i => (s + i) % (m + 1) == v) = q := by
      intro q
      induction q with
      | zero => simp
      | succ q ih =>
        rw [show (q + 1) * (m + 1) = q * (m + 1) + (m + 1) by omega]
        rw [List.range_add, List.countP_append, ih]
        have hsec : (List.map (· + q * (m + 1)) (List.range (m + 1))).countP (fun i => (s + i) % (m + 1) == v) = 1 := by
          have hmap : (List.map (· + q * (m + 1)) (List.range (m + 1))).countP (fun i => (s + i) % (m + 1) == v) =
              (List.range (m + 1)).countP (fun j => (s + (j + q * (m + 1))) % (m + 1) == v) := by
            induction (List.range (m+1)) with
            | nil => simp
            | cons j js ihj => simp [List.countP_cons, ihj]
          rw [hmap]
          rw [List.countP_eq_length_filter]
          have hfind : ∃ j ∈ List.range (m+1), ((s + (j + q * (m + 1))) % (m + 1) == v) = true := by
            let j := (v + (m+1) - (s + q * (m+1)) % (m+1)) % (m+1)
            refine ⟨j, List.mem_range.mpr (Nat.mod_lt _ hmod), ?_⟩
            simp only [beq_iff_eq]; omega
          rcases hfind with ⟨j, hj, hjv⟩
          have huniq : (List.range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == v) = [j] := by
            apply List.ext_getElem
            · have hle1 : ((List.range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == v)).length ≤ 1 := by
                have hall : ∀ x ∈ (List.range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == v), x = j := by
                  intro x hx; rw [List.mem_filter, List.mem_range] at hx
                  have hxeq := eq_of_beq hx.2
                  have hjeq := eq_of_beq hjv
                  omega
                match hf : (List.range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == v) with
                | [] => simp
                | [_] => simp
                | a :: b :: cs =>
                  have ha := hall a (by rw [hf]; simp); have hb := hall b (by rw [hf]; simp [List.mem_cons])
                  subst ha; subst hb
                  have hnd := (List.nodup_range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == v)
                  rw [hf] at hnd; simp [List.nodup_cons] at hnd
              have hge1 : 1 ≤ ((List.range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == v)).length :=
                List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hj, hjv⟩)
              omega
            · intro i hi1 hi2
              have hi0 : i = 0 := by simp at hi2; omega; subst hi0
              have hget := List.getElem_mem hi1
              rw [List.mem_filter, List.mem_range] at hget
              have hxeq := eq_of_beq hget.2
              have hjeq := eq_of_beq hjv
              simp; omega
          rw [huniq]; simp
        rw [hsec]; omega
    exact hcycle k
  -- now compute weighted as sum over expand
  -- alternatively use mass directly: weighted = sum_{v in keys} mass(v) * f(v)
  -- keys = range(m+1), mass of v is k if v<=m else 0
  -- easier: use the expand form and count
  have hsum_expand : (((List.range (k * (m + 1))).map (fun i => (s + i) % (m + 1))).map
      (fun v => (decide (lo ≤ v ∧ v < hi)).toNat)).sum = k * (hi - lo) := by
    -- use weighted_eq_map_of_count in reverse? No, already have it. Alternatively compute via mass.
    -- rewrite as sum over keys
    have hw := weighted_eq_map_of_count B (k*(m+1), m, s) (fun v => (decide (lo ≤ v ∧ v < hi)).toNat)
      (mass_eq_count (k*(m+1),m,s)) (cover (k*(m+1),m,s))
    -- hw : weighted B (k*(m+1),m,s) f = ((B.expand (k*(m+1),m,s)).map f).sum
    -- Also weighted = sum over keys.eraseDups of mass*f
    -- keys = range(m+1)
    have hw2 : weighted B (k*(m+1), m, s) (fun v => (decide (lo ≤ v ∧ v < hi)).toNat) =
        ((List.range (m+1)).eraseDups.map (fun v => B.mass (k*(m+1),m,s) v * (decide (lo ≤ v ∧ v < hi)).toNat)).sum := by
      simp [weighted, B, bag]
    rw [← hw]
    rw [hw2]
    have herase : (List.range (m+1)).eraseDups = List.range (m+1) := List.eraseDups_eq_self.mpr (List.nodup_range _)
    rw [herase]
    -- each v in range(m+1) has mass k
    have hterm : ((List.range (m+1)).map (fun v => B.mass (k*(m+1),m,s) v * (decide (lo ≤ v ∧ v < hi)).toNat)).sum =
        ((List.range (m+1)).map (fun v => k * (decide (lo ≤ v ∧ v < hi)).toNat)).sum := by
      congr 1; apply List.map_congr_left; intro v hv
      rw [List.mem_range] at hv
      rw [hmass_full v (Nat.lt_succ_iff.mp hv)]
    rw [hterm]
    -- factor out k
    have hfactor : ((List.range (m+1)).map (fun v => k * (decide (lo ≤ v ∧ v < hi)).toNat)).sum =
        k * ((List.range (m+1)).map (fun v => (decide (lo ≤ v ∧ v < hi)).toNat)).sum := by
      induction (List.range (m+1)) with
      | nil => simp
      | cons v vs ih => simp [ih, Nat.mul_add]
    rw [hfactor]
    congr 1
    -- sum of indicator on range(m+1) for [lo,hi) is hi-lo
    have hind : ((List.range (m+1)).map (fun v => (decide (lo ≤ v ∧ v < hi)).toNat)).sum = hi - lo := by
      have h1 : ((List.range (m+1)).map (fun v => (decide (lo ≤ v ∧ v < hi)).toNat)).sum =
          ((List.range (m+1)).filter (fun v => decide (lo ≤ v ∧ v < hi))).length := by
        have h2 : ((List.range (m+1)).map (fun v => (decide (lo ≤ v ∧ v < hi)).toNat)).sum =
            ((List.range (m+1)).map (fun v => if decide (lo ≤ v ∧ v < hi) then 1 else 0)).sum := by
          congr 1; apply List.map_congr_left; intro v hv; by_cases h : decide (lo ≤ v ∧ v < hi) = true <;> simp [h]
        rw [h2, sum_map_if_one]
      rw [h1]
      -- filter range(m+1) by lo<=v<hi
      have hfilter : (List.range (m+1)).filter (fun v => decide (lo ≤ v ∧ v < hi)) = List.range' lo (hi - lo) := by
        apply List.ext_getElem
        · simp only [List.length_range']
          rw [List.length_filter]
          -- count via range' decomposition
          have hsub : ∀ n, ((List.range n).filter (fun v => decide (lo ≤ v ∧ v < hi))).length =
              if n ≤ lo then 0 else if n ≤ hi then n - lo else hi - lo := by
            intro n
            induction n with
            | zero => simp; omega
            | succ n ih =>
              rw [List.range_succ, List.filter_append, List.length_append]
              simp only [List.filter, List.length_cons, List.length_nil]
              have ih' := ih
              by_cases hnlo : n < lo
              · have hdec : decide (lo ≤ n ∧ n < hi) = false := by simp; omega
                simp [hdec, ih']; omega
              · push_neg at hnlo
                by_cases hnhi : n < hi
                · have hdec : decide (lo ≤ n ∧ n < hi) = true := by simp; omega
                  simp [hdec, ih']; omega
                · push_neg at hnhi
                  have hdec : decide (lo ≤ n ∧ n < hi) = false := by simp; omega
                  simp [hdec, ih']; omega
          have := hsub (m+1)
          simp at this
          omega
        · intro i hi1 hi2
          simp only [List.length_range'] at hi2
          rw [List.getElem_filter]
          simp [List.getElem_range, List.getElem_range']
          -- the i-th element satisfying the condition in range(m+1) is lo+i
          -- prove by: elements are lo, lo+1, ..., hi-1
          have hmem_range' : lo + i ∈ List.range (m+1) := by rw [List.mem_range]; omega
          have hcond : decide (lo ≤ lo + i ∧ lo + i < hi) = true := by simp; omega
          -- the filtered list in order is lo, lo+1, ...
          -- use nth of filter
          have hlist : (List.range (m+1)).filter (fun v => decide (lo ≤ v ∧ v < hi)) = List.range' lo (hi - lo) := by
            apply List.ext_getElem
            · simp only [List.length_range']
              rw [List.length_filter]
              have hsub : ∀ n, ((List.range n).filter (fun v => decide (lo ≤ v ∧ v < hi))).length =
                  if n ≤ lo then 0 else if n ≤ hi then n - lo else hi - lo := by
                intro n
                induction n with
                | zero => simp; omega
                | succ n ih =>
                  rw [List.range_succ, List.filter_append, List.length_append]
                  simp only [List.filter, List.length_cons, List.length_nil]
                  by_cases hnlo : n < lo
                  · have hdec : decide (lo ≤ n ∧ n < hi) = false := by simp; omega
                    simp [hdec, ih]; omega
                  · push_neg at hnlo
                    by_cases hnhi : n < hi
                    · have hdec : decide (lo ≤ n ∧ n < hi) = true := by simp; omega
                      simp [hdec, ih]; omega
                    · push_neg at hnhi
                      have hdec : decide (lo ≤ n ∧ n < hi) = false := by simp; omega
                      simp [hdec, ih]; omega
              have := hsub (m+1); simp at this; omega
            · intro j hj1 hj2
              simp only [List.length_range'] at hj2
              rw [List.getElem_filter]
              simp [List.getElem_range', List.getElem_range]
          rw [hlist]; simp [List.getElem_range']
      rw [hfilter, List.length_range']
    exact hind
  exact hsum_expand

theorem sound_CompleteTurns : CompleteTurns.Goal := by
  intro n m s k
  by_cases hkm : k ≤ m
  · -- use mass_eq_count and cycle counting
    have h : B.mass ((n+1)*(m+1), m, s) k = n + 1 := by
      rw [mass_eq_count]
      simp only [B, bag]
      have hcount : ((List.range ((n + 1) * (m + 1))).map (fun i => (s + i) % (m + 1))).count k =
          (List.range ((n + 1) * (m + 1))).countP (fun i => (s + i) % (m + 1) == k) := by
        induction (List.range ((n + 1) * (m + 1))) with
        | nil => simp
        | cons i is ih => simp [List.count_cons, List.countP_cons, ih]
      rw [hcount]
      have hmod : m + 1 > 0 := Nat.succ_pos m
      have hcycle : ∀ q, (List.range (q * (m + 1))).countP (fun i => (s + i) % (m + 1) == k) = q := by
        intro q
        induction q with
        | zero => simp
        | succ q ih =>
          rw [show (q + 1) * (m + 1) = q * (m + 1) + (m + 1) by omega]
          rw [List.range_add, List.countP_append, ih]
          have hsec : (List.map (· + q * (m + 1)) (List.range (m + 1))).countP (fun i => (s + i) % (m + 1) == k) = 1 := by
            have hmap : (List.map (· + q * (m + 1)) (List.range (m + 1))).countP (fun i => (s + i) % (m + 1) == k) =
                (List.range (m + 1)).countP (fun j => (s + (j + q * (m + 1))) % (m + 1) == k) := by
              induction (List.range (m+1)) with
              | nil => simp
              | cons j js ihj => simp [List.countP_cons, ihj]
            rw [hmap, List.countP_eq_length_filter]
            let j := (k + (m+1) - (s + q * (m+1)) % (m+1)) % (m+1)
            have hjmem : j ∈ List.range (m+1) := List.mem_range.mpr (Nat.mod_lt _ hmod)
            have hjk : ((s + (j + q * (m + 1))) % (m + 1) == k) = true := by simp only [beq_iff_eq]; omega
            have huniq : (List.range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == k) = [j] := by
              apply List.ext_getElem
              · have hle1 : ((List.range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == k)).length ≤ 1 := by
                  have hall : ∀ x ∈ (List.range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == k), x = j := by
                    intro x hx; rw [List.mem_filter, List.mem_range] at hx
                    have hxeq := eq_of_beq hx.2; have hjeq := eq_of_beq hjk; omega
                  match hf : (List.range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == k) with
                  | [] => simp
                  | [_] => simp
                  | a :: b :: cs =>
                    have ha := hall a (by rw [hf]; simp); have hb := hall b (by rw [hf]; simp [List.mem_cons])
                    subst ha; subst hb
                    have hnd := (List.nodup_range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == k)
                    rw [hf] at hnd; simp [List.nodup_cons] at hnd
                have hge1 : 1 ≤ ((List.range (m+1)).filter (fun j => (s + (j + q * (m + 1))) % (m + 1) == k)).length :=
                  List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hjmem, hjk⟩)
                omega
              · intro i hi1 hi2
                have hi0 : i = 0 := by simp at hi2; omega; subst hi0
                have hget := List.getElem_mem hi1
                rw [List.mem_filter, List.mem_range] at hget
                have hxeq := eq_of_beq hget.2; have hjeq := eq_of_beq hjk
                simp; omega
            rw [huniq]; simp
          rw [hsec]; omega
      exact hcycle (n + 1)
    simp [h, hkm]
  · simp only [B, bag, hkm, ite_false]

end RotationCycle

/-! ## EdgeDegrees -/

namespace EdgeDegrees
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.EdgeDegrees

private abbrev B : Bag (List (Nat × Nat)) := bag

private theorem mass_eq_count (p : List (Nat × Nat)) (k : Nat) :
    B.mass p k = (B.expand p).count k := by
  simp only [B, bag]
  induction p with
  | nil => simp
  | cons e es ih =>
    obtain ⟨a, b⟩ := e
    simp only [List.flatMap_cons, List.count_append, List.count_cons, List.count_nil,
      List.map_cons, List.sum_cons, ih]
    omega

private theorem cover (p : List (Nat × Nat)) (k : Nat) (hk : 0 < (B.expand p).count k) :
    k ∈ B.keys p := by
  simp only [B, bag] at hk ⊢
  have hmem : k ∈ p.flatMap (fun (a,b) => [a,b]) := List.count_pos_iff.mp hk
  rw [List.mem_flatMap] at hmem
  rcases hmem with ⟨⟨a,b⟩, he, hk2⟩
  simp at hk2
  rw [List.mem_append, List.mem_map, List.mem_map]
  cases hk2 with
  | inl h => left; exact ⟨(a,b), he, h⟩
  | inr h => right; exact ⟨(a,b), he, h⟩

private theorem mem_live_iff (p : List (Nat × Nat)) (k : Nat) :
    k ∈ live B p ↔ 0 < B.mass p k := by
  unfold live; constructor
  · intro h; rw [List.mem_filter] at h; exact by simpa using h.2
  · intro h; rw [List.mem_filter]
    exact ⟨by rw [List.mem_eraseDups]; exact cover p k (by rw [← mass_eq_count]; exact h), by simpa⟩

private theorem live_nodup (p : List (Nat × Nat)) : (live B p).Nodup := (nodup_eraseDups _).filter _

theorem sound_DistinctOutput : DistinctOutput.Goal := by
  intro p
  rw [List.perm_iff_count]; intro k
  by_cases hk : k ∈ live B p
  · have hk2 : k ∈ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
    rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
  · have hk2 : k ∉ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
    rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rw [weighted_eq_map_of_count B p _ (mass_eq_count p) (cover p)]
  have h : (fun x => if lo ≤ x && x < hi then x * x else 0) =
      (fun x => if decide (lo ≤ x ∧ x < hi) then x * x else 0) := by
    funext x; by_cases h1 : lo ≤ x <;> by_cases h2 : x < hi <;> simp [h1, h2]
  rw [h, map_if_sum]; congr 1; apply List.filter_congr; intro x hx; simp

theorem sound_OrderStatistic : OrderStatistic.Goal :=
  orderStatistic_of_count B (fun p k => mass_eq_count p k) (fun p k hk => cover p k hk)

private theorem pairs_length (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = ((xs.map (fun x => (ys.filter (pred x)).length)).sum) := by
  unfold pairs
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.flatMap_cons, List.length_append, ih, List.map_cons, List.sum_cons, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a1 c1 slack
  rw [weighted_eq_map_of_count B a1 _ (mass_eq_count a1) (cover a1)]
  have hinner : ∀ x, weighted B c1 (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat) =
      ((B.expand c1).map (fun y => if x ≤ y && y ≤ x + slack then 1 else 0)).sum := by
    intro x; rw [weighted_eq_map_of_count B c1 _ (mass_eq_count c1) (cover c1)]
    congr 1; funext y; by_cases h : x ≤ y ∧ y ≤ x + slack <;> simp [h]
  simp_rw [hinner]; rw [pairs_length]
  congr 1; apply List.map_congr_left; intro x hx; rw [map_if_sum]; simp

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a1 c1
  rw [weighted_eq_map_of_count B a1 _ (mass_eq_count a1) (cover a1)]
  have hL : ((B.expand a1).map (fun x => if B.mass c1 x == 0 then x else 0)).sum =
      ((B.expand a1).map (fun x => if !(B.expand c1).contains x then x else 0)).sum := by
    congr 1; apply List.map_congr_left; intro x hx
    simp only [mass_eq_count, List.contains_iff_mem, beq_iff_eq]
    by_cases h : x ∈ B.expand c1 <;> simp [h, List.count_eq_zero_of_not_mem]
  rw [hL, map_if_sum]; simp

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro p
  have hperm : (live B p).Perm (B.expand p).eraseDups := by
    rw [List.perm_iff_count]; intro k
    by_cases hk : k ∈ live B p
    · have hk2 : k ∈ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
      rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
    · have hk2 : k ∉ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
      rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]
  rw [perm_sum_eq (hperm.map (fun k => comb (B.mass p k)))]
  have hmass : ((B.expand p).eraseDups.map (fun k => comb (B.mass p k))).sum =
      ((B.expand p).eraseDups.map (fun k => comb ((B.expand p).count k))).sum := by
    congr 1; apply List.map_congr_left; intro k hk; rw [mass_eq_count]
  rw [hmass]
  exact sum_eraseDups_comb (B.expand p)

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a1 c1 k
  have hL : ((live B a1).flatMap (fun x => List.replicate (min (B.mass a1 x) (B.mass c1 x)) x)).count k =
      min (B.mass a1 k) (B.mass c1 k) := by
    by_cases hk : k ∈ live B a1
    · have hsplit : ((live B a1).flatMap (fun x => List.replicate (min (B.mass a1 x) (B.mass c1 x)) x)) =
          (((live B a1).filter (· == k)).flatMap (fun x => List.replicate (min (B.mass a1 x) (B.mass c1 x)) x)) ++
          (((live B a1).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a1 x) (B.mass c1 x)) x)) := by
        rw [← List.flatMap_append]; congr 1
        conv_lhs => rw [← List.filter_append (fun x => x == k) (fun x => !(x == k)) (live B a1)]
        congr 1; ext x; by_cases hx : x == k <;> simp [hx]
      rw [hsplit, List.count_append]
      have hzero : (((live B a1).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a1 x) (B.mass c1 x)) x)).count k = 0 := by
        rw [List.count_eq_zero_of_not_mem]
        intro hm; rw [List.mem_flatMap] at hm
        rcases hm with ⟨x, hx, hxk⟩
        rw [List.mem_filter] at hx; rw [List.mem_replicate] at hxk
        have hxne : x ≠ k := by intro heq; subst heq; simp at hx
        exact hxne hxk.2
      rw [hzero, Nat.add_zero]
      have hfilter : (live B a1).filter (· == k) = [k] := by
        apply List.ext_getElem
        · rw [List.length_filter]
          have hle1 : ((live B a1).filter (· == k)).length ≤ 1 := by
            have hnod := (live_nodup a1).filter (· == k)
            have hall : ∀ x ∈ (live B a1).filter (· == k), x = k := by
              intro x hx; rw [List.mem_filter] at hx; exact eq_of_beq hx.2
            match hf : (live B a1).filter (· == k) with
            | [] => simp
            | [_] => simp
            | x :: y :: zs =>
              have hxv := hall x (by rw [hf]; simp)
              have hyv := hall y (by rw [hf]; simp [List.mem_cons])
              subst hxv; subst hyv; rw [hf] at hnod; simp [List.nodup_cons] at hnod
          have hge1 : 1 ≤ ((live B a1).filter (· == k)).length :=
            List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hk, by simp⟩)
          omega
        · intro i hi1 hi2
          have hi0 : i = 0 := by simp at hi2; omega
          subst hi0
          have hget := List.getElem_mem hi1
          rw [List.mem_filter] at hget
          simp [eq_of_beq hget.2]
      rw [hfilter]; simp [List.count_replicate_self]
    · have hma : B.mass a1 k = 0 := by
        by_contra h; exact hk ((mem_live_iff a1 k).mpr (Nat.pos_of_ne_zero h))
      have hmin : min (B.mass a1 k) (B.mass c1 k) = 0 := by simp [hma]
      rw [hmin, List.count_eq_zero_of_not_mem]
      intro hm; rw [List.mem_flatMap] at hm
      rcases hm with ⟨x, hx, hxk⟩
      rw [List.mem_replicate] at hxk; rcases hxk with ⟨_, rfl⟩
      exact hk hx
  simp only [B]; rw [hL, mass_eq_count, mass_eq_count]

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a1 c1
  rw [weighted_eq_map_of_count B a1 _ (mass_eq_count a1) (cover a1)]
  have hL : ((B.expand a1).map (fun x => max 1 (B.mass c1 x))).sum =
      (((B.expand a1).filter (fun x => (B.expand c1).contains x)).map (fun x => B.mass c1 x)).sum +
      (((B.expand a1).filter (fun x => !(B.expand c1).contains x)).map (fun _ => 1)).sum := by
    have hsplit := sum_filter_split (B.expand a1) (fun x => max 1 (B.mass c1 x)) (fun x => (B.expand c1).contains x)
    rw [hsplit]; congr 1
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hmem : x ∈ B.expand c1 := List.contains_iff_mem.mp hx.2
      have hpos : 0 < B.mass c1 x := by rw [mass_eq_count]; exact List.count_pos_iff.mpr hmem
      omega
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hnmem : x ∉ B.expand c1 := by intro hm; have := List.contains_iff_mem.mpr hm; simp_all
      have h0 : B.mass c1 x = 0 := by rw [mass_eq_count, List.count_eq_zero_of_not_mem hnmem]
      simp [h0]
  rw [hL]; congr 1
  · rw [pairs_length]
    have hpair : ((B.expand a1).map (fun x => ((B.expand c1).filter (fun y => x == y)).length)).sum =
        (((B.expand a1).filter (fun x => (B.expand c1).contains x)).map (fun x => B.mass c1 x)).sum := by
      have hsplit2 := sum_filter_split (B.expand a1) (fun x => ((B.expand c1).filter (fun y => x == y)).length) (fun x => (B.expand c1).contains x)
      rw [hsplit2]
      have hz : (((B.expand a1).filter (fun x => !(B.expand c1).contains x)).map (fun x => ((B.expand c1).filter (fun y => x == y)).length)).sum = 0 := by
        apply list_sum_eq_zero; intro v hv
        rw [List.mem_map] at hv; rcases hv with ⟨x, hx, rfl⟩
        rw [List.mem_filter] at hx
        have hnmem : x ∉ B.expand c1 := by intro hm; simp [List.contains_iff_mem.mpr hm] at hx
        rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
        intro y hy; simp only [beq_iff_eq]; intro heq; subst heq; exact hnmem hy
      rw [hz, Nat.add_zero]
      congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hc : ((B.expand c1).filter (fun y => x == y)).length = (B.expand c1).count x := by
        rw [List.count_eq_length_filter]; congr 1; apply List.filter_congr; intro y hy; simp [Bool.beq_comm]
      rw [hc, ← mass_eq_count]
    rw [hpair]
  · rw [sum_map_const_nat]; simp

theorem sound_CutWeight : CutWeight.Goal := by
  intro es inside
  rw [weighted_eq_map_of_count B es _ (mass_eq_count es) (cover es)]
  simp only [B, bag]
  induction es with
  | nil => simp
  | cons e es ih =>
    obtain ⟨a, b⟩ := e
    simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons,
      List.map_nil, List.countP_cons]
    rw [ih]
    by_cases ha : inside a = true <;> by_cases hb : inside b = true <;> simp [ha, hb] <;> omega

theorem sound_LoopDegree : LoopDegree.Goal := by
  intro es x
  simp only [B, bag, List.map_cons, List.count_cons]
  simp

end EdgeDegrees

/-! ## RepeatedSlices -/

namespace RepeatedSlices
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RepeatedSlices

private abbrev B : Bag (List Nat) := bag

private theorem mass_eq_count (p : List Nat) (k : Nat) :
    B.mass p k = (B.expand p).count k := by
  simp only [B, bag]
  induction p with
  | nil => simp
  | cons x xs ih =>
    simp only [List.length_cons, List.range_succ, List.flatMap_append, List.flatMap_singleton,
      List.count_append, List.drop_zero, List.count_cons]
    have hdrop_map : (List.map (fun i => List.drop (i + 1) (x :: xs)) (List.range xs.length)) =
        (List.map (fun i => List.drop i xs) (List.range xs.length)) := by
      apply List.ext_getElem
      · simp [List.length_map, List.length_range]
      · intro i hi1 hi2
        simp only [List.getElem_map, List.getElem_range, List.length_map, List.length_range] at hi1 hi2 ⊢
        simp [List.drop_succ_cons]
    rw [hdrop_map]
    have ih' : ((List.filter (fun e => e.fst == k) xs.zipIdx).map (fun e => e.snd + 1)).sum =
        (List.flatMap (fun i => List.drop i xs) (List.range xs.length)).count k := ih
    rw [List.zipIdx_cons, List.filter_cons, List.map_cons, List.sum_cons]
    simp only [Nat.zero_add]
    have hshift : (List.filter (fun e => e.fst == k) (xs.zipIdx.map (fun (a,i) => (a,i+1)))).map (fun e => e.snd + 1) =
        (List.filter (fun e => e.fst == k) xs.zipIdx).map (fun e => e.snd + 1 + 1) := by
      have h1 : List.filter (fun e => e.fst == k) (xs.zipIdx.map (fun (a,i) => (a,i+1))) =
          (List.filter (fun e => e.fst == k) xs.zipIdx).map (fun (a,i) => (a,i+1)) := by
        rw [List.filter_map]; simp [Function.comp]
      rw [h1, List.map_map]; simp [Function.comp]
    rw [hshift]
    have hsum_shift : ((List.filter (fun e => e.fst == k) xs.zipIdx).map (fun e => e.snd + 1 + 1)).sum =
        ((List.filter (fun e => e.fst == k) xs.zipIdx).map (fun e => e.snd + 1)).sum +
        ((List.filter (fun e => e.fst == k) xs.zipIdx).map (fun _ => 1)).sum := by
      have h : ∀ l : List (Nat × Nat), (l.map (fun e => e.snd + 1 + 1)).sum = (l.map (fun e => e.snd + 1)).sum + (l.map (fun _ => 1)).sum := by
        intro l; induction l with
        | nil => simp
        | cons e es ih2 => simp [ih2]; omega
      exact h _
    rw [hsum_shift, ih']
    have hcount_filter : ((List.filter (fun e => e.fst == k) xs.zipIdx).map (fun _ => 1)).sum =
        (List.filter (fun e => e.fst == k) xs.zipIdx).length := by simp [sum_map_const_nat]
    rw [hcount_filter]
    have hlen_count : (List.filter (fun e => e.fst == k) xs.zipIdx).length = xs.count k := by
      have h1 : (List.filter (fun e => e.fst == k) xs.zipIdx).length =
          (List.filter (fun x => x == k) (xs.zipIdx.map Prod.fst)).length := by
        rw [List.filter_map]; rfl
      rw [h1, List.map_zipIdx_fst, List.count_eq_length_filter]
    rw [hlen_count]
    by_cases hx : x == k <;> simp [hx] <;> omega

private theorem cover (p : List Nat) (k : Nat) (hk : 0 < (B.expand p).count k) :
    k ∈ B.keys p := by
  simp only [B, bag] at hk ⊢
  have hmem : k ∈ (List.range p.length).flatMap (fun i => p.drop i) := List.count_pos_iff.mp hk
  rw [List.mem_flatMap] at hmem
  rcases hmem with ⟨i, hi, hk2⟩
  exact List.mem_of_mem_drop hk2

private theorem mem_live_iff (p : List Nat) (k : Nat) :
    k ∈ live B p ↔ 0 < B.mass p k := by
  unfold live; constructor
  · intro h; rw [List.mem_filter] at h; exact by simpa using h.2
  · intro h; rw [List.mem_filter]
    exact ⟨by rw [List.mem_eraseDups]; exact cover p k (by rw [← mass_eq_count]; exact h), by simpa⟩

private theorem live_nodup (p : List Nat) : (live B p).Nodup := (nodup_eraseDups _).filter _

theorem sound_DistinctOutput : DistinctOutput.Goal := by
  intro p
  rw [List.perm_iff_count]; intro k
  by_cases hk : k ∈ live B p
  · have hk2 : k ∈ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
    rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
  · have hk2 : k ∉ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
    rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rw [weighted_eq_map_of_count B p _ (mass_eq_count p) (cover p)]
  have h : (fun x => if lo ≤ x && x < hi then x * x else 0) =
      (fun x => if decide (lo ≤ x ∧ x < hi) then x * x else 0) := by
    funext x; by_cases h1 : lo ≤ x <;> by_cases h2 : x < hi <;> simp [h1, h2]
  rw [h, map_if_sum]; congr 1; apply List.filter_congr; intro x hx; simp

theorem sound_OrderStatistic : OrderStatistic.Goal :=
  orderStatistic_of_count B (fun p k => mass_eq_count p k) (fun p k hk => cover p k hk)

private theorem pairs_length (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = ((xs.map (fun x => (ys.filter (pred x)).length)).sum) := by
  unfold pairs
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.flatMap_cons, List.length_append, ih, List.map_cons, List.sum_cons, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c slack
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hinner : ∀ x, weighted B c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat) =
      ((B.expand c).map (fun y => if x ≤ y && y ≤ x + slack then 1 else 0)).sum := by
    intro x; rw [weighted_eq_map_of_count B c _ (mass_eq_count c) (cover c)]
    congr 1; funext y; by_cases h : x ≤ y ∧ y ≤ x + slack <;> simp [h]
  simp_rw [hinner]; rw [pairs_length]
  congr 1; apply List.map_congr_left; intro x hx; rw [map_if_sum]; simp

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => if B.mass c x == 0 then x else 0)).sum =
      ((B.expand a).map (fun x => if !(B.expand c).contains x then x else 0)).sum := by
    congr 1; apply List.map_congr_left; intro x hx
    simp only [mass_eq_count, List.contains_iff_mem, beq_iff_eq]
    by_cases h : x ∈ B.expand c <;> simp [h, List.count_eq_zero_of_not_mem]
  rw [hL, map_if_sum]; simp

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro p
  have hperm : (live B p).Perm (B.expand p).eraseDups := by
    rw [List.perm_iff_count]; intro k
    by_cases hk : k ∈ live B p
    · have hk2 : k ∈ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
      rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
    · have hk2 : k ∉ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
      rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]
  rw [perm_sum_eq (hperm.map (fun k => comb (B.mass p k)))]
  have hmass : ((B.expand p).eraseDups.map (fun k => comb (B.mass p k))).sum =
      ((B.expand p).eraseDups.map (fun k => comb ((B.expand p).count k))).sum := by
    congr 1; apply List.map_congr_left; intro k hk; rw [mass_eq_count]
  rw [hmass]
  exact sum_eraseDups_comb (B.expand p)

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  have hL : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k =
      min (B.mass a k) (B.mass c k) := by
    by_cases hk : k ∈ live B a
    · have hsplit : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) =
          (((live B a).filter (· == k)).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) ++
          (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) := by
        rw [← List.flatMap_append]; congr 1
        conv_lhs => rw [← List.filter_append (fun x => x == k) (fun x => !(x == k)) (live B a)]
        congr 1; ext x; by_cases hx : x == k <;> simp [hx]
      rw [hsplit, List.count_append]
      have hzero : (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k = 0 := by
        rw [List.count_eq_zero_of_not_mem]
        intro hm; rw [List.mem_flatMap] at hm
        rcases hm with ⟨x, hx, hxk⟩
        rw [List.mem_filter] at hx; rw [List.mem_replicate] at hxk
        have hxne : x ≠ k := by intro heq; subst heq; simp at hx
        exact hxne hxk.2
      rw [hzero, Nat.add_zero]
      have hfilter : (live B a).filter (· == k) = [k] := by
        apply List.ext_getElem
        · rw [List.length_filter]
          have hle1 : ((live B a).filter (· == k)).length ≤ 1 := by
            have hnod := (live_nodup a).filter (· == k)
            have hall : ∀ x ∈ (live B a).filter (· == k), x = k := by
              intro x hx; rw [List.mem_filter] at hx; exact eq_of_beq hx.2
            match hf : (live B a).filter (· == k) with
            | [] => simp
            | [_] => simp
            | x :: y :: zs =>
              have hxv := hall x (by rw [hf]; simp)
              have hyv := hall y (by rw [hf]; simp [List.mem_cons])
              subst hxv; subst hyv; rw [hf] at hnod; simp [List.nodup_cons] at hnod
          have hge1 : 1 ≤ ((live B a).filter (· == k)).length :=
            List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hk, by simp⟩)
          omega
        · intro i hi1 hi2
          have hi0 : i = 0 := by simp at hi2; omega
          subst hi0
          have hget := List.getElem_mem hi1
          rw [List.mem_filter] at hget
          simp [eq_of_beq hget.2]
      rw [hfilter]; simp [List.count_replicate_self]
    · have hma : B.mass a k = 0 := by
        by_contra h; exact hk ((mem_live_iff a k).mpr (Nat.pos_of_ne_zero h))
      have hmin : min (B.mass a k) (B.mass c k) = 0 := by simp [hma]
      rw [hmin, List.count_eq_zero_of_not_mem]
      intro hm; rw [List.mem_flatMap] at hm
      rcases hm with ⟨x, hx, hxk⟩
      rw [List.mem_replicate] at hxk; rcases hxk with ⟨_, rfl⟩
      exact hk hx
  simp only [B]; rw [hL, mass_eq_count, mass_eq_count]

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => max 1 (B.mass c x))).sum =
      (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum +
      (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun _ => 1)).sum := by
    have hsplit := sum_filter_split (B.expand a) (fun x => max 1 (B.mass c x)) (fun x => (B.expand c).contains x)
    rw [hsplit]; congr 1
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hmem : x ∈ B.expand c := List.contains_iff_mem.mp hx.2
      have hpos : 0 < B.mass c x := by rw [mass_eq_count]; exact List.count_pos_iff.mpr hmem
      omega
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hnmem : x ∉ B.expand c := by intro hm; have := List.contains_iff_mem.mpr hm; simp_all
      have h0 : B.mass c x = 0 := by rw [mass_eq_count, List.count_eq_zero_of_not_mem hnmem]
      simp [h0]
  rw [hL]; congr 1
  · rw [pairs_length]
    have hpair : ((B.expand a).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum =
        (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum := by
      have hsplit2 := sum_filter_split (B.expand a) (fun x => ((B.expand c).filter (fun y => x == y)).length) (fun x => (B.expand c).contains x)
      rw [hsplit2]
      have hz : (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum = 0 := by
        apply list_sum_eq_zero; intro v hv
        rw [List.mem_map] at hv; rcases hv with ⟨x, hx, rfl⟩
        rw [List.mem_filter] at hx
        have hnmem : x ∉ B.expand c := by intro hm; simp [List.contains_iff_mem.mpr hm] at hx
        rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
        intro y hy; simp only [beq_iff_eq]; intro heq; subst heq; exact hnmem hy
      rw [hz, Nat.add_zero]
      congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hc : ((B.expand c).filter (fun y => x == y)).length = (B.expand c).count x := by
        rw [List.count_eq_length_filter]; congr 1; apply List.filter_congr; intro y hy; simp [Bool.beq_comm]
      rw [hc, ← mass_eq_count]
    rw [hpair]
  · rw [sum_map_const_nat]; simp

theorem sound_AppendedSuffixes : AppendedSuffixes.Goal := by
  intro a b k
  simp only [B, bag]
  rw [List.zipIdx_append]
  simp only [List.filter_append, List.map_append, List.sum_append]
  congr 1
  · congr 1; apply List.filter_congr; intro e he; rfl
  · have hshift : (List.filter (fun e => e.fst == k) (b.zipIdx.map (fun (x,i) => (x, i + a.length)))).map (fun e => e.snd + 1) =
        (List.filter (fun e => e.fst == k) b.zipIdx).map (fun e => e.snd + 1 + a.length) := by
      have h1 : List.filter (fun e => e.fst == k) (b.zipIdx.map (fun (x,i) => (x, i + a.length))) =
          (List.filter (fun e => e.fst == k) b.zipIdx).map (fun (x,i) => (x, i + a.length)) := by
        rw [List.filter_map]; simp [Function.comp]
      rw [h1, List.map_map]; simp [Function.comp]
    rw [hshift]
    have hsum : ((List.filter (fun e => e.fst == k) b.zipIdx).map (fun e => e.snd + 1 + a.length)).sum =
        ((List.filter (fun e => e.fst == k) b.zipIdx).map (fun e => e.snd + 1)).sum +
        a.length * (List.filter (fun e => e.fst == k) b.zipIdx).length := by
      have h1 : ∀ l : List (Nat×Nat), (l.map (fun e => e.snd + 1 + a.length)).sum = (l.map (fun e => e.snd + 1)).sum + (l.map (fun _ => a.length)).sum := by
        intro l; induction l with
        | nil => simp
        | cons e es ih => simp [ih]; omega
      rw [h1, sum_map_const_nat, Nat.mul_comm]
    rw [hsum]
    have hlen : (List.filter (fun e => e.fst == k) b.zipIdx).length = b.count k := by
      have h1 : (List.filter (fun e => e.fst == k) b.zipIdx).length =
          (List.filter (fun x => x == k) (b.zipIdx.map Prod.fst)).length := by
        rw [List.filter_map]; rfl
      rw [h1, List.map_zipIdx_fst, List.count_eq_length_filter]
    rw [hlen]; omega

theorem sound_TriangularSize : TriangularSize.Goal := by
  intro xs
  rw [weighted_eq_map_of_count B xs _ (mass_eq_count xs) (cover xs)]
  simp only [B, bag]
  have hlen : ((List.range xs.length).flatMap (fun i => xs.drop i)).length = xs.length * (xs.length + 1) / 2 := by
    rw [List.length_flatMap]
    have h1 : ((List.range xs.length).map (fun i => (List.drop i xs).length)).sum =
        ((List.range xs.length).map (fun i => xs.length - i)).sum := by
      congr 1; apply List.map_congr_left; intro i hi; rw [List.length_drop]
    rw [h1]
    have hsum : ∀ n, ((List.range n).map (fun i => n - i)).sum = n * (n + 1) / 2 := by
      intro n
      induction n with
      | zero => simp
      | succ n ih =>
        rw [List.range_succ, List.map_append, List.sum_append, List.map_singleton, List.sum_singleton]
        have hmap : (List.map (fun i => (n + 1) - i) (List.range n)) =
            (List.map (fun i => n - i + 1) (List.range n)) := by
          apply List.map_congr_left; intro i hi
          rw [List.mem_range] at hi; omega
        rw [hmap]
        have hsum1 : ((List.range n).map (fun i => n - i + 1)).sum =
            ((List.range n).map (fun i => n - i)).sum + ((List.range n).map (fun _ => 1)).sum := by
          have h : ∀ l : List Nat, (l.map (fun i => (n - i) + 1)).sum = (l.map (fun i => n - i)).sum + (l.map (fun _ => 1)).sum := by
            intro l; induction l with
            | nil => simp
            | cons a as ih2 => simp [ih2]; omega
          exact h _
        rw [hsum1, ih, sum_map_const_nat, List.length_range]
        simp; omega
    exact hsum xs.length
  have hmap1 : ((List.flatMap (fun i => List.drop i xs) (List.range xs.length)).map (fun _ => 1)).sum =
      ((List.flatMap (fun i => List.drop i xs) (List.range xs.length))).length := by
    simp [sum_map_const_nat]
  rw [hmap1, hlen]

end RepeatedSlices

/-! ## DeletedOrdinals -/

namespace DeletedOrdinals
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DeletedOrdinals

private abbrev B : Bag (List Nat × List Nat) := bag

private theorem countP_eq_countP_filter_eraseDups (xs ds : List Nat) (k : Nat) :
    (xs.zipIdx.filter (fun e => ds.contains e.2)).countP (fun e => e.1 == k) =
    (ds.eraseDups.filter (· < xs.length)).countP (fun i => xs[i]? == some k) := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  -- LHS: filter by (fst==k) and (ds.contains snd) in zipIdx
  -- RHS: filter by (<xs.length) and (xs[i]?==some k) in ds.eraseDups
  apply Nat.le_antisymm
  · -- LHS <= RHS: each element of LHS corresponds to unique index in RHS
    apply List.le_of_eq_of_le _ (List.Sublist.length_le (List.filter_sublist _))
    -- show LHS length <= RHS length via card
    have hsub : (List.filter (fun e => e.1 == k) (List.filter (fun e => ds.contains e.2) xs.zipIdx)).length ≤
        (List.filter (fun i => xs[i]? == some k) (List.filter (fun i => i < xs.length) ds.eraseDups)).length := by
      -- map LHS to indices, show subset
      have hmap : (List.filter (fun e => e.1 == k) (List.filter (fun e => ds.contains e.2) xs.zipIdx)).map Prod.snd
          ⊆ (List.filter (fun i => xs[i]? == some k) (List.filter (fun i => i < xs.length) ds.eraseDups)) := by
        intro i hi
        rw [List.mem_map] at hi
        rcases hi with ⟨e, he, rfl⟩
        rw [List.mem_filter, List.mem_filter] at he
        have he1 : e.1 == k := he.1.2
        have he2 : ds.contains e.2 := he.2.2
        have hmem_zip : e ∈ xs.zipIdx := he.2.1
        have hidx : e.2 < xs.length := by
          have := List.mem_zipIdx hmem_zip; omega
        rw [List.mem_filter, List.mem_filter]
        refine ⟨⟨?_, hidx⟩, ?_⟩
        · rw [List.mem_eraseDups, List.contains_iff_mem] at he2 ⊢
          exact he2
        · have hget := List.getElem?_zipIdx (l := xs) (i := e.2)
          have heq_e : e = (xs[e.2], e.2) := by
            have h := List.mem_zipIdx hmem_zip
            ext <;> simp_all
          rw [heq_e] at he1
          simp at he1
          rw [List.getElem?_eq_getElem hidx]
          simp [he1]
      have hnodup_lhs : (List.filter (fun e => e.1 == k) (List.filter (fun e => ds.contains e.2) xs.zipIdx)).Nodup := by
        apply List.Nodup.filter; apply List.Nodup.filter
        exact List.Nodup.of_map Prod.snd (by simp [List.map_zipIdx_snd])
      have hnodup_rhs : (List.filter (fun i => xs[i]? == some k) (List.filter (fun i => i < xs.length) ds.eraseDups)).Nodup := by
        apply List.Nodup.filter; apply List.Nodup.filter
        exact nodup_eraseDups ds
      have hinj : (List.filter (fun e => e.1 == k) (List.filter (fun e => ds.contains e.2) xs.zipIdx)).length =
          ((List.filter (fun e => e.1 == k) (List.filter (fun e => ds.contains e.2) xs.zipIdx)).map Prod.snd).length := by
        rw [List.length_map]
      rw [hinj]
      apply List.Nodup.length_le_of_subset
      · -- nodup of map snd
        apply List.Nodup.of_map id
        rw [List.map_id]
        apply List.Nodup.map_on
        · intro a ha b hb hab
          rw [List.mem_filter, List.mem_filter] at ha hb
          have ha_eq : a = (xs[a.2], a.2) := by have h := List.mem_zipIdx ha.2.1; ext <;> simp_all
          have hb_eq : b = (xs[b.2], b.2) := by have h := List.mem_zipIdx hb.2.1; ext <;> simp_all
          rw [ha_eq, hb_eq]; simp_all
        · exact hnodup_lhs
      · exact hmap
    calc (List.filter (fun e => e.1 == k) (List.filter (fun e => ds.contains e.2) xs.zipIdx)).length
        ≤ (List.filter (fun i => xs[i]? == some k) (List.filter (fun i => i < xs.length) ds.eraseDups)).length := hsub
      _ = (List.filter (fun i => xs[i]? == some k) (ds.eraseDups.filter (fun i => i < xs.length))).length := by
          congr 1; rw [List.filter_filter]; congr 1; apply List.filter_congr; intro x hx; simp [Bool.and_comm]
  · -- RHS <= LHS
    have hsub : (List.filter (fun i => xs[i]? == some k) (List.filter (fun i => i < xs.length) ds.eraseDups)).length ≤
        (List.filter (fun e => e.1 == k) (List.filter (fun e => ds.contains e.2) xs.zipIdx)).length := by
      have hmap : (List.filter (fun i => xs[i]? == some k) (List.filter (fun i => i < xs.length) ds.eraseDups)).map (fun i => (k, i))
          ⊆ (List.filter (fun e => e.1 == k) (List.filter (fun e => ds.contains e.2) xs.zipIdx)) := by
        intro e he
        rw [List.mem_map] at he
        rcases he with ⟨i, hi, rfl⟩
        rw [List.mem_filter, List.mem_filter] at hi
        have hi_len : i < xs.length := hi.2.2
        have hi_get : xs[i]? == some k := hi.1.2
        rw [List.mem_filter, List.mem_filter]
        refine ⟨by simpa using hi_get, ?_, ?_⟩
        · rw [List.mem_iff_getElem]
          refine ⟨i, by simp [List.length_zipIdx]; exact hi_len, ?_⟩
          simp [List.getElem_zipIdx]
          have hget := List.getElem?_eq_getElem hi_len
          rw [hget] at hi_get
          simp at hi_get
          simp [hi_get]
        · simp [List.mem_eraseDups.mp hi.2.1]
      have hnodup_rhs : (List.filter (fun i => xs[i]? == some k) (List.filter (fun i => i < xs.length) ds.eraseDups)).Nodup := by
        apply List.Nodup.filter; apply List.Nodup.filter; exact nodup_eraseDups ds
      have hnodup_lhs : (List.filter (fun e => e.1 == k) (List.filter (fun e => ds.contains e.2) xs.zipIdx)).Nodup := by
        apply List.Nodup.filter; apply List.Nodup.filter
        exact List.Nodup.of_map Prod.snd (by simp [List.map_zipIdx_snd])
      have hinj_len : (List.filter (fun i => xs[i]? == some k) (List.filter (fun i => i < xs.length) ds.eraseDups)).length =
          ((List.filter (fun i => xs[i]? == some k) (List.filter (fun i => i < xs.length) ds.eraseDups)).map (fun i => (k, i))).length := by
        rw [List.length_map]
      rw [hinj_len]
      apply List.Nodup.length_le_of_subset
      · apply List.Nodup.map; intro a b hab; simp at hab; exact hab.2
        exact hnodup_rhs
      · exact hmap
    calc (List.filter (fun i => xs[i]? == some k) (ds.eraseDups.filter (fun i => i < xs.length))).length
        = (List.filter (fun i => xs[i]? == some k) (List.filter (fun i => i < xs.length) ds.eraseDups)).length := by
          congr 1; rw [List.filter_filter]; congr 1; apply List.filter_congr; intro x hx; simp [Bool.and_comm]
      _ ≤ (List.filter (fun e => e.1 == k) (List.filter (fun e => ds.contains e.2) xs.zipIdx)).length := hsub

private theorem mass_eq_count (p : List Nat × List Nat) (k : Nat) :
    B.mass p k = (B.expand p).count k := by
  obtain ⟨xs, ds⟩ := p
  simp only [B, bag]
  have hcount : ((xs.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst).count k =
      (xs.zipIdx.filter (fun e => !ds.contains e.2)).countP (fun e => e.1 == k) := by
    rw [List.countP_eq_length_filter, List.count_eq_length_filter, List.filter_map]; rfl
  rw [hcount]
  have htotal : (xs.zipIdx.countP (fun e => e.1 == k)) = xs.count k := by
    rw [List.countP_eq_length_filter, List.count_eq_length_filter]
    have h1 : (List.filter (fun e => e.fst == k) xs.zipIdx).length =
        (List.filter (fun x => x == k) (xs.zipIdx.map Prod.fst)).length := by
      rw [List.filter_map]; rfl
    rw [h1, List.map_zipIdx_fst]
  have hsplit : (xs.zipIdx.countP (fun e => e.1 == k)) =
      (xs.zipIdx.filter (fun e => !ds.contains e.2)).countP (fun e => e.1 == k) +
      (xs.zipIdx.filter (fun e => ds.contains e.2)).countP (fun e => e.1 == k) := by
    rw [List.countP_eq_length_filter, List.countP_eq_length_filter, List.countP_eq_length_filter]
    have hsplit2 : (List.filter (fun e => e.1 == k) xs.zipIdx).length =
        (List.filter (fun e => e.1 == k) (List.filter (fun e => !ds.contains e.2) xs.zipIdx)).length +
        (List.filter (fun e => e.1 == k) (List.filter (fun e => ds.contains e.2) xs.zipIdx)).length := by
      have hh := List.length_filter_add_length_filter_not (p := fun e : Nat×Nat => !ds.contains e.2) (l := List.filter (fun e => e.1 == k) xs.zipIdx)
      rw [List.filter_filter, List.filter_filter] at hh
      have hc1 : (List.filter (fun e => !ds.contains e.2 && (e.1 == k)) xs.zipIdx) =
          (List.filter (fun e => (e.1 == k) && !ds.contains e.2) xs.zipIdx) := by
        apply List.filter_congr; intro e he; simp [Bool.and_comm]
      have hc2 : (List.filter (fun e => (!(!ds.contains e.2)) && (e.1 == k)) xs.zipIdx) =
          (List.filter (fun e => (e.1 == k) && ds.contains e.2) xs.zipIdx) := by
        apply List.filter_congr; intro e he; cases ds.contains e.2 <;> simp [Bool.and_comm]
      rw [hc1, hc2] at hh
      linarith
    exact hsplit2
  rw [countP_eq_countP_filter_eraseDups] at hsplit
  omega

private theorem cover (p : List Nat × List Nat) (k : Nat) (hk : 0 < (B.expand p).count k) :
    k ∈ B.keys p := by
  obtain ⟨xs, ds⟩ := p
  simp only [B, bag] at hk ⊢
  have hmem : k ∈ (xs.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst := List.count_pos_iff.mp hk
  rw [List.mem_map] at hmem
  rcases hmem with ⟨e, he, rfl⟩
  rw [List.mem_filter] at he
  exact (List.mem_zipIdx he.1).1

private theorem mem_live_iff (p : List Nat × List Nat) (k : Nat) :
    k ∈ live B p ↔ 0 < B.mass p k := by
  unfold live; constructor
  · intro h; rw [List.mem_filter] at h; exact by simpa using h.2
  · intro h; rw [List.mem_filter]
    exact ⟨by rw [List.mem_eraseDups]; exact cover p k (by rw [← mass_eq_count]; exact h), by simpa⟩

private theorem live_nodup (p : List Nat × List Nat) : (live B p).Nodup := (nodup_eraseDups _).filter _

theorem sound_DistinctOutput : DistinctOutput.Goal := by
  intro p
  rw [List.perm_iff_count]; intro k
  by_cases hk : k ∈ live B p
  · have hk2 : k ∈ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
    rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
  · have hk2 : k ∉ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
    rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rw [weighted_eq_map_of_count B p _ (mass_eq_count p) (cover p)]
  have h : (fun x => if lo ≤ x && x < hi then x * x else 0) =
      (fun x => if decide (lo ≤ x ∧ x < hi) then x * x else 0) := by
    funext x; by_cases h1 : lo ≤ x <;> by_cases h2 : x < hi <;> simp [h1, h2]
  rw [h, map_if_sum]; congr 1; apply List.filter_congr; intro x hx; simp

theorem sound_OrderStatistic : OrderStatistic.Goal :=
  orderStatistic_of_count B (fun p k => mass_eq_count p k) (fun p k hk => cover p k hk)

private theorem pairs_length (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = ((xs.map (fun x => (ys.filter (pred x)).length)).sum) := by
  unfold pairs
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.flatMap_cons, List.length_append, ih, List.map_cons, List.sum_cons, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c slack
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hinner : ∀ x, weighted B c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat) =
      ((B.expand c).map (fun y => if x ≤ y && y ≤ x + slack then 1 else 0)).sum := by
    intro x; rw [weighted_eq_map_of_count B c _ (mass_eq_count c) (cover c)]
    congr 1; funext y; by_cases h : x ≤ y ∧ y ≤ x + slack <;> simp [h]
  simp_rw [hinner]; rw [pairs_length]
  congr 1; apply List.map_congr_left; intro x hx; rw [map_if_sum]; simp

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => if B.mass c x == 0 then x else 0)).sum =
      ((B.expand a).map (fun x => if !(B.expand c).contains x then x else 0)).sum := by
    congr 1; apply List.map_congr_left; intro x hx
    simp only [mass_eq_count, List.contains_iff_mem, beq_iff_eq]
    by_cases h : x ∈ B.expand c <;> simp [h, List.count_eq_zero_of_not_mem]
  rw [hL, map_if_sum]; simp

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro p
  have hperm : (live B p).Perm (B.expand p).eraseDups := by
    rw [List.perm_iff_count]; intro k
    by_cases hk : k ∈ live B p
    · have hk2 : k ∈ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
      rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
    · have hk2 : k ∉ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
      rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]
  rw [perm_sum_eq (hperm.map (fun k => comb (B.mass p k)))]
  have hmass : ((B.expand p).eraseDups.map (fun k => comb (B.mass p k))).sum =
      ((B.expand p).eraseDups.map (fun k => comb ((B.expand p).count k))).sum := by
    congr 1; apply List.map_congr_left; intro k hk; rw [mass_eq_count]
  rw [hmass]
  exact sum_eraseDups_comb (B.expand p)

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  have hL : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k =
      min (B.mass a k) (B.mass c k) := by
    by_cases hk : k ∈ live B a
    · have hsplit : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) =
          (((live B a).filter (· == k)).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) ++
          (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) := by
        rw [← List.flatMap_append]; congr 1
        conv_lhs => rw [← List.filter_append (fun x => x == k) (fun x => !(x == k)) (live B a)]
        congr 1; ext x; by_cases hx : x == k <;> simp [hx]
      rw [hsplit, List.count_append]
      have hzero : (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k = 0 := by
        rw [List.count_eq_zero_of_not_mem]
        intro hm; rw [List.mem_flatMap] at hm
        rcases hm with ⟨x, hx, hxk⟩
        rw [List.mem_filter] at hx; rw [List.mem_replicate] at hxk
        have hxne : x ≠ k := by intro heq; subst heq; simp at hx
        exact hxne hxk.2
      rw [hzero, Nat.add_zero]
      have hfilter : (live B a).filter (· == k) = [k] := by
        apply List.ext_getElem
        · rw [List.length_filter]
          have hle1 : ((live B a).filter (· == k)).length ≤ 1 := by
            have hnod := (live_nodup a).filter (· == k)
            have hall : ∀ x ∈ (live B a).filter (· == k), x = k := by
              intro x hx; rw [List.mem_filter] at hx; exact eq_of_beq hx.2
            match hf : (live B a).filter (· == k) with
            | [] => simp
            | [_] => simp
            | x :: y :: zs =>
              have hxv := hall x (by rw [hf]; simp)
              have hyv := hall y (by rw [hf]; simp [List.mem_cons])
              subst hxv; subst hyv; rw [hf] at hnod; simp [List.nodup_cons] at hnod
          have hge1 : 1 ≤ ((live B a).filter (· == k)).length :=
            List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hk, by simp⟩)
          omega
        · intro i hi1 hi2
          have hi0 : i = 0 := by simp at hi2; omega
          subst hi0
          have hget := List.getElem_mem hi1
          rw [List.mem_filter] at hget
          simp [eq_of_beq hget.2]
      rw [hfilter]; simp [List.count_replicate_self]
    · have hma : B.mass a k = 0 := by
        by_contra h; exact hk ((mem_live_iff a k).mpr (Nat.pos_of_ne_zero h))
      have hmin : min (B.mass a k) (B.mass c k) = 0 := by simp [hma]
      rw [hmin, List.count_eq_zero_of_not_mem]
      intro hm; rw [List.mem_flatMap] at hm
      rcases hm with ⟨x, hx, hxk⟩
      rw [List.mem_replicate] at hxk; rcases hxk with ⟨_, rfl⟩
      exact hk hx
  simp only [B]; rw [hL, mass_eq_count, mass_eq_count]

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => max 1 (B.mass c x))).sum =
      (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum +
      (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun _ => 1)).sum := by
    have hsplit := sum_filter_split (B.expand a) (fun x => max 1 (B.mass c x)) (fun x => (B.expand c).contains x)
    rw [hsplit]; congr 1
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hmem : x ∈ B.expand c := List.contains_iff_mem.mp hx.2
      have hpos : 0 < B.mass c x := by rw [mass_eq_count]; exact List.count_pos_iff.mpr hmem
      omega
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hnmem : x ∉ B.expand c := by intro hm; have := List.contains_iff_mem.mpr hm; simp_all
      have h0 : B.mass c x = 0 := by rw [mass_eq_count, List.count_eq_zero_of_not_mem hnmem]
      simp [h0]
  rw [hL]; congr 1
  · rw [pairs_length]
    have hpair : ((B.expand a).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum =
        (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum := by
      have hsplit2 := sum_filter_split (B.expand a) (fun x => ((B.expand c).filter (fun y => x == y)).length) (fun x => (B.expand c).contains x)
      rw [hsplit2]
      have hz : (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum = 0 := by
        apply list_sum_eq_zero; intro v hv
        rw [List.mem_map] at hv; rcases hv with ⟨x, hx, rfl⟩
        rw [List.mem_filter] at hx
        have hnmem : x ∉ B.expand c := by intro hm; simp [List.contains_iff_mem.mpr hm] at hx
        rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
        intro y hy; simp only [beq_iff_eq]; intro heq; subst heq; exact hnmem hy
      rw [hz, Nat.add_zero]
      congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hc : ((B.expand c).filter (fun y => x == y)).length = (B.expand c).count x := by
        rw [List.count_eq_length_filter]; congr 1; apply List.filter_congr; intro y hy; simp [Bool.beq_comm]
      rw [hc, ← mass_eq_count]
    rw [hpair]
  · rw [sum_map_const_nat]; simp

theorem sound_PrefixOfLive : PrefixOfLive.Goal := by
  intro xs ds t k
  -- split xs into take t ++ drop t and account for deletions
  simp only [B, bag]
  -- use count-based argument via mass_eq_count
  -- mass (xs,ds) k = xs.count k - ds.eraseDups.countP (fun i => xs[i]? == some k)
  -- mass (xs.take t, ds) k = (xs.take t).count k - ds.eraseDups.countP (fun i => (xs.take t)[i]? == some k)
  -- mass (xs.drop t, (ds.filter (· >= t)).map (·-t)) k = (xs.drop t).count k - ...
  have hcount : xs.count k = (xs.take t).count k + (xs.drop t).count k := by
    have h := List.take_append_drop t xs
    conv_lhs => rw [← h]
    rw [List.count_append]
  rw [hcount]
  -- split deleted indices
  have hdel : (ds.eraseDups.countP (fun i => xs[i]? == some k)) =
      (ds.eraseDups.countP (fun i => i < t && xs[i]? == some k)) +
      (ds.eraseDups.countP (fun i => !(i < t) && xs[i]? == some k)) := by
    rw [List.countP_eq_length_filter, List.countP_eq_length_filter, List.countP_eq_length_filter]
    have hh := List.length_filter_add_length_filter_not (p := fun i : Nat => i < t) (l := List.filter (fun i => xs[i]? == some k) ds.eraseDups)
    rw [List.filter_filter, List.filter_filter] at hh
    have hc1 : (List.filter (fun i => decide (i < t) && (xs[i]? == some k)) ds.eraseDups) =
        (List.filter (fun i => (xs[i]? == some k) && decide (i < t)) ds.eraseDups) := by
      apply List.filter_congr; intro e he; simp [Bool.and_comm]
    have hc2 : (List.filter (fun i => (!decide (i < t)) && (xs[i]? == some k)) ds.eraseDups) =
        (List.filter (fun i => (xs[i]? == some k) && !decide (i < t)) ds.eraseDups) := by
      apply List.filter_congr; intro e he; simp [Bool.and_comm]
    rw [hc1, hc2] at hh
    -- convert decide (i<t) to i<t
    have heq1 : (List.filter (fun i => decide (i < t) && xs[i]? == some k) ds.eraseDups) =
        (List.filter (fun i => (i < t) && (xs[i]? == some k)) ds.eraseDups) := by
      apply List.filter_congr; intro i hi; simp
    have heq2 : (List.filter (fun i => (!decide (i < t)) && xs[i]? == some k) ds.eraseDups) =
        (List.filter (fun i => (!(i < t)) && (xs[i]? == some k)) ds.eraseDups) := by
      apply List.filter_congr; intro i hi; simp
    linarith
  rw [hdel]
  -- (take t).count k - countP(take t part) + ((drop t).count k - countP(drop t part)) = xs.count k - total
  -- need: countP in ds.eraseDups for take t = countP for (take t) with same ds
  -- and countP for drop t shifted = countP for !(i<t) in original
  have ht : (ds.eraseDups.countP (fun i => i < t && xs[i]? == some k)) =
      (ds.eraseDups.countP (fun i => (xs.take t)[i]? == some k)) := by
    congr 1; ext i
    by_cases hit : i < t
    · simp [hit, List.getElem?_take_of_lt hit]
    · simp [hit]
  have hd : (ds.eraseDups.countP (fun i => !(i < t) && xs[i]? == some k)) =
      (((ds.filter (· >= t)).map (· - t)).eraseDups.countP (fun i => (xs.drop t)[i]? == some k)) := by
    -- bijection between indices >= t in ds with xs[i]?==some k and shifted indices in filtered/mapped ds
    rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
    apply Nat.le_antisymm
    · -- LHS <= RHS
      apply List.le_of_eq_of_le _ (List.Sublist.length_le (List.filter_sublist _))
      have hle : (List.filter (fun i => !(i < t) && (xs[i]? == some k)) ds.eraseDups).length ≤
          (List.filter (fun i => (xs.drop t)[i]? == some k) (((ds.filter (· >= t)).map (· - t)).eraseDups)).length := by
        have hsub : ((List.filter (fun i => !(i < t) && (xs[i]? == some k)) ds.eraseDups).map (· - t)) ⊆
            (List.filter (fun i => (xs.drop t)[i]? == some k) (((ds.filter (· >= t)).map (· - t)).eraseDups)) := by
          intro j hj
          rw [List.mem_map] at hj
          rcases hj with ⟨i, hi, rfl⟩
          rw [List.mem_filter] at hi
          have hnot : !(i < t) = true := hi.2.1
          have hge : t ≤ i := by simp at hnot; omega
          have hk2 : xs[i]? == some k := hi.2.2
          rw [List.mem_filter]
          constructor
          · rw [List.mem_eraseDups, List.mem_map, List.mem_filter]
            exact ⟨i, ⟨List.mem_eraseDups.mp hi.1, by simpa using hge⟩, rfl⟩
          · -- (xs.drop t)[i-t]? == some k
            have hdrop : (xs.drop t)[i-t]? = xs[i]? := by
              rw [List.getElem?_drop]
              congr 1; omega
            rw [hdrop]; exact hk2
        have hnodup_lhs : (List.filter (fun i => !(i < t) && (xs[i]? == some k)) ds.eraseDups).Nodup := by
          apply List.Nodup.filter; exact nodup_eraseDups ds
        have hinj : (List.filter (fun i => !(i < t) && (xs[i]? == some k)) ds.eraseDups).length =
            ((List.filter (fun i => !(i < t) && (xs[i]? == some k)) ds.eraseDups).map (· - t)).length := by rw [List.length_map]
        rw [hinj]
        apply List.Nodup.length_le_of_subset
        · apply List.Nodup.map
          · intro a b hab
            rw [List.mem_filter] at a_mem b_mem
            have ha_ge : t ≤ a := by have := a_mem.2.1; simp at this; omega
            have hb_ge : t ≤ b := by have := b_mem.2.1; simp at this; omega
            omega
          · exact hnodup_lhs
          -- wait, Nodup.map needs Injective. But easier: use Nodup.map_on
        · exact hsub
      -- fix the nodup.map issue
      have hnodup_lhs : (List.filter (fun i => !(i < t) && (xs[i]? == some k)) ds.eraseDups).Nodup := by
        apply List.Nodup.filter; exact nodup_eraseDups ds
      have hinj : (List.filter (fun i => !(i < t) && (xs[i]? == some k)) ds.eraseDups).length =
          ((List.filter (fun i => !(i < t) && (xs[i]? == some k)) ds.eraseDups).map (· - t)).length := by rw [List.length_map]
      rw [hinj]
      have hnodup_map : ((List.filter (fun i => !(i < t) && (xs[i]? == some k)) ds.eraseDups).map (· - t)).Nodup := by
        rw [List.nodup_map_iff_inj_on (by intro a ha b hb; exact id)]
        intro a ha b hb hab
        rw [List.mem_filter] at ha hb
        have ha_ge : t ≤ a := by have := ha.2.1; simp at this; omega
        have hb_ge : t ≤ b := by have := hb.2.1; simp at this; omega
        omega
      exact List.Nodup.length_le_of_subset hnodup_map hsub
    · -- RHS <= LHS
      have hle : (List.filter (fun i => (xs.drop t)[i]? == some k) (((ds.filter (· >= t)).map (· - t)).eraseDups)).length ≤
          (List.filter (fun i => !(i < t) && (xs[i]? == some k)) ds.eraseDups).length := by
        have hsub : ((List.filter (fun i => (xs.drop t)[i]? == some k) (((ds.filter (· >= t)).map (· - t)).eraseDups)).map (· + t)) ⊆
            (List.filter (fun i => !(i < t) && (xs[i]? == some k)) ds.eraseDups) := by
          intro j hj
          rw [List.mem_map] at hj
          rcases hj with ⟨i, hi, rfl⟩
          rw [List.mem_filter] at hi
          rw [List.mem_eraseDups, List.mem_map, List.mem_filter] at hi
          rcases hi.1 with ⟨orig, horig, rfl⟩
          have hge : t ≤ orig := by simpa using horig.2
          rw [List.mem_filter]
          refine ⟨by rw [List.mem_eraseDups]; exact horig.1, ?_, ?_⟩
          · simp; omega
          · have hdrop : (xs.drop t)[orig - t]? = xs[orig]? := by
              rw [List.getElem?_drop]; congr 1; omega
            rw [hdrop] at hi; exact hi.2
        have hnodup_rhs : (List.filter (fun i => (xs.drop t)[i]? == some k) (((ds.filter (· >= t)).map (· - t)).eraseDups)).Nodup := by
          apply List.Nodup.filter; exact nodup_eraseDups _
        have hinj : (List.filter (fun i => (xs.drop t)[i]? == some k) (((ds.filter (· >= t)).map (· - t)).eraseDups)).length =
            ((List.filter (fun i => (xs.drop t)[i]? == some k) (((ds.filter (· >= t)).map (· - t)).eraseDups)).map (· + t)).length := by rw [List.length_map]
        rw [hinj]
        have hnodup_map : ((List.filter (fun i => (xs.drop t)[i]? == some k) (((ds.filter (· >= t)).map (· - t)).eraseDups)).map (· + t)).Nodup := by
          rw [List.nodup_map_iff_inj_on (by intro a ha b hb; exact id)]
          intro a ha b hb hab; omega
        exact List.Nodup.length_le_of_subset hnodup_map hsub
      exact hle
  rw [ht, hd]; omega

theorem sound_DeleteFootprint : DeleteFootprint.Goal := by
  intro xs ds
  rw [weighted_eq_map_of_count B (xs,ds) _ (mass_eq_count (xs,ds)) (cover (xs,ds))]
  simp only [B, bag]
  -- ((xs.zipIdx.filter (fun e => !ds.contains e.2)).map Prod.fst).map (fun _ => 1) sum = length
  have h1 : (((List.filter (fun e => !ds.contains e.snd) xs.zipIdx).map Prod.fst).map (fun _ => 1)).sum =
      ((List.filter (fun e => !ds.contains e.snd) xs.zipIdx).map Prod.fst).length := by
    simp [sum_map_const_nat]
  rw [h1, List.length_map]
  -- length = xs.length - (ds.eraseDups.filter (· < xs.length)).length
  have hsplit : (List.filter (fun e => !ds.contains e.snd) xs.zipIdx).length +
      (List.filter (fun e => ds.contains e.snd) xs.zipIdx).length = xs.zipIdx.length := by
    have hh := List.length_filter_add_length_filter_not (p := fun e : Nat×Nat => !ds.contains e.snd) (l := xs.zipIdx)
    have hc : (List.filter (fun e => (!(!ds.contains e.snd))) xs.zipIdx) =
        (List.filter (fun e => ds.contains e.snd) xs.zipIdx) := by
      apply List.filter_congr; intro e he; cases ds.contains e.snd <;> simp
    linarith
  have hdel : (List.filter (fun e => ds.contains e.snd) xs.zipIdx).length =
      (List.filter (fun i => i < xs.length) ds.eraseDups).length := by
    apply Nat.le_antisymm
    · have hmap : (List.filter (fun e => ds.contains e.snd) xs.zipIdx).map Prod.snd ⊆
          (List.filter (fun i => i < xs.length) ds.eraseDups) := by
        intro i hi; rw [List.mem_map] at hi; rcases hi with ⟨e, he, rfl⟩
        rw [List.mem_filter] at he
        have hidx : e.2 < xs.length := by have := List.mem_zipIdx he.1; omega
        rw [List.mem_filter, List.mem_eraseDups, List.contains_iff_mem]
        exact ⟨he.2, hidx⟩
      have hnodup_lhs : (List.filter (fun e => ds.contains e.snd) xs.zipIdx).Nodup := by
        apply List.Nodup.filter; exact List.Nodup.of_map Prod.snd (by simp [List.map_zipIdx_snd])
      rw [show (List.filter (fun e => ds.contains e.snd) xs.zipIdx).length = ((List.filter (fun e => ds.contains e.snd) xs.zipIdx).map Prod.snd).length by rw [List.length_map]]
      have hnodup_map : ((List.filter (fun e => ds.contains e.snd) xs.zipIdx).map Prod.snd).Nodup := by
        rw [List.nodup_map_iff_inj_on (by intro a ha b hb; exact id)]
        intro a ha b hb hab
        rw [List.mem_filter] at ha hb
        have ha_eq : a = (xs[a.2], a.2) := by have h := List.mem_zipIdx ha.1; ext <;> simp_all
        have hb_eq : b = (xs[b.2], b.2) := by have h := List.mem_zipIdx hb.1; ext <;> simp_all
        rw [ha_eq, hb_eq]; simp_all
      exact List.Nodup.length_le_of_subset hnodup_map hmap
    · have hnodup_rhs : (List.filter (fun i => i < xs.length) ds.eraseDups).Nodup :=
        (nodup_eraseDups ds).filter _
      have hsub : (List.filter (fun i => i < xs.length) ds.eraseDups) ⊆
          ((List.filter (fun e => ds.contains e.snd) xs.zipIdx).map Prod.snd) := by
        intro i hi
        rw [List.mem_filter] at hi
        rw [List.mem_map]
        refine ⟨(xs[i]'hi.2, i), ?_, rfl⟩
        rw [List.mem_filter]
        constructor
        · rw [List.mem_iff_getElem]
          refine ⟨i, by simp [List.length_zipIdx]; exact hi.2, ?_⟩
          simp [List.getElem_zipIdx]
        · simp [List.mem_eraseDups.mp hi.1]
      have hlen_map : ((List.filter (fun e => ds.contains e.snd) xs.zipIdx).map Prod.snd).length =
          (List.filter (fun e => ds.contains e.snd) xs.zipIdx).length := List.length_map
      rw [← hlen_map]
      exact List.Nodup.length_le_of_subset hnodup_rhs hsub
  rw [List.length_zipIdx] at hsplit
  omega

end DeletedOrdinals

/-! ## CoGroup -/

namespace CoGroup
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.CoGroup

private abbrev B : Bag (List Nat × List Nat) := bag

private theorem mass_eq_count (p : List Nat × List Nat) (k : Nat) :
    B.mass p k = (B.expand p).count k := by
  obtain ⟨xs, ys⟩ := p
  simp only [B, bag]
  induction xs with
  | nil => simp
  | cons x xs ih =>
    simp only [List.flatMap_cons, List.count_append, List.count_cons, ih]
    by_cases hx : x == k
    · have hxk : x = k := eq_of_beq hx
      subst hxk
      simp only [beq_self_eq_true, ite_true]
      have hfilter : (List.filter (fun y => y == k) ys).count k = ys.count k := by
        rw [List.count_filter]; simp
        rw [List.count_eq_length_filter]
      rw [hfilter]; omega
    · simp only [hx, ite_false, Nat.zero_add]
      have hfilter : (List.filter (fun y => y == x) ys).count k = 0 := by
        rw [List.count_eq_zero_of_not_mem]
        intro hm; rw [List.mem_filter] at hm
        have := eq_of_beq hm.2; subst this; simp at hx
      rw [hfilter]; omega

private theorem cover (p : List Nat × List Nat) (k : Nat) (hk : 0 < (B.expand p).count k) :
    k ∈ B.keys p := by
  obtain ⟨xs, ys⟩ := p
  simp only [B, bag] at hk ⊢
  have hmem : k ∈ xs.flatMap (fun x => ys.filter (· == x)) := List.count_pos_iff.mp hk
  rw [List.mem_flatMap] at hmem
  rcases hmem with ⟨x, hx, hk2⟩
  rw [List.mem_filter] at hk2
  have hkx : k = x := (eq_of_beq hk2.2).symm
  subst hkx; exact hx

private theorem mem_live_iff (p : List Nat × List Nat) (k : Nat) :
    k ∈ live B p ↔ 0 < B.mass p k := by
  unfold live; constructor
  · intro h; rw [List.mem_filter] at h; exact by simpa using h.2
  · intro h; rw [List.mem_filter]
    exact ⟨by rw [List.mem_eraseDups]; exact cover p k (by rw [← mass_eq_count]; exact h), by simpa⟩

private theorem live_nodup (p : List Nat × List Nat) : (live B p).Nodup := (nodup_eraseDups _).filter _

theorem sound_DistinctOutput : DistinctOutput.Goal := by
  intro p
  rw [List.perm_iff_count]; intro k
  by_cases hk : k ∈ live B p
  · have hk2 : k ∈ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
    rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
  · have hk2 : k ∉ (B.expand p).eraseDups := by
      rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
    rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]

theorem sound_FilteredMoment : FilteredMoment.Goal := by
  intro p lo hi
  rw [weighted_eq_map_of_count B p _ (mass_eq_count p) (cover p)]
  have h : (fun x => if lo ≤ x && x < hi then x * x else 0) =
      (fun x => if decide (lo ≤ x ∧ x < hi) then x * x else 0) := by
    funext x; by_cases h1 : lo ≤ x <;> by_cases h2 : x < hi <;> simp [h1, h2]
  rw [h, map_if_sum]; congr 1; apply List.filter_congr; intro x hx; simp

theorem sound_OrderStatistic : OrderStatistic.Goal :=
  orderStatistic_of_count B (fun p k => mass_eq_count p k) (fun p k hk => cover p k hk)

private theorem pairs_length (xs ys : List Nat) (pred : Nat → Nat → Bool) :
    (pairs xs ys pred).length = ((xs.map (fun x => (ys.filter (pred x)).length)).sum) := by
  unfold pairs
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.flatMap_cons, List.length_append, ih, List.map_cons, List.sum_cons, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c slack
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hinner : ∀ x, weighted B c (fun y => (decide (x ≤ y ∧ y ≤ x + slack)).toNat) =
      ((B.expand c).map (fun y => if x ≤ y && y ≤ x + slack then 1 else 0)).sum := by
    intro x; rw [weighted_eq_map_of_count B c _ (mass_eq_count c) (cover c)]
    congr 1; funext y; by_cases h : x ≤ y ∧ y ≤ x + slack <;> simp [h]
  simp_rw [hinner]; rw [pairs_length]
  congr 1; apply List.map_congr_left; intro x hx; rw [map_if_sum]; simp

theorem sound_AntiRevenue : AntiRevenue.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => if B.mass c x == 0 then x else 0)).sum =
      ((B.expand a).map (fun x => if !(B.expand c).contains x then x else 0)).sum := by
    congr 1; apply List.map_congr_left; intro x hx
    simp only [mass_eq_count, List.contains_iff_mem, beq_iff_eq]
    by_cases h : x ∈ B.expand c <;> simp [h, List.count_eq_zero_of_not_mem]
  rw [hL, map_if_sum]; simp

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro p
  have hperm : (live B p).Perm (B.expand p).eraseDups := by
    rw [List.perm_iff_count]; intro k
    by_cases hk : k ∈ live B p
    · have hk2 : k ∈ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff, mem_live_iff, mass_eq_count] at hk ⊢; exact hk
      rw [count_eq_one_of_mem_nodup (live_nodup p) hk, count_eq_one_of_mem_nodup (nodup_eraseDups _) hk2]
    · have hk2 : k ∉ (B.expand p).eraseDups := by
        rw [List.mem_eraseDups, List.count_pos_iff]; intro h; exact hk (by rw [mem_live_iff, mass_eq_count]; exact h)
      rw [List.count_eq_zero_of_not_mem hk, List.count_eq_zero_of_not_mem hk2]
  rw [perm_sum_eq (hperm.map (fun k => comb (B.mass p k)))]
  have hmass : ((B.expand p).eraseDups.map (fun k => comb (B.mass p k))).sum =
      ((B.expand p).eraseDups.map (fun k => comb ((B.expand p).count k))).sum := by
    congr 1; apply List.map_congr_left; intro k hk; rw [mass_eq_count]
  rw [hmass]
  exact sum_eraseDups_comb (B.expand p)

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  have hL : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k =
      min (B.mass a k) (B.mass c k) := by
    by_cases hk : k ∈ live B a
    · have hsplit : ((live B a).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) =
          (((live B a).filter (· == k)).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) ++
          (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)) := by
        rw [← List.flatMap_append]; congr 1
        conv_lhs => rw [← List.filter_append (fun x => x == k) (fun x => !(x == k)) (live B a)]
        congr 1; ext x; by_cases hx : x == k <;> simp [hx]
      rw [hsplit, List.count_append]
      have hzero : (((live B a).filter (fun x => !(x == k))).flatMap (fun x => List.replicate (min (B.mass a x) (B.mass c x)) x)).count k = 0 := by
        rw [List.count_eq_zero_of_not_mem]
        intro hm; rw [List.mem_flatMap] at hm
        rcases hm with ⟨x, hx, hxk⟩
        rw [List.mem_filter] at hx; rw [List.mem_replicate] at hxk
        have hxne : x ≠ k := by intro heq; subst heq; simp at hx
        exact hxne hxk.2
      rw [hzero, Nat.add_zero]
      have hfilter : (live B a).filter (· == k) = [k] := by
        apply List.ext_getElem
        · rw [List.length_filter]
          have hle1 : ((live B a).filter (· == k)).length ≤ 1 := by
            have hnod := (live_nodup a).filter (· == k)
            have hall : ∀ x ∈ (live B a).filter (· == k), x = k := by
              intro x hx; rw [List.mem_filter] at hx; exact eq_of_beq hx.2
            match hf : (live B a).filter (· == k) with
            | [] => simp
            | [_] => simp
            | x :: y :: zs =>
              have hxv := hall x (by rw [hf]; simp)
              have hyv := hall y (by rw [hf]; simp [List.mem_cons])
              subst hxv; subst hyv; rw [hf] at hnod; simp [List.nodup_cons] at hnod
          have hge1 : 1 ≤ ((live B a).filter (· == k)).length :=
            List.length_pos_of_mem (by rw [List.mem_filter]; exact ⟨hk, by simp⟩)
          omega
        · intro i hi1 hi2
          have hi0 : i = 0 := by simp at hi2; omega
          subst hi0
          have hget := List.getElem_mem hi1
          rw [List.mem_filter] at hget
          simp [eq_of_beq hget.2]
      rw [hfilter]; simp [List.count_replicate_self]
    · have hma : B.mass a k = 0 := by
        by_contra h; exact hk ((mem_live_iff a k).mpr (Nat.pos_of_ne_zero h))
      have hmin : min (B.mass a k) (B.mass c k) = 0 := by simp [hma]
      rw [hmin, List.count_eq_zero_of_not_mem]
      intro hm; rw [List.mem_flatMap] at hm
      rcases hm with ⟨x, hx, hxk⟩
      rw [List.mem_replicate] at hxk; rcases hxk with ⟨_, rfl⟩
      exact hk hx
  simp only [B]; rw [hL, mass_eq_count, mass_eq_count]

theorem sound_OuterWeight : OuterWeight.Goal := by
  intro a c
  rw [weighted_eq_map_of_count B a _ (mass_eq_count a) (cover a)]
  have hL : ((B.expand a).map (fun x => max 1 (B.mass c x))).sum =
      (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum +
      (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun _ => 1)).sum := by
    have hsplit := sum_filter_split (B.expand a) (fun x => max 1 (B.mass c x)) (fun x => (B.expand c).contains x)
    rw [hsplit]; congr 1
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hmem : x ∈ B.expand c := List.contains_iff_mem.mp hx.2
      have hpos : 0 < B.mass c x := by rw [mass_eq_count]; exact List.count_pos_iff.mpr hmem
      omega
    · congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hnmem : x ∉ B.expand c := by intro hm; have := List.contains_iff_mem.mpr hm; simp_all
      have h0 : B.mass c x = 0 := by rw [mass_eq_count, List.count_eq_zero_of_not_mem hnmem]
      simp [h0]
  rw [hL]; congr 1
  · rw [pairs_length]
    have hpair : ((B.expand a).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum =
        (((B.expand a).filter (fun x => (B.expand c).contains x)).map (fun x => B.mass c x)).sum := by
      have hsplit2 := sum_filter_split (B.expand a) (fun x => ((B.expand c).filter (fun y => x == y)).length) (fun x => (B.expand c).contains x)
      rw [hsplit2]
      have hz : (((B.expand a).filter (fun x => !(B.expand c).contains x)).map (fun x => ((B.expand c).filter (fun y => x == y)).length)).sum = 0 := by
        apply list_sum_eq_zero; intro v hv
        rw [List.mem_map] at hv; rcases hv with ⟨x, hx, rfl⟩
        rw [List.mem_filter] at hx
        have hnmem : x ∉ B.expand c := by intro hm; simp [List.contains_iff_mem.mpr hm] at hx
        rw [List.length_eq_zero_iff, List.filter_eq_nil_iff]
        intro y hy; simp only [beq_iff_eq]; intro heq; subst heq; exact hnmem hy
      rw [hz, Nat.add_zero]
      congr 1; apply List.map_congr_left; intro x hx
      rw [List.mem_filter] at hx
      have hc : ((B.expand c).filter (fun y => x == y)).length = (B.expand c).count x := by
        rw [List.count_eq_length_filter]; congr 1; apply List.filter_congr; intro y hy; simp [Bool.beq_comm]
      rw [hc, ← mass_eq_count]
    rw [hpair]
  · rw [sum_map_const_nat]; simp

theorem sound_SharedProbe : SharedProbe.Goal := by
  intro a b c
  rw [weighted_eq_map_of_count B (a,b) _ (mass_eq_count (a,b)) (cover (a,b))]
  rw [weighted_eq_map_of_count B (b,c) _ (mass_eq_count (b,c)) (cover (b,c))]
  simp only [B, bag]
  have hL : ((a.flatMap (fun x => b.filter (· == x))).map (fun k => c.count k)).sum =
      (a.map (fun x => b.count x * c.count x)).sum := by
    induction a with
    | nil => simp
    | cons x xs ih =>
      simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons, ih]
      congr 1
      have h1 : ((b.filter (· == x)).map (fun k => c.count k)).sum = b.count x * c.count x := by
        have h2 : ((b.filter (· == x)).map (fun k => c.count k)).sum =
            ((b.filter (· == x)).map (fun _ => c.count x)).sum := by
          congr 1; apply List.map_congr_left; intro y hy
          rw [List.mem_filter] at hy; have := eq_of_beq hy.2; simp [this]
        rw [h2, sum_map_const_nat, List.count_eq_length_filter, Nat.mul_comm]
      exact h1
  have hR : ((b.flatMap (fun x => c.filter (· == x))).map (fun k => a.count k)).sum =
      (b.map (fun x => c.count x * a.count x)).sum := by
    induction b with
    | nil => simp
    | cons x xs ih =>
      simp only [List.flatMap_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons, ih]
      congr 1
      have h1 : ((c.filter (· == x)).map (fun k => a.count k)).sum = c.count x * a.count x := by
        have h2 : ((c.filter (· == x)).map (fun k => a.count k)).sum =
            ((c.filter (· == x)).map (fun _ => a.count x)).sum := by
          congr 1; apply List.map_congr_left; intro y hy
          rw [List.mem_filter] at hy; have := eq_of_beq hy.2; simp [this]
        rw [h2, sum_map_const_nat, List.count_eq_length_filter, Nat.mul_comm]
      exact h1
  rw [hL, hR]
  -- both = sum_{k in (a++b).eraseDups} a.count k * b.count k * c.count k
  have hLa : (a.map (fun x => b.count x * c.count x)).sum =
      (((a ++ b).eraseDups.map (fun k => a.count k * b.count k * c.count k)).sum) := by
    have h1 : (a.map (fun x => b.count x * c.count x)).sum =
        ((a.eraseDups.map (fun k => a.count k * (b.count k * c.count k))).sum) := by
      have h := sum_eraseDups_count_mul a (fun x => b.count x * c.count x)
      rw [← h]
    rw [h1]
    have h2 : ((a.eraseDups.map (fun k => a.count k * (b.count k * c.count k))).sum) =
        ((a.eraseDups.map (fun k => a.count k * b.count k * c.count k)).sum) := by
      congr 1; apply List.map_congr_left; intro k hk; omega
    rw [h2]
    symm
    apply sum_eraseDups_zero_filter
    intro k hk
    rw [List.mem_eraseDups, List.mem_append]
    left; rw [List.mem_eraseDups]; exact List.count_pos_iff.mp hk
  have hRb : (b.map (fun x => c.count x * a.count x)).sum =
      (((a ++ b).eraseDups.map (fun k => a.count k * b.count k * c.count k)).sum) := by
    have h1 : (b.map (fun x => c.count x * a.count x)).sum =
        ((b.eraseDups.map (fun k => b.count k * (c.count k * a.count k))).sum) := by
      have h := sum_eraseDups_count_mul b (fun x => c.count x * a.count x)
      rw [← h]
    rw [h1]
    have h2 : ((b.eraseDups.map (fun k => b.count k * (c.count k * a.count k))).sum) =
        ((b.eraseDups.map (fun k => a.count k * b.count k * c.count k)).sum) := by
      congr 1; apply List.map_congr_left; intro k hk; omega
    rw [h2]
    symm
    apply sum_eraseDups_zero_filter
    intro k hk
    rw [List.mem_eraseDups, List.mem_append]
    right; rw [List.mem_eraseDups]; exact List.count_pos_iff.mp hk
  rw [hLa, hRb]

theorem sound_BuildSwap : BuildSwap.Goal := by
  intro a b k
  simp only [B, bag]
  ac_rfl

end CoGroup

end TinylambOpt.ResearchProofs.StorageExecution.BagKernels
