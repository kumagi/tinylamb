import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_SelfPairs

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.AdditiveProduct

private abbrev B := bag

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
        simp [ih, Nat.succ_mul, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · have hkx : (k == x) = false := Bool.eq_false_of_ne_true (by
          intro h
          exact hx (beq_iff_eq.mpr (beq_iff_eq.mp h).symm))
        simp [List.count_cons, hkx, hx, ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

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
          simp [Nat.add_mul, Nat.add_comm, Nat.add_left_comm]
        rw [hhead]
        have hrest_count : ∀ a ∈ rest.eraseDups, (List.count a (k :: ks)) = rest.count a := by
          intro a ha
          have hane : (a == k) = false := by
            apply Bool.eq_false_of_ne_true
            intro h
            have hamem : a ∈ rest := List.mem_eraseDups.mp ha
            have hamem' : a ∈ ks.filter (fun b => !(b == k)) := by simpa [rest] using hamem
            have hff := (List.mem_filter.mp hamem').2
            simpa [h] using hff
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          have hka : (k == a) = false := Bool.eq_false_of_ne_true (by
            intro h
            have hkaeq : k = a := beq_iff_eq.mp h
            have htrue : (a == k) = true := beq_iff_eq.mpr hkaeq.symm
            rw [hane] at htrue
            cases htrue)
          simp [List.count_cons, hka, hrestc]
        rw [show ((rest.eraseDups.map fun a => List.count a (k :: ks) * f a).sum) =
             ((rest.eraseDups.map fun a => rest.count a * f a).sum) by
             apply congrArg List.sum
             apply List.map_congr_left
             intro a ha
             rw [hrest_count a ha]]
        rw [ihrest]
        have hks : (ks.map f).sum = ks.count k * f k + (rest.map f).sum := by
          simpa [rest] using sum_map_eq_count_head k ks f
        omega
  exact H keys rfl

private theorem count_eraseDups_le_one {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α) :
    l.eraseDups.count a ≤ 1 := by
  let motive : Nat → Prop := fun n => ∀ (l : List α), l.length = n → ∀ a, l.eraseDups.count a ≤ 1
  have H : motive l.length := by
    apply Nat.strongRecOn
    intro n ih l hlen a
    cases l with
    | nil => simp
    | cons b bs =>
        rw [List.eraseDups_cons, List.count_cons]
        let rest := bs.filter (fun c => !(c == b))
        have hrestlen : rest.length < n := by
          rw [← hlen]
          simp [rest]
          exact Nat.lt_of_le_of_lt (List.length_filter_le (fun c : α => !(c == b)) bs)
            (Nat.lt_succ_self _)
        have ihrest : ∀ c, rest.eraseDups.count c ≤ 1 := by
          intro c
          exact ih rest.length hrestlen rest rfl c
        by_cases hba : b == a
        · have hzero : rest.eraseDups.count a = 0 := by
            rw [List.count_eq_zero]
            intro hm
            rw [List.mem_eraseDups] at hm
            have hm' : a ∈ rest := by simpa [rest] using hm
            have hf := (List.mem_filter.mp hm').2
            have hab : (a == b) = true := beq_iff_eq.mpr (beq_iff_eq.mp hba).symm
            simp [hab] at hf
          simpa [hba, rest, hzero] using ihrest a
        · have hbfalse : (b == a) = false := Bool.eq_false_of_ne_true hba
          simp [hbfalse]
          exact ihrest a
  exact H l rfl a

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup :=
  List.nodup_iff_count.mpr (fun a => count_eraseDups_le_one l a)

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
      rw [ht] at hp
      cases hp
  have hz₂ : ∀ d ∈ l₂, p d = false → f d = 0 := by
    intro d hd hp
    by_cases hne : f d = 0
    · exact hne
    · have ht : p d = true := by simp [p, hne]
      rw [ht] at hp
      cases hp
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

private theorem sum_map_congr {l : List Nat} {f g : Nat → Nat}
    (h : ∀ k ∈ l, f k = g k) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a t ih =>
      simp [h a (by simp), ih (fun k hk => h k (by simp [hk]))]

private theorem sum_map_eq_update {l : List Nat} (hnd : l.Nodup) (a c : Nat)
    (f g : Nat → Nat) (ha : a ∈ l)
    (hf : ∀ k, k ∈ l → k ≠ a → f k = g k) (hfa : f a = g a + c) :
    (l.map f).sum = (l.map g).sum + c := by
  induction l with
  | nil => simp at ha
  | cons b t ih =>
      rw [List.nodup_cons] at hnd
      rcases hnd with ⟨hbnot, htnd⟩
      simp at ha
      rcases ha with rfl | hat
      · have hcong : ∀ k ∈ t, f k = g k := by
          intro k hk
          apply hf k (by simp [hk])
          intro hka
          apply hbnot
          simpa [hka] using hk
        rw [List.map_cons, List.map_cons, List.sum_cons, List.sum_cons, hfa,
          sum_map_congr hcong]
        omega
      · have hbne : b ≠ a := by
          intro h
          subst b
          exact hbnot hat
        have hhead : f b = g b := hf b (by simp) hbne
        have htail := ih htnd hat (fun k hk hka => hf k (by simp [hk]) (by
          intro h; exact hka (by simpa [h])))
        simp [hhead, htail, Nat.add_assoc]

/-! ## Basic arithmetic on `comb` -/

private theorem comb_zero : comb 0 = 0 := by
  unfold comb
  simp

private theorem comb_succ (n : Nat) : comb (n + 1) = comb n + n := by
  unfold comb
  simp only [Nat.add_sub_cancel]
  have h1 : n * (n - 1) = n * n - n := by
    rw [Nat.mul_sub_left_distrib]
    simp
  have h2 : (n + 1) * n = n * n + n := by
    rw [Nat.add_mul]
    simp
  have hnn : n ≤ n * n := by
    cases n with
    | zero => simp
    | succ m =>
        rw [Nat.succ_mul]
        omega
  have hmul : (n + 1) * n = n * (n - 1) + n * 2 := by omega
  rw [hmul]
  exact Nat.add_mul_div_right (n * (n - 1)) n (by decide : 0 < 2)

/-! ## The generic pair-count identity -/

private def pairCount (l : List Nat) : Nat :=
  ((l.zipIdx).map (fun p => (l.drop (p.2 + 1)).count p.1)).sum

private theorem zipIdx_offset (l : List Nat) (d : Nat) :
    l.zipIdx d = l.zipIdx.map (fun p => (p.1, d + p.2)) := by
  induction l generalizing d with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons, ih (d + 1), ih 1]
      simp [Nat.add_assoc, List.map_map, Function.comp_apply]

private theorem pairCount_cons (x : Nat) (xs : List Nat) :
    pairCount (x :: xs) = xs.count x + pairCount xs := by
  unfold pairCount
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons, List.drop_succ_cons, List.drop_zero]
  rw [zipIdx_offset]
  simp only [List.map_map, Function.comp_def, Nat.zero_add]
  have hcong : (List.map (fun p : Nat × Nat => List.count p.1 (List.drop (1 + p.2) xs)) xs.zipIdx).sum =
      (List.map (fun p : Nat × Nat => List.count p.1 (List.drop (p.2 + 1) xs)) xs.zipIdx).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro p hp
    have h : 1 + p.2 = p.2 + 1 := Nat.add_comm 1 p.2
    rw [h]
  rw [hcong]

private theorem left_cons (x : Nat) (xs : List Nat) :
    (((x :: xs).eraseDups.map fun k => comb ((x :: xs).count k)).sum) =
      ((xs.eraseDups.map fun k => comb (xs.count k)).sum) + xs.count x := by
  have hnd : (x :: xs).eraseDups.Nodup := eraseDups_nodup _
  have hmemx : x ∈ (x :: xs).eraseDups := by simp
  have h1 := sum_map_eq_update hnd x (xs.count x)
      (fun k => comb ((x :: xs).count k)) (fun k => comb (xs.count k)) hmemx
      (by
        intro k hk hkx
        congr 1
        have hxbeq : (x == k) = false := by
          apply Bool.eq_false_of_ne_true
          intro heq
          exact hkx (beq_iff_eq.mp heq).symm
        simp [List.count_cons, hxbeq])
      (by
        simp [comb_succ])
  have h2 : (((x :: xs).eraseDups.map fun k => comb (xs.count k)).sum) =
      ((xs.eraseDups.map fun k => comb (xs.count k)).sum) := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro d hd hdnot
      have hdmem : d ∈ x :: xs := by
        rw [← List.mem_eraseDups]
        exact hd
      simp only [List.mem_cons] at hdmem
      rcases hdmem with rfl | hdxs
      · have hnotmem : d ∉ xs := by
          intro h
          exact hdnot (by rw [List.mem_eraseDups]; exact h)
        have hc : xs.count d = 0 := List.count_eq_zero.mpr hnotmem
        simp [hc, comb_zero]
      · exfalso
        exact hdnot (by rw [List.mem_eraseDups]; exact hdxs)
    · intro d hd hdnot
      exfalso
      apply hdnot
      rw [List.mem_eraseDups]
      rw [List.mem_eraseDups] at hd
      exact List.mem_cons_of_mem x hd
  omega

private theorem pairs_generic (l : List Nat) :
    (l.eraseDups.map (fun k => comb (l.count k))).sum = pairCount l := by
  induction l with
  | nil => simp [pairCount]
  | cons x xs ih =>
      rw [left_cons, ih, pairCount_cons]
      omega

/-! ## AdditiveProduct bag facts -/

private theorem count_add_map (x k : Nat) (b : List Nat) :
    (b.map (fun y => x + y)).count k = if x ≤ k then b.count (k - x) else 0 := by
  induction b with
  | nil => simp
  | cons y ys ih =>
      simp only [List.map_cons, List.count_cons, ih]
      by_cases hx : x ≤ k
      · by_cases hxy : x + y = k
        · have hy : y = k - x := by omega
          simp [hx, hy, Nat.add_sub_of_le hx]
        · have hy : y ≠ k - x := by intro hy'; apply hxy; omega
          simp [hx, hxy, hy]
      · have hne : x + y ≠ k := by omega
        simp [hx, hne]

private theorem count_expand_eq_mass (a b : List Nat) (k : Nat) :
    (B.expand (a, b)).count k = B.mass (a, b) k := by
  unfold B bag
  simp only
  rw [List.count_flatMap]
  change ((a.map (fun x => (b.map (fun y => x + y)).count k)).sum) = _
  rw [← dedup_weight_sum (fun x => (b.map (fun y => x + y)).count k) a]
  apply congrArg List.sum
  apply List.map_congr_left
  intro x hx
  rw [count_add_map]
  by_cases hxk : x ≤ k <;> simp [hxk]

private theorem mem_keys_of_mem_expand (a b : List Nat) (k : Nat) :
    k ∈ B.expand (a, b) → k ∈ B.keys (a, b) := by
  intro h
  change k ∈ a.flatMap (fun x => b.map (fun y => x + y)) at h
  rw [List.mem_flatMap] at h
  rcases h with ⟨x, hxa, hkx⟩
  rw [List.mem_map] at hkx
  rcases hkx with ⟨y, hyb, hxy⟩
  change k ∈ a.eraseDups.flatMap (fun x => b.eraseDups.map (fun y => x + y))
  rw [List.mem_flatMap]
  refine ⟨x, ?_, ?_⟩
  · rw [List.mem_eraseDups]
    exact hxa
  · rw [List.mem_map]
    refine ⟨y, ?_, ?_⟩
    · rw [List.mem_eraseDups]
      exact hyb
    · omega

private theorem mass_zero_of_not_mem_expand (a b : List Nat) (k : Nat)
    (h : k ∉ B.expand (a, b)) : B.mass (a, b) k = 0 := by
  rw [← count_expand_eq_mass a b k]
  exact List.count_eq_zero.mpr h

/-! ## Goal -/

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro p
  rcases p with ⟨a, b⟩
  have hmass : ∀ k, B.mass (a, b) k = (B.expand (a, b)).count k := by
    intro k
    exact (count_expand_eq_mass a b k).symm
  have hgeneric := pairs_generic (B.expand (a, b))
  have hlive : (((live B (a, b)).map fun k => comb (B.mass (a, b) k)).sum) =
      (((B.keys (a, b)).eraseDups.map fun k => comb (B.mass (a, b) k)).sum) := by
    unfold live
    symm
    apply sum_map_filter_eq
    intro k hk hp
    have hnot : ¬ 0 < B.mass (a, b) k := by
      intro hpos
      have ht : decide (0 < B.mass (a, b) k) = true := by simp [hpos]
      simp [ht] at hp
    have hzero : B.mass (a, b) k = 0 := Nat.eq_zero_of_not_pos hnot
    rw [hzero, comb_zero]
  have hkeys : (((B.keys (a, b)).eraseDups.map fun k => comb (B.mass (a, b) k)).sum) =
      (((B.expand (a, b)).eraseDups.map fun k => comb ((B.expand (a, b)).count k)).sum) := by
    have hm : (((B.keys (a, b)).eraseDups.map fun k => comb (B.mass (a, b) k)).sum) =
        (((B.keys (a, b)).eraseDups.map fun k => comb ((B.expand (a, b)).count k)).sum) := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro k hk
      rw [hmass]
    rw [hm]
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro d hd hdnot
      have hdnot' : d ∉ B.expand (a, b) := by
        intro hde
        exact hdnot (by rw [List.mem_eraseDups]; exact hde)
      simp [List.count_eq_zero.mpr hdnot', comb_zero]
    · intro d hd hdnot
      have hde : d ∈ B.expand (a, b) := by
        rw [← List.mem_eraseDups]
        exact hd
      exfalso
      apply hdnot
      rw [List.mem_eraseDups]
      exact mem_keys_of_mem_expand a b d hde
  have hpair : (((B.expand (a, b)).zipIdx.map (fun (x, i) =>
      ((B.expand (a, b)).drop (i + 1)).count x)).sum) = pairCount (B.expand (a, b)) := by
    unfold pairCount
    apply congrArg List.sum
    apply List.map_congr_left
    intro p hp
    cases p
    rfl
  change ((live B (a, b)).map (fun k => comb (B.mass (a, b) k))).sum =
    ((B.expand (a, b)).zipIdx.map (fun (x, i) =>
      ((B.expand (a, b)).drop (i + 1)).count x)).sum
  rw [hlive, hkeys, hgeneric, hpair]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_AdditiveProduct_SelfPairs
