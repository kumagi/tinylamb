import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_SelfPairs

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

private abbrev B := bag

/-! ## Generic list lemmas -/

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
          simp [hba, rest, hzero]
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
          intro h; exact hka (by simp [h])))
        simp [hhead, htail, Nat.add_assoc]

/-! ## Arithmetic on `comb` -/

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

/-! ## ExceptionOverlay bag facts -/

private theorem count_map_eq_countP_map (l : List Nat) (f : Nat → Nat) (k : Nat) :
    (l.map f).count k = l.countP (fun i => f i == k) := by
  rw [List.count_eq_countP]
  induction l with
  | nil => simp
  | cons a as ih => simp [ih, List.countP_cons]

private theorem countP_split {α : Type} (l : List α) (p q : α → Bool) :
    l.countP p = (l.filter q).countP p + (l.filter (fun x => !q x)).countP p := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases hq : q a = true
      · simp [List.countP_cons, hq, ih]
        omega
      · have hqfalse : q a = false := Bool.eq_false_of_ne_true hq
        simp [List.countP_cons, hqfalse, ih]
        omega

private theorem option_getD_beq_some_of_isSome (o : Option Nat) (d k : Nat)
    (ho : o.isSome = true) : (o.getD d == k) = true ↔ (o == some k) = true := by
  cases o with
  | none => simp at ho
  | some v => simp

private theorem option_getD_beq_default_of_not_isSome (o : Option Nat) (d k : Nat)
    (ho : o.isSome = false) : (o.getD d == k) = true ↔ (d == k) = true := by
  cases o with
  | none => simp
  | some v => simp at ho

private theorem lookup_isSome_iff_mem_fst (es : List (Nat × Nat)) (i : Nat) :
    (es.lookup i).isSome = true ↔ i ∈ es.map Prod.fst := by
  induction es with
  | nil => simp
  | cons q qs ih =>
      rcases q with ⟨j, v⟩
      rw [List.lookup_cons]
      by_cases hij : i == j
      · have hijEq : i = j := beq_iff_eq.mp hij
        simp [hijEq]
      · have hneq : i ≠ j := by intro h; exact hij (beq_iff_eq.mpr h)
        simp [hij, hneq, ih]

private theorem nodup_filter_bool {α : Type} (l : List α) (p : α → Bool) (hn : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.nodup_cons] at hn
      rcases hn with ⟨hanot, htnd⟩
      by_cases ha : p a = true
      · rw [List.filter_cons_of_pos ha]
        exact List.nodup_cons.mpr ⟨by
          intro h
          exact hanot (List.mem_filter.mp h).1, ih htnd⟩
      · rw [List.filter_cons_of_neg ha]
        exact ih htnd

private theorem good_perm_ps (n : Nat) (es : List (Nat × Nat)) :
    ((List.range n).filter (fun i => (es.lookup i).isSome)).Perm
      (((es.map Prod.fst).eraseDups.filter (fun i => i < n))) := by
  let good := (List.range n).filter (fun i => (es.lookup i).isSome)
  let ps := (es.map Prod.fst).eraseDups.filter (fun i => i < n)
  have hgoodnd : good.Nodup := nodup_filter_bool _ _ List.nodup_range
  have hpsnd : ps.Nodup := nodup_filter_bool _ _ (eraseDups_nodup _)
  have hmem : ∀ i, i ∈ good ↔ i ∈ ps := by
    intro i
    simp only [good, ps, List.mem_filter, List.mem_range, List.mem_eraseDups]
    rw [lookup_isSome_iff_mem_fst]
    constructor
    · rintro ⟨hi, him⟩
      exact ⟨him, by simpa using hi⟩
    · rintro ⟨him, hi⟩
      exact ⟨by simpa using hi, him⟩
  apply List.perm_iff_count.mpr
  intro i
  by_cases hi : i ∈ good
  · have hips : i ∈ ps := (hmem i).mp hi
    rw [count_eq_one_of_mem_nodup hgoodnd hi, count_eq_one_of_mem_nodup hpsnd hips]
  · have hips : i ∉ ps := by intro h; exact hi ((hmem i).mpr h)
    rw [List.count_eq_zero.mpr hi, List.count_eq_zero.mpr hips]

private theorem mass_eq_count (n d : Nat) (es : List (Nat × Nat)) (k : Nat) :
    B.mass (n, d, es) k = (B.expand (n, d, es)).count k := by
  unfold B bag
  dsimp only
  rw [count_map_eq_countP_map]
  let ps := (es.map Prod.fst).eraseDups.filter (fun i => i < n)
  let good := (List.range n).filter (fun i => (es.lookup i).isSome)
  let bad := (List.range n).filter (fun i => !(es.lookup i).isSome)
  have hperm : good.Perm ps := good_perm_ps n es
  have hsplit := countP_split (List.range n) (fun i => (es.lookup i).getD d == k)
      (fun i => (es.lookup i).isSome)
  have hgood : good.countP (fun i => (es.lookup i).getD d == k) =
      ps.countP (fun i => es.lookup i == some k) := by
    rw [List.Perm.countP_eq (fun i => (es.lookup i).getD d == k) hperm]
    apply List.countP_congr
    intro i hi
    have himap : i ∈ es.map Prod.fst := by
      have hi' : i ∈ (es.map Prod.fst).eraseDups := (List.mem_filter.mp hi).1
      exact (List.mem_eraseDups.mp hi')
    have hisome : (es.lookup i).isSome = true := (lookup_isSome_iff_mem_fst es i).mpr himap
    exact option_getD_beq_some_of_isSome (es.lookup i) d k hisome
  have hbad : bad.countP (fun i => (es.lookup i).getD d == k) =
      (if k == d then bad.length else 0) := by
    have hcong : bad.countP (fun i => (es.lookup i).getD d == k) =
        bad.countP (fun i => d == k) := by
      apply List.countP_congr
      intro i hi
      have hnot : (es.lookup i).isSome = false := by
        have ht := (List.mem_filter.mp hi).2
        revert ht
        cases (es.lookup i).isSome <;> simp
      exact option_getD_beq_default_of_not_isSome (es.lookup i) d k hnot
    rw [hcong]
    by_cases hkd : k == d
    · have hkdeq : k = d := beq_iff_eq.mp hkd
      subst k
      simp
    · have hdkfalse : (d == k) = false := by
        apply Bool.eq_false_of_ne_true
        intro h
        exact hkd (beq_iff_eq.mpr (beq_iff_eq.mp h).symm)
      simp [hdkfalse, hkd]
  have hlen_split := countP_split (List.range n) (fun _ : Nat => true)
      (fun i => (es.lookup i).isSome)
  have hlen_eq : n = good.length + bad.length := by
    simpa [good, bad] using hlen_split
  have hbadlen : bad.length = n - ps.length := by
    have hlenp := hperm.length_eq
    omega
  rw [hsplit, hgood, hbad]
  by_cases hkd : k == d
  · have hkdeq : k = d := beq_iff_eq.mp hkd
    subst k
    simp [ps, hbadlen]
    omega
  · simp [ps, hkd]

private theorem mem_keys_of_mem_expand (n d : Nat) (es : List (Nat × Nat)) (k : Nat) :
    k ∈ B.expand (n, d, es) → k ∈ B.keys (n, d, es) := by
  intro h
  change k ∈ (List.range n).map (fun i => (es.lookup i).getD d) at h
  rw [List.mem_map] at h
  rcases h with ⟨i, hi, rfl⟩
  rcases hlk : es.lookup i with _ | v
  · change d ∈ d :: es.map Prod.snd
    simp
  · change v ∈ d :: es.map Prod.snd
    simp only [List.mem_cons]
    right
    rw [List.lookup_eq_some_iff] at hlk
    rcases hlk with ⟨l1, l2, hes, hno⟩
    rw [hes]
    simp

/-! ## Goal -/

theorem sound_SelfPairs : SelfPairs.Goal := by
  intro p
  rcases p with ⟨n, d, es⟩
  have hmass : ∀ k, B.mass (n, d, es) k = (B.expand (n, d, es)).count k := by
    intro k
    exact mass_eq_count n d es k
  have hgeneric := pairs_generic (B.expand (n, d, es))
  have hlive : (((live B (n, d, es)).map fun k => comb (B.mass (n, d, es) k)).sum) =
      (((B.keys (n, d, es)).eraseDups.map fun k => comb (B.mass (n, d, es) k)).sum) := by
    unfold live
    symm
    apply sum_map_filter_eq
    intro k hk hp
    have hzero : B.mass (n, d, es) k = 0 := by
      by_cases hpos : 0 < B.mass (n, d, es) k
      · have ht : decide (0 < B.mass (n, d, es) k) = true := by simp [hpos]
        simp [ht] at hp
      · omega
    rw [hzero, comb_zero]
  have hkeys : (((B.keys (n, d, es)).eraseDups.map fun k => comb (B.mass (n, d, es) k)).sum) =
      (((B.expand (n, d, es)).eraseDups.map fun k => comb ((B.expand (n, d, es)).count k)).sum) := by
    have hm : (((B.keys (n, d, es)).eraseDups.map fun k => comb (B.mass (n, d, es) k)).sum) =
        (((B.keys (n, d, es)).eraseDups.map fun k => comb ((B.expand (n, d, es)).count k)).sum) := by
      apply congrArg List.sum
      apply List.map_congr_left
      intro k hk
      rw [hmass]
    rw [hm]
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro q hq hqnot
      have hqnot' : q ∉ B.expand (n, d, es) := by
        intro hqe
        exact hqnot (by rw [List.mem_eraseDups]; exact hqe)
      simp [List.count_eq_zero.mpr hqnot', comb_zero]
    · intro q hq hqnot
      exfalso
      apply hqnot
      rw [List.mem_eraseDups]
      exact mem_keys_of_mem_expand n d es q (List.mem_eraseDups.mp hq)
  change ((live B (n, d, es)).map (fun k => comb (B.mass (n, d, es) k))).sum =
    pairCount (B.expand (n, d, es))
  rw [hlive, hkeys, hgeneric]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_SelfPairs
