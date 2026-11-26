import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `DeletedOrdinals.OuterWeight` — LEFT JOIN contributes one unmatched right placeholder
  or the complete right multiplicity.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_OuterWeight

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DeletedOrdinals

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
            have hamem' : a ∈ ks.filter (fun b => !(b == k)) := by simpa [rest] using hamem
            have hff := (List.mem_filter.mp hamem').2
            simp [h] at hff
          have hrestc : rest.count a = ks.count a := by
            simp [rest, hane]
          have hka : (k == a) = false := Bool.eq_false_of_ne_true (by
            intro h
            have hkaeq : k = a := beq_iff_eq.mp h
            have haeq : (a == k) = true := beq_iff_eq.mpr hkaeq.symm
            simp [haeq] at hane)
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
        rw [hks]
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
            have hf := (List.mem_filter.mp hm).2
            have hab : (a == b) = true := beq_iff_eq.mpr (beq_iff_eq.mp hba).symm
            simp [hab] at hf
          have hz' : (List.filter (fun c : α => !(c == b)) bs).eraseDups.count a = 0 := by
            simpa [rest] using hzero
          simp [hba, hz']
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

private theorem length_filter_eq_sum (l : List Nat) (p : Nat → Bool) :
    (l.filter p).length = (l.map fun x => if p x then 1 else 0).sum := by
  induction l with
  | nil => simp
  | cons x l ih => by_cases hp : p x <;> simp [hp, ih, Nat.add_comm]

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

/-! ## DeletedOrdinals bag facts -/

private theorem contains_eraseDups_eq (ds : List Nat) (i : Nat) :
    ds.contains i = ds.eraseDups.contains i := by
  apply Bool.eq_iff_iff.mpr
  simp [List.mem_eraseDups]

private theorem count_map_fst_eq_countP_fst (L : List (Nat × Nat)) (k : Nat) :
    (L.map Prod.fst).count k = L.countP (fun e => e.1 == k) := by
  induction L with
  | nil => simp
  | cons e L ih =>
      rcases e with ⟨a, i⟩
      simp [List.count_cons, List.countP_cons, ih]

private theorem countP_and_not {α : Type} (l : List α) (p q : α → Bool) :
    l.countP (fun a => p a && !q a) + l.countP (fun a => p a && q a) = l.countP p := by
  induction l with
  | nil => simp
  | cons a l ih =>
      by_cases hq : q a = true
      · by_cases hp : p a = true <;> simp [hq, hp, ih] <;> omega
      · by_cases hp : p a = true <;> simp [hq, hp, ih] <;> omega

private theorem countP_or_disjoint {α : Type} (l : List α) (r p q : α → Bool)
    (hd : ∀ a ∈ l, ¬ (p a = true ∧ q a = true)) :
    l.countP (fun a => r a && (p a || q a)) =
      l.countP (fun a => r a && p a) + l.countP (fun a => r a && q a) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      have hd' : ∀ b ∈ l, ¬ (p b = true ∧ q b = true) := by
        intro b hb
        exact hd b (by simp [hb])
      rw [List.countP_cons, List.countP_cons, List.countP_cons, ih hd']
      have hda : ¬ (p a = true ∧ q a = true) := hd a (by simp)
      by_cases hp : p a = true
      · have hqf : q a = false := Bool.eq_false_of_ne_true (by
          intro hq
          exact hda ⟨hp, hq⟩)
        by_cases hr : r a = true <;> simp [hp, hqf, hr] <;> omega
      · have hpf : p a = false := Bool.eq_false_of_ne_true hp
        by_cases hq : q a = true <;> by_cases hr : r a = true <;>
          simp [hpf, hq, hr] <;> omega

private theorem countP_zipIdx_single (xs : List Nat) (s i k : Nat) :
    (xs.zipIdx s).countP (fun e => (e.1 == k) && decide (e.2 = i)) =
      (if s ≤ i && xs[i - s]? == some k then 1 else 0) := by
  induction xs generalizing s with
  | nil => simp
  | cons a xs ih =>
      rw [List.zipIdx_cons, List.countP_cons, ih (s + 1)]
      by_cases hi : i < s
      · have hnot : ¬ s ≤ i := Nat.not_le.mpr hi
        have hnot' : ¬ s + 1 ≤ i := by omega
        have hne : s ≠ i := by omega
        simp [hnot, hnot', hne]
      · have hle : s ≤ i := Nat.le_of_not_gt hi
        by_cases heq : i = s
        · subst i
          simp <;> omega
        · have hlt : s < i := by omega
          have hs1 : s + 1 ≤ i := hlt
          have hne : s ≠ i := by omega
          have hsub : i - s = (i - (s + 1)) + 1 := by omega
          rw [hsub, List.getElem?_cons_succ]
          simp [hne, hs1, hle]

private theorem countP_zipIdx_memD (xs D : List Nat) (k : Nat) (hD : D.Nodup) :
    xs.zipIdx.countP (fun e => (e.1 == k) && D.contains e.2) =
      D.countP (fun i => xs[i]? == some k) := by
  induction D with
  | nil => simp
  | cons i D ih =>
      obtain ⟨hi_not, hDtail⟩ := List.nodup_cons.mp hD
      rw [List.countP_cons]
      rw [show xs.zipIdx.countP (fun e => (e.1 == k) && (i :: D).contains e.2) =
              xs.zipIdx.countP (fun e => (e.1 == k) && (decide (e.2 = i) || D.contains e.2)) by
              apply congrArg (List.countP · xs.zipIdx)
              funext e
              simp]
      rw [countP_or_disjoint xs.zipIdx (fun e => e.1 == k)
          (fun e => decide (e.2 = i)) (fun e => D.contains e.2) (by
            intro e he hboth
            rcases hboth with ⟨h1, h2⟩
            have heqi : e.2 = i := of_decide_eq_true h1
            apply hi_not
            have : e.2 ∈ D := by simpa using h2
            simpa [heqi] using this)]
      rw [countP_zipIdx_single]
      rw [ih hDtail]
      simp
      omega

private theorem count_expand_eq_mass (xs ds : List Nat) (k : Nat) :
    (B.expand (xs, ds)).count k = B.mass (xs, ds) k := by
  change List.count k ((xs.zipIdx.filter (fun e => !(ds.contains e.2))).map Prod.fst) =
    xs.count k - ds.eraseDups.countP (fun i => xs[i]? == some k)
  have hcontains : (xs.zipIdx.filter (fun e => !(ds.contains e.2))) =
      (xs.zipIdx.filter (fun e => !(ds.eraseDups.contains e.2))) := by
    apply List.filter_congr
    intro e he
    rw [contains_eraseDups_eq]
  rw [hcontains]
  rw [count_map_fst_eq_countP_fst]
  rw [List.countP_filter]
  have htotal : xs.count k = xs.zipIdx.countP (fun e => e.1 == k) := by
    calc
      xs.count k = (xs.zipIdx.map Prod.fst).count k := by rw [List.zipIdx_map_fst 0 xs]
      _ = xs.zipIdx.countP (fun e => e.1 == k) := count_map_fst_eq_countP_fst xs.zipIdx k
  have hpart := countP_and_not xs.zipIdx (fun e => e.1 == k)
      (fun e => ds.eraseDups.contains e.2)
  have hB : xs.zipIdx.countP (fun e => (e.1 == k) && ds.eraseDups.contains e.2) =
      ds.eraseDups.countP (fun i => xs[i]? == some k) :=
    countP_zipIdx_memD xs ds.eraseDups k (eraseDups_nodup ds)
  rw [hB, ← htotal] at hpart
  omega

private theorem mass_eq_count (xs ds : List Nat) (k : Nat) :
    B.mass (xs, ds) k = (B.expand (xs, ds)).count k := by
  exact (count_expand_eq_mass xs ds k).symm

private theorem mass_eq_count_pair (p : List Nat × List Nat) (k : Nat) :
    B.mass p k = (B.expand p).count k := by
  rcases p with ⟨xs, ds⟩
  exact mass_eq_count xs ds k

private theorem mem_keys_of_mem_expand (xs ds : List Nat) (k : Nat) :
    k ∈ B.expand (xs, ds) → k ∈ B.keys (xs, ds) := by
  intro h
  change k ∈ (xs.zipIdx.filter (fun e => !(ds.contains e.2))).map Prod.fst at h
  rw [List.mem_map] at h
  rcases h with ⟨e, he, rfl⟩
  change e.1 ∈ xs
  have hezip : e ∈ xs.zipIdx := (List.mem_filter.mp he).1
  rw [← List.zipIdx_map_fst 0 xs]
  exact List.mem_map.mpr ⟨e, hezip, rfl⟩

private theorem mass_zero_of_not_mem_expand (xs ds : List Nat) (k : Nat)
    (h : k ∉ B.expand (xs, ds)) : B.mass (xs, ds) k = 0 := by
  rw [mass_eq_count]
  exact List.count_eq_zero.mpr h

private theorem weighted_eq_expand_sum (p : List Nat × List Nat) (f : Nat → Nat) :
    weighted B p f = ((B.expand p).map f).sum := by
  rcases p with ⟨xs, ds⟩
  let m : Nat → Nat := fun k => B.mass (xs, ds) k
  change ((B.keys (xs, ds)).eraseDups.map fun k => m k * f k).sum =
    ((B.expand (xs, ds)).map f).sum
  have hL : ((B.keys (xs, ds)).eraseDups.map fun k => m k * f k).sum =
      ((B.expand (xs, ds)).eraseDups.map fun k => m k * f k).sum := by
    apply sum_eq_of_mem_or_zero
    · exact eraseDups_nodup _
    · exact eraseDups_nodup _
    · intro x hx hxnot
      have hxnot' : x ∉ B.expand (xs, ds) := by
        intro hxE
        apply hxnot
        rw [List.mem_eraseDups]
        exact hxE
      simp [m, mass_zero_of_not_mem_expand xs ds x hxnot']
    · intro x hx hxnot
      exfalso
      apply hxnot
      rw [List.mem_eraseDups]
      rw [List.mem_eraseDups] at hx
      exact mem_keys_of_mem_expand xs ds x hx
  have hR : ((B.expand (xs, ds)).eraseDups.map fun k => m k * f k).sum =
      ((B.expand (xs, ds)).map f).sum := by
    rw [← dedup_weight_sum f (B.expand (xs, ds))]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    dsimp [m]
    rw [mass_eq_count]
  exact hL.trans hR

/-! ## Goal -/

theorem sound_OuterWeight : DeletedOrdinals.OuterWeight.Goal := by
  change outerWeight B
  intro a c
  let xs := B.expand a
  let ys := B.expand c
  have hleft : weighted B a (fun x => max 1 (B.mass c x)) =
      ((xs.map fun x => max 1 (ys.count x)).sum) := by
    rw [weighted_eq_expand_sum]
    change ((xs.map fun x => max 1 (B.mass c x)).sum) =
      ((xs.map fun x => max 1 (ys.count x)).sum)
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    rw [mass_eq_count_pair]
  have hpairs : (pairs xs ys (· == ·)).length = (xs.map fun x => ys.count x).sum := by
    unfold pairs
    rw [List.length_flatMap]
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    rw [List.length_map]
    simpa [BEq.comm] using (List.count_eq_length_filter (a := x) (l := ys)).symm
  have hunmatched : (xs.filter fun x => !(ys.contains x)).length =
      (xs.map fun x => if ys.count x = 0 then 1 else 0).sum := by
    rw [length_filter_eq_sum]
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    by_cases h : ys.contains x = true
    · have hmem : x ∈ ys := List.contains_iff_mem.mp h
      have hnzero : ys.count x ≠ 0 := by
        intro hz
        exact (List.count_eq_zero.mp hz) hmem
      simp [hnzero, hmem]
    · have hnotmem : x ∉ ys := by
        intro hmem
        exact h (List.contains_iff_mem.mpr hmem)
      have hz : ys.count x = 0 := List.count_eq_zero.mpr hnotmem
      simp [hz, hnotmem]
  have hdecomp : (xs.map fun x => max 1 (ys.count x)).sum =
      (xs.map fun x => ys.count x + if ys.count x = 0 then 1 else 0).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro x hx
    exact max_one_eq (ys.count x)
  rw [hleft, hdecomp, sum_map_add, ← hpairs, ← hunmatched]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_OuterWeight
