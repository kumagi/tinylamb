import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_IntersectAll

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DeletedOrdinals

/-! ## Generic list lemmas -/

private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (l : List α) : l.eraseDups.Nodup := by
  let motive : Nat → Prop := fun n => ∀ l : List α, l.length = n → l.eraseDups.Nodup
  have h : motive l.length := Nat.strongRecOn l.length (fun n ih l hl => by
    cases l with
    | nil => simp [List.eraseDups_nil]
    | cons b bs =>
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro hb
          rw [List.mem_eraseDups] at hb
          have hb' := (List.mem_filter.mp hb).2
          simp at hb'
        · apply ih (bs.filter (fun x => !x == b)).length
          · have hle := List.length_filter_le (fun x : α => !x == b) bs
            simp only [List.length_cons] at hl
            omega
          · rfl)
  exact h l rfl

private theorem count_flatMap_replicate (xs : List Nat) (f : Nat → Nat) (k : Nat) :
    (xs.flatMap (fun x => List.replicate (f x) x)).count k =
      (xs.map (fun x => if x = k then f x else 0)).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, List.count_replicate, beq_iff_eq]

private theorem sum_eq_single_of_nodup (xs : List Nat) (hn : xs.Nodup) (k : Nat) (f : Nat → Nat) :
    (xs.map (fun x => if x = k then f x else 0)).sum = if k ∈ xs then f k else 0 := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.map_cons, List.sum_cons]
      by_cases hxk : x = k
      · subst x
        have hnot : k ∉ xs := hn.1
        simpa [hnot] using (ih hn.2)
      · have hi := ih hn.2
        have hkx : k ≠ x := fun h => hxk h.symm
        by_cases hk : k ∈ xs
        · simp [hxk, hkx, hk, hi]
        · simp [hxk, hkx, hk, hi]

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
    (bag.expand (xs, ds)).count k = bag.mass (xs, ds) k := by
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

private theorem mem_keys_of_mem_expand (xs ds : List Nat) (k : Nat) :
    k ∈ bag.expand (xs, ds) → k ∈ bag.keys (xs, ds) := by
  intro h
  change k ∈ (xs.zipIdx.filter (fun e => !(ds.contains e.2))).map Prod.fst at h
  rw [List.mem_map] at h
  rcases h with ⟨e, he, rfl⟩
  change e.1 ∈ xs
  have hezip : e ∈ xs.zipIdx := (List.mem_filter.mp he).1
  rw [← List.zipIdx_map_fst 0 xs]
  exact List.mem_map.mpr ⟨e, hezip, rfl⟩

private theorem nodup_live (p : List Nat × List Nat) : (live bag p).Nodup := by
  unfold live
  exact List.Sublist.nodup List.filter_sublist (eraseDups_nodup (bag.keys p))

private theorem mem_live_of_mass_pos (p : List Nat × List Nat) (k : Nat)
    (h : 0 < bag.mass p k) : k ∈ live bag p := by
  have hc : 0 < (bag.expand p).count k := by
    rw [count_expand_eq_mass]
    exact h
  have hmem : k ∈ bag.expand p := List.count_pos_iff.mp hc
  obtain ⟨xs, ds⟩ := p
  have hkeys : k ∈ bag.keys (xs, ds) := mem_keys_of_mem_expand xs ds k hmem
  unfold live
  rw [List.mem_filter]
  constructor
  · rw [List.mem_eraseDups]
    exact hkeys
  · simp [h]

/-! ## Goal -/

theorem sound_IntersectAll : IntersectAll.Goal := by
  intro a c k
  obtain ⟨xs, ds⟩ := a
  obtain ⟨ys, es⟩ := c
  rw [count_flatMap_replicate]
  rw [sum_eq_single_of_nodup (live bag (xs, ds)) (nodup_live (xs, ds))]
  rw [count_expand_eq_mass, count_expand_eq_mass]
  by_cases hk : k ∈ live bag (xs, ds)
  · simp [hk]
  · have hm0 : bag.mass (xs, ds) k = 0 := by
      by_cases hz : bag.mass (xs, ds) k = 0
      · exact hz
      · exact False.elim (hk (mem_live_of_mass_pos (xs, ds) k (Nat.pos_of_ne_zero hz)))
    simp [hk, hm0]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_IntersectAll
