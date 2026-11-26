import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_PrefixOfLive

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DeletedOrdinals

/-! Small-input truth checks (empty parts, duplicate deletes, ordinals past the end). -/

example : bag.mass (([7, 7, 7] : List Nat), [0, 1]) 7 =
    bag.mass (([7] : List Nat), [0, 1]) 7 + bag.mass (([7, 7] : List Nat), [0]) 7 := by
  decide

example : bag.mass (([5, 7, 5] : List Nat), [0, 2, 2]) 5 =
    bag.mass (([] : List Nat), [0, 2, 2]) 5 +
      bag.mass (([5, 7, 5] : List Nat), [0, 2, 2]) 5 := by
  decide

example : bag.mass (([5, 7] : List Nat), [0, 1, 5]) 5 =
    bag.mass (([5, 7] : List Nat), [0, 1, 5]) 5 + bag.mass (([] : List Nat), [0]) 5 := by
  decide

example : bag.mass (([5, 7, 5, 7, 5] : List Nat), [1, 1, 3, 9]) 5 =
    bag.mass (([5, 7, 5] : List Nat), [1, 1, 3, 9]) 5 +
      bag.mass (([7, 5] : List Nat), [0, 6]) 5 := by
  decide

/-! ## Generic list facts -/

private theorem nodup_eraseDups_aux {α : Type} [BEq α] [LawfulBEq α] :
    ∀ n : Nat, ∀ xs : List α, xs.length = n → xs.eraseDups.Nodup := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro xs hlen
      cases xs with
      | nil => exact List.nodup_nil
      | cons x xs =>
          rw [List.eraseDups_cons, List.nodup_cons]
          have hlt : (xs.filter fun y => !y == x).length < n := by
            have hle := List.length_filter_le (fun y => !y == x) xs
            simp only [List.length_cons] at hlen
            omega
          constructor
          · intro hmem
            rw [List.mem_eraseDups, List.mem_filter] at hmem
            obtain ⟨_, hne⟩ := hmem
            simp at hne
          · exact ih _ hlt _ rfl

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.eraseDups.Nodup := nodup_eraseDups_aux xs.length xs rfl

private theorem nodup_filter {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) :
    (l.filter p).Nodup := by
  rw [List.nodup_iff_pairwise_ne] at h ⊢
  exact List.Pairwise.filter p h

private theorem countP_compl {α : Type} (l : List α) (p : α → Bool) :
    l.countP p + l.countP (fun x => !p x) = l.length := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [h] at ih ⊢ <;> omega

private theorem countP_map_snd (l : List (Nat × Nat)) (q : Nat → Bool) :
    (l.map Prod.snd).countP q = l.countP (fun e => q e.2) := by
  rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
  induction l with
  | nil => simp
  | cons e es ih =>
      by_cases h : q e.2 = true <;> simp [h, ih]

private theorem filter_pred_congr {α : Type} (l : List α) (p q : α → Bool)
    (h : ∀ x ∈ l, p x = q x) : l.filter p = l.filter q := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.filter_cons]
      have hpq := h x (by simp)
      have htail := ih (by intro y hy; exact h y (by simp [hy]))
      simp [hpq, htail]

private theorem filter_map_swap {α β : Type} (l : List α) (f : α → β) (p : β → Bool) :
    (l.map f).filter p = (l.filter (fun x => p (f x))).map f := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.filter_cons]
      by_cases h : p (f x) = true <;> simp [h, ih]

/-! ## Positions carrying a given key -/

private theorem zipIdx_snd_nodup (l : List Nat) (n : Nat) :
    ((l.zipIdx n).map Prod.snd).Nodup := by
  induction l generalizing n with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        rcases List.mem_map.mp hmem with ⟨e, he, heq⟩
        obtain ⟨y, i⟩ := e
        simp only at heq
        subst i
        obtain ⟨hlo, hi, hval⟩ := List.mem_zipIdx (k := n + 1) he
        omega
      · simpa using ih (n + 1)

private theorem idxs_nodup (xs : List Nat) (k : Nat) :
    ((xs.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map Prod.snd).Nodup := by
  have hsub : ((xs.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map Prod.snd).Sublist
      (xs.zipIdx.map Prod.snd) :=
    List.Sublist.map Prod.snd List.filter_sublist
  exact hsub.nodup (zipIdx_snd_nodup xs 0)

private theorem mem_idxs_iff_getElem? (xs : List Nat) (k i : Nat) :
    i ∈ ((xs.zipIdx.filter (fun e : Nat × Nat => e.1 == k)).map Prod.snd) ↔
      xs[i]? = some k := by
  constructor
  · intro h
    rcases List.mem_map.mp h with ⟨e, he, heq⟩
    obtain ⟨x, j⟩ := e
    simp only at heq
    subst j
    have hf := List.mem_filter.mp he
    have hpred : (x == k) = true := hf.2
    have hzip := List.mem_zipIdx (k := 0) hf.1
    have ha : i < xs.length := by simpa using hzip.2.1
    have hx : x = xs[i] := by simpa using hzip.2.2
    have hxk : x = k := eq_of_beq hpred
    rw [List.getElem?_eq_getElem ha]
    rw [← hx, hxk]
  · intro hget
    obtain ⟨ha, hval⟩ := List.getElem?_eq_some_iff.mp hget
    apply List.mem_map.mpr
    refine ⟨(k, i), ?_, rfl⟩
    apply List.mem_filter.mpr
    constructor
    · have hzget : (xs.zipIdx)[i]? = some (k, i) := by
        rw [List.getElem?_zipIdx]
        simp [List.getElem?_eq_getElem ha, hval]
      obtain ⟨hidx, hidxval⟩ := List.getElem?_eq_some_iff.mp hzget
      rw [← hidxval]
      exact List.getElem_mem hidx
    · simp

private theorem count_eq_zipIdx_filter_length (xs : List Nat) (k o : Nat) :
    ((xs.zipIdx o).filter (fun e : Nat × Nat => e.1 == k)).length = xs.count k := by
  induction xs generalizing o with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons]
      by_cases hx : (x == k) = true
      · simp [hx, ih (o + 1), List.count_cons]
      · simp [hx, ih (o + 1), List.count_cons]

private theorem countP_eraseDups_eq (ds l : List Nat) (hl : l.Nodup)
    (P : Nat → Bool) (hP : ∀ i : Nat, P i ↔ i ∈ l) :
    ds.eraseDups.countP P = l.countP (fun i => ds.contains i) := by
  have hnd1 : (ds.eraseDups.filter P).Nodup := nodup_filter P (nodup_eraseDups ds)
  have hnd2 : (l.filter (fun i => ds.contains i)).Nodup :=
    nodup_filter (fun i => ds.contains i) hl
  have hperm : (ds.eraseDups.filter P).Perm (l.filter (fun i => ds.contains i)) := by
    apply (List.perm_ext_iff_of_nodup hnd1 hnd2).mpr
    intro i
    simp only [List.mem_filter, List.mem_eraseDups, List.contains_iff_mem]
    rw [hP i]
    constructor <;> intro h <;> exact ⟨h.2, h.1⟩
  simpa only [List.countP_eq_length_filter] using hperm.length_eq

/-- The multiplicity kernel counts the live physical positions of a key. -/
private theorem mass_eq_filter (xs ds : List Nat) (k : Nat) :
    bag.mass (xs, ds) k =
      (xs.zipIdx.filter (fun e : Nat × Nat => e.1 == k && !ds.contains e.2)).length := by
  let L := xs.zipIdx.filter (fun e : Nat × Nat => e.1 == k)
  have hcount : xs.count k = L.length := by
    dsimp [L]
    exact (count_eq_zipIdx_filter_length xs k 0).symm
  have hP : ∀ i : Nat, (xs[i]? == some k) ↔ i ∈ L.map Prod.snd := by
    intro i
    rw [beq_iff_eq]
    dsimp [L]
    exact (mem_idxs_iff_getElem? xs k i).symm
  have hdel : ds.eraseDups.countP (fun i => xs[i]? == some k) =
      L.countP (fun e => ds.contains e.2) := by
    have h := countP_eraseDups_eq ds (L.map Prod.snd)
      (by dsimp [L]; exact idxs_nodup xs k) (fun i => xs[i]? == some k) hP
    rw [h, countP_map_snd]
  have hsplit := countP_compl L (fun e => ds.contains e.2)
  simp only [List.countP_eq_length_filter] at hsplit
  change xs.count k - ds.eraseDups.countP (fun i => xs[i]? == some k) = _
  rw [hcount, hdel]
  have hfilt : (xs.zipIdx.filter (fun e : Nat × Nat => e.1 == k && !ds.contains e.2)) =
      L.filter (fun e => !ds.contains e.2) := by
    dsimp [L]
    rw [List.filter_filter]
    apply filter_pred_congr
    intro e he
    rw [Bool.and_comm]
  rw [hfilt]
  simp only [List.countP_eq_length_filter]
  omega

/-! ## Absolute ordinals (delete lists shifted by a prefix length) -/

private def shifted (ds : List Nat) (o : Nat) : List Nat :=
  (ds.filter (fun i => o ≤ i)).map (fun i => i - o)

private theorem shifted_zero (ds : List Nat) : shifted ds 0 = ds := by
  unfold shifted
  have hf : ds.filter (fun i => 0 ≤ i) = ds := by
    apply List.filter_eq_self.mpr
    intro i hi
    simp
  rw [hf]
  simp

private theorem mem_shifted (ds : List Nat) (o j : Nat) :
    j ∈ shifted ds o ↔ o + j ∈ ds := by
  unfold shifted
  constructor
  · intro h
    rcases List.mem_map.mp h with ⟨i, hi, hij⟩
    have hi' := List.mem_filter.mp hi
    have hle : o ≤ i := by simpa using hi'.2
    have hieq : i = o + j := by
      calc
        i = (i - o) + o := (Nat.sub_add_cancel hle).symm
        _ = j + o := by rw [hij]
        _ = o + j := Nat.add_comm j o
    simpa [hieq] using hi'.1
  · intro h
    refine List.mem_map.mpr ⟨o + j, ?_, ?_⟩
    · exact List.mem_filter.mpr ⟨h, by simp⟩
    · exact Nat.add_sub_cancel_left o j

private theorem contains_shifted (ds : List Nat) (o j : Nat) :
    (shifted ds o).contains j = ds.contains (o + j) := by
  have hiff : ((shifted ds o).contains j = true) ↔ (ds.contains (o + j) = true) := by
    rw [List.contains_iff_mem, List.contains_iff_mem, mem_shifted]
  cases h1 : (shifted ds o).contains j <;> cases h2 : ds.contains (o + j) <;> simp_all

private def liveIdx (xs ds : List Nat) (k o : Nat) : Nat :=
  (xs.zipIdx.filter (fun e : Nat × Nat => e.1 == k && !ds.contains (o + e.2))).length

private theorem mass_eq_liveIdx (xs ds : List Nat) (k o : Nat) :
    bag.mass (xs, shifted ds o) k = liveIdx xs ds k o := by
  rw [mass_eq_filter]
  unfold liveIdx
  apply congrArg List.length
  apply filter_pred_congr
  intro e he
  rw [contains_shifted ds o e.2]

/-! ## Splitting the position stream at a prefix boundary -/

private theorem zipIdx_add_offset (xs : List Nat) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun e : Nat × Nat => (e.1, m + e.2)) := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      have hnat : n + m + 1 = (n + 1) + m := by omega
      rw [hnat, ih (n + 1)]
      simp [Nat.add_comm]

private theorem zipIdx_add (xs : List Nat) (n : Nat) :
    xs.zipIdx n = (xs.zipIdx).map (fun e : Nat × Nat => (e.1, n + e.2)) := by
  simpa using zipIdx_add_offset xs 0 n

private theorem zipIdx_take_drop (xs : List Nat) (t : Nat) :
    xs.zipIdx =
      (xs.take t).zipIdx ++ (xs.drop t).zipIdx.map (fun e : Nat × Nat => (e.1, t + e.2)) := by
  have happ : xs.take t ++ xs.drop t = xs := List.take_append_drop t xs
  calc
    xs.zipIdx = (xs.take t ++ xs.drop t).zipIdx := by rw [happ]
    _ = (xs.take t).zipIdx ++ (xs.drop t).zipIdx ((xs.take t).length) := by
      rw [List.zipIdx_append]
      simp
    _ = (xs.take t).zipIdx ++ (xs.drop t).zipIdx.map (fun e : Nat × Nat => (e.1, t + e.2)) := by
      by_cases hle : t ≤ xs.length
      · rw [List.length_take_of_le hle, zipIdx_add (xs.drop t) t]
      · have hdrop : xs.drop t = [] := List.drop_eq_nil_iff.mpr (by omega)
        simp [hdrop]

private theorem filter_map_shift_length (l : List (Nat × Nat)) (ds : List Nat) (k t : Nat) :
    ((l.map (fun e : Nat × Nat => (e.1, t + e.2))).filter
        (fun e : Nat × Nat => e.1 == k && !ds.contains (0 + e.2))).length =
      (l.filter (fun e : Nat × Nat => e.1 == k && !ds.contains (t + e.2))).length := by
  rw [filter_map_swap]
  simp only [List.length_map]
  apply congrArg List.length
  apply filter_pred_congr
  intro e he
  simp

/-! ## Goal -/

theorem sound_PrefixOfLive : DeletedOrdinals.PrefixOfLive.Goal := by
  intro xs ds t k
  have hpref : bag.mass (xs.take t, ds) k = liveIdx (xs.take t) ds k 0 := by
    simpa [shifted_zero] using mass_eq_liveIdx (xs.take t) ds k 0
  have hsuf : bag.mass (xs.drop t, (ds.filter (fun i => i ≥ t)).map (fun i => i - t)) k =
      liveIdx (xs.drop t) ds k t := by
    simpa [shifted] using mass_eq_liveIdx (xs.drop t) ds k t
  have hfull : bag.mass (xs, ds) k = liveIdx xs ds k 0 := by
    simpa [shifted_zero] using mass_eq_liveIdx xs ds k 0
  rw [hpref, hsuf, hfull]
  unfold liveIdx
  rw [zipIdx_take_drop xs t]
  simp only [List.filter_append, List.length_append]
  rw [filter_map_shift_length] <;> simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_PrefixOfLive
