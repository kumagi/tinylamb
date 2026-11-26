import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `DeletedOrdinals.DeleteFootprint` — `∀ xs ds, weighted bag (xs,ds) (fun _ => 1) =
  xs.length-(ds.eraseDups.filter (· < xs.length)).length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_DeleteFootprint

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.DeletedOrdinals

/-! ## Small-input truth checks (empty parts, duplicate deletes, ordinals past the end). -/

example : weighted bag (([7, 7, 7] : List Nat), [0, 1]) (fun _ => 1) =
    3 - ([0, 1].eraseDups.filter (· < 3)).length := by
  decide

example : weighted bag (([5, 7, 5] : List Nat), [0, 2, 2, 9]) (fun _ => 1) =
    3 - ([0, 2, 2, 9].eraseDups.filter (· < 3)).length := by
  decide

example : weighted bag (([3, 1, 2, 1] : List Nat), [0, 0, 3, 8]) (fun _ => 1) =
    4 - ([0, 0, 3, 8].eraseDups.filter (· < 4)).length := by
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

/-! ## Summing multiplicities over the key support -/

private theorem sum_map_add (l : List Nat) (f g : Nat → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_map_bool_eq_countP (l : List Nat) (p : Nat → Bool) :
    (l.map (fun k => if p k then 1 else 0)).sum = l.countP p := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : p x = true <;> simp [hx, ih, Nat.add_comm]

private theorem countP_eq_one_of_nodup_mem {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (hl : l.Nodup) (a : α) (ha : a ∈ l) :
    l.countP (fun k => a == k) = 1 := by
  rw [List.countP_eq_length_filter]
  have hnd : (l.filter (fun k => a == k)).Nodup := nodup_filter _ hl
  have hperm : (l.filter (fun k => a == k)).Perm [a] := by
    apply (List.perm_ext_iff_of_nodup hnd (by simp)).mpr
    intro k
    constructor
    · intro hk
      have hk' := List.mem_filter.mp hk
      have hake : a = k := eq_of_beq hk'.2
      simp [hake]
    · intro hk
      have hka : k = a := by simpa using hk
      subst k
      exact List.mem_filter.mpr ⟨ha, by simp⟩
  simpa using hperm.length_eq

private theorem sum_map_beq_eq_one (l : List Nat) (hl : l.Nodup) (a : Nat) (ha : a ∈ l) :
    (l.map (fun k => if a == k then 1 else 0)).sum = 1 := by
  rw [sum_map_bool_eq_countP]
  exact countP_eq_one_of_nodup_mem l hl a ha

private theorem length_filter_cons_fst (e : Nat × Nat) (z : List (Nat × Nat)) (k : Nat) :
    ((e :: z).filter (fun x : Nat × Nat => x.1 == k)).length =
      (z.filter (fun x : Nat × Nat => x.1 == k)).length + if e.1 == k then 1 else 0 := by
  by_cases h : (e.1 == k) = true <;> simp [h]

private theorem sum_map_zero (l : List Nat) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem sum_filter_fst_eq_length (z : List (Nat × Nat)) (l : List Nat)
    (hl : l.Nodup) (hcover : ∀ e ∈ z, e.1 ∈ l) :
    (l.map (fun k => (z.filter (fun e : Nat × Nat => e.1 == k)).length)).sum =
      z.length := by
  induction z with
  | nil => simpa using sum_map_zero l
  | cons e z ih =>
      have hcover' : ∀ x ∈ z, x.1 ∈ l := by
        intro x hx
        exact hcover x (by simp [hx])
      have hihe := ih hcover'
      simp only [List.length_cons]
      have hsum :
          (l.map (fun k => ((e :: z).filter (fun x : Nat × Nat => x.1 == k)).length)).sum =
            (l.map (fun k => (z.filter (fun x : Nat × Nat => x.1 == k)).length +
              if e.1 == k then 1 else 0)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro k hk
        exact length_filter_cons_fst e z k
      rw [hsum, sum_map_add, hihe,
        sum_map_beq_eq_one l hl e.1 (hcover e (by simp))]

private theorem sum_filter_fst_eq_length_filter (z : List (Nat × Nat)) (l : List Nat)
    (hl : l.Nodup) (hcover : ∀ e ∈ z, e.1 ∈ l) (f : Nat × Nat → Bool) :
    (l.map (fun k => (z.filter (fun e : Nat × Nat => e.1 == k && f e)).length)).sum =
      (z.filter f).length := by
  rw [← sum_filter_fst_eq_length (z.filter f) l hl]
  · apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [List.filter_filter]
  · intro e he
    exact hcover e (List.mem_filter.mp he).1

/-- Positions in a zipIdx carry values from the original list. -/
private theorem zipIdx_fst_mem_aux (xs : List Nat) :
    ∀ n, ∀ e ∈ xs.zipIdx n, e.1 ∈ xs := by
  induction xs with
  | nil => intro n e he; simp at he
  | cons x xs ih =>
      intro n e he
      rw [List.zipIdx_cons] at he
      rcases List.mem_cons.mp he with heq | he
      · rw [heq]
        simp
      · have hmem := ih (n + 1) e he
        simp [hmem]

private theorem weighted_eq_filter_len (xs ds : List Nat) :
    weighted bag (xs, ds) (fun _ => 1) =
      (xs.zipIdx.filter (fun e : Nat × Nat => !ds.contains e.2)).length := by
  unfold weighted
  change (xs.eraseDups.map (fun k => bag.mass (xs, ds) k * 1)).sum = _
  simp only [Nat.mul_one]
  simp only [mass_eq_filter]
  exact sum_filter_fst_eq_length_filter xs.zipIdx xs.eraseDups (nodup_eraseDups xs)
    (by
      intro e he
      rw [List.mem_eraseDups]
      exact zipIdx_fst_mem_aux xs 0 e he)
    (fun e : Nat × Nat => !ds.contains e.2)

/-! ## Deleted ordinals -/

private theorem zipIdx_map_snd_eq_range'_aux (xs : List Nat) :
    ∀ n, (xs.zipIdx n).map Prod.snd = List.range' n xs.length := by
  induction xs with
  | nil => intro n; simp
  | cons x xs ih =>
      intro n
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.length_cons]
      rw [ih (n + 1)]
      rw [← List.range'_succ]

private theorem zipIdx_map_snd_eq_range (xs : List Nat) :
    (xs.zipIdx).map Prod.snd = List.range xs.length := by
  simp [List.range'_eq_map_range]

private theorem deleted_count_eq_zipIdx_count (xs ds : List Nat) :
    (ds.eraseDups.filter (fun i => i < xs.length)).length =
      xs.zipIdx.countP (fun e : Nat × Nat => ds.contains e.2) := by
  rw [← List.countP_eq_length_filter]
  have h := countP_eraseDups_eq ds (List.range xs.length) List.nodup_range
      (fun i => i < xs.length) (by intro i; rw [List.mem_range]; simp)
  calc
    ds.eraseDups.countP (fun i => i < xs.length) =
        (List.range xs.length).countP (fun i => ds.contains i) := h
    _ = (xs.zipIdx.map Prod.snd).countP (fun i => ds.contains i) := by
      rw [zipIdx_map_snd_eq_range]
    _ = xs.zipIdx.countP (fun e : Nat × Nat => ds.contains e.2) :=
      countP_map_snd xs.zipIdx (fun i => ds.contains i)

/-! ## Goal -/

theorem sound_DeleteFootprint : DeletedOrdinals.DeleteFootprint.Goal := by
  intro xs ds
  rw [weighted_eq_filter_len]
  calc
    (xs.zipIdx.filter (fun e : Nat × Nat => !ds.contains e.2)).length =
        xs.zipIdx.countP (fun e : Nat × Nat => !ds.contains e.2) := by
      rw [List.countP_eq_length_filter]
    _ = xs.zipIdx.length - xs.zipIdx.countP (fun e : Nat × Nat => ds.contains e.2) := by
      have h := countP_compl xs.zipIdx (fun e : Nat × Nat => ds.contains e.2)
      omega
    _ = xs.length - (ds.eraseDups.filter (fun i => i < xs.length)).length := by
      rw [List.length_zipIdx, ← deleted_count_eq_zipIdx_count]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_DeletedOrdinals_DeleteFootprint
