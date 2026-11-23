import TinylambOpt.ResearchGoals.ColumnMmapEvictEpoch

/-!
# Pinned-block-safe LRU reclamation: correctness of `evictToTarget`

Models `table/tlcs_file.cpp` (`EvictDecodedCacheForReclaim` /
`EvictIdleColumnarHeadroom`): repeated epochs of `pickVictim` + `evictOne`
reclaim bytes from resident, unpinned entries in `(lruTick, id)` order until
`target` is met or no eligible entry remains.  We show, by fuel induction on
the epoch counter with a four-part invariant (id-trace equality, reclaimed-
bytes conservation, indexwise field preservation, and the LRU ordering
between evicted and still-resident entries):

* the entry trace keeps its length and every per-index field except
  `resident`; pinned entries never lose residency,
* `reclaimed + residentBytes` is conserved (each epoch removes exactly the
  victim's bytes from the resident footprint),
* termination: either the target was reached or no unpinned resident entry
  remains (fuel suffices because each epoch consumes one eligible entry),
* total-order LRU: any evicted entry is LRU-before every entry that stays
  resident and unpinned.

`pickVictim` is characterized by a fold lemma over the left fold.
-/

namespace TinylambOpt.ResearchProofs.ColumnMmapEvictEpoch
open TinylambOpt.ResearchGoals.ColumnMmapEvictEpoch

/-! ## Boolean and LRU-order helpers -/

private theorem ite_evict_pos {vid : Nat} {e : Entry} (h : e.id = vid) :
    (if e.id == vid then { e with resident := false } else e) = { e with resident := false } :=
  if_pos (Iff.of_eq (Nat.beq_eq_true_eq e.id vid) |>.mpr h)

private theorem ite_evict_neg {vid : Nat} {e : Entry} (h : e.id ≠ vid) :
    (if e.id == vid then { e with resident := false } else e) = e := by
  rw [ite_eq_right (fun hc => h (Iff.of_eq (Nat.beq_eq_true_eq e.id vid) |>.mp hc))]

private def elig (e : Entry) : Bool := e.resident && e.pinned == 0

private theorem elig_of {e : Entry} (hr : e.resident = true) (hp : e.pinned = 0) :
    elig e = true := by
  show (e.resident && e.pinned == 0) = true
  rw [hr, hp]
  decide

private theorem elig_imp {e : Entry} (h : elig e = true) : e.resident = true ∧ e.pinned = 0 := by
  rcases e with ⟨i, p, t, b, r⟩
  cases r with
  | false => cases h
  | true =>
      cases p with
      | zero => exact ⟨rfl, rfl⟩
      | succ n => cases h

private theorem earlier_true_iff (a b : Entry) :
    earlierVictim a b = true ↔ a.lruTick < b.lruTick ∨ (a.lruTick = b.lruTick ∧ a.id < b.id) := by
  unfold earlierVictim
  simp only [Bool.or_eq_true, Bool.and_eq_true, Nat.beq_eq_true_eq, decide_eq_true_eq]

private theorem not_earlier_lru {a b : Entry} (h : earlierVictim a b = false) : lruLe b a := by
  have hn : ¬(a.lruTick < b.lruTick ∨ (a.lruTick = b.lruTick ∧ a.id < b.id)) := by
    rintro hc
    have ht := (earlier_true_iff a b).mpr hc
    rw [ht] at h
    exact absurd h (by decide)
  rcases Nat.lt_trichotomy b.lruTick a.lruTick with h1 | h1 | h2
  · exact Or.inl h1
  · exact Or.inr ⟨h1, Nat.le_of_not_lt fun hid => hn (Or.inr ⟨h1.symm, hid⟩)⟩
  · exact absurd (Or.inl h2) hn

private theorem earlier_imp_lru {a b : Entry} (h : earlierVictim a b = true) : lruLe a b := by
  rcases (earlier_true_iff a b).mp h with h1 | ⟨heq, h2⟩
  · exact Or.inl h1
  · exact Or.inr ⟨heq, Nat.le_of_lt h2⟩

private theorem lruLe_not_earlier {a b : Entry} (h : lruLe a b) : earlierVictim b a = false := by
  have hn : ¬(earlierVictim b a = true) := by
    rintro hc
    rcases (earlier_true_iff b a).mp hc with h1 | ⟨heq, h2⟩
    · cases h with
      | inl hl => exact absurd h1 (Nat.not_lt_of_ge (Nat.le_of_lt hl))
      | inr hr => omega
    · cases h with
      | inl hl => omega
      | inr hr => omega
  cases ht : earlierVictim b a with
  | true => exact absurd ht hn
  | false => rfl

private theorem lruLe_refl (a : Entry) : lruLe a a := Or.inr ⟨rfl, Nat.le_refl _⟩

private theorem lruLe_trans {a b c : Entry} (h1 : lruLe a b) (h2 : lruLe b c) : lruLe a c := by
  rcases h1 with t1 | ⟨e1, i1⟩
  · rcases h2 with t2 | ⟨e2, i2⟩
    · exact Or.inl (Nat.lt_trans t1 t2)
    · exact Or.inl (by omega)
  · rcases h2 with t2 | ⟨e2, i2⟩
    · exact Or.inl (by omega)
    · exact Or.inr ⟨e1.trans e2, Nat.le_trans i1 i2⟩

/-! ## `List` membership / indexing helpers -/

private theorem exists_getElem?_some {α : Type} {a : α} :
    ∀ (l : List α), a ∈ l → ∃ i : Nat, l[i]? = some a := by
  intro l
  induction l with
  | nil => intro h; exact absurd h (List.not_mem_nil (a := a))
  | cons x xs ih =>
      intro h
      rcases List.mem_cons.mp h with rfl | h'
      · exact ⟨0, List.getElem?_cons_zero⟩
      · obtain ⟨i, hi⟩ := ih h'
        exact ⟨i+1, by rw [List.getElem?_cons_succ, hi]⟩

private theorem getElem?_some_mem {α : Type} {l : List α} {i : Nat} {a : α}
    (h : l[i]? = some a) : a ∈ l := by
  induction l generalizing i with
  | nil => cases h
  | cons x xs ih =>
      cases i with
      | zero =>
          rw [List.getElem?_cons_zero] at h
          have hx : x = a := Option.some.inj h
          subst hx
          exact List.mem_cons_self
      | succ j =>
          rw [List.getElem?_cons_succ] at h
          exact List.mem_cons_of_mem x (ih h)

private theorem nodup_proj_eq {xs : List Entry} (h : (xs.map Entry.id).Nodup) :
    ∀ {a b : Entry}, a ∈ xs → b ∈ xs → a.id = b.id → a = b := by
  revert h
  induction xs with
  | nil => intro _ a b ha _ _; exact absurd ha (List.not_mem_nil (a := a))
  | cons x xs ih =>
      intro h a b ha hb hid
      rw [List.map_cons, List.nodup_cons] at h
      obtain ⟨hne, htail⟩ := h
      rcases List.mem_cons.mp ha with rfl | ha'
      · rcases List.mem_cons.mp hb with rfl | hb'
        · rfl
        · exfalso
          exact hne (by rw [hid]; exact List.mem_map.mpr ⟨b, hb', rfl⟩)
      · rcases List.mem_cons.mp hb with rfl | hb'
        · exact absurd hid (fun hc => hne (by rw [← hc]; exact List.mem_map.mpr ⟨a, ha', rfl⟩))
        · exact ih htail ha' hb' hid

/-! ## `evictOne` helpers -/

private theorem evictOne_eq_map (vid : Nat) (xs : List Entry) :
    evictOne vid xs =
      xs.map (fun e => if e.id == vid then { e with resident := false } else e) := rfl

private theorem map_entryId_evictOne (vid : Nat) (xs : List Entry) :
    (evictOne vid xs).map Entry.id = xs.map Entry.id := by
  rw [evictOne_eq_map, List.map_map,
    List.map_congr_left (fun e _ => by
      show Entry.id (if e.id == vid then { e with resident := false } else e) = Entry.id e
      by_cases hc : e.id = vid <;> simp [hc])]

private theorem mem_evictOne {vid : Nat} {xs : List Entry} {z : Entry} :
    z ∈ evictOne vid xs →
      (∃ e0 ∈ xs, e0.id = vid ∧ z = { e0 with resident := false }) ∨ (z ∈ xs ∧ z.id ≠ vid) := by
  intro hz
  rw [evictOne_eq_map] at hz
  obtain ⟨e0, he0, hfe0⟩ := List.mem_map.mp hz
  by_cases hc : e0.id = vid
  · have hf : (if e0.id == vid then { e0 with resident := false } else e0) =
      { e0 with resident := false } := ite_evict_pos hc
    rw [hf] at hfe0
    exact Or.inl ⟨e0, he0, hc, hfe0.symm⟩
  · have hf : (if e0.id == vid then { e0 with resident := false } else e0) = e0 :=
      ite_evict_neg hc
    rw [hf] at hfe0
    exact Or.inr ⟨hfe0 ▸ he0, fun he => hc ((congrArg Entry.id hfe0).trans he)⟩

/-! ## Byte / eligibility accounting across one eviction -/

private theorem residentBytes_evict (v : Entry) (hv : v.resident = true) :
    ∀ (xs : List Entry), (xs.map Entry.id).Nodup → v ∈ xs →
      residentBytes xs = v.bytes + residentBytes (evictOne v.id xs) := by
  intro xs
  induction xs with
  | nil => intro _ hm; exact absurd hm (List.not_mem_nil (a := v))
  | cons a as ih =>
      intro hn hm
      rw [List.map_cons, List.nodup_cons] at hn
      obtain ⟨hne, htail⟩ := hn
      rcases List.mem_cons.mp hm with rfl | hm'
      · -- the head is the victim
        have hheadf : (if v.id == v.id then { v with resident := false } else v) =
            { v with resident := false } := ite_evict_pos rfl
        have hasf : as.map (fun e => if e.id == v.id then { e with resident := false } else e) = as := by
          rw [List.map_congr_left (fun e he => by
            show (if e.id == v.id then { e with resident := false } else e) = e
            by_cases hc : e.id = v.id
            · exfalso
              exact hne (by rw [← hc]; exact List.mem_map.mpr ⟨e, he, rfl⟩)
            · exact ite_evict_neg hc)]
          exact List.map_id as
        rw [evictOne_eq_map, List.map_cons, hheadf, hasf]
        simp only [residentBytes, List.filter_cons_of_pos hv,
          List.filter_cons_of_neg (show ¬ Entry.resident { v with resident := false } = true from by simp),
          List.map_cons, List.sum_cons]
      · -- the victim lives in the tail
        have had : a.id ≠ v.id := fun hc =>
          hne (by rw [hc]; exact List.mem_map.mpr ⟨v, hm', rfl⟩)
        have haf : (if a.id == v.id then { a with resident := false } else a) = a :=
          ite_evict_neg had
        have hiz := ih htail hm'
        rw [evictOne_eq_map] at hiz ⊢
        rw [List.map_cons] at ⊢
        rw [haf]
        by_cases hz : a.resident = true
        · simp only [residentBytes, List.filter_cons_of_pos hz, List.map_cons, List.sum_cons] at hiz ⊢
          omega
        · simp only [residentBytes, List.filter_cons_of_neg hz, List.map_cons, List.sum_cons] at hiz ⊢
          omega

private theorem length_elig_evict (v : Entry) (hv : v.resident = true) (hp : v.pinned = 0) :
    ∀ (xs : List Entry), (xs.map Entry.id).Nodup → v ∈ xs →
      ((evictOne v.id xs).filter (fun e => e.resident && e.pinned == 0)).length + 1 =
        (xs.filter (fun e => e.resident && e.pinned == 0)).length := by
  intro xs
  induction xs with
  | nil => intro _ hm; exact absurd hm (List.not_mem_nil (a := v))
  | cons a as ih =>
      intro hn hm
      rw [List.map_cons, List.nodup_cons] at hn
      obtain ⟨hne, htail⟩ := hn
      rcases List.mem_cons.mp hm with rfl | hm'
      · have hheadf : (if v.id == v.id then { v with resident := false } else v) =
            { v with resident := false } := ite_evict_pos rfl
        have hasf : as.map (fun e => if e.id == v.id then { e with resident := false } else e) = as := by
          rw [List.map_congr_left (fun e he => by
            show (if e.id == v.id then { e with resident := false } else e) = e
            by_cases hc : e.id = v.id
            · exfalso
              exact hne (by rw [← hc]; exact List.mem_map.mpr ⟨e, he, rfl⟩)
            · exact ite_evict_neg hc)]
          exact List.map_id as
        have hel : (v.resident && v.pinned == 0) = true := by
          show (v.resident && v.pinned == 0) = true
          rw [hv, hp]
          decide
        have hnf : ¬(({ v with resident := false }.resident && v.pinned == 0) = true) := by simp
        rw [evictOne_eq_map, List.map_cons, hheadf, hasf]
        simp [hel, hnf]
      · have had : a.id ≠ v.id := fun hc =>
          hne (by rw [hc]; exact List.mem_map.mpr ⟨v, hm', rfl⟩)
        have haf : (if a.id == v.id then { a with resident := false } else a) = a :=
          ite_evict_neg had
        have hiz := ih htail hm'
        rw [evictOne_eq_map] at hiz
        rw [evictOne_eq_map, List.map_cons, haf]
        rw [List.filter_cons, List.filter_cons]
        by_cases hz : (a.resident && a.pinned == 0) = true
        · rw [if_pos hz, if_pos hz, List.length_cons, List.length_cons]
          omega
        · rw [if_neg hz, if_neg hz]
          omega

/-! ## `pickVictim` fold specification -/

private def pstep (best : Option Entry) (e : Entry) : Option Entry :=
  if !(elig e) then best
  else match best with
    | none => some e
    | some b => if earlierVictim e b then some e else some b

private theorem pstep_none_true :
    ∀ a : Entry, elig a = true → pstep (none : Option Entry) a = some a := by
  intro a ha
  show (if !(elig a) then (none : Option Entry) else some a) = some a
  rw [ha]
  rfl

private theorem pstep_none_false :
    ∀ a : Entry, elig a = false → pstep (none : Option Entry) a = none := by
  intro a ha
  show (if !(elig a) then (none : Option Entry) else some a) = none
  rw [ha]
  rfl

private theorem pstep_some_true :
    ∀ (a b : Entry), elig a = true →
      pstep (some b) a = (if earlierVictim a b then some a else some b) := by
  intro a b ha
  show (if !(elig a) then some b else if earlierVictim a b then some a else some b) = _
  rw [ha]
  rfl

private theorem pstep_some_false :
    ∀ (a b : Entry), elig a = false → pstep (some b) a = some b := by
  intro a b ha
  show (if !(elig a) then some b else if earlierVictim a b then some a else some b) = _
  rw [ha]
  rfl

private theorem pickVictim_fold (xs : List Entry) :
    pickVictim xs = xs.foldl pstep none := rfl

private theorem fold_spec :
    ∀ (xs : List Entry) (best : Option Entry),
      (∀ e, best = some e → elig e = true) →
      (xs.foldl pstep best = none →
          best = none ∧ ∀ e ∈ xs, elig e = false) ∧
      (∀ v, xs.foldl pstep best = some v →
          (best = some v ∨ (v ∈ xs ∧ v.resident = true ∧ v.pinned = 0)) ∧
          ∀ e, (e ∈ xs ∧ elig e = true) ∨ (best = some e ∧ elig e = true) → lruLe v e) := by
  intro xs
  induction xs with
  | nil =>
      intro best _
      refine ⟨fun h => ⟨h, fun e he => absurd he (List.not_mem_nil (a := e))⟩, ?_⟩
      intro v hv
      refine ⟨Or.inl hv, fun e he => ?_⟩
      rcases he with ⟨hmem, _⟩ | ⟨hbest, _⟩
      · exact absurd hmem (List.not_mem_nil (a := e))
      · cases hbest
        cases hv
        exact lruLe_refl _
  | cons a as ih =>
      intro best hbestc
      refine ⟨?_, ?_⟩
      · -- foldl = none
        intro h
        rw [List.foldl_cons] at h
        cases best with
        | none =>
            cases he : elig a with
            | true =>
                rw [pstep_none_true a he] at h
                obtain ⟨hf, has⟩ := (ih (some a) (fun e hhe => by cases hhe; exact he)).1 h
                cases hf
            | false =>
                rw [pstep_none_false a he] at h
                obtain ⟨hf, has⟩ := (ih none (fun e hhe => absurd hhe (by simp))).1 h
                exact ⟨hf, fun e hem => by
                  rcases List.mem_cons.mp hem with rfl | hem'
                  · exact he
                  · exact has e hem'⟩
        | some b =>
            cases he : elig a with
            | true =>
                rw [pstep_some_true a b he] at h
                split at h
                · obtain ⟨hf, _⟩ := (ih (some a) (fun e hhe => by cases hhe; exact he)).1 h
                  cases hf
                · obtain ⟨hf, _⟩ :=
                    (ih (some b) (fun e hhe => by cases hhe; exact hbestc b rfl)).1 h
                  cases hf
            | false =>
                rw [pstep_some_false a b he] at h
                obtain ⟨hf, has⟩ :=
                  (ih (some b) (fun e hhe => by cases hhe; exact hbestc b rfl)).1 h
                exact ⟨hf, fun e hem => by
                  rcases List.mem_cons.mp hem with rfl | hem'
                  · exact he
                  · exact has e hem'⟩
      · -- foldl = some v
        intro v hv
        rw [List.foldl_cons] at hv
        cases best with
        | none =>
            cases hael : elig a with
            | true =>
                rw [pstep_none_true a hael] at hv
                obtain ⟨hd1, hd2⟩ :=
                  (ih (some a) (fun e hhe => by cases hhe; exact hael)).2 v hv
                refine ⟨?_, ?_⟩
                · rcases hd1 with hv' | ⟨hvm, vr, vp⟩
                  · cases hv'
                    exact Or.inr ⟨List.mem_cons_self, (elig_imp hael).1, (elig_imp hael).2⟩
                  · exact Or.inr ⟨List.mem_cons_of_mem a hvm, vr, vp⟩
                · intro e he
                  rcases he with ⟨hem, he'⟩ | ⟨ha, he'⟩
                  · cases List.mem_cons.mp hem with
                    | inl heq => rw [heq]; exact hd2 a (Or.inr ⟨rfl, hael⟩)
                    | inr hem' => exact hd2 e (Or.inl ⟨hem', he'⟩)
                  · cases ha
            | false =>
                rw [pstep_none_false a hael] at hv
                obtain ⟨hd1, hd2⟩ := (ih none (fun e hhe => absurd hhe (by simp))).2 v hv
                refine ⟨?_, ?_⟩
                · rcases hd1 with hv' | ⟨hvm, vr, vp⟩
                  · exact absurd hv' (by simp)
                  · exact Or.inr ⟨List.mem_cons_of_mem a hvm, vr, vp⟩
                · intro e he
                  rcases he with ⟨hem, he'⟩ | ⟨hbest2, he'⟩
                  · rcases List.mem_cons.mp hem with rfl | hem'
                    · exact absurd he' (by simp [hael])
                    · exact hd2 e (Or.inl ⟨hem', he'⟩)
                  · cases hbest2
        | some b =>
            cases hael : elig a with
            | true =>
                rw [pstep_some_true a b hael] at hv
                obtain ⟨ha1, ha2⟩ := elig_imp hael
                by_cases hP : earlierVictim a b = true
                · rw [if_pos hP] at hv
                  obtain ⟨hd1, hd2⟩ :=
                    (ih (some a) (fun e hhe => by cases hhe; exact hael)).2 v hv
                  refine ⟨?_, ?_⟩
                  · rcases hd1 with hv' | ⟨hvm, vr, vp⟩
                    · cases hv'
                      exact Or.inr ⟨List.mem_cons_self, ha1, ha2⟩
                    · exact Or.inr ⟨List.mem_cons_of_mem a hvm, vr, vp⟩
                  · intro e he
                    rcases he with ⟨hem, he'⟩ | ⟨ha, he'⟩
                    · cases List.mem_cons.mp hem with
                      | inl heq => rw [heq]; exact hd2 a (Or.inr ⟨rfl, hael⟩)
                      | inr hem' => exact hd2 e (Or.inl ⟨hem', he'⟩)
                    · cases ha
                      exact lruLe_trans (hd2 a (Or.inr ⟨rfl, hael⟩)) (earlier_imp_lru hP)
                · rw [if_neg hP] at hv
                  obtain ⟨hd1, hd2⟩ :=
                    (ih (some b) (fun e hhe => by cases hhe; exact hbestc b rfl)).2 v hv
                  refine ⟨?_, ?_⟩
                  · rcases hd1 with hv' | ⟨hvm, vr, vp⟩
                    · exact Or.inl hv'
                    · exact Or.inr ⟨List.mem_cons_of_mem a hvm, vr, vp⟩
                  · have hev : earlierVictim a b = false := by
                      cases ht : earlierVictim a b with
                      | true => exact absurd ht hP
                      | false => rfl
                    have hbElig : elig b = true := hbestc b rfl
                    intro e he
                    rcases he with ⟨hem, he'⟩ | ⟨hbe, he'⟩
                    · rcases List.mem_cons.mp hem with rfl | hem'
                      · exact lruLe_trans (hd2 b (Or.inr ⟨rfl, hbElig⟩)) (not_earlier_lru hev)
                      · exact hd2 e (Or.inl ⟨hem', he'⟩)
                    · cases hbe
                      exact hd2 b (Or.inr ⟨rfl, hbElig⟩)
            | false =>
                rw [pstep_some_false a b hael] at hv
                obtain ⟨hd1, hd2⟩ :=
                  (ih (some b) (fun e hhe => by cases hhe; exact hbestc b rfl)).2 v hv
                refine ⟨?_, ?_⟩
                · rcases hd1 with hb' | ⟨hvm, vr, vp⟩
                  · exact Or.inl hb'
                  · exact Or.inr ⟨List.mem_cons_of_mem a hvm, vr, vp⟩
                · intro e he
                  rcases he with ⟨hem, he'⟩ | ⟨hbe, he'⟩
                  · rcases List.mem_cons.mp hem with rfl | hem'
                    · exact absurd he' (by simp [hael])
                    · exact hd2 e (Or.inl ⟨hem', he'⟩)
                  · exact hd2 e (Or.inr ⟨hbe, he'⟩)

private theorem pickVictim_some_spec {xs : List Entry} {v : Entry} (h : pickVictim xs = some v) :
    v ∈ xs ∧ v.resident = true ∧ v.pinned = 0 ∧
      ∀ e ∈ xs, elig e = true → earlierVictim e v = false := by
  rw [pickVictim_fold] at h
  obtain ⟨hd1, hd2⟩ := (fold_spec xs none (fun e hhe => absurd hhe (by simp))).2 v h
  rcases hd1 with hb | ⟨hvm, vr, vp⟩
  · cases hb
  · exact ⟨hvm, vr, vp, fun e hem he => lruLe_not_earlier (hd2 e (Or.inl ⟨hem, he⟩))⟩

private theorem pickVictim_none_spec {xs : List Entry} (h : pickVictim xs = none) :
    ∀ e ∈ xs, elig e = false :=
  ((fold_spec xs none (fun e hhe => absurd hhe (by simp))).1 h).2

/-! ## Termination disjunct helper -/

private theorem forall_resident_pinned_of_no_elig {xs : List Entry}
    (h : ∀ e ∈ xs, elig e = false) : ∀ e' ∈ xs, e'.resident = true → 0 < e'.pinned := by
  intro e' hem hres
  by_cases hp : e'.pinned = 0
  · have h2 : elig e' = true := elig_of hres hp
    exact absurd ((h e' hem).symm.trans h2) Bool.false_ne_true
  · exact Nat.pos_of_ne_zero hp

private theorem forall_resident_pinned_of_filter_nil {xs : List Entry}
    (h : (xs.filter (fun e => e.resident && e.pinned == 0)).length = 0) :
    ∀ e' ∈ xs, e'.resident = true → 0 < e'.pinned := by
  have hnil : xs.filter (fun e => e.resident && e.pinned == 0) = [] :=
    List.eq_nil_of_length_eq_zero h
  intro e' hem hres
  by_cases hp : e'.pinned = 0
  · have he : e' ∈ xs.filter (fun e => e.resident && e.pinned == 0) :=
      List.mem_filter.mpr ⟨hem, elig_of hres hp⟩
    rw [hnil] at he
    exact absurd he (List.not_mem_nil (a := e'))
  · exact Nat.pos_of_ne_zero hp

/-! ## The fuel recursion -/

private theorem go_zero (target acc : Nat) (cur : List Entry) :
    evictToTarget.go target 0 cur acc = ⟨cur, acc⟩ := by
  show evictToTarget.go target 0 cur acc = (⟨cur, acc⟩ : EvictResult)
  rfl

private theorem go_succ (target acc : Nat) (f : Nat) (cur : List Entry) :
    evictToTarget.go target (f+1) cur acc =
      (if target ≤ acc then ⟨cur, acc⟩
       else match pickVictim cur with
         | none => ⟨cur, acc⟩
         | some v => evictToTarget.go target f (evictOne v.id cur) (acc + v.bytes)) := by
  show evictToTarget.go target (f+1) cur acc =
      (if target ≤ acc then ⟨cur, acc⟩
       else match pickVictim cur with
         | none => ⟨cur, acc⟩
         | some v => evictToTarget.go target f (evictOne v.id cur) (acc + v.bytes))
  rfl

private theorem aux (target : Nat) (entries : List Entry) (hn : (entries.map Entry.id).Nodup) :
    ∀ (f : Nat) (cur : List Entry) (acc : Nat),
      cur.map Entry.id = entries.map Entry.id →
      acc + residentBytes cur = residentBytes entries →
      (∀ i : Nat, ∀ e e' : Entry, entries[i]? = some e → cur[i]? = some e' →
        e'.id = e.id ∧ e'.pinned = e.pinned ∧ e'.lruTick = e.lruTick ∧ e'.bytes = e.bytes ∧
        (0 < e.pinned → e'.resident = e.resident)) →
      (∀ e e' rem : Entry, e ∈ entries → e' ∈ cur → rem ∈ cur → e.id = e'.id →
        e.resident = true → e'.resident = false → rem.resident = true → rem.pinned = 0 →
        lruLe e rem) →
      (cur.filter (fun e => e.resident && e.pinned == 0)).length ≤ f →
      let res := evictToTarget.go target f cur acc
      res.entries.length = entries.length ∧
      (∀ i : Nat, ∀ e e' : Entry, entries[i]? = some e → res.entries[i]? = some e' →
        e'.id = e.id ∧ e'.pinned = e.pinned ∧ e'.lruTick = e.lruTick ∧ e'.bytes = e.bytes ∧
        (0 < e.pinned → e'.resident = e.resident)) ∧
      res.reclaimed + residentBytes res.entries = residentBytes entries ∧
      (target ≤ res.reclaimed ∨ ∀ e' ∈ res.entries, e'.resident = true → 0 < e'.pinned) ∧
      (∀ e e' rem : Entry, e ∈ entries → e' ∈ res.entries → rem ∈ res.entries → e.id = e'.id →
        e.resident = true → e'.resident = false → rem.resident = true → rem.pinned = 0 →
        lruLe e rem) := by
  intro f
  induction f with
  | zero =>
      intro cur acc hA hB hC hD hE
      rw [go_zero]
      refine ⟨?_, hC, hB, ?_, hD⟩
      · have h := congrArg List.length hA
        rw [List.length_map, List.length_map] at h
        exact h
      · exact Or.inr (forall_resident_pinned_of_filter_nil (Nat.le_zero.mp hE))
  | succ f ih =>
      intro cur acc hA hB hC hD hE
      have hncur : (cur.map Entry.id).Nodup := by rw [hA]; exact hn
      have hlen : cur.length = entries.length := by
        have h := congrArg List.length hA
        rw [List.length_map, List.length_map] at h
        exact h
      by_cases hta : target ≤ acc
      · rw [go_succ, if_pos hta]
        exact ⟨hlen, hC, hB, Or.inl hta, hD⟩
      · cases h : pickVictim cur with
        | none =>
            rw [go_succ, if_neg hta, h]
            exact ⟨hlen, hC, hB,
              Or.inr (forall_resident_pinned_of_no_elig (pickVictim_none_spec h)), hD⟩
        | some v =>
            obtain ⟨vmem, vres, vpin, vmin⟩ := pickVictim_some_spec h
            rw [go_succ, if_neg hta, h]
            refine ih (evictOne v.id cur) (acc + v.bytes) ?_ ?_ ?_ ?_ ?_
            · -- ids preserved
              rw [map_entryId_evictOne, hA]
            · -- byte conservation
              have hf := residentBytes_evict v vres cur hncur vmem
              omega
            · -- indexwise fields
              intro i e e' hi hcur'
              have h1 :
                  Option.map (fun z => if z.id == v.id then { z with resident := false } else z)
                    cur[i]? = some e' := by
                rw [evictOne_eq_map, List.getElem?_map] at hcur'
                exact hcur'
              cases h0 : cur[i]? with
              | none => rw [h0, Option.map_none] at h1; cases h1
              | some e0 =>
                  rw [h0] at h1
                  obtain rfl :
                      (fun z => if z.id == v.id then { z with resident := false } else z) e0 = e' :=
                    Option.some.inj h1
                  by_cases hc : e0.id = v.id
                  · have hfe :
                        (fun z => if z.id == v.id then { z with resident := false } else z) e0 =
                          { e0 with resident := false } := ite_evict_pos hc
                    rw [hfe]
                    obtain ⟨hf1, hf2, hf3, hf4, hf5⟩ := hC i e e0 hi h0
                    refine ⟨hf1, hf2, hf3, hf4, ?_⟩
                    intro hpos
                    have hev : e0 = v := nodup_proj_eq hncur (getElem?_some_mem h0) vmem hc
                    have h0p : e0.pinned = 0 := by rw [hev]; exact vpin
                    rw [← hf2, h0p] at hpos
                    exact absurd hpos (Nat.lt_irrefl 0)
                  · have hfe :
                        (fun z => if z.id == v.id then { z with resident := false } else z) e0 = e0 :=
                      ite_evict_neg hc
                    rw [hfe]
                    exact hC i e e0 hi h0
            · -- LRU ordering invariant
              intro e e' rem h1 h2 h3 hid hr1 hr2 hr3 hp
              have hrem : rem ∈ cur ∧ rem.id ≠ v.id := by
                rcases mem_evictOne h3 with ⟨e0, he0, hid0, hze⟩ | ⟨hr0, hne⟩
                · exfalso
                  rw [hze] at hr3
                  exact absurd hr3 (by simp)
                · exact ⟨hr0, hne⟩
              rcases mem_evictOne h2 with ⟨e0, he0, hid0, hze⟩ | ⟨he', hne'⟩
              · -- `e'` is the flipped victim itself
                have he0v : e0 = v := nodup_proj_eq hncur he0 vmem hid0
                obtain ⟨i, hi⟩ := exists_getElem?_some entries h1
                obtain ⟨e1, hci, h1i⟩ : ∃ e1 : Entry, cur[i]? = some e1 ∧ e1.id = e.id := by
                  have h2' : (cur.map Entry.id)[i]? = (entries.map Entry.id)[i]? := by rw [hA]
                  have h3' : (entries.map Entry.id)[i]? = some e.id := by
                    rw [List.getElem?_map, hi]
                    rfl
                  rw [h3'] at h2'
                  rw [List.getElem?_map] at h2'
                  cases hci : cur[i]? with
                  | none => rw [hci, Option.map_none] at h2'; cases h2'
                  | some e1 =>
                      rw [hci, Option.map_some] at h2'
                      exact ⟨e1, rfl, Option.some.inj h2'⟩
                have h1v : e1 = v :=
                  nodup_proj_eq hncur (getElem?_some_mem hci) vmem
                    (h1i.trans (hid.trans ((congrArg Entry.id hze).trans hid0)))
                obtain ⟨hfv1, hfv2, hfv3, hfv4, hfv5⟩ := hC i e e1 hi hci
                rw [h1v] at hfv1 hfv2 hfv3
                have hrv : earlierVictim rem v = false :=
                  vmin rem hrem.1 (elig_of hr3 hp)
                have hlr : lruLe v rem := not_earlier_lru hrv
                rcases hlr with hlt | ⟨hle_, hid'⟩
                · exact Or.inl (hfv3 ▸ hlt)
                · exact Or.inr ⟨hfv3 ▸ hle_, hfv1 ▸ hid'⟩
              · -- `e'` survived unchanged
                refine hD e e' rem h1 he' hrem.1 hid hr1 hr2 hr3 hp
            · -- fuel
              have hf := length_elig_evict v vres vpin cur hncur vmem
              omega

private theorem spec (target : Nat) (entries : List Entry)
    (hn : (entries.map Entry.id).Nodup) :
    let res := evictToTarget target entries
    res.entries.length = entries.length ∧
    (∀ i : Nat, ∀ e e' : Entry,
      entries[i]? = some e → res.entries[i]? = some e' →
        e'.id = e.id ∧ e'.pinned = e.pinned ∧ e'.lruTick = e.lruTick ∧ e'.bytes = e.bytes ∧
        (0 < e.pinned → e'.resident = e.resident)) ∧
    res.reclaimed + residentBytes res.entries = residentBytes entries ∧
    (target ≤ res.reclaimed ∨ ∀ e' ∈ res.entries, e'.resident = true → 0 < e'.pinned) ∧
    (∀ e e' rem : Entry,
      e ∈ entries → e' ∈ res.entries → rem ∈ res.entries → e.id = e'.id →
        e.resident = true → e'.resident = false →
        rem.resident = true → rem.pinned = 0 → lruLe e rem) := by
  intro _
  refine aux target entries hn entries.length entries 0 ?_ ?_ ?_ ?_ ?_
  · rfl
  · show (0 : Nat) + residentBytes entries = residentBytes entries
    exact Nat.zero_add _
  · intro i e e' h1 h2
    have he : e = e' := Option.some.inj (h1.symm.trans h2)
    subst he
    exact ⟨rfl, rfl, rfl, rfl, fun _ => rfl⟩
  · intro e e' rem h1 h2 _ hid hr1 hr2 _ _
    have : e = e' := nodup_proj_eq hn h1 h2 hid
    rw [← this] at hr2
    exact absurd hr2 (by simp [hr1])
  · exact List.length_filter_le _ _

theorem sound : TinylambOpt.ResearchGoals.ColumnMmapEvictEpoch.Goal := by
  intro target entries hn
  obtain ⟨h1, h2, h3, h4, h5⟩ := spec target entries hn
  exact ⟨h1, h2, h3, h4, h5⟩

end TinylambOpt.ResearchProofs.ColumnMmapEvictEpoch
