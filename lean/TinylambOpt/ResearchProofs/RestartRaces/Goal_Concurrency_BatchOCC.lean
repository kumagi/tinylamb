import TinylambOpt.ResearchGoals.RestartRaces.Concurrency
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Concurrency_BatchOCC

open TinylambOpt.ResearchGoals.RestartRaces.Concurrency.BatchOCC

private def foldTx (base : Nat → Option Int) (pre : List Tx) : Nat → Option Int :=
  pre.foldl applyTx base

private theorem applyTx_none (s : Nat → Option Int) (tx : Tx) (k : Nat) (h : tx.writes k = none) :
    applyTx s tx k = s k := by simp [applyTx, h]

private theorem foldl_no_write (s : Nat → Option Int) (pre : List Tx) (k : Nat)
    (h : ∀ t ∈ pre, t.writes k = none) : (pre.foldl applyTx s) k = s k := by
  induction pre generalizing s with
  | nil => simp
  | cons t pre ih =>
      simp only [List.foldl_cons]
      rw [ih (applyTx s t) (fun u hu => h u (by simp [hu]))]
      exact applyTx_none s t k (h t (by simp))

private theorem foldl_write_value (s : Nat → Option Int) (pre : List Tx) (u : Tx) (k : Nat)
    (h : ∀ t ∈ pre, t.writes k = none) :
    (pre.foldl applyTx (applyTx s u)) k = applyTx s u k := by
  exact foldl_no_write (applyTx s u) pre k h

private theorem valid_reads_at {base s : Nat → Option Int} {order : List Tx}
    (hval : valid base s order) :
    ∀ pre t suf, order = pre ++ t :: suf → t.reads.map (foldTx s pre) = t.reads.map base := by
  intro pre t suf h
  induction pre generalizing order s with
  | nil =>
      cases h
      exact hval.1
  | cons u pre ih =>
      have horder : order = u :: (pre ++ t :: suf) := by simpa using h
      subst order
      rw [show foldTx s (u :: pre) = foldTx (applyTx s u) pre by rfl]
      exact ih (order := pre ++ t :: suf) (s := applyTx s u) hval.2 rfl

private theorem decomp_of_mem {α : Type} {a : α} {l : List α} (h : a ∈ l) :
    ∃ pre suf, l = pre ++ a :: suf := by
  induction l with
  | nil => simp at h
  | cons x xs ih =>
      simp only [List.mem_cons] at h
      rcases h with rfl | hx
      · exact ⟨[], xs, rfl⟩
      · obtain ⟨pre, suf, hs⟩ := ih hx
        exact ⟨x :: pre, suf, by simp [hs]⟩


private def rankOf (order : List Tx) (id : Nat) : Nat :=
  (order.takeWhile (fun t => t.id != id)).length

private theorem rankOf_cons_eq (t : Tx) (ts : List Tx) (id : Nat) (h : t.id = id) :
    rankOf (t :: ts) id = 0 := by
  simp [rankOf, h]

private theorem rankOf_cons_ne (t : Tx) (ts : List Tx) (id : Nat) (h : t.id ≠ id) :
    rankOf (t :: ts) id = rankOf ts id + 1 := by
  simp [rankOf, h]

private theorem exists_min (rank : Nat → Nat) (l : List Nat) (hne : l ≠ []) :
    ∃ m ∈ l, ∀ x ∈ l, rank m ≤ rank x := by
  cases l with
  | nil => contradiction
  | cons a rest =>
      by_cases hrest : rest = []
      · subst rest
        exact ⟨a, by simp, by simp⟩
      · obtain ⟨m, hm, hmin⟩ := exists_min rank rest hrest
        by_cases ham : rank a ≤ rank m
        · refine ⟨a, by simp, ?_⟩
          intro x hx
          simp only [List.mem_cons] at hx
          rcases hx with rfl | hx
          · omega
          · exact Nat.le_trans ham (hmin x hx)
        · refine ⟨m, by simp [hm], ?_⟩
          intro x hx
          simp only [List.mem_cons] at hx
          rcases hx with rfl | hx
          · exact Nat.le_of_not_ge ham
          · exact hmin x hx

private theorem nodup_tail {l : List Nat} (h : l.Nodup) : l.tail.Nodup := by
  cases l with
  | nil => simp
  | cons a l => simpa [List.Nodup] using h.tail

private theorem perm_filter_id (cohort : List Tx) (m : Tx) (hm : m ∈ cohort)
    (hnod : (cohort.map Tx.id).Nodup) :
    cohort.Perm (m :: cohort.filter (fun t => t.id != m.id)) := by
  induction cohort with
  | nil => simp at hm
  | cons t ts ih =>
      change List.Pairwise (fun x y : Nat => x ≠ y) (List.map Tx.id (t :: ts)) at hnod
      simp only [List.map_cons, List.pairwise_cons] at hnod
      simp only [List.mem_cons] at hm
      rcases hm with rfl | hm
      · have hnot : ∀ u ∈ ts, u.id ≠ m.id := by
          intro u hu heq
          have hmem : u.id ∈ List.map Tx.id ts := List.mem_map_of_mem hu
          have hmhead : m.id ∈ List.map Tx.id ts := by simpa [heq] using hmem
          exact (hnod.1 m.id hmhead) rfl
        have hfilt : ts.filter (fun u => u.id != m.id) = ts := by
          apply List.filter_eq_self.mpr
          intro u hu
          simpa [hnot u hu]
        simp [hfilt]
      · have htailnod : (List.map Tx.id ts).Nodup := hnod.2
        have ih' := ih hm htailnod
        have htne : t.id ≠ m.id := by
          intro htm
          have hmem : m.id ∈ List.map Tx.id ts := by
            simpa [htm] using (List.mem_map_of_mem hm : m.id ∈ List.map Tx.id ts)
          exact (hnod.1 m.id hmem) htm
        simp only [List.filter_cons]
        simp [htne]
        exact (List.Perm.cons t ih').trans (List.Perm.swap m t (List.filter (fun u => u.id != m.id) ts))

private theorem map_eq_pointwise {α β : Type} (l : List α) (f g : α → β)
    (h : l.map f = l.map g) : ∀ x ∈ l, f x = g x := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.cons.injEq] at h
      intro x hx
      simp only [List.mem_cons] at hx
      rcases hx with rfl | hx
      · exact h.1
      · exact ih h.2 x hx

private theorem applyTx_write_eq (s t : Nat → Option Int) (tx : Tx) (k : Nat)
    (h : (tx.writes k).isSome) : applyTx s tx k = applyTx t tx k := by
  cases hw : tx.writes k with
  | none => simp [hw] at h
  | some v => simp [applyTx, hw]

private theorem edge_rank (base : Nat → Option Int) {s : Nat → Option Int} {order : List Tx}
    (hval : valid base s order) (hnod : (order.map Tx.id).Nodup)
    (hdisj : ∀ a ∈ order, ∀ b ∈ order, a.id ≠ b.id → ∀ k, (a.writes k).isSome → b.writes k = none)
    {a b : Tx} (ha : a ∈ order) (hb : b ∈ order) (hedge : edge base a b) :
    rankOf order a.id < rankOf order b.id := by
  induction order generalizing s with
  | nil => simp at ha
  | cons t ts ih =>
      change List.Pairwise (fun x y : Nat => x ≠ y) (List.map Tx.id (t :: ts)) at hnod
      simp only [List.map_cons, List.pairwise_cons] at hnod
      have hdisj_tail : ∀ x ∈ ts, ∀ y ∈ ts, x.id ≠ y.id → ∀ k, (x.writes k).isSome → y.writes k = none := by
        intro x hx y hy
        exact hdisj x (by simp [hx]) y (by simp [hy])
      by_cases hta : t.id = a.id
      · rcases List.mem_cons.mp ha with hta_eq | ha_ts
        · subst a
          have hbts : b ∈ ts := by
            rcases List.mem_cons.mp hb with htb_eq | hbts
            · subst b
              exact (hedge.1 rfl).elim
            · exact hbts
          rw [rankOf_cons_eq t ts t.id rfl]
          have hbne : t.id ≠ b.id := by simpa using hedge.1
          rw [rankOf_cons_ne t ts b.id hbne]
          omega
        · have hmem : a.id ∈ List.map Tx.id ts := List.mem_map_of_mem ha_ts
          exfalso
          exact (hnod.1 a.id hmem) hta
      · by_cases htb : t.id = b.id
        · rcases List.mem_cons.mp hb with htb_eq | hb_ts
          · subst b
            obtain ⟨k, hkreads, hkchange⟩ := hedge.2
            have hat : a ∈ ts := by
              rcases List.mem_cons.mp ha with hta_eq | ha_ts
              · subst a
                exact (hta rfl).elim
              · exact ha_ts
            obtain ⟨pre, suf, hts⟩ := decomp_of_mem hat
            have hvaltail : valid base (applyTx s t) ts := hval.2
            have hreads := valid_reads_at hvaltail pre a suf hts
            have hstatebase : (foldTx (applyTx s t) pre) k = base k := by
              exact map_eq_pointwise a.reads (foldTx (applyTx s t) pre) base hreads k hkreads
            have htsome : (t.writes k).isSome := by
              cases hw : t.writes k with
              | none => simp [applyTx, hw] at hkchange
              | some v => simp [hw]
            have hnowrite : ∀ u ∈ pre, u.writes k = none := by
              intro u hu
              have hu_ts : u ∈ ts := by rw [hts]; simp [hu]
              have hutne : u.id ≠ t.id := by
                intro hEq
                have hmemid : u.id ∈ List.map Tx.id ts := List.mem_map_of_mem hu_ts
                have htmem : t.id ∈ List.map Tx.id ts := by simpa [hEq] using hmemid
                exact (hnod.1 t.id htmem) rfl
              exact hdisj t (by simp) u (by simp [hu_ts]) hutne.symm k htsome
            have hstate : (foldTx (applyTx s t) pre) k = applyTx base t k := by
              dsimp [foldTx]
              rw [foldl_no_write (applyTx s t) pre k hnowrite]
              exact applyTx_write_eq s base t k htsome
            rw [hstate] at hstatebase
            exfalso
            exact hkchange hstatebase
          · have hmem : b.id ∈ List.map Tx.id ts := List.mem_map_of_mem hb_ts
            exfalso
            exact (hnod.1 b.id hmem) htb
        · have hast : a ∈ ts := by
            rcases List.mem_cons.mp ha with hta_eq | ha_ts
            · subst a
              exact (hta rfl).elim
            · exact ha_ts
          have hbst : b ∈ ts := by
            rcases List.mem_cons.mp hb with htb_eq | hb_ts
            · subst b
              exact (htb rfl).elim
            · exact hb_ts
          have hres := ih hval.2 hnod.2 hdisj_tail hast hbst
          rw [rankOf_cons_ne t ts a.id hta, rankOf_cons_ne t ts b.id htb]
          omega

private theorem nodup_filter_id (l : List Tx) (m : Tx) (hnod : (l.map Tx.id).Nodup) :
    ((l.filter (fun x => x.id != m.id)).map Tx.id).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      change List.Pairwise (fun a b : Nat => a ≠ b) (List.map Tx.id (x :: xs)) at hnod
      simp only [List.map_cons, List.pairwise_cons] at hnod
      simp only [List.filter_cons]
      by_cases hx : x.id = m.id
      · have hb : (x.id != m.id) = false := by simpa [hx]
        simp [hb]
        exact ih hnod.2
      · have hb : (x.id != m.id) = true := by simpa [hx]
        simp [hb]
        constructor
        · intro y hy hym
          exact (hnod.1 y.id (List.mem_map_of_mem hy)).symm
        · exact ih hnod.2

private theorem length_filter_id_lt (l : List Tx) (m : Tx) (hm : m ∈ l)
    (hnod : (l.map Tx.id).Nodup) : (l.filter (fun x => x.id != m.id)).length < l.length := by
  induction l with
  | nil => simp at hm
  | cons x xs ih =>
      change List.Pairwise (fun a b : Nat => a ≠ b) (List.map Tx.id (x :: xs)) at hnod
      simp only [List.map_cons, List.pairwise_cons] at hnod
      simp only [List.mem_cons] at hm
      simp only [List.filter_cons]
      by_cases hx : x.id = m.id
      · have hb : (x.id != m.id) = false := by simpa [hx]
        simp [hb]
        have hle := List.length_filter_le (fun y : Tx => y.id != m.id) xs
        omega
      · have hmxs : m ∈ xs := by
          rcases hm with hmx | hm
          · subst m
            exact (hx rfl).elim
          · exact hm
        have htail : (xs.filter (fun y => y.id != m.id)).length < xs.length :=
          ih hmxs hnod.2
        have hb : (x.id != m.id) = true := by simpa [hx]
        rw [hb]
        simp only [List.filter_cons, hb, ↓reduceIte, List.length_cons]
        omega

private theorem rank_to_valid_len (base : Nat → Option Int) :
    ∀ n (cohort : List Tx), cohort.length ≤ n →
      (cohort.map Tx.id).Nodup →
      (∀ a ∈ cohort, ∀ b ∈ cohort, a.id ≠ b.id → ∀ k, (a.writes k).isSome → b.writes k = none) →
      (rank : Nat → Nat) →
      (∀ a ∈ cohort, ∀ b ∈ cohort, edge base a b → rank a.id < rank b.id) →
      ∀ s, (∀ t ∈ cohort, ∀ k ∈ t.reads, s k = base k) →
        ∃ order, order.Perm cohort ∧ valid base s order := by
  intro n
  induction n with
  | zero =>
      intro cohort hlen hnod hdisj rank hrank s hagree
      have hnil : cohort = [] := by
        cases cohort with
        | nil => rfl
        | cons x xs => simp at hlen
      subst cohort
      exact ⟨[], by simp, by simp [valid]⟩
  | succ n ih =>
      intro cohort hlen hnod hdisj rank hrank s hagree
      cases cohort with
      | nil => exact ⟨[], by simp, by simp [valid]⟩
      | cons t ts =>
          have hne : (List.map Tx.id (t :: ts)) ≠ [] := by simp
          obtain ⟨mid, hmidmem, hmin⟩ := exists_min rank (List.map Tx.id (t :: ts)) hne
          obtain ⟨m, hm, hmid_eq⟩ := List.mem_map.mp hmidmem
          have hmin' : ∀ x ∈ t :: ts, rank m.id ≤ rank x.id := by
            intro x hx
            have hmem : x.id ∈ List.map Tx.id (t :: ts) := List.mem_map_of_mem hx
            rw [hmid_eq]
            exact hmin x.id hmem
          let tail := (t :: ts).filter (fun x => x.id != m.id)
          have hpermhead := perm_filter_id (t :: ts) m hm hnod
          have htailnod : (tail.map Tx.id).Nodup := nodup_filter_id (t :: ts) m hnod
          have htaillen : tail.length ≤ n := by
            have hlt := length_filter_id_lt (t :: ts) m hm hnod
            simp only [List.length_cons] at hlen
            have hlt' : tail.length < ts.length + 1 := by
              dsimp [tail] at hlt
              simpa using hlt
            omega
          have htaildisj : ∀ a ∈ tail, ∀ b ∈ tail, a.id ≠ b.id → ∀ k, (a.writes k).isSome → b.writes k = none := by
            intro a ha b hb
            exact hdisj a (List.mem_filter.mp ha).1 b (List.mem_filter.mp hb).1
          have htailrank : ∀ a ∈ tail, ∀ b ∈ tail, edge base a b → rank a.id < rank b.id := by
            intro a ha b hb
            exact hrank a (List.mem_filter.mp ha).1 b (List.mem_filter.mp hb).1
          have hread0 : m.reads.map s = m.reads.map base := by
            apply List.map_congr_left
            intro k hk
            exact hagree m hm k hk
          have hagreeTail : ∀ u ∈ tail, ∀ k ∈ u.reads, (applyTx s m) k = base k := by
            intro u hu k hk
            by_cases hw : (m.writes k).isSome
            · have heq : applyTx s m k = applyTx base m k := applyTx_write_eq s base m k hw
              rw [heq]
              by_cases hchange : applyTx base m k = base k
              · exact hchange
              · exfalso
                have huid : u.id ≠ m.id := by
                  have hb := (List.mem_filter.mp hu).2
                  simpa using hb
                have hed : edge base u m := by
                  refine ⟨huid, k, hk, hchange⟩
                have hlt := hrank u (List.mem_filter.mp hu).1 m hm hed
                have hle := hmin' u (List.mem_filter.mp hu).1
                omega
            · have hnone : m.writes k = none := by
                cases hb : m.writes k with
                | none => rfl
                | some v => simp [hb] at hw
              rw [applyTx_none s m k hnone]
              exact hagree u (List.mem_filter.mp hu).1 k hk
          obtain ⟨order', hperm', hval'⟩ := ih tail htaillen htailnod htaildisj rank htailrank
            (applyTx s m) hagreeTail
          refine ⟨m :: order', ?_, ?_⟩
          · exact (List.Perm.cons m hperm').trans hpermhead.symm
          · exact ⟨hread0, hval'⟩

private theorem rank_to_valid (base : Nat → Option Int) {cohort : List Tx}
    (hnod : (cohort.map Tx.id).Nodup)
    (hdisj : ∀ a ∈ cohort, ∀ b ∈ cohort, a.id ≠ b.id → ∀ k, (a.writes k).isSome → b.writes k = none)
    (rank : Nat → Nat)
    (hrank : ∀ a ∈ cohort, ∀ b ∈ cohort, edge base a b → rank a.id < rank b.id) :
    ∀ s, (∀ t ∈ cohort, ∀ k ∈ t.reads, s k = base k) →
      ∃ order, order.Perm cohort ∧ valid base s order := by
  exact rank_to_valid_len base cohort.length cohort (by omega) hnod hdisj rank hrank

theorem sound : Goal := by
  intro base cohort hnod hdisj
  constructor
  · rintro ⟨order, hperm, hval⟩
    refine ⟨rankOf order, ?_⟩
    have hnodOrder : (order.map Tx.id).Nodup := (hperm.map Tx.id).symm.nodup hnod
    have hdisjOrder : ∀ a ∈ order, ∀ b ∈ order, a.id ≠ b.id → ∀ k,
        (a.writes k).isSome → b.writes k = none := by
      intro a ha b hb
      exact hdisj a (hperm.mem_iff.mp ha) b (hperm.mem_iff.mp hb)
    intro a ha b hb hed
    exact edge_rank base hval hnodOrder hdisjOrder (hperm.mem_iff.mpr ha) (hperm.mem_iff.mpr hb) hed
  · rintro ⟨rank, hrank⟩
    exact rank_to_valid base hnod hdisj rank hrank base (by
      intro t ht k hk
      rfl)

end TinylambOpt.ResearchProofs.RestartRaces.Concurrency_BatchOCC
