import TinylambOpt.ResearchGoals.RestartRaces.Catalog
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Catalog_UniqueHandoff
open TinylambOpt.ResearchGoals.RestartRaces.Catalog.UniqueHandoff

private theorem inj_of_nodup_filterMap {α β : Type} (f : α → Option β) :
    ∀ l : List α, (l.filterMap f).Nodup →
      ∀ a ∈ l, ∀ b ∈ l, ∀ v, f a = some v → f b = some v → a = b := by
  intro l
  induction l with
  | nil =>
      intro h a ha
      simp at ha
  | cons x xs ih =>
      intro h a ha b hb v hfa hfb
      cases hgx : f x with
      | none =>
          simp [hgx] at h
          have hax : a ≠ x := by
            intro hax
            subst a
            simp [hgx] at hfa
          have hbx : b ≠ x := by
            intro hbx
            subst b
            simp [hgx] at hfb
          apply ih h a (by simpa [hax] using ha) b (by simpa [hbx] using hb) v hfa hfb
      | some vx =>
          simp [hgx] at h
          have htail := h.2
          have hxnot : ∀ y ∈ xs, f y ≠ some vx := by
            intro y hy hfy
            exact h.1 y hy hfy
          by_cases hax : a = x
          · subst a
            have hv : v = vx := by simpa [hgx] using hfa.symm
            subst v
            by_cases hbx : b = x
            · subst b; rfl
            · exact False.elim (hxnot b (by simpa [hbx] using hb) hfb)
          · have haxs : a ∈ xs := by simpa [hax] using ha
            by_cases hbx : b = x
            · subst b
              have hv : v = vx := by simpa [hgx] using hfb.symm
              subst v
              exact False.elim (hxnot a haxs hfa)
            · have hbxs : b ∈ xs := by simpa [hbx] using hb
              exact ih htail a haxs b hbxs v hfa hfb

private theorem same_key_same_row (rows : List Row) (done : List Nat)
    (hu : unique rows done) (r1 r2 : Row) (k : Nat)
    (h1 : r1 ∈ rows) (h2 : r2 ∈ rows)
    (hk1 : (if done.contains r1.owner then r1.newKey else r1.oldKey) = some k)
    (hk2 : (if done.contains r2.owner then r2.newKey else r2.oldKey) = some k) : r1 = r2 := by
  let f := fun r : Row => if done.contains r.owner then r.newKey else r.oldKey
  exact inj_of_nodup_filterMap f rows hu r1 h1 r2 h2 k hk1 hk2

private theorem tail_nodup_filterMap {α β : Type} (x : α) (xs : List α) (g : α → Option β)
    (h : ((x :: xs).filterMap g).Nodup) : (xs.filterMap g).Nodup := by
  cases hg : g x with
  | none => simpa [hg] using h
  | some v =>
      have h' : (v :: xs.filterMap g).Nodup := by simpa [hg] using h
      exact (List.nodup_cons.mp h').2

private theorem needs_cons {rows : List Row} {a b : Nat} {x : Row} :
    needs rows a b → needs (x :: rows) a b := by
  rintro ⟨hne, r1, hr1, r2, hr2, k, h1, h2, ho, hn⟩
  exact ⟨hne, r1, by simp [hr1], r2, by simp [hr2], k, h1, h2, ho, hn⟩

private theorem idxOf_lt_of_mem_take (n : Nat) {a : Nat} {l : List Nat} :
    a ∈ l.take n → l.idxOf a < n := by
  induction n generalizing l with
  | zero => intro h; simp at h
  | succ n ih =>
      intro h
      cases l with
      | nil => simp at h
      | cons x xs =>
          by_cases hxa : a = x
          · subst x; simp
          · have haxs : a ∈ xs.take n := by simpa [hxa] using h
            have hlt := ih (l := xs) haxs
            have hxa' : ¬ x = a := by intro hh; exact hxa hh.symm
            simpa [List.idxOf_cons, hxa', beq_iff_eq] using Nat.succ_lt_succ hlt

private theorem mem_take_of_idxOf_lt (n : Nat) {a : Nat} {l : List Nat} (ha : a ∈ l) :
    l.idxOf a < n → a ∈ l.take n := by
  induction n generalizing l with
  | zero => intro h; omega
  | succ n ih =>
      intro h
      cases l with
      | nil => simp at ha
      | cons x xs =>
          by_cases hxa : a = x
          · subst x; simp
          · have hlt : xs.idxOf a < n := by
              have hxa' : ¬ x = a := by intro hh; exact hxa hh.symm
              simpa [List.idxOf_cons, hxa', beq_iff_eq] using h
            have hmem := ih (l := xs) (by simpa [hxa] using ha) hlt
            simpa [hxa] using hmem

private theorem idxOf_lt_of_rank_lt (rank : Nat → Nat) {l : List Nat}
    (hsorted : l.Pairwise (fun a b => rank a ≤ rank b))
    {a b : Nat} (ha : a ∈ l) (hb : b ∈ l) (hr : rank a < rank b) : l.idxOf a < l.idxOf b := by
  have hia : l.idxOf a < l.length := List.idxOf_lt_length_of_mem ha
  have hib : l.idxOf b < l.length := List.idxOf_lt_length_of_mem hb
  have hga : l[l.idxOf a] = a := List.getElem_idxOf hia
  have hgb : l[l.idxOf b] = b := List.getElem_idxOf hib
  by_cases hltok : l.idxOf a < l.idxOf b
  · exact hltok
  have hle : l.idxOf b ≤ l.idxOf a := Nat.le_of_not_gt hltok
  have hneidx : l.idxOf b ≠ l.idxOf a := by
    intro heq
    have hgaQ : l[l.idxOf a]? = some a := by rw [List.getElem?_eq_getElem hia, hga]
    have hgbQ : l[l.idxOf b]? = some b := by rw [List.getElem?_eq_getElem hib, hgb]
    rw [← heq] at hgaQ
    have : a = b := by simpa using hgaQ.symm.trans hgbQ
    subst b
    omega
  have hltidx : l.idxOf b < l.idxOf a := Nat.lt_of_le_of_ne hle hneidx
  have hrel := hsorted.rel_getElem_of_lt hib hia hltidx
  rw [hgb, hga] at hrel
  omega

private theorem takes_of_prefixes (i : Nat) {rows : List Row} {done l : List Nat} :
    prefixes rows done l → i ≤ l.length → unique rows ((l.take i).reverse ++ done) := by
  induction i generalizing done l with
  | zero =>
      intro h hi
      cases l with
      | nil => simpa [prefixes] using h
      | cons a as => simpa [prefixes] using h.1
  | succ i ih =>
      intro h hi
      cases l with
      | nil => simp at hi
      | cons a as =>
          have ht : i ≤ as.length := by simpa using hi
          have htail : prefixes rows (a :: done) as := by simpa [prefixes] using h.2
          have hrec := ih (done := a :: done) (l := as) htail ht
          simpa [List.reverse_cons, List.append_assoc] using hrec

private theorem prefixes_of_takes {rows : List Row} {done l : List Nat}
    (h : ∀ i, i ≤ l.length → unique rows ((l.take i).reverse ++ done)) : prefixes rows done l := by
  induction l generalizing done with
  | nil => simpa [prefixes] using h 0 (by simp)
  | cons a as ih =>
      constructor
      · simpa using h 0 (by simp)
      · apply ih (done := a :: done)
        intro i hi
        have hi' : i + 1 ≤ (a :: as).length := by simpa using hi
        have hh := h (i + 1) hi'
        simpa [List.reverse_cons, List.append_assoc] using hh

private theorem not_mem_of_same_key {α β : Type} (f : α → Option β) (r : α) (rest : List α) (v : β)
    (h : ((r :: rest).filterMap f).Nodup) (hfr : f r = some v) : r ∉ rest := by
  have h' : (v :: rest.filterMap f).Nodup := by simpa [hfr] using h
  have hnot := (List.nodup_cons.mp h').1
  intro hrin
  exact hnot (List.mem_filterMap.mpr ⟨r, hrin, hfr⟩)

private theorem unique_of_closed_prefix (rows : List Row) (batch done : List Nat) (rank : Nat → Nat)
    (hdone : ∀ x ∈ done, x ∈ batch)
    (hclosed : ∀ a ∈ batch, ∀ b ∈ done, rank a < rank b → a ∈ done)
    (hrank : ∀ a ∈ batch, ∀ b ∈ batch, needs rows a b → rank a < rank b)
    (hout : ∀ r ∈ rows, r.owner ∉ batch → r.oldKey = r.newKey)
    (hu0 : unique rows []) (hu1 : unique rows batch) : unique rows done := by
  let f := fun r : Row => if done.contains r.owner then r.newKey else r.oldKey
  induction rows with
  | nil => simp [unique]
  | cons r rest ih =>
      change (List.filterMap f (r :: rest)).Nodup
      simp only [List.filterMap_cons]
      have hu0t : unique rest [] := by
        simpa [unique] using tail_nodup_filterMap r rest (fun q => q.oldKey) (by simpa [unique] using hu0)
      have hu1t : unique rest batch := by
        simpa [unique] using tail_nodup_filterMap r rest
          (fun q => if batch.contains q.owner then q.newKey else q.oldKey) (by simpa [unique] using hu1)
      have hrankt : ∀ a ∈ batch, ∀ b ∈ batch, needs rest a b → rank a < rank b :=
        fun a ha b hb hn => hrank a ha b hb (needs_cons hn)
      have houtt : ∀ q ∈ rest, q.owner ∉ batch → q.oldKey = q.newKey :=
        fun q hq => hout q (by simp [hq])
      cases hfr : f r with
      | none =>
          simp [hfr]
          exact ih hrankt houtt hu0t hu1t
      | some v =>
          simp [hfr]
          constructor
          · intro y hyrest hfy
            by_cases hrd : r.owner ∈ done
            · by_cases hyd : y.owner ∈ done
              · have hrnew : r.newKey = some v := by simpa [f, List.contains_eq_mem, hrd] using hfr
                have hynew : y.newKey = some v := by simpa [f, List.contains_eq_mem, hyd] using hfy
                have hrbatch : r.owner ∈ batch := hdone r.owner hrd
                have hybatch : y.owner ∈ batch := hdone y.owner hyd
                have hkR : (if batch.contains r.owner then r.newKey else r.oldKey) = some v := by
                  simpa [List.contains_eq_mem, hrbatch] using hrnew
                have hkY : (if batch.contains y.owner then y.newKey else y.oldKey) = some v := by
                  simpa [List.contains_eq_mem, hybatch] using hynew
                have heq := same_key_same_row (r :: rest) batch hu1 r y v (by simp) (by simp [hyrest]) hkR hkY
                have hnotin : r ∉ rest := not_mem_of_same_key (fun q => if batch.contains q.owner then q.newKey else q.oldKey)
                  r rest v (by simpa [unique] using hu1) hkR
                exact hnotin (by simpa [heq] using hyrest)
              · have hrnew : r.newKey = some v := by simpa [f, List.contains_eq_mem, hrd] using hfr
                have hyold : y.oldKey = some v := by simpa [f, List.contains_eq_mem, hyd] using hfy
                by_cases hybatch : y.owner ∈ batch
                · have hrbatch : r.owner ∈ batch := hdone r.owner hrd
                  have hneed : needs (r :: rest) y.owner r.owner := by
                    refine ⟨?_, ?_⟩
                    · intro heq
                      exact hyd (heq ▸ hrd)
                    · exact ⟨y, by simp [hyrest], r, by simp, v, rfl, rfl, hyold, hrnew⟩
                  have hrnk := hrank y.owner hybatch r.owner hrbatch hneed
                  have hyin : y.owner ∈ done := hclosed y.owner hybatch r.owner hrd hrnk
                  exact (by exact hyd hyin)
                · have hynew : y.newKey = some v := by
                    simpa [hout y (by simp [hyrest]) hybatch] using hyold
                  have hrbatch : r.owner ∈ batch := hdone r.owner hrd
                  have hkR : (if batch.contains r.owner then r.newKey else r.oldKey) = some v := by
                    simpa [List.contains_eq_mem, hrbatch] using hrnew
                  have hkY : (if batch.contains y.owner then y.newKey else y.oldKey) = some v := by
                    simpa [List.contains_eq_mem, hybatch] using hyold
                  have heq := same_key_same_row (r :: rest) batch hu1 r y v (by simp) (by simp [hyrest]) hkR hkY
                  have hnotin : r ∉ rest := not_mem_of_same_key (fun q => if batch.contains q.owner then q.newKey else q.oldKey)
                    r rest v (by simpa [unique] using hu1) hkR
                  exact hnotin (by simpa [heq] using hyrest)
            · by_cases hyd : y.owner ∈ done
              · have hrold : r.oldKey = some v := by simpa [f, List.contains_eq_mem, hrd] using hfr
                have hynew : y.newKey = some v := by simpa [f, List.contains_eq_mem, hyd] using hfy
                by_cases hrbatch : r.owner ∈ batch
                · have hybatch : y.owner ∈ batch := hdone y.owner hyd
                  have hneed : needs (r :: rest) r.owner y.owner := by
                    refine ⟨?_, ?_⟩
                    · intro heq
                      exact hrd (heq ▸ hyd)
                    · exact ⟨r, by simp, y, by simp [hyrest], v, rfl, rfl, hrold, hynew⟩
                  have hrnk := hrank r.owner hrbatch y.owner hybatch hneed
                  have rdin : r.owner ∈ done := hclosed r.owner hrbatch y.owner hyd hrnk
                  exact (by exact hrd rdin)
                · have hrnew : r.newKey = some v := by
                    simpa [hout r (by simp) hrbatch] using hrold
                  have hybatch : y.owner ∈ batch := hdone y.owner hyd
                  have hkR : (if batch.contains r.owner then r.newKey else r.oldKey) = some v := by
                    simpa [List.contains_eq_mem, hrbatch] using hrold
                  have hkY : (if batch.contains y.owner then y.newKey else y.oldKey) = some v := by
                    simpa [List.contains_eq_mem, hybatch] using hynew
                  have heq := same_key_same_row (r :: rest) batch hu1 r y v (by simp) (by simp [hyrest]) hkR hkY
                  have hnotin : r ∉ rest := not_mem_of_same_key (fun q => if batch.contains q.owner then q.newKey else q.oldKey)
                    r rest v (by simpa [unique] using hu1) hkR
                  exact hnotin (by simpa [heq] using hyrest)
              · have hrold : r.oldKey = some v := by simpa [f, List.contains_eq_mem, hrd] using hfr
                have hyold : y.oldKey = some v := by simpa [f, List.contains_eq_mem, hyd] using hfy
                have heq := same_key_same_row (r :: rest) [] hu0 r y v (by simp) (by simp [hyrest]) hrold hyold
                have hnotin : r ∉ rest := not_mem_of_same_key (fun q => q.oldKey)
                  r rest v (by simpa [unique] using hu0) hrold
                exact hnotin (by simpa [heq] using hyrest)
          · exact ih hrankt houtt hu0t hu1t

theorem sound : Goal := by
  intro rs batch hbatch hout huniq0 huniqAll
  classical
  constructor
  · rintro ⟨order, hperm, hpref⟩
    let rank : Nat → Nat := fun x => order.idxOf x
    refine ⟨rank, ?_⟩
    intro a ha b hb hneed
    have haord : a ∈ order := hperm.mem_iff.2 ha
    have hbord : b ∈ order := hperm.mem_iff.2 hb
    have hia : order.idxOf a < order.length := List.idxOf_lt_length_of_mem haord
    have hib : order.idxOf b < order.length := List.idxOf_lt_length_of_mem hbord
    by_cases hltidx : order.idxOf a < order.idxOf b
    · simpa [rank] using hltidx
    exfalso
    have hle : order.idxOf b ≤ order.idxOf a := Nat.le_of_not_gt hltidx
    have hneidx : order.idxOf b ≠ order.idxOf a := by
      intro heq
      have hgaQ : order[order.idxOf a]? = some a := by
        rw [List.getElem?_eq_getElem hia, List.getElem_idxOf hia]
      have hgbQ : order[order.idxOf b]? = some b := by
        rw [List.getElem?_eq_getElem hib, List.getElem_idxOf hib]
      rw [← heq] at hgaQ
      have hab : a = b := by simpa using hgaQ.symm.trans hgbQ
      exact hneed.1 hab
    have hltidx' : order.idxOf b < order.idxOf a := Nat.lt_of_le_of_ne hle hneidx
    rcases hneed.2 with ⟨r1, hr1, r2, hr2, k, hr1own, hr2own, hk1old, hk2new⟩
    let t := order.take (order.idxOf b + 1)
    have htakes := takes_of_prefixes (order.idxOf b + 1) (rows := rs) (done := []) (l := order) hpref
    have huniq0' := htakes (by omega)
    have huniq : unique rs t.reverse := by simpa [t] using huniq0'
    have hbmem : b ∈ t := by
      dsimp [t]
      exact mem_take_of_idxOf_lt (l := order) (a := b) (order.idxOf b + 1) hbord (by omega)
    have hamem : a ∉ t := by
      dsimp [t]
      intro hain
      have hai := idxOf_lt_of_mem_take (a := a) (l := order) (order.idxOf b + 1) hain
      omega
    have hgbmem : (t.reverse).contains b = true := by simp [List.contains_eq_mem, hbmem]
    have hganot : (t.reverse).contains a = false := by simp [List.contains_eq_mem, hamem]
    have hnot1 : t.reverse.contains r1.owner = false := by simpa [hr1own] using hganot
    have hmem2 : t.reverse.contains r2.owner = true := by simpa [hr2own] using hgbmem
    have hn1 : r1.owner ∉ t := by simpa [List.contains_eq_mem] using hnot1
    have hm2 : r2.owner ∈ t := by simpa [List.contains_eq_mem] using hmem2
    have hk1 : (if t.reverse.contains r1.owner then r1.newKey else r1.oldKey) = some k := by
      simp [List.contains_eq_mem, hn1, hk1old]
    have hk2 : (if t.reverse.contains r2.owner then r2.newKey else r2.oldKey) = some k := by
      simp [List.contains_eq_mem, hm2, hk2new]
    have heq := same_key_same_row rs t.reverse huniq r1 r2 k hr1 hr2 hk1 hk2
    have howner : r1.owner = r2.owner := by simpa [heq]
    have habeq : a = b := by
      calc a = r1.owner := hr1own.symm
           _ = r2.owner := howner
           _ = b := hr2own
    exact hneed.1 habeq
  · rintro ⟨rank, hrank⟩
    let le : Nat → Nat → Bool := fun a b => decide (rank a ≤ rank b)
    let order := batch.mergeSort le
    have hperm : order.Perm batch := by simpa [order] using List.mergeSort_perm batch le
    have hnod : order.Nodup := (hperm.nodup_iff).2 hbatch
    have hsorted : order.Pairwise (fun a b => rank a ≤ rank b) := by
      dsimp [order, le]
      simpa using (List.pairwise_mergeSort (le := fun a b : Nat => decide (rank a ≤ rank b))
        (by intro x y z hxy hyz; simp only [decide_eq_true_eq] at hxy hyz ⊢; omega)
        (by intro x y; simp only [Bool.or_eq_true, decide_eq_true_eq, decide_eq_false_iff_not]; omega) batch)
    refine ⟨order, hperm, ?_⟩
    apply prefixes_of_takes
    intro i hi
    let done := (order.take i).reverse
    have done_sub : ∀ x ∈ done, x ∈ batch := by
      intro x hx
      have hxord : x ∈ order.take i := by simpa [done] using hx
      have hxall : x ∈ order := (List.take_prefix i order).subset hxord
      exact hperm.subset hxall
    have hclosed : ∀ a ∈ batch, ∀ b ∈ done, rank a < rank b → a ∈ done := by
      intro a hab b hbd hr
      have hbmem : b ∈ order.take i := by simpa [done] using hbd
      have hib : order.idxOf b < i := idxOf_lt_of_mem_take (a := b) (l := order) i hbmem
      have hidx := idxOf_lt_of_rank_lt rank hsorted (hperm.mem_iff.2 hab)
        (hperm.mem_iff.2 (done_sub b hbd)) hr
      have hmem : a ∈ order.take i := mem_take_of_idxOf_lt (l := order) (a := a) i (hperm.mem_iff.2 hab) (by omega)
      simpa [done] using hmem
    have unique_done : unique rs done :=
      unique_of_closed_prefix rs batch done rank done_sub hclosed hrank hout huniq0 huniqAll
    simpa [done] using unique_done
