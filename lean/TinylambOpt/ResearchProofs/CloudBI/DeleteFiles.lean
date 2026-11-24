import TinylambOpt.ResearchGoals.CloudBI.DeleteFiles
import TinylambOpt.ResearchProofs.CloudBI.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.CloudBI.DeleteFiles

open TinylambOpt.ResearchGoals.CloudBI.DeleteFiles
open TinylambOpt.ResearchProofs.CloudBI.Helpers

/-!
# Proof strategy overview

We prove the eleven Cloud-BI delete-file propositions from
`ResearchGoals/CloudBI/DeleteFiles.lean`:

* `EqualityHorizon`: folding the per-`(scope, key)` maximum delete sequence into
  a hash index agrees with scanning the delete list; a row sits below the
  horizon exactly when an applicable equality delete is strictly newer.
* `PositionMask`: the position-delete set built once per file agrees with the
  per-row scan of deletes (`foldl_contains` + pointwise atom rewriting).
* `OverlapCount`: inclusion–exclusion over the two delete kinds removes exactly
  the union of dead rows once (`countP_add_split`).
* `FooterCount`: distinct in-range ordinals of applicable position deletes
  subtract exactly the pruned positions from the footer count; proved by a
  permutation between the erased-duplicate filter and the range filter.
* `ManifestHorizon`: pruning manifests that cannot contain a delete newer than
  the row never changes whether any applicable delete hits the row.
* `NullAwarePruning`: dropping bounds candidates whose key interval (or NULL
  flag) excludes them never removes a hitting delete.
* `FieldIdProbe`: positional lookup through the layout index equals the
  `zip`/`find?` key search for every requested field.
* `CompactOrdinal`: after compaction, a survivor's new ordinal is its survivor
  rank among the kept prefix.
* `DeleteAwareLimit`: overfetching by an upper bound on the dead-row count
  yields the same LIMIT prefix as filtering first.
* `SkippedOrdinalBase`: row-group skipping keeps global `zipIdx` ordinals; the
  fold's output equals one filterMap over the flattened group list.
* `BitmapEpoch`: a key-only bitmap stays valid across adjacent sequence
  intervals.

Only core Lean and bundled `Std` lemmas plus the shared `Helpers` library are
used; no Mathlib.
-/

/-! ## Shared helpers -/

/-- Commutativity of the `BEq` boolean test for lawful equality types. -/
private theorem beq_comm_bool {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  by_cases h : (a == b) = true
  · have hab : a = b := beq_iff_eq.mp h
    subst hab
    rfl
  · have hne : ¬((b == a) = true) := fun hh => h (beq_iff_eq.mpr (Eq.symm (beq_iff_eq.mp hh)))
    rw [Bool.eq_false_of_ne_true h, Bool.eq_false_of_ne_true hne]

/-- Reassociation of boolean disjunction. -/
private theorem or_ac2 (a b c : Bool) : ((a || b) || c) = (b || (a || c)) := by
  cases a <;> cases b <;> cases c <;> rfl

/-- Both sides of a true conjunction are true. -/
private theorem band {a b : Bool} (h : (a && b) = true) : a = true ∧ b = true := by
  cases a <;> cases b <;> simp_all

/-- `any` over a filter factors into a conjunctive predicate. -/
private theorem any_filter (l : List α) (p q : α → Bool) :
    (l.filter p).any q = l.any (fun x => p x && q x) := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      by_cases hp : p a = true
      · rw [List.filter_cons_of_pos hp, List.any_cons, List.any_cons, ih]
        by_cases hq : q a = true <;> simp [hp, hq]
      · rw [List.filter_cons_of_neg hp, List.any_cons, ih]
        simp [hp, Bool.false_or]

/-! ## EqualityHorizon -/

/-- One step of the `eqIndex` fold. -/
private def fstep (m : Std.HashMap (Option Part × Option Int) Nat) (d : EqDel) :
    Std.HashMap (Option Part × Option Int) Nat :=
  m.insert (d.scope, d.key) (max (m.getD (d.scope, d.key) 0) d.seq)

private theorem eqHorizon_aux (ds : List EqDel) :
    ∀ (m : Std.HashMap (Option Part × Option Int) Nat) (r : Row),
      (r.seq < max ((ds.foldl fstep m).getD (none, r.key) 0)
          ((ds.foldl fstep m).getD (some r.part, r.key) 0)) ↔
        (r.seq < max (m.getD (none, r.key) 0) (m.getD (some r.part, r.key) 0) ∨
            ds.any (eqHit r) = true) := by
  induction ds with
  | nil =>
      intro m r
      rw [List.foldl_nil, List.any_nil, Bool.false_eq_true, or_false]
  | cons d ds ih =>
      intro m r
      rw [List.foldl_cons, ih (fstep m d) r, List.any_cons]
      by_cases hhit : eqHit r d = true
      · have h1 : ((d.scope.all (fun x => x == r.part) && d.key == r.key) && r.seq < d.seq) =
          true := hhit
        have hc := band h1
        have hd2 := band hc.1
        have hlt : r.seq < d.seq := decide_eq_true_eq.mp hc.2
        have hhall' : d.scope.all (fun x => x == r.part) = true := hd2.1
        have hkey : d.key = r.key := beq_iff_eq.mp hd2.2
        have hk : (d.scope, d.key) = (none, r.key) ∨
            (d.scope, d.key) = (some r.part, r.key) := by
          cases hd : d.scope with
          | none => exact Or.inl (by simp [hkey])
          | some q =>
              have hq : q = r.part := by
                have h2 : (some q).all (fun x => x == r.part) = true := by rw [← hd]; exact hhall'
                simpa using h2
              exact Or.inr (by simp [hq, hkey])
        have hnew : r.seq < max ((fstep m d).getD (none, r.key) 0)
            ((fstep m d).getD (some r.part, r.key) 0) := by
          unfold fstep
          rw [Std.HashMap.getD_insert, Std.HashMap.getD_insert]
          rcases hk with hkeq | hkeq
          · rw [ite_eq_left (beq_iff_eq.mpr hkeq)]
            rw [ite_eq_right (by intro hc; rw [beq_iff_eq.mp hc] at hkeq; simp at hkeq)]
            exact Nat.lt_of_lt_of_le hlt
              (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_left _ _))
          · rw [ite_eq_right (by intro hc; rw [beq_iff_eq.mp hc] at hkeq; simp at hkeq)]
            rw [ite_eq_left (beq_iff_eq.mpr hkeq)]
            exact Nat.lt_of_lt_of_le hlt
              (Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_right _ _))
        exact ⟨fun _ => Or.inr (by rw [hhit, Bool.true_or]), fun _ => Or.inl hnew⟩
      · rw [Bool.eq_false_of_ne_true hhit, Bool.false_or]
        by_cases hK : ((d.scope, d.key) == (none, r.key)) = true
        · obtain ⟨hsk, hdk⟩ : d.scope = none ∧ d.key = r.key := by simpa using hK
          have heq : eqHit r d = decide (r.seq < d.seq) := by
            show ((d.scope.all (fun x => x == r.part) && d.key == r.key) && r.seq < d.seq) = _
            rw [hsk, hdk]
            rw [show (none.all (fun x => x == r.part)) = true from rfl,
              show (r.key == r.key) = true from beq_iff_eq.mpr rfl]
            simp only [Bool.true_and]
          have hfalse : eqHit r d = false := Bool.eq_false_of_ne_true hhit
          rw [heq] at hfalse
          have hge : d.seq ≤ r.seq :=
            Nat.le_of_not_lt (decide_eq_false_iff_not.mp hfalse)
          have e1 : (fstep m d).getD (none, r.key) 0 = max (m.getD (none, r.key) 0) d.seq := by
            unfold fstep
            rw [Std.HashMap.getD_insert, ite_eq_left hK, hsk, hdk]
          have e2 : (fstep m d).getD (some r.part, r.key) 0 =
              m.getD (some r.part, r.key) 0 := by
            unfold fstep
            rw [Std.HashMap.getD_insert, ite_eq_right]
            intro hc
            have h2 : (d.scope, d.key) = (some r.part, r.key) := beq_iff_eq.mp hc
            rw [hsk, hdk] at h2
            exact absurd h2 (by simp)
          rw [e1, e2]
          refine ⟨fun h => h.imp ?_ id, fun h => h.imp ?_ id⟩ <;> intro g <;> omega
        · by_cases hL : ((d.scope, d.key) == (some r.part, r.key)) = true
          · obtain ⟨hsk, hdk⟩ : d.scope = some r.part ∧ d.key = r.key := by simpa using hL
            have heq : eqHit r d = decide (r.seq < d.seq) := by
              show ((d.scope.all (fun x => x == r.part) && d.key == r.key) && r.seq < d.seq) = _
              rw [hsk, hdk]
              rw [show ((some r.part).all (fun x => x == r.part)) = true from
                  beq_iff_eq.mpr rfl,
                show (r.key == r.key) = true from beq_iff_eq.mpr rfl]
              simp only [Bool.true_and]
            have hfalse : eqHit r d = false := Bool.eq_false_of_ne_true hhit
            rw [heq] at hfalse
            have hge : d.seq ≤ r.seq :=
              Nat.le_of_not_lt (decide_eq_false_iff_not.mp hfalse)
            have e1 : (fstep m d).getD (none, r.key) 0 = m.getD (none, r.key) 0 := by
              unfold fstep
              rw [Std.HashMap.getD_insert, ite_eq_right hK]
            have e2 : (fstep m d).getD (some r.part, r.key) 0 =
                max (m.getD (some r.part, r.key) 0) d.seq := by
              unfold fstep
              rw [Std.HashMap.getD_insert, ite_eq_left hL, hsk, hdk]
            rw [e1, e2]
            refine ⟨fun h => h.imp ?_ id, fun h => h.imp ?_ id⟩ <;> intro g <;> omega
          · have e1 : (fstep m d).getD (none, r.key) 0 = m.getD (none, r.key) 0 := by
              unfold fstep
              rw [Std.HashMap.getD_insert, ite_eq_right hK]
            have e2 : (fstep m d).getD (some r.part, r.key) 0 =
                m.getD (some r.part, r.key) 0 := by
              unfold fstep
              rw [Std.HashMap.getD_insert, ite_eq_right hL]
            rw [e1, e2]

/-- Soundness of the equality-delete horizon index: a row sits below the per-key maximum
delete sequence iff some applicable equality delete is strictly newer than the row. -/
theorem sound_EqualityHorizon : EqualityHorizon.Goal := by
  intro ds r
  dsimp only [EqualityHorizon.Goal]
  have he : eqIndex ds = ds.foldl fstep {} := rfl
  have hbase : max (({} : Std.HashMap (Option Part × Option Int) Nat).getD (none, r.key) 0)
      (({} : Std.HashMap (Option Part × Option Int) Nat).getD (some r.part, r.key) 0) = 0 := by
    simp [Std.HashMap.getD_empty]
  have h := eqHorizon_aux ds {} r
  rw [← he, hbase] at h
  exact ⟨fun hv => (h.mp hv).elim (fun hz => absurd hz (Nat.not_lt_zero _)) id,
    fun hv => h.mpr (Or.inr hv)⟩

/-! ## PositionMask -/

private theorem foldl_contains (ds : List α) (f : α → Nat) :
    ∀ (s : Std.HashSet Nat) (a : Nat),
      (ds.foldl (fun (t : Std.HashSet Nat) d => t.insert (f d)) s).contains a =
        (ds.any (fun d => f d == a) || s.contains a) := by
  induction ds with
  | nil => intro s a; simp
  | cons d ds ih =>
      intro s a
      rw [List.foldl_cons, ih (s.insert (f d)), Std.HashSet.contains_insert, List.any_cons]
      exact (or_ac2 (f d == a) (ds.any (fun d => f d == a)) (s.contains a)).symm

/-- Soundness of building the position-delete mask once per file: for rows sharing the
file, partition and sequence, set membership agrees with the per-row scan of deletes. -/
theorem sound_PositionMask : PositionMask.Goal := by
  intro rows ds file part seq hrows
  apply List.filter_congr
  intro r hr
  obtain ⟨h1, h2, h3⟩ := hrows r hr
  have hmem : (ordinalSet ds file part seq).contains r.pos =
      ds.any (fun d => d.file == file && d.part == part && seq ≤ d.seq && d.pos == r.pos) := by
    unfold ordinalSet
    rw [foldl_contains, Std.HashSet.contains_empty, Bool.or_false, any_filter]
  rw [hmem]
  congr 1
  refine List.any_congr rfl (fun d => ?_)
  show (d.file == file && d.part == part && seq ≤ d.seq && d.pos == r.pos) =
    (r.file == d.file && r.pos == d.pos && r.part == d.part && r.seq ≤ d.seq)
  rw [← h1, ← h2, ← h3, beq_comm_bool d.file r.file, beq_comm_bool d.part r.part,
    beq_comm_bool d.pos r.pos]
  by_cases ha : (r.file == d.file) = true <;> by_cases hp : (r.pos == d.pos) = true <;>
    by_cases hb : (r.part == d.part) = true <;> by_cases hc : (r.seq ≤ d.seq) = true <;>
      simp [ha, hp, hb, hc]

/-! ## OverlapCount -/

private theorem countP_add_split (l : List α) (p q : α → Bool) :
    l.countP (fun x => p x || q x) + l.countP (fun x => p x && q x) =
      l.countP p + l.countP q := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.countP_cons, List.countP_cons, List.countP_cons, List.countP_cons]
      cases hp : p a <;> cases hq : q a <;> simp <;> omega

private theorem countP_le_length (l : List α) (p : α → Bool) : l.countP p ≤ l.length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.countP_cons, List.length_cons]
      cases hp : p a <;> simp <;> omega

private theorem filter_not_countP (l : List α) (p : α → Bool) :
    (l.filter (fun x => !p x)).length = l.length - l.countP p := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      cases hp : p a
      · rw [List.filter_cons_of_pos (by simpa using hp), List.length_cons, List.length_cons,
          List.countP_cons, hp]
        simp only [Bool.false_eq_true, reduceIte]
        rw [ih]
        have hle := countP_le_length as p
        omega
      · rw [List.filter_cons_of_neg (by simpa using hp), List.length_cons,
          List.countP_cons, hp]
        simp only [reduceIte]
        rw [ih]
        have hle := countP_le_length as p
        omega

/-- Soundness of the footer-count subtraction: inclusion–exclusion over the two delete
kinds removes exactly the union of dead rows once. -/
theorem sound_OverlapCount : OverlapCount.Goal := by
  intro rows ps es
  dsimp only [OverlapCount.Goal]
  rw [show (fun r => !ps.any (posHit r) && !es.any (eqHit r)) =
      (fun r => !(ps.any (posHit r) || es.any (eqHit r))) from by
        funext r; simp [Bool.not_or]]
  rw [filter_not_countP]
  have hsplit := countP_add_split rows (fun r => ps.any (posHit r)) (fun r => es.any (eqHit r))
  have key : rows.countP (fun r => ps.any (posHit r) || es.any (eqHit r)) =
      rows.countP (fun r => ps.any (posHit r)) + rows.countP (fun r => es.any (eqHit r)) -
        rows.countP (fun r => ps.any (posHit r) && es.any (eqHit r)) := by
    omega
  rw [key]

/-! ## FooterCount -/

private theorem any_map (l : List α) (f : α → β) (p : β → Bool) :
    (l.map f).any p = l.any (fun x => p (f x)) := by
  induction l with
  | nil => rfl
  | cons a as ih => rw [List.map_cons, List.any_cons, List.any_cons, ih]

private theorem any_beq_eq_mem (T : List Nat) (x : Nat) :
    (T.any (fun q => q == x)) = true ↔ x ∈ T := by
  induction T with
  | nil => simp
  | cons a as ih =>
      rw [List.any_cons, List.mem_cons]
      constructor
      · intro h
        rw [Bool.or_eq_true] at h
        exact h.elim (fun hh => Or.inl (Eq.symm (beq_iff_eq.mp hh)))
          (fun hh => Or.inr (ih.mp hh))
      · intro h
        rw [Bool.or_eq_true]
        exact h.elim (fun hh => Or.inl (beq_iff_eq.mpr hh.symm)) (fun hh => Or.inr (ih.mpr hh))

private theorem nodup_filter {α : Type} [BEq α] [LawfulBEq α] (l : List α) (h : l.Nodup)
    (p : α → Bool) : (l.filter p).Nodup := by
  induction l with
  | nil => exact List.nodup_nil
  | cons a as ih =>
      by_cases hp : p a = true
      · rw [List.filter_cons_of_pos hp]
        rw [List.nodup_cons] at h ⊢
        refine ⟨?_, ih h.2⟩
        intro hmem
        exact h.1 (List.mem_filter.mp hmem).1
      · rw [List.filter_cons_of_neg hp]
        rw [List.nodup_cons] at h
        exact ih h.2

private theorem footer_aux (T : List Nat) (n : Nat) :
    n - ((T.filter (fun q => q < n)).eraseDups).length =
      ((List.range n).filter (fun p => !(T.any (fun q => q == p)))).length := by
  have hn1 : ((T.filter (fun q => q < n)).eraseDups).Nodup := nodup_eraseDups _
  have hn2 : ((List.range n).filter (fun p => T.any (fun q => q == p))).Nodup :=
    nodup_filter _ List.nodup_range _
  have hperm : List.Perm ((T.filter (fun q => q < n)).eraseDups)
      ((List.range n).filter (fun p => T.any (fun q => q == p))) := by
    refine perm_of_nodup_mem hn1 _ hn2 (fun x => ?_)
    simp only [List.mem_eraseDups, List.mem_filter, List.mem_range, any_beq_eq_mem,
      decide_eq_true_eq]
    exact ⟨fun ⟨h1, h2⟩ => ⟨h2, h1⟩, fun ⟨h1, h2⟩ => ⟨h2, h1⟩⟩
  have hlen := hperm.length_eq
  have hpart := (filter_not_partition (fun p => T.any (fun q => q == p)) (List.range n)).length_eq
  simp only [List.length_append, List.length_range] at hlen hpart
  omega

/-- Soundness of metadata-only COUNT: distinct in-range ordinals of the applicable
position deletes subtract exactly the number of positions pruned from the footer count. -/
theorem sound_FooterCount : FooterCount.Goal := by
  intro n file seq part ps
  dsimp only [FooterCount.Goal]
  let L := ps.filter (fun d => d.file == file && d.part == part && seq ≤ d.seq)
  have hLd : L = ps.filter (fun d => d.file == file && d.part == part && seq ≤ d.seq) := rfl
  have hpos : ∀ p, ps.any (fun d => posHit ⟨file, p, part, seq, none⟩ d) =
      (L.map PosDel.pos).any (fun q => q == p) := by
    intro p
    have h1 : ps.any (fun d => posHit ⟨file, p, part, seq, none⟩ d) =
        ps.any (fun d => (d.file == file && d.part == part && seq ≤ d.seq) && d.pos == p) :=
      List.any_congr rfl (fun d => by
        show (file == d.file && p == d.pos && part == d.part && seq ≤ d.seq) = _
        rw [beq_comm_bool d.file file, beq_comm_bool d.part part, beq_comm_bool d.pos p]
        by_cases ha : (file == d.file) = true <;> by_cases hp : (p == d.pos) = true <;>
          by_cases hb : (part == d.part) = true <;> by_cases hc : (seq ≤ d.seq) = true <;>
            simp [ha, hp, hb, hc])
    have h2 : ps.any (fun d => (d.file == file && d.part == part && seq ≤ d.seq) && d.pos == p) =
        L.any (fun d => d.pos == p) := by
      rw [← any_filter, hLd]
    have h3 : L.any (fun d => d.pos == p) = (L.map PosDel.pos).any (fun q => q == p) :=
      (any_map L PosDel.pos (fun q => q == p)).symm
    exact h1.trans (h2.trans h3)
  rw [show (fun p => !ps.any (fun d => posHit ⟨file, p, part, seq, none⟩ d)) =
      (fun p => !(L.map PosDel.pos).any (fun q => q == p)) from by
        funext p; exact congrArg (fun b => !b) (hpos p)]
  exact footer_aux (L.map PosDel.pos) n

/-! ## ManifestHorizon -/

private theorem hit_implies_open (m : Manifest) (hm : synopsisOK m) (r : Row) (d : EqDel)
    (hd : d ∈ m.entries) (hh : eqHit r d = true) : mayOpen r m = true := by
  have hscope : m.scope.all (fun x => x == r.part) = true := by
    have hall : d.scope.all (fun x => x == r.part) = true := (band (band hh).1).1
    cases hs : m.scope with
    | none => exact rfl
    | some p =>
        have hd' : d.scope = some p := (hm d hd).1 p hs
        have hq : p = r.part := by
          have h2 : (some p).all (fun x => x == r.part) = true := by rw [← hd']; exact hall
          simpa using h2
        subst hq
        exact show ((some r.part).all (fun x => x == r.part)) = true from
          beq_iff_eq.mpr rfl
  have hupper : m.upper.all (fun u => r.seq < u) = true := by
    have hlt : decide (r.seq < d.seq) = true := (band hh).2
    have hlt' : r.seq < d.seq := decide_eq_true_eq.mp hlt
    cases hu : m.upper with
    | none => exact rfl
    | some u =>
        have hdle : d.seq ≤ u := (hm d hd).2 u hu
        show (decide (r.seq < u)) = true
        exact decide_eq_true_eq.mpr (Nat.lt_of_lt_of_le hlt' hdle)
  show ((m.scope.all (fun x => x == r.part)) && (m.upper.all (fun u => r.seq < u))) = true
  rw [hscope, hupper]
  simp

/-- Soundness of manifest-level pruning: pruning manifests that cannot contain a delete
newer than the row never changes whether any applicable delete hits the row. -/
theorem sound_ManifestHorizon : ManifestHorizon.Goal := by
  intro ms
  induction ms with
  | nil => intro r H; rfl
  | cons m ms ih =>
      intro r H
      have Hm : synopsisOK m := H m (by simp)
      have Ht : ∀ m' ∈ ms, synopsisOK m' := fun m' hm => H m' (by simp [hm])
      by_cases h : mayOpen r m = true
      · rw [List.filter_cons_of_pos h, List.flatMap_cons, List.flatMap_cons, List.any_append,
          List.any_append, ih r Ht]
      · have hnone : (m.entries.any (eqHit r)) = false := by
          rw [List.any_eq_false]
          intro d hd hh
          exact absurd (hit_implies_open m Hm r d hd hh) h
        rw [List.filter_cons_of_neg h, List.flatMap_cons, List.any_append, ih r Ht, hnone,
          Bool.false_or]

/-! ## NullAwarePruning -/

/-- Soundness of bounds-based equality-delete pruning: dropping candidates whose key
interval (or NULL flag) excludes them never removes a delete that could hit a row,
because a hitting delete carries exactly the row's key. -/
theorem sound_NullAwarePruning : NullAwarePruning.Goal := by
  intro rows ds lo hi hasNull Hrows
  dsimp only [NullAwarePruning.Goal]
  apply List.filter_congr
  intro r hr
  have key : (ds.filter (fun d => possible lo hi hasNull d.key)).any (eqHit r) =
      ds.any (eqHit r) := by
    rw [any_filter]
    refine List.any_congr rfl (fun d => ?_)
    show ((possible lo hi hasNull d.key) && eqHit r d) = eqHit r d
    cases hc : possible lo hi hasNull d.key <;> cases hh : eqHit r d <;> try rfl
    exfalso
    have hkey : d.key = r.key := beq_iff_eq.mp ((band (band hh).1).2)
    have hpos : possible lo hi hasNull d.key = true := by
      have hpr := Hrows r hr
      rw [← hkey] at hpr
      exact hpr
    rw [hc] at hpos
    exact absurd hpos (by simp)
  rw [key]

/-! ## FieldIdProbe -/

private theorem map_pointwise : ∀ (fs : List α) (f g : α → β),
    (∀ a ∈ fs, f a = g a) → fs.map f = fs.map g := by
  intro fs
  induction fs with
  | nil => intro f g _; rfl
  | cons x xs ih =>
      intro f g h
      rw [List.map_cons, List.map_cons, h x (List.mem_cons.mpr (Or.inl rfl)),
        ih f g (fun a ha => h a (List.mem_cons.mpr (Or.inr ha)))]

private theorem probe_pt : ∀ (layout : List Nat) (f : Nat) (row : List (Option Int)),
    layout.length = row.length →
    (row[List.idxOf f layout]?).join =
      ((layout.zip row).find? (fun kv => kv.1 == f)).bind Prod.snd := by
  intro layout
  induction layout with
  | nil =>
      intro f row hlen
      cases row with
      | nil => simp [List.idxOf]
      | cons r restrow => simp at hlen
  | cons a rest ih =>
      intro f row hlen
      cases row with
      | nil => simp at hlen
      | cons r restrow =>
          rw [List.length_cons, List.length_cons] at hlen
          have hlen' : rest.length = restrow.length := by omega
          by_cases heq : a = f
          · subst heq
            have hbe : (a == a) = true := beq_iff_eq.mpr rfl
            have hidx : List.idxOf a (a :: rest) = 0 := by
              show List.findIdx (fun x => x == a) (a :: rest) = 0
              rw [List.findIdx_cons]
              exact ite_eq_left hbe
            have hlhs : ((r :: restrow)[List.idxOf a (a :: rest)]?).join = r := by
              rw [hidx, List.getElem?_cons_zero]
              cases r with
              | none => rfl
              | some v => rfl
            have hrhs : (((a :: rest).zip (r :: restrow)).find? (fun kv => kv.1 == a)).bind Prod.snd = r := by
              show (((a, r) :: rest.zip restrow).find? (fun kv => kv.1 == a)).bind Prod.snd = r
              rw [List.find?_cons]
              simp
            rw [hlhs, hrhs]
          · have hbe : (a == f) = false := by
              cases h : a == f with
              | true => exact absurd (beq_iff_eq.mp h) heq
              | false => rfl
            have hidx : List.idxOf f (a :: rest) = List.idxOf f rest + 1 := by
              show List.findIdx (fun x => x == f) (a :: rest) = List.findIdx (fun x => x == f) rest + 1
              rw [List.findIdx_cons]
              exact ite_eq_right (by simpa using hbe)
            have hlhs : ((r :: restrow)[List.idxOf f (a :: rest)]?).join
                = (restrow[List.idxOf f rest]?).join := by
              rw [hidx, List.getElem?_cons_succ]
            have hmid : ((a :: rest).zip (r :: restrow)).find? (fun kv => kv.1 == f)
                = (rest.zip restrow).find? (fun kv => kv.1 == f) := by
              show ((a, r) :: rest.zip restrow).find? (fun kv => kv.1 == f) = _
              rw [List.find?_cons]
              simp [hbe]
            rw [hlhs, hmid]
            exact ih f restrow hlen'

/-- Soundness of field-id probing: positional lookup through the layout index equals the
`zip`/`find?` key search for every requested field. -/
theorem sound_FieldIdProbe : FieldIdProbe.Goal := by
  intro layout fields row hlen _
  dsimp only
  rw [List.map_map]
  exact map_pointwise fields _ _ (fun x _ => probe_pt layout x row hlen.symm)

/-! ## CompactOrdinal -/

/-- After applying pinned deletes, compaction densely renumbers survivors: a survivor's
new ordinal is its survivor rank among the kept prefix. -/
theorem sound_CompactOrdinal : CompactOrdinal.Goal := by
  intro rows
  induction rows with
  | nil =>
      intro ps es i r hi _
      cases i <;> simp at hi
  | cons f rest ih =>
      intro ps es i r hi keep hkr
      cases i with
      | zero =>
          rw [List.getElem?_cons_zero] at hi
          have hf : f = r := by simpa using hi
          subst hf
          rw [List.take_zero, List.countP_nil, List.filter_cons]
          simp [hkr]
      | succ n =>
          rw [List.getElem?_cons_succ] at hi
          cases hkf : keep f with
          | true =>
              rw [show (f :: rest).take (Nat.succ n) = f :: rest.take n from rfl,
                List.countP_cons, ite_eq_left hkf,
                List.filter_cons_of_pos hkf, List.getElem?_cons_succ]
              exact ih ps es n r hi hkr
          | false =>
              rw [show (f :: rest).take (Nat.succ n) = f :: rest.take n from rfl,
                List.countP_cons, ite_eq_right (by simp [hkf]),
                List.filter_cons_of_neg (by simp [hkf]),
                Nat.add_zero]
              exact ih ps es n r hi hkr

/-! ## DeleteAwareLimit -/

private theorem dl_aux : ∀ (l : List α) (dead : α → Bool) (k upper : Nat),
    List.countP dead l ≤ upper →
    ((l.take (k + upper)).filter (fun r => !dead r)).take k
      = (l.filter (fun r => !dead r)).take k := by
  intro l
  induction l with
  | nil => intro dead k upper h; simp
  | cons a as ih =>
      intro dead k upper h
      cases hnd : dead a with
      | true =>
          have hup : 1 ≤ upper := by
            have h1 : 1 ≤ List.countP dead (a :: as) := by
              rw [List.countP_cons, ite_eq_left hnd]
              omega
            omega
          have hrest : List.countP dead as ≤ upper - 1 := by
            rw [List.countP_cons, ite_eq_left hnd] at h
            omega
          cases hk : k with
          | zero => simp
          | succ k =>
              rw [show k + 1 + upper = k + upper + 1 from by omega]
              show ((a :: as.take (k + upper)).filter (fun r => !dead r)).take (k + 1)
                = (List.filter (fun r => !dead r) (a :: as)).take (k + 1)
              rw [List.filter_cons, List.filter_cons]
              simp [hnd]
              rw [show k + upper = k + 1 + (upper - 1) from by omega]
              exact ih dead (k + 1) (upper - 1) hrest
      | false =>
          have hrest : List.countP dead as ≤ upper := by
            have h1 : List.countP dead (a :: as) = List.countP dead as := by
              rw [List.countP_cons]
              simp [hnd]
            omega
          cases hk : k with
          | zero => simp
          | succ k =>
              rw [show k + 1 + upper = k + upper + 1 from by omega]
              show ((a :: as.take (k + upper)).filter (fun r => !dead r)).take (k + 1)
                = (List.filter (fun r => !dead r) (a :: as)).take (k + 1)
              rw [List.filter_cons, List.filter_cons]
              simp [hnd]
              rw [ih dead k upper hrest]

/-- Ordered scan can overfetch by an upper bound on the number of dead rows: only
qualifying survivors consume LIMIT slots. -/
theorem sound_DeleteAwareLimit : DeleteAwareLimit.Goal := by
  intro ordered ps es k upper dead Hdead
  exact dl_aux ordered dead k upper Hdead

/-! ## SkippedOrdinalBase -/

private theorem filterMap_append (f : α → Option β) : ∀ (xs ys : List α),
    (xs ++ ys).filterMap f = xs.filterMap f ++ ys.filterMap f := by
  intro xs
  induction xs with
  | nil => intro ys; rfl
  | cons x xs ih =>
      intro ys
      cases hfx : f x with
      | none => simp only [List.cons_append, List.filterMap_cons, hfx, ih]
      | some b => simp only [List.cons_append, List.filterMap_cons, hfx, ih]

private theorem zipIdx_fM_shift (keep : Int → Bool) (deleted : Nat → Bool) :
    ∀ (g : List Int) (k j : Nat),
      (g.zipIdx k).filterMap (fun (v, i) =>
        if keep v && !deleted (j + i) then some v else none)
        = (g.zipIdx (k + j)).filterMap (fun (v, i) =>
          if keep v && !deleted i then some v else none) := by
  intro g
  induction g with
  | nil => intro k j; rfl
  | cons v rest ih =>
      intro k j
      simp only [List.zipIdx_cons, List.filterMap_cons]
      have h1 : (if keep v && !deleted (j + k) then some v else none)
          = (if keep v && !deleted (k + j) then some v else none) := by
        rw [show j + k = k + j from Nat.add_comm j k]
      rw [h1, ih (k + 1) j]
      rw [show k + 1 + j = k + j + 1 from by omega]

private theorem zipIdx_fM_nil (keep : Int → Bool) (deleted : Nat → Bool) :
    ∀ (g : List Int) (k : Nat), (∀ v ∈ g, keep v = false) →
      (g.zipIdx k).filterMap (fun (v, i) =>
        if keep v && !deleted i then some v else none) = [] := by
  intro g
  induction g with
  | nil => intro k _; rfl
  | cons v rest ih =>
      intro k h
      simp only [List.zipIdx_cons, List.filterMap_cons]
      rw [h v (List.mem_cons.mpr (Or.inl rfl))]
      rw [ih (k + 1) (fun w hw => h w (List.mem_cons.mpr (Or.inr hw)))]
      simp

private theorem scan_eq (keep : Int → Bool) (deleted : Nat → Bool) :
    ∀ (gs : List (List Int)) (base : Nat) (out : List Int),
      (gs.foldl (fun (base, out) g =>
          let live := g.zipIdx.filterMap (fun (v, i) =>
            if keep v && !deleted (base + i) then some v else none)
          (base + g.length, if g.any keep then out ++ live else out)) (base, out)).2
        = out ++ (gs.flatten.zipIdx base).filterMap (fun (v, i) =>
          if keep v && !deleted i then some v else none) := by
  intro gs
  induction gs with
  | nil => intro base out; simp
  | cons g gs ih =>
      intro base out
      have hfold : (g :: gs).foldl (fun (base, out) g =>
          let live := g.zipIdx.filterMap (fun (v, i) =>
            if keep v && !deleted (base + i) then some v else none)
          (base + g.length, if g.any keep then out ++ live else out)) (base, out)
          = gs.foldl (fun (base, out) g =>
            let live := g.zipIdx.filterMap (fun (v, i) =>
              if keep v && !deleted (base + i) then some v else none)
            (base + g.length, if g.any keep then out ++ live else out))
              (base + g.length, if g.any keep
                then out ++ (g.zipIdx.filterMap (fun (v, i) =>
                  if keep v && !deleted (base + i) then some v else none))
                else out) := rfl
      rw [hfold, ih (base + g.length)
        (if g.any keep
          then out ++ (g.zipIdx.filterMap (fun (v, i) =>
            if keep v && !deleted (base + i) then some v else none))
          else out)]
      have hsh : (g.zipIdx.filterMap (fun (v, i) =>
          if keep v && !deleted (base + i) then some v else none))
          = (g.zipIdx base).filterMap (fun (v, i) =>
            if keep v && !deleted i then some v else none) := by
        rw [zipIdx_fM_shift keep deleted g 0 base, Nat.zero_add]
      simp only [List.flatten_cons, List.zipIdx_append, filterMap_append]
      rw [hsh]
      clear hsh hfold ih
      by_cases hA : g.any keep = true
      · rw [ite_eq_left hA, List.append_assoc]
      · rw [ite_eq_right hA]
        have hA' : g.any keep = false := by cases h2 : g.any keep <;> simp_all
        rw [List.any_eq_false] at hA'
        have hfalse : ∀ v ∈ g, keep v = false := by
          intro v hv
          have := hA' v hv
          cases h : keep v <;> simp_all
        rw [zipIdx_fM_nil keep deleted g base hfalse, List.nil_append]

/-- Row-group skipping keeps the global `zipIdx` ordinals: a rejected group still
advances the base offset by its full record count. -/
theorem sound_SkippedOrdinalBase : SkippedOrdinalBase.Goal := by
  intro gs keep deleted
  exact scan_eq keep deleted gs 0 []

/-! ## BitmapEpoch -/

private theorem eqHit_epoch (d : EqDel) (r : Row) (newer : Nat)
    (hr : r.seq ≤ newer)
    (hd : d.scope.all (fun s => s == r.part) = true → d.key = r.key →
      ¬ (r.seq < d.seq ∧ d.seq ≤ newer)) :
    eqHit r d = eqHit { r with seq := newer } d := by
  by_cases h1 : (d.scope.all (fun s => s == r.part)) = true
  · by_cases h2 : (d.key == r.key) = true
    · by_cases h3 : (decide (r.seq < d.seq)) = true
      · have hlt : r.seq < d.seq := decide_eq_true_eq.mp h3
        have h2' : d.key = r.key := beq_iff_eq.mp h2
        have hn : ¬ (d.seq ≤ newer) := by
          intro hc
          exact hd h1 h2' ⟨hlt, hc⟩
        have h4 : decide (newer < d.seq) = true := by
          rw [decide_eq_true_eq]
          omega
        simp [eqHit, h1, h2, h3, h4]
      · rw [decide_eq_true_eq] at h3
        have h4 : decide (newer < d.seq) = false := by
          rw [decide_eq_false_iff_not]
          omega
        simp [eqHit, h1, h2, h3, h4]
    · simp [eqHit, h1, h2]
  · simp [eqHit, h1]

/-- A key-only bitmap stays valid across adjacent sequence intervals. -/
theorem sound_BitmapEpoch : BitmapEpoch.Goal := by
  intro ds
  induction ds with
  | nil => intro r newer hr hd; rfl
  | cons d ds ih =>
      intro r newer hr hd
      rw [List.any_cons, List.any_cons]
      rw [eqHit_epoch d r newer hr (hd d (List.mem_cons.mpr (Or.inl rfl)))]
      rw [ih r newer hr (fun d hd' => hd d (List.mem_cons.mpr (Or.inr hd')))]

end TinylambOpt.ResearchProofs.CloudBI.DeleteFiles
