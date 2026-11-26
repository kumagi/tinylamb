import TinylambOpt.ResearchGoals.AnalyticalOperators.FilePruningAndDeleteMasks
import TinylambOpt.ResearchProofs.CloudBI.Common

/-!
These models connect SQL operators to their reference rows, order, or aggregate results.

Proved (7 claims, including the following)

- `ArithmeticFooter` — When file row IDs form an arithmetic progression, derive
  COUNT/SUM from the footer and subtract only distinct, in-range deleted positions.
  Handles complete deletion.
- `FileKeyAntiJoin` — Decorrelate the equality anti-join by compiling pairs (file,key),
  then test each data row once. The reference is the file join followed by delete
  application.
- `IncrementalSurvivors` — `∀ old fresh rows, incremental old fresh rows = rows.filter
  (fun r => !(old ++ fresh).any (eqHit r))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.AnalyticalOperators.FilePruningAndDeleteMasks

open TinylambOpt.ResearchGoals.AnalyticalOperators.FilePruningAndDeleteMasks
open TinylambOpt.ResearchGoals.CloudBI.DeleteFiles
open TinylambOpt.ResearchProofs.CloudBI.Helpers

/-!
# Soundness proofs for the DuckDB-gap file-mask goals

All seven goals are TRUE; each is proved by a concrete Lean term.

* `ReferencedFiles`: the uniqueness hypothesis makes each row's file-filter a
  singleton, so the flat-map is exactly the row measure list.
* `FileKeyAntiJoin`: `(file id, key)` pairs collected by `keysByFile` are in
  one-to-one correspondence with applicable equality deletes for the unique
  file referenced by a row; per-row case analysis closes the claim.
* `SharedDeleteTable`: the compiled table is keyed by distinct signatures,
  and its key set is exactly the applicable-delete key set of the file.
* `PartitionSpecialization`: for a file in the target partition every
  applicable delete satisfies the specialization predicate (sequence frontier
  plus partition/global scope), so the per-file delete filters agree.
* `IncrementalSurvivors`: an equality delete that hits a row contributes that
  row's key to the changed-key set; the nested filters therefore are exactly
  the filter by the concatenated delete set.
* `LateEqualityKey`: a right fold over rows keeps precisely the rows that are
  neither position- nor equality-deleted and counts every non-position-deleted
  row; `countP` complement closes the count.
* `ArithmeticFooter`: the in-range distinct deleted positions are a permutation
  of the pruned positions in `[0,n)`; partition the range into kept/deleted,
  use the footer sum formula (via `List.sum_range'`) and subtract the deleted
  values once.

Only core Lean, bundled `Std`, and the shared CloudBI helper library are used.
-/

/-! ## shared helpers -/

private theorem beq_comm_bool {α : Type} [BEq α] [LawfulBEq α] (a b : α) : (a == b) = (b == a) := by
  by_cases h : (a == b) = true
  · have hab : a = b := beq_iff_eq.mp h
    subst hab
    rfl
  · have hne : ¬((b == a) = true) := fun hh => h (beq_iff_eq.mpr (Eq.symm (beq_iff_eq.mp hh)))
    rw [Bool.eq_false_of_ne_true h, Bool.eq_false_of_ne_true hne]

private theorem band {a b : Bool} (h : (a && b) = true) : a = true ∧ b = true := by
  cases a <;> cases b <;> simp_all

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

private theorem filter_eq_singleton_of_countP_one {α : Type} (l : List α) (p : α → Bool)
    (h : l.countP p = 1) : ∃ a, l.filter p = [a] := by
  rw [List.countP_eq_length_filter] at h
  cases hfl : l.filter p with
  | nil =>
      rw [hfl] at h
      simp at h
  | cons a t =>
      rw [hfl] at h
      cases t with
      | nil => exact ⟨a, rfl⟩
      | cons b t' => simp at h

private theorem unique_file (fs : List File) (fid : Nat)
    (h : fs.countP (fun f => f.id == fid) = 1) :
    ∃ f, f ∈ fs ∧ (f.id == fid) = true ∧ ∀ g ∈ fs, (g.id == fid) = true → g = f := by
  obtain ⟨f, hf⟩ := filter_eq_singleton_of_countP_one fs (fun f => f.id == fid) h
  have hmem : f ∈ fs.filter (fun f => f.id == fid) := by rw [hf]; simp
  have hffs : f ∈ fs := (List.mem_filter.mp hmem).1
  have hfid : (f.id == fid) = true := (List.mem_filter.mp hmem).2
  refine ⟨f, hffs, hfid, ?_⟩
  intro g hgs hgid
  have hgmem : g ∈ fs.filter (fun f => f.id == fid) := List.mem_filter.mpr ⟨hgs, hgid⟩
  rw [hf] at hgmem
  simpa using hgmem

/-! ## ReferencedFiles -/

theorem ReferencedFiles_sound : ReferencedFiles.Goal := by
  intro rows fs href
  have hmain : rows.flatMap (fun (fid,_,v) => (fs.filter (fun f => f.id == fid)).map (fun _ => v))
      = rows.map (fun r => r.2.2) := by
    induction rows with
    | nil => rfl
    | cons r rs ih =>
        rcases r with ⟨fid, kk, v⟩
        have h1 := href (fid,kk,v) (by simp)
        obtain ⟨f, hf⟩ := filter_eq_singleton_of_countP_one fs (fun f => f.id == fid) h1
        have htail : ∀ r' ∈ rs, fs.countP (fun f => f.id == r'.1) = 1 := by
          intro r' hr'; exact href r' (by simp [hr'])
        simp [List.flatMap_cons, hf, ih htail]
  rw [hmain]

/-! ## FileKeyAntiJoin -/

private theorem mem_keysByFile {fid : Nat} {k : Option Int} {fs : List File} {ds : List EqDel} :
    ((fid,k) ∈ keysByFile fs ds) ↔
      ∃ f ∈ fs, f.id = fid ∧ ∃ d ∈ ds, applies f d = true ∧ d.key = k := by
  unfold keysByFile
  rw [List.mem_eraseDups, List.mem_flatMap]
  constructor
  · rintro ⟨f, hf, hmem⟩
    rw [List.mem_map] at hmem
    obtain ⟨d, hd, hpair⟩ := hmem
    refine ⟨f, hf, congrArg Prod.fst hpair, d, (List.mem_filter.mp hd).1, (List.mem_filter.mp hd).2, congrArg Prod.snd hpair⟩
  · rintro ⟨f, hf, hid, d, hds, happl, hkey⟩
    refine ⟨f, hf, ?_⟩
    rw [List.mem_map]
    refine ⟨d, List.mem_filter.mpr ⟨hds, happl⟩, ?_⟩
    exact Prod.ext hid hkey

private theorem pairs_contains_iff (fs : List File) (ds : List EqDel) (f : File)
    (hffs : f ∈ fs) (huniq : ∀ g ∈ fs, (g.id == f.id) = true → g = f) (k : Option Int) :
    ((keysByFile fs ds).contains (f.id, k)) = true ↔
      (ds.any (eqHit ⟨f.id, 0, f.part, f.seq, k⟩)) = true := by
  rw [List.contains_iff_mem, mem_keysByFile]
  constructor
  · rintro ⟨g, hgs, hgid, d, hds, happl, hdk⟩
    have hgidb : (g.id == f.id) = true := beq_iff_eq.mpr hgid
    have hgf := huniq g hgs hgidb
    have happl_f : applies f d = true := by simpa [hgf] using happl
    subst k
    rw [List.any_eq_true]
    refine ⟨d, hds, ?_⟩
    have hc := band happl_f
    have hseq : f.seq < d.seq := decide_eq_true_eq.mp hc.2
    simp [eqHit, hc.1, hseq]
  · intro h
    rw [List.any_eq_true] at h
    obtain ⟨d, hds, hhit⟩ := h
    have hc := band hhit
    have hc2 := band hc.1
    have hkey : d.key = k := beq_iff_eq.mp hc2.2
    refine ⟨f, hffs, rfl, d, hds, ?_, hkey⟩
    have hscope : d.scope.all (fun s => s == f.part) = true := by
      simpa using hc2.1
    have hseq : f.seq < d.seq := decide_eq_true_eq.mp hc.2
    simp [applies, hscope, hseq]


/-! ## FileKeyAntiJoin (continued) -/

private theorem fk_row (fs : List File) (ds : List EqDel) (f : File)
    (hffs : f ∈ fs) (huniq : ∀ g ∈ fs, (g.id == f.id) = true → g = f)
    (hcount : fs.countP (fun g : File => (g.id == f.id)) = 1)
    (k : Option Int) (v : Int) :
      (fs.filter (fun g : File => (g.id == f.id) &&
        !(ds.any (eqHit ⟨f.id, 0, g.part, g.seq, k⟩)))).map (fun _ => v) =
    (if ((keysByFile fs ds).contains (f.id, k)) = true then ([] : List Int) else [v]) := by
  by_cases hp : ((keysByFile fs ds).contains (f.id, k)) = true
  · rw [ite_eq_left hp]
    have hany := (pairs_contains_iff fs ds f hffs huniq k).mp hp
    have hfilt : fs.filter (fun g : File => (g.id == f.id) &&
        !(ds.any (eqHit ⟨f.id, 0, g.part, g.seq, k⟩))) = [] := by
      apply TinylambOpt.ResearchProofs.CloudBI.Helpers.filter_eq_nil_of_all_false
      intro g hg
      by_cases hge : (g.id == f.id) = true
      · have gf := huniq g hg hge
        subst g
        simp [hany]
      · simp [hge]
    rw [hfilt]
    rfl
  · rw [ite_eq_right hp]
    have hany_false : (ds.any (eqHit ⟨f.id, 0, f.part, f.seq, k⟩)) = false := by
      cases h' : ds.any (eqHit ⟨f.id, 0, f.part, f.seq, k⟩)
      · rfl
      · exfalso
        exact hp ((pairs_contains_iff fs ds f hffs huniq k).mpr h')
    obtain ⟨g0, hg0⟩ := filter_eq_singleton_of_countP_one fs
      (fun g : File => (g.id == f.id)) hcount
    have hg0mem : g0 ∈ fs.filter (fun g : File => (g.id == f.id)) := by rw [hg0]; simp
    have g0eq : g0 = f := huniq g0 (List.mem_filter.mp hg0mem).1 (List.mem_filter.mp hg0mem).2
    have hf1 : fs.filter (fun g : File => (g.id == f.id)) = [f] := by simpa [g0eq] using hg0
    have hfilt : fs.filter (fun g : File => (g.id == f.id) &&
        !(ds.any (eqHit ⟨f.id, 0, g.part, g.seq, k⟩))) = [f] := by
      calc
        fs.filter (fun g : File => (g.id == f.id) &&
            !(ds.any (eqHit ⟨f.id, 0, g.part, g.seq, k⟩)))
            = fs.filter (fun g : File => (g.id == f.id)) := by
                apply List.filter_congr
                intro g hg
                by_cases hge : (g.id == f.id) = true
                · have gf := huniq g hg hge
                  subst g
                  simp [hany_false]
                · simp [hge]
        _ = [f] := hf1
    rw [hfilt]
    simp

theorem FileKeyAntiJoin_sound : FileKeyAntiJoin.Goal := by
  intro rows fs ds href
  induction rows with
  | nil => rfl
  | cons r rs ih =>
      rcases r with ⟨fid, k, v⟩
      have h1 := href (fid,k,v) (by simp)
      obtain ⟨f, hffs, hfidb, huniqg⟩ := unique_file fs fid h1
      have hfid : f.id = fid := beq_iff_eq.mp hfidb
      subst fid
      have htail : ∀ r' ∈ rs, fs.countP (fun g => g.id == r'.1) = 1 := by
        intro r' hr'; exact href r' (by simp [hr'])
      have ih' := ih htail
      dsimp only at ih' ⊢
      rw [List.filter_cons, List.flatMap_cons]
      by_cases hnot : ((!((keysByFile fs ds).contains (f.id, k))) = true)
      · have hcf : ((keysByFile fs ds).contains (f.id, k)) = false := by
          cases b : (keysByFile fs ds).contains (f.id, k) <;> simp_all
        rw [ite_eq_left hnot]
        simp only [List.map_cons]
        have hstep := fk_row fs ds f hffs huniqg h1 k v
        rw [hstep]
        rw [hcf]
        simp only [Bool.false_eq_true, reduceIte, List.cons_append, List.nil_append]
        exact congrArg (fun t => v :: t) (by simpa only [List.contains] using ih')
      · have hct : ((keysByFile fs ds).contains (f.id, k)) = true := by
          cases b : (keysByFile fs ds).contains (f.id, k) <;> simp_all
        rw [ite_eq_right hnot]
        have hstep := fk_row fs ds f hffs huniqg h1 k v
        rw [hstep]
        rw [hct]
        simp only [reduceIte, List.nil_append]
        simpa only [List.contains] using ih' 


/-! ## SharedDeleteTable -/

private theorem length_eraseDups_le {α : Type} [BEq α] [LawfulBEq α] :
    (l : List α) → l.eraseDups.length ≤ l.length
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons, List.length_cons, List.length_cons]
      have hf := length_eraseDups_le (List.filter (fun b => !(b == a)) as)
      have hfl := List.length_filter_le (fun b => !(b == a)) as
      omega
  termination_by l => l.length
  decreasing_by
    exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) (Nat.lt_succ_self _)


private theorem lookup_map_of_mem_nodup {K V : Type} [BEq K] [LawfulBEq K]
    (g : K → V) : ∀ (xs : List K), xs.Nodup → ∀ k, k ∈ xs →
      (xs.map (fun x => (x, g x))).lookup k = some (g k) := by
  intro xs hnd k hk
  induction xs with
  | nil => simp at hk
  | cons a as ih =>
      simp only [List.map_cons]
      by_cases hka : (k == a) = true
      · have hkeq : k = a := beq_iff_eq.mp hka
        subst k
        simp
      · have hkas : k ∈ as := by
          cases List.mem_cons.mp hk with
          | inl h => exact absurd (beq_iff_eq.mpr h) hka
          | inr h => exact h
        have hndas : as.Nodup := (List.nodup_cons.mp hnd).2
        simp [List.lookup_cons, hka, ih hndas hkas]

private theorem compile_lookup (fs : List File) (ds : List EqDel) (f : File) (hf : f ∈ fs) :
    (compile fs ds).lookup (signature f) =
      some (((ds.filter (applies ⟨0, f.part, f.seq⟩)).map EqDel.key).eraseDups) := by
  unfold compile
  let sigs := (fs.map signature).eraseDups
  have hmem : signature f ∈ sigs := by
    exact List.mem_eraseDups.mpr (List.mem_map.mpr ⟨f, hf, rfl⟩)
  have hnd : sigs.Nodup := nodup_eraseDups _
  have hl := lookup_map_of_mem_nodup
    (fun (x : Part × Nat) => ((ds.filter (applies ⟨0, x.1, x.2⟩)).map EqDel.key).eraseDups)
    sigs hnd (signature f) hmem
  simpa [sigs, signature] using hl

private theorem keyset_contains (ds : List EqDel) (f : File) (k : Option Int) :
    ((((ds.filter (applies ⟨0, f.part, f.seq⟩)).map EqDel.key).eraseDups).contains k) = true ↔
      (ds.any (eqHit ⟨f.id, 0, f.part, f.seq, k⟩)) = true := by
  rw [List.contains_iff_mem, List.mem_eraseDups, List.mem_map, List.any_eq_true]
  constructor
  · rintro ⟨d, hd_filt, hkeq⟩
    have hds : d ∈ ds := (List.mem_filter.mp hd_filt).1
    have happl : applies ⟨0, f.part, f.seq⟩ d = true := (List.mem_filter.mp hd_filt).2
    subst k
    refine ⟨d, hds, ?_⟩
    have hc := band happl
    have hseq : f.seq < d.seq := decide_eq_true_eq.mp hc.2
    simp [eqHit, hc.1, hseq]
  · rintro ⟨d, hds, hhit⟩
    have hc := band hhit
    have hc2 := band hc.1
    have hkey : d.key = k := beq_iff_eq.mp hc2.2
    refine ⟨d, List.mem_filter.mpr ⟨hds, ?_⟩, hkey⟩
    have hscope : d.scope.all (fun s => s == f.part) = true := by simpa using hc2.1
    have hseq : f.seq < d.seq := decide_eq_true_eq.mp hc.2
    simp [applies, hscope, hseq]

theorem SharedDeleteTable_sound : SharedDeleteTable.Goal := by
  intro fs ds f hf
  dsimp only [SharedDeleteTable.Goal]
  constructor
  · intro k
    rw [compile_lookup fs ds f hf]
    simp only [Option.getD_some]
    have hc := keyset_contains ds f k
    by_cases hk : ((((ds.filter (applies ⟨0, f.part, f.seq⟩)).map EqDel.key).eraseDups).contains k) = true
    · have ha := hc.mp hk
      rw [hk, ha]
    · have hkf : ((((ds.filter (applies ⟨0, f.part, f.seq⟩)).map EqDel.key).eraseDups).contains k) = false :=
        Bool.eq_false_of_ne_true hk
      have hanyf : (ds.any (eqHit ⟨f.id, 0, f.part, f.seq, k⟩)) = false := by
        cases h' : ds.any (eqHit ⟨f.id, 0, f.part, f.seq, k⟩)
        · rfl
        · exfalso
          exact hk (hc.mpr h')
      rw [hkf, hanyf]
  · unfold compile
    rw [List.length_map]
    simpa using (length_eraseDups_le (fs.map signature))

/-! ## PartitionSpecialization -/

private theorem flatMap_eq_of_per_file {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      simp only [List.flatMap_cons]
      rw [h a (by simp), ih (fun b hb => h b (by simp [hb]))]

theorem PartitionSpecialization_sound : PartitionSpecialization.Goal := by
  intro fs ds p oldest hpre
  dsimp only [PartitionSpecialization.Goal]
  let target := fs.filter (fun (f : File) => f.part == p)
  let selected := ds.filter (fun (d : EqDel) => (d.scope == some p || d.scope.isNone) && oldest < d.seq)
  have hsel : ∀ f, f ∈ fs → f.part = p →
      (List.filter (fun d : EqDel => (d.scope == some p || d.scope.isNone) && oldest < d.seq) ds).filter (applies f)
        = ds.filter (applies f) := by
    intro f hffs hfp
    have hfp_beq : (f.part == p) = true := beq_iff_eq.mpr hfp
    have hseq_ge : oldest ≤ f.seq := hpre f hffs hfp
    rw [List.filter_filter]
    apply List.filter_congr
    intro d hd
    by_cases hap : (applies f d) = true
    · have hc := band hap
      have hlt : f.seq < d.seq := decide_eq_true_eq.mp hc.2
      have hscope_ok : ((d.scope == some p) || d.scope.isNone) = true := by
        cases hs : d.scope with
        | none => simp
        | some q =>
            have hqfp : (q == f.part) = true := by
              have hh := hc.1
              rw [hs] at hh
              simpa using hh
            have hqf : q = f.part := beq_iff_eq.mp hqfp
            have hqp : (q == p) = true := beq_iff_eq.mpr (hqf.trans hfp)
            simp [hqp]
      have hseq_ok : (decide (oldest < d.seq)) = true :=
        decide_eq_true_eq.mpr (Nat.lt_of_le_of_lt hseq_ge hlt)
      have hP : ((d.scope == some p || d.scope.isNone) && decide (oldest < d.seq)) = true := by
        simp only [hscope_ok, hseq_ok, Bool.true_and]
      simp only [hap, hP, Bool.true_and]
    · simp [hap]
  simp only [keysByFile]
  apply congrArg List.eraseDups
  apply flatMap_eq_of_per_file
  intro f hft
  have hffs : f ∈ fs := (List.mem_filter.mp hft).1
  have hfp : f.part = p := beq_iff_eq.mp (List.mem_filter.mp hft).2
  exact congrArg (List.map (fun d => (f.id, d.key))) (hsel f hffs hfp)

/-! ## IncrementalSurvivors -/

private theorem changed_contains_of_fresh (fresh : List EqDel) (r : Row)
    (h : (fresh.any (eqHit r)) = true) :
    (((fresh.map EqDel.key).eraseDups).contains r.key) = true := by
  rw [List.any_eq_true] at h
  obtain ⟨d, hd, hhit⟩ := h
  have hc := band hhit
  have hc2 := band hc.1
  have hkey : d.key = r.key := beq_iff_eq.mp hc2.2
  rw [List.contains_iff_mem, List.mem_eraseDups, List.mem_map]
  exact ⟨d, hd, hkey⟩

theorem IncrementalSurvivors_sound : IncrementalSurvivors.Goal := by
  intro old fresh rows
  unfold incremental
  rw [List.filter_filter]
  rw [show (fun r : Row =>
      ((!(((fresh.map EqDel.key).eraseDups).contains r.key) || !(fresh.any (eqHit r))) &&
        !(old.any (eqHit r)))) =
      (fun r : Row =>
        (!old.any (eqHit r)) &&
        (!(((fresh.map EqDel.key).eraseDups).contains r.key) || !(fresh.any (eqHit r)))) from by
        funext r; exact Bool.and_comm _ _]
  apply List.filter_congr
  intro r hr
  have happ : ((old ++ fresh).any (eqHit r)) = (old.any (eqHit r) || fresh.any (eqHit r)) :=
    List.any_append
  by_cases ho : (old.any (eqHit r)) = true
  · have hl : ((!(old.any (eqHit r)) &&
        (!(((fresh.map EqDel.key).eraseDups).contains r.key) ||
          !(fresh.any (eqHit r)))) = false) := by simp [ho]
    have hr' : (!((old ++ fresh).any (eqHit r))) = false := by simp [happ, ho]
    rw [hl, hr']
  · have hof : (old.any (eqHit r)) = false := Bool.eq_false_of_ne_true ho
    by_cases hf : (fresh.any (eqHit r)) = true
    · have hcont := changed_contains_of_fresh fresh r hf
      have hl : ((!(old.any (eqHit r)) &&
          (!(((fresh.map EqDel.key).eraseDups).contains r.key) ||
            !(fresh.any (eqHit r)))) = false) := by
        simp only [hof, hcont, hf, Bool.not_true, Bool.not_false, Bool.false_or, Bool.true_and]
      have hr' : (!((old ++ fresh).any (eqHit r))) = false := by simp [happ, hof, hf]
      rw [hl, hr']
    · have hff : (fresh.any (eqHit r)) = false := Bool.eq_false_of_ne_true hf
      have hl : ((!(old.any (eqHit r)) &&
          (!(((fresh.map EqDel.key).eraseDups).contains r.key) ||
            !(fresh.any (eqHit r)))) = true) := by simp [hof, hff]
      have hr' : (!((old ++ fresh).any (eqHit r))) = true := by simp [happ, hof, hff]
      rw [hl, hr']

/-! ## LateEqualityKey -/

private theorem countP_le_length (l : List α) (p : α → Bool) : l.countP p ≤ l.length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      rw [List.countP_cons, List.length_cons]
      cases h : p a <;> simp <;> omega

private theorem countP_not (l : List α) (p : α → Bool) :
    l.countP (fun x => !p x) = l.length - l.countP p := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.countP_cons, List.countP_cons, List.length_cons]
      cases h : p a <;> simp [ih] <;>
        (have hle := countP_le_length as p; omega)

private theorem probe_foldr_aux (ps : List PosDel) (es : List EqDel)
    (rows : List Row) (kept : List Row) (n : Nat) :
    rows.foldr (fun r (kept, n) =>
      if ps.any (posHit r) then (kept, n)
      else (if es.any (eqHit r) then kept else r :: kept, n + 1)) (kept, n) =
      (rows.filter (fun r => !(es.any (eqHit r) || ps.any (posHit r))) ++ kept,
       n + rows.countP (fun r => !(ps.any (posHit r)))) := by
  induction rows with
  | nil => simp
  | cons r rs ih =>
      rw [List.foldr_cons, ih]
      dsimp only
      by_cases hp : (ps.any (posHit r)) = true
      · have hgood : ¬ ((!(es.any (eqHit r) || ps.any (posHit r))) = true) := by
          simp [hp]
        rw [ite_eq_left hp]
        simp only [List.filter_cons, ite_eq_right hgood, List.countP_cons]
        simp [hp]
      · have hpf : (ps.any (posHit r)) = false := Bool.eq_false_of_ne_true hp
        by_cases he : (es.any (eqHit r)) = true
        · rw [ite_eq_right hp, ite_eq_left he]
          have hgood : ¬ ((!(es.any (eqHit r) || ps.any (posHit r))) = true) := by
            simp [he]
          rw [List.filter_cons, ite_eq_right hgood, List.countP_cons]
          simp [hpf]
          omega
        · rw [ite_eq_right hp, ite_eq_right he]
          have hgood : ((!(es.any (eqHit r) || ps.any (posHit r))) = true) := by
            simp [he, hpf]
          rw [List.filter_cons, ite_eq_left hgood, List.countP_cons]
          simp [hpf]
          omega

theorem LateEqualityKey_sound : LateEqualityKey.Goal := by
  intro ps es rows
  unfold probeAfterPosition
  rw [probe_foldr_aux]
  simp only [Nat.zero_add]
  rw [countP_not, List.append_nil]


/-! ## ArithmeticFooter -/

private theorem sum_range_int (n : Nat) :
    ((List.range n).map (fun p : Nat => (p : Int))).sum = ((n * (n - 1) / 2 : Nat) : Int) := by
  have hnat : (List.range n).sum = n * (n - 1) / 2 := by
    simp [List.range_eq_range', List.sum_range']
  have hcast : ((List.range n).map (fun p : Nat => (p : Int))).sum = ((List.range n).sum : Int) := by
    induction (List.range n) with
    | nil => rfl
    | cons a as ih =>
        simp only [List.map_cons, List.sum_cons]
        rw [ih]
        simp
  rw [hcast, hnat]

private theorem sum_linear (l : List Nat) (base stride : Int) :
    (l.map (fun p : Nat => base + stride * (p : Int))).sum =
      (l.length : Int) * base + stride * ((l.map (fun p : Nat => (p : Int))).sum) := by
  induction l with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      have h1 : (((ps.length + 1 : Nat) : Int) * base) = (ps.length : Int) * base + base := by
        rw [show ((ps.length + 1 : Nat) : Int) = (ps.length : Int) + 1 by omega]
        rw [Int.add_mul, Int.one_mul]
      have h2 : stride * ((p : Int) + ((List.map (fun q : Nat => (q : Int)) ps).sum)) =
          stride * (p : Int) + stride * ((List.map (fun q : Nat => (q : Int)) ps).sum) := by
        rw [Int.mul_add]
      rw [h1, h2]
      omega

private theorem nodup_filter {α : Type} [BEq α] [LawfulBEq α] (l : List α) (h : l.Nodup)
    (p : α → Bool) : (l.filter p).Nodup := by
  induction l with
  | nil => exact List.Pairwise.nil
  | cons a as ih =>
      by_cases hp : p a = true
      · rw [List.filter_cons_of_pos hp]
        rw [List.nodup_cons] at h ⊢
        exact ⟨fun hm => h.1 (List.mem_filter.mp hm).1, ih h.2⟩
      · rw [List.filter_cons_of_neg hp]
        exact ih (List.nodup_cons.mp h).2

theorem ArithmeticFooter_sound : ArithmeticFooter.Goal := by
  intro n base stride deleted
  dsimp only [ArithmeticFooter.Goal]
  let val := fun (p : Nat) => base + stride * (p : Int)
  let dead := (deleted.filter (fun p => p < n)).eraseDups
  let alive := n - dead.length
  let total := (n : Int) * base + stride * ((n * (n - 1) / 2 : Nat) : Int)
  let good := (List.range n).filter (fun p => !deleted.contains p)
  let bad := (List.range n).filter (fun p => deleted.contains p)
  have hperm : dead.Perm bad := by
    apply perm_of_nodup_mem (nodup_eraseDups _)
    · exact nodup_filter _ List.nodup_range _
    · intro p
      constructor
      · intro h
        have hm := (List.mem_filter.mp (List.mem_eraseDups.mp h))
        have hlt : p < n := decide_eq_true_eq.mp hm.2
        exact List.mem_filter.mpr ⟨by simpa using hlt, List.contains_iff_mem.mpr hm.1⟩
      · intro h
        have hm := List.mem_filter.mp h
        have hlt : p < n := List.mem_range.mp hm.1
        exact List.mem_eraseDups.mpr (List.mem_filter.mpr ⟨List.contains_iff_mem.mp hm.2, decide_eq_true_eq.mpr hlt⟩)
  have hpart := filter_not_partition (fun p : Nat => deleted.contains p) (List.range n)
  have hlen_eq := hpart.length_eq
  simp only [List.length_append, List.length_range] at hlen_eq
  have hbadlen := hperm.length_eq
  have hlen_bg : bad.length + good.length = n := by simpa [bad, good] using hlen_eq
  have hgoodlen : good.length = alive := by
    dsimp [alive]
    omega
  have hsum_part : (good.map val).sum + (bad.map val).sum = ((List.range n).map val).sum := by
    have hs := map_sum_perm_int val hpart
    rw [List.map_append, List.sum_append] at hs
    have hs' : (bad.map val).sum + (good.map val).sum = ((List.range n).map val).sum := by
      simpa [good, bad] using hs
    omega
  have hdeadsum : (dead.map val).sum = (bad.map val).sum := map_sum_perm_int val hperm
  have hsumall : ((List.range n).map val).sum = total := by
    dsimp [val, total]
    rw [sum_linear, sum_range_int]
    simp
  have hgoodsum : (good.map val).sum = total - (dead.map val).sum := by
    omega
  have hle : dead.length ≤ n := by
    have hbadle := List.length_filter_le (fun p : Nat => deleted.contains p) (List.range n)
    simp only [List.length_range] at hbadle
    omega
  have halive_int : ((alive : Nat) : Int) = (n : Int) - (dead.length : Int) := by
    dsimp [alive]
    exact Int.ofNat_sub hle
  change summary (good.map val) = (alive, if alive == 0 then none else some (total - (dead.map val).sum))
  by_cases hzero : (alive == 0) = true
  · have halive0 : alive = 0 := decide_eq_true_eq.mp hzero
    have hgl0 : good.length = 0 := hgoodlen.trans halive0
    have hgnil : good = [] := List.eq_nil_of_length_eq_zero hgl0
    rw [hgnil]
    simp [summary, halive0]
  · have hzero_false : (alive == 0) = false := Bool.eq_false_of_ne_true hzero
    have halivepos : 0 < alive := Nat.pos_of_ne_zero (by
      intro hz
      apply hzero
      rw [hz]
      rfl)
    have hvslenpos : 0 < (good.map val).length := by simp [hgoodlen, halivepos]
    have hisempty : (good.map val).isEmpty = false := by
      cases hm : good.map val with
      | nil =>
          have hlen : good.length = 0 := by
            have hh := congrArg List.length hm
            simpa using hh
          omega
      | cons a as => rfl
    unfold summary
    rw [hisempty]
    have hif : (if alive == 0 then none else some (total - (dead.map val).sum)) =
        some (total - (dead.map val).sum) := by rw [hzero_false]; rfl
    rw [hif]
    simp [hgoodlen, hgoodsum]

end TinylambOpt.ResearchProofs.AnalyticalOperators.FilePruningAndDeleteMasks

-- C++ references to this model or its proof.
-- query/select_grouped_rewrites.cpp
-- query/select_rewrites.hpp
