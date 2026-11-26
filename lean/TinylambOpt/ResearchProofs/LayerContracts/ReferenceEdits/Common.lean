import TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common

open TinylambOpt.ResearchGoals.LayerContracts.ReferenceEdits

theorem contains_true {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α) :
    l.contains a = true ↔ a ∈ l := by
  change List.elem a l = true ↔ a ∈ l
  rw [List.elem_eq_mem]
  simp

theorem mem_violations (s : DB) (k : Nat) : k ∈ violations s ↔ bad s k = true := by
  simp [violations, bad]

theorem ok_iff_no_bad (s : DB) : ok s = true ↔ ∀ k, bad s k = false := by
  simp [ok, violations, bad]

theorem bad_eq_of_contains (s t : DB) (k : Nat)
    (hc : t.children.contains (some k) = s.children.contains (some k))
    (hp : t.parents.contains k = s.parents.contains k) : bad t k = bad s k := by
  have hc2 : (some k ∈ t.children) ↔ (some k ∈ s.children) := by
    rw [← contains_true t.children (some k), ← contains_true s.children (some k), hc]
  have hp2 : (k ∈ t.parents) ↔ (k ∈ s.parents) := by
    rw [← contains_true t.parents k, ← contains_true s.parents k, hp]
  simp [bad, hc2, hp2]

theorem map_rewrite_contains (c : List (Option Nat)) (a k b : Nat)
    (hka : k ≠ a) (hkb : k ≠ b) :
    (c.map (fun x => if x == some a then some b else x)).contains (some k) =
      c.contains (some k) := by
  apply Bool.eq_iff_iff.mpr
  simp only [contains_true]
  rw [List.mem_map]
  constructor
  · intro h
    rcases h with ⟨x, hx, hxk⟩
    by_cases hxa : x = some a
    · have hbk : b = k := by simpa [hxa] using hxk
      exact False.elim (hkb hbk.symm)
    · have hxk' : x = some k := by simpa [hxa] using hxk
      simpa [hxk'] using hx
  · intro h
    exact ⟨some k, h, by simp [hka]⟩

theorem map_null_contains (c : List (Option Nat)) (a k : Nat) (hka : k ≠ a) :
    (c.map (fun x => if x == some a then none else x)).contains (some k) =
      c.contains (some k) := by
  apply Bool.eq_iff_iff.mpr
  simp only [contains_true]
  rw [List.mem_map]
  constructor
  · rintro ⟨x, hx, hxk⟩
    by_cases hxa : x = some a
    · simp [hxa] at hxk
    · have hxk' : x = some k := by simpa [hxa] using hxk
      simpa [hxk'] using hx
  · intro h
    exact ⟨some k, h, by simp [hka]⟩

theorem filter_purge_contains (c : List (Option Nat)) (a k : Nat) (hka : k ≠ a) :
    (c.filter (fun x => x != some a)).contains (some k) = c.contains (some k) := by
  apply Bool.eq_iff_iff.mpr
  simp only [contains_true, List.mem_filter]
  simp [hka]

theorem erase_contains_ne (p : List Nat) (a k : Nat) (hka : k ≠ a) :
    (p.erase a).contains k = p.contains k := by
  apply Bool.eq_iff_iff.mpr
  simp only [contains_true]
  exact List.mem_erase_of_ne hka


def rwFun (a : Nat) (v : Option Nat) (x : Option Nat) : Option Nat :=
  if x == some a then v else x

theorem rwFun_comm (a b : Nat) (v w : Option Nat)
    (hab : a ≠ b) (hv : v ≠ some b) (hw : w ≠ some a) (x : Option Nat) :
    rwFun b w (rwFun a v x) = rwFun a v (rwFun b w x) := by
  have hba : b ≠ a := Ne.symm hab
  cases x with
  | none => simp [rwFun]
  | some j =>
      by_cases hja : j = a <;> by_cases hjb : j = b <;> simp [rwFun, hja, hjb, hab, hba, hv, hw]

theorem map_rwFun_comm (l : List (Option Nat)) (a b : Nat) (v w : Option Nat)
    (hab : a ≠ b) (hv : v ≠ some b) (hw : w ≠ some a) :
    (l.map (rwFun a v)).map (rwFun b w) = (l.map (rwFun b w)).map (rwFun a v) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [rwFun_comm a b v w hab hv hw, ih]

theorem filter_comm (l : List (Option Nat)) (a c : Nat) :
    (l.filter (fun x => x != some a)).filter (fun x => x != some c) =
      (l.filter (fun x => x != some c)).filter (fun x => x != some a) := by
  rw [List.filter_filter, List.filter_filter]
  simp [Bool.and_comm]



theorem parents_add_del_comm (l : List Nat) (a b c d : Nat)
    (hac : a ≠ c) (had : a ≠ d) (hbc : b ≠ c) (hbd : b ≠ d) :
    ((d :: (b :: l).erase a).erase c).Perm ((b :: (d :: l).erase c).erase a) := by
  have hca : c ≠ a := Ne.symm hac
  have hda : d ≠ a := Ne.symm had
  have hcb : c ≠ b := Ne.symm hbc
  have hdb : d ≠ b := Ne.symm hbd
  by_cases hab : a = b
  · subst b
    by_cases hcd : c = d
    · subst d
      simp
    · have hdc : d ≠ c := Ne.symm hcd
      simp [hdc, hca, hda]
  · have hba : b ≠ a := Ne.symm hab
    by_cases hcd : c = d
    · subst d
      simp [hba, hcb, hca]
    · have hdc : d ≠ c := Ne.symm hcd
      simp [hba, hdc, hbc, hda, hdb]
      rw [List.erase_comm a c]
      exact List.Perm.swap b d _

theorem parents_del_add_comm (l : List Nat) (a b c d : Nat)
    (hac : a ≠ c) (had : a ≠ d) (hbc : b ≠ c) (hbd : b ≠ d) :
    (d :: ((b :: l.erase a).erase c)).Perm (b :: ((d :: l.erase c).erase a)) := by
  have hca : c ≠ a := Ne.symm hac
  have hda : d ≠ a := Ne.symm had
  have hcb : c ≠ b := Ne.symm hbc
  have hdb : d ≠ b := Ne.symm hbd
  by_cases hab : a = b
  · subst b
    by_cases hcd : c = d
    · subst d
      simp [hac, hca]
      rw [List.erase_comm a c]
      exact List.Perm.swap a c _
    · have hdc : d ≠ c := Ne.symm hcd
      simp [hac, hca, hdc, hda]
      rw [List.erase_comm a c]
      exact List.Perm.swap a d _
  · have hba : b ≠ a := Ne.symm hab
    by_cases hcd : c = d
    · subst d
      simp [hbc, hca, hda, hbd]
      rw [List.erase_comm a c]
      exact List.Perm.swap b c _
    · have hdc : d ≠ c := Ne.symm hcd
      simp [hbc, hda, hba, hdc, hdb]
      rw [List.erase_comm a c]
      exact List.Perm.swap b d _

theorem swapFun_comm (a b c d : Nat)
    (hac : a ≠ c) (had : a ≠ d) (hbc : b ≠ c) (hbd : b ≠ d) (x : Option Nat) :
    rwFun d (some c) (rwFun c (some d) (rwFun b (some a) (rwFun a (some b) x))) =
      rwFun b (some a) (rwFun a (some b) (rwFun d (some c) (rwFun c (some d) x))) := by
  calc
    rwFun d (some c) (rwFun c (some d) (rwFun b (some a) (rwFun a (some b) x)))
        = rwFun d (some c) (rwFun b (some a) (rwFun c (some d) (rwFun a (some b) x))) := by
            rw [rwFun_comm b c (some a) (some d) hbc (by simp [hac]) (by simpa using (Ne.symm hbd))]
    _ = rwFun d (some c) (rwFun b (some a) (rwFun a (some b) (rwFun c (some d) x))) := by
            rw [rwFun_comm a c (some b) (some d) hac (by simp [hbc]) (by simpa using (Ne.symm had))]
    _ = rwFun b (some a) (rwFun d (some c) (rwFun a (some b) (rwFun c (some d) x))) := by
            rw [rwFun_comm b d (some a) (some c) hbd (by simp [had]) (by simpa using (Ne.symm hbc))]
    _ = rwFun b (some a) (rwFun a (some b) (rwFun d (some c) (rwFun c (some d) x))) := by
            rw [rwFun_comm a d (some b) (some c) had (by simp [hbd]) (by simpa using (Ne.symm hac))]

theorem map_swapFun_comm (l : List (Option Nat)) (a b c d : Nat)
    (hac : a ≠ c) (had : a ≠ d) (hbc : b ≠ c) (hbd : b ≠ d) :
    (((l.map (rwFun a (some b))).map (rwFun b (some a))).map (rwFun c (some d))).map (rwFun d (some c)) =
      (((l.map (rwFun c (some d))).map (rwFun d (some c))).map (rwFun a (some b))).map (rwFun b (some a)) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [swapFun_comm a b c d hac had hbc hbd, ih]

theorem erase_comm (l : List Nat) (a c : Nat) : (l.erase a).erase c = (l.erase c).erase a :=
  List.erase_comm a c



theorem rwFun_self (a : Nat) (x : Option Nat) : rwFun a (some a) x = x := by
  cases x with
  | none => simp [rwFun]
  | some j => by_cases h : j = a <;> simp [rwFun, h]

theorem map_rwFun_self (l : List (Option Nat)) (a : Nat) : l.map (rwFun a (some a)) = l := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [rwFun_self, ih]

theorem rwFun_swap_idem (a b : Nat) (x : Option Nat) :
    rwFun b (some a) (rwFun a (some b) (rwFun b (some a) (rwFun a (some b) x))) =
      rwFun b (some a) (rwFun a (some b) x) := by
  cases x with
  | none => simp [rwFun]
  | some j =>
      by_cases hab : a = b
      · subst b
        by_cases hja : j = a <;> simp [rwFun, hja]
      · by_cases hja : j = a <;> by_cases hjb : j = b <;>
          simp [rwFun, hja, hjb, hab, Ne.symm hab]

theorem map_rwFun_swap_idem (l : List (Option Nat)) (a b : Nat) :
    (((l.map (rwFun a (some b))).map (rwFun b (some a))).map (rwFun a (some b))).map (rwFun b (some a)) =
      (l.map (rwFun a (some b))).map (rwFun b (some a)) := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [rwFun_swap_idem a b, ih]

theorem ok_addP (s : DB) (k : Nat) (hs : ok s = true) : ok (step s (.addP k)) = true := by
  rw [ok_iff_no_bad]
  intro j
  have hsall : ∀ j, bad s j = false := (ok_iff_no_bad s).mp hs
  by_cases hjk : j = k
  · subst j
    simp [step, bad]
  · by_cases hj : j ∈ s.parents
    · have : (k :: s.parents).contains j = true := by simp [contains_true, hj]
      simp [step, bad, contains_true, hj, this]
    · have hcprop : ¬ some j ∈ s.children := by
        intro hmem
        have hbad : bad s j = true := by simp [bad, contains_true, hmem, hj]
        rw [hsall j] at hbad
        contradiction
      have hc : s.children.contains (some j) = false := by
        cases hbool : s.children.contains (some j) <;> simp_all
      simp [step, bad, hc, contains_true, hj, hcprop]




theorem bool_not_eq_of_iff {b c : Bool} (h : b = false ↔ c = true) : !b = c := by
  cases b <;> cases c <;> simp_all

theorem bad_iff (s : DB) (k : Nat) : bad s k = true ↔ some k ∈ s.children ∧ k ∉ s.parents := by
  simp [bad]

theorem ok_iff_prop (s : DB) : ok s = true ↔ ∀ k, ¬ (some k ∈ s.children ∧ k ∉ s.parents) := by
  rw [ok_iff_no_bad]
  constructor
  · intro h k hk
    have hb := h k
    rw [← bad_iff] at hk
    rw [hk] at hb
    contradiction
  · intro h k
    cases hb : bad s k with
    | false => rfl
    | true => exact False.elim (h k ((bad_iff s k).mp hb))

theorem ok_purge (s : DB) (a : Nat) (hs : ok s = true) : ok (step s (.purge a)) = true := by
  rw [ok_iff_prop]
  intro k hk
  have hmem : some k ∈ s.children := (List.mem_filter.mp hk.1).1
  exact ((ok_iff_prop s).mp hs k) ⟨hmem, hk.2⟩

theorem ok_rewrite_none (s : DB) (a : Nat) (hs : ok s = true) :
    ok (step s (.rewrite a none)) = true := by
  rw [ok_iff_prop]
  intro k hk
  have hmap : some k ∈ s.children.map (rwFun a none) := by simpa [step, rwFun] using hk.1
  rcases List.mem_map.mp hmap with ⟨x, hx, hxk⟩
  by_cases hxa : x = some a
  · have : none = some k := by simpa [rwFun, hxa] using hxk
    contradiction
  · have hxk' : x = some k := by simpa [rwFun, hxa] using hxk
    exact ((ok_iff_prop s).mp hs k) ⟨by simpa [hxk'] using hx, hk.2⟩

theorem ok_rewrite_some (s : DB) (a b : Nat) (hs : ok s = true) (hb : b ∈ s.parents) :
    ok (step s (.rewrite a (some b))) = true := by
  rw [ok_iff_prop]
  intro k hk
  by_cases hkb : k = b
  · subst k
    exact hk.2 hb
  · have hmap : some k ∈ s.children.map (rwFun a (some b)) := by simpa [step, rwFun] using hk.1
    rcases List.mem_map.mp hmap with ⟨x, hx, hxk⟩
    by_cases hxa : x = some a
    · have hbk : b = k := by simpa [rwFun, hxa] using hxk
      exact hkb hbk.symm
    · have hxk' : x = some k := by simpa [rwFun, hxa] using hxk
      exact ((ok_iff_prop s).mp hs k) ⟨by simpa [hxk'] using hx, hk.2⟩

theorem ok_delP_of_no_children (s : DB) (a : Nat) (hs : ok s = true)
    (hno : ¬ some a ∈ s.children) : ok (step s (.delP a)) = true := by
  rw [ok_iff_prop]
  intro k hk
  have hmem : some k ∈ s.children := by simpa [step] using hk.1
  by_cases hka : k = a
  · subst k
    exact hno hmem
  · have hpar : k ∉ s.parents := by
      intro hp
      have : k ∈ s.parents.erase a := (List.mem_erase_of_ne hka).mpr hp
      exact hk.2 (by simpa [step] using this)
    exact ((ok_iff_prop s).mp hs k) ⟨hmem, hpar⟩


theorem ok_delC (s : DB) (k : Option Nat) (hs : ok s = true) :
    ok (step s (.delC k)) = true := by
  rw [ok_iff_prop]
  intro j hj
  have hmem : some j ∈ s.children := List.mem_of_mem_erase (by simpa [step] using hj.1)
  exact ((ok_iff_prop s).mp hs j) ⟨hmem, hj.2⟩

theorem no_child_after_rewrite_some (s : DB) (a b : Nat) (hab : a ≠ b) :
    ¬ some a ∈ (step s (.rewrite a (some b))).children := by
  dsimp [step]
  simp only [rwFun, List.mem_map]
  rintro ⟨x, hx, hxk⟩
  by_cases hxa : x = some a
  · have : b = a := by simpa [hxa] using hxk
    exact hab this.symm
  · have : x = some a := by simpa [hxa] using hxk
    exact hxa this

theorem no_child_after_purge (s : DB) (a : Nat) :
    ¬ some a ∈ (step s (.purge a)).children := by
  simp [step]

theorem no_child_after_rewrite_none (s : DB) (a : Nat) :
    ¬ some a ∈ (step s (.rewrite a none)).children := by
  simp [step, rwFun]

theorem ok_addC_some (s : DB) (a : Nat) (hs : ok s = true) (ha : a ∈ s.parents) :
    ok (step s (.addC (some a))) = true := by
  rw [ok_iff_prop]
  intro k hk
  by_cases hka : k = a
  · subst k
    exact hk.2 ha
  · have hmem : some k ∈ s.children := by
      have hc : some k ∈ some a :: s.children := by simpa [step] using hk.1
      rcases List.mem_cons.mp hc with h | h
      · exfalso
        apply hka
        injection h
      · exact h
    exact ((ok_iff_prop s).mp hs k) ⟨hmem, hk.2⟩

theorem bad_delP_of_bad (s : DB) (a k : Nat) (hk : bad s k = true) :
    bad (step s (.delP a)) k = true := by
  rw [bad_iff] at hk ⊢
  rcases hk with ⟨hmem, hpar⟩
  constructor
  · simpa [step] using hmem
  · intro hp
    apply hpar
    exact List.mem_of_mem_erase (by simpa [step] using hp)

theorem ok_delP_mono (s : DB) (a : Nat) (h : ok (step s (.delP a)) = true) : ok s = true := by
  rw [ok_iff_prop] at h ⊢
  intro k hk
  have hkbad : bad s k = true := (bad_iff s k).mpr hk
  have hb2 : bad (step s (.delP a)) k = true := bad_delP_of_bad s a k hkbad
  exact h k ((bad_iff (step s (.delP a)) k).mp hb2)



theorem ok_iff_not_bad_single (m : Model) (hf : frameKeys m)
    (hw : ∀ a b, m.watch a b = [a]) (s : DB) (a b : Nat) (hs : ok s = true) :
    ok (finish m s a b) = true ↔ bad (finish m s a b) a = false := by
  let t := finish m s a b
  constructor
  · intro hok
    exact (ok_iff_no_bad t).mp hok a
  · intro hba
    rw [ok_iff_prop]
    intro k hk
    by_cases hka : k = a
    · subst k
      have hnb : ¬ (some a ∈ t.children ∧ a ∉ t.parents) := by
        intro hh
        have : bad t a = true := (bad_iff t a).mpr hh
        rw [hba] at this
        contradiction
      exact hnb hk
    · have hfk : bad t k = bad s k := by
        simpa [t] using hf s a b k (by simp [hw, hka])
      have hks : bad s k = false := (ok_iff_no_bad s).mp hs k
      have hkt : bad t k = true := (bad_iff t k).mpr hk
      rw [hfk, hks] at hkt
      contradiction




theorem count_none_map_rewrite (l : List (Option Nat)) (a b : Nat) :
    (l.map (rwFun a (some b))).count none = l.count none := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases x with
      | none => simpa [rwFun] using ih
      | some j => by_cases h : j = a <;> simp [rwFun, h, ih]

theorem count_some_map_rewrite_src (l : List (Option Nat)) (a b : Nat) (hab : a ≠ b) :
    (l.map (rwFun a (some b))).count (some a) = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases x with
      | none => simpa [rwFun] using ih
      | some j =>
          by_cases hja : j = a
          · subst j
            simp [rwFun, hab, Ne.symm hab, ih]
          · simp [rwFun, hja, ih]

theorem count_some_map_rewrite_tgt (l : List (Option Nat)) (a b : Nat) (hab : a ≠ b) :
    (l.map (rwFun a (some b))).count (some b) = l.count (some a) + l.count (some b) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      cases x with
      | none => simpa [rwFun] using ih
      | some j =>
          by_cases hja : j = a
          · subst j
            simp [rwFun, hab, ih] <;> omega
          · by_cases hjb : j = b
            · subst j
              simp [rwFun, hja, Ne.symm hab, ih] <;> omega
            · simp [rwFun, hja, hjb, ih] <;> omega

theorem count_none_map_null (l : List (Option Nat)) (a : Nat) :
    (l.map (rwFun a none)).count none = l.count none + l.count (some a) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      cases x with
      | none => simp [rwFun, ih] <;> omega
      | some j =>
          by_cases hja : j = a
          · subst j
            simp [rwFun, ih] <;> omega
          · simp [rwFun, hja, ih] <;> omega

theorem count_some_purge (l : List (Option Nat)) (a : Nat) :
    (l.filter (fun x => x != some a)).count (some a) = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases x with
      | none => simpa using ih
      | some j => by_cases h : j = a <;> simp [h, ih]

theorem length_filter_purge (l : List (Option Nat)) (a : Nat) :
    (l.filter (fun x => x != some a)).length + l.count (some a) = l.length := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases x with
      | none => simp [ih] <;> omega
      | some j => by_cases h : j = a <;> simp [h, ih] <;> omega


theorem count_none_map_rewrite_lit (l : List (Option Nat)) (a b : Nat) :
    (l.map (fun x => if x = some a then some b else x)).count none = l.count none := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases x with
      | none => simpa using ih
      | some j => by_cases h : j = a <;> simp [h, ih]

theorem count_some_map_rewrite_src_lit (l : List (Option Nat)) (a b : Nat) (hab : a ≠ b) :
    (l.map (fun x => if x = some a then some b else x)).count (some a) = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases x with
      | none => simpa using ih
      | some j =>
          by_cases hja : j = a
          · subst j
            simp [hab, Ne.symm hab, ih]
          · simp [hja, ih]

theorem count_some_map_rewrite_tgt_lit (l : List (Option Nat)) (a b : Nat) (hab : a ≠ b) :
    (l.map (fun x => if x = some a then some b else x)).count (some b) = l.count (some a) + l.count (some b) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      cases x with
      | none => simpa using ih
      | some j =>
          by_cases hja : j = a
          · subst j
            simp [hab, Ne.symm hab, ih] <;> omega
          · by_cases hjb : j = b
            · subst j
              simp [hja, Ne.symm hab, ih] <;> omega
            · simp [hja, hjb, ih] <;> omega

theorem count_none_map_null_lit (l : List (Option Nat)) (a : Nat) :
    (l.map (fun x => if x = some a then none else x)).count none = l.count none + l.count (some a) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      cases x with
      | none => simp [ih] <;> omega
      | some j =>
          by_cases hja : j = a
          · subst j
            simp [ih] <;> omega
          · simp [hja, ih] <;> omega

theorem count_some_map_rewrite_none_src_lit (l : List (Option Nat)) (a : Nat) :
    (l.map (fun x => if x = some a then none else x)).count (some a) = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases x with
      | none => simpa using ih
      | some j => by_cases h : j = a <;> simp [h, ih]


theorem not_mem_erase_of_count_one {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (a : α) (h : l.count a = 1) : a ∉ l.erase a := by
  apply List.count_eq_zero.mp
  simp [h]

theorem mem_erase_of_count_gt_one {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (a : α) (h : 1 < l.count a) : a ∈ l.erase a := by
  by_cases hm : a ∈ l.erase a
  · exact hm
  · have hc := List.count_eq_zero.mpr hm
    have hcalc : (l.erase a).count a = l.count a - 1 := by simp
    rw [hcalc] at hc
    omega


theorem singleProbe_of_head_iff (m : Model) (hw : ∀ a b, (m.watch a b).head? = some a)
    (s : DB) (a b : Nat)
    (hiff : bad (finish m s a b) a = false ↔ ok (finish m s a b) = true) :
    (!(m.watch a b).head?.any (bad (finish m s a b))) = ok (finish m s a b) := by
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro hl
    apply hiff.mp
    have hany : (m.watch a b).head?.any (bad (finish m s a b)) = false := by
      simpa using hl
    rw [hw] at hany
    simpa using hany
  · intro ho
    have hb := hiff.mpr ho
    have hany : (m.watch a b).head?.any (bad (finish m s a b)) = false := by
      rw [hw]
      simpa [hb]
    simpa [hany]

theorem deltaCheck_of_frameKeys (m : Model) (hf : frameKeys m) : deltaCheck m := by
  intro s a b hsok
  let t := finish m s a b
  have hs : ∀ k, bad s k = false := (ok_iff_no_bad s).mp hsok
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro hl
    rw [ok_iff_no_bad]
    intro k
    by_cases hkw : k ∈ m.watch a b
    · have hall : ∀ k ∈ m.watch a b, bad t k = false := by
        simpa [localOK, t] using hl
      exact hall k hkw
    · rw [hf s a b k hkw]
      exact hs k
  · intro ht
    have hall : ∀ k, bad t k = false := (ok_iff_no_bad t).mp ht
    simp [localOK, t, hall]

theorem newViolations_of_frameKeys (m : Model) (hf : frameKeys m) :
    ∀ s a b, ∀ k,
      k ∈ (violations (finish m s a b)).filter (fun x => !bad s x) ↔
        k ∈ m.watch a b ∧ bad (finish m s a b) k ∧ !bad s k := by
  intro s a b k
  let t := finish m s a b
  have hv : k ∈ violations t ↔ bad t k = true := mem_violations t k
  change k ∈ (violations t).filter (fun x => !bad s x) ↔
        k ∈ m.watch a b ∧ bad t k ∧ !bad s k
  simp only [List.mem_filter]
  by_cases hkw : k ∈ m.watch a b
  · simp [hkw, hv, and_left_comm, and_assoc]
  · have hbad := hf s a b k hkw
    simp [hkw, hbad, hv, t]

theorem step_perm (s t : DB) (e : Edit) (h : sameBag s t) : sameBag (step s e) (step t e) := by
  rcases h with ⟨hp, hc⟩
  cases e with
  | addP k => exact ⟨List.Perm.cons k hp, hc⟩
  | delP k => exact ⟨hp.erase k, hc⟩
  | addC k => exact ⟨hp, List.Perm.cons k hc⟩
  | delC k => exact ⟨hp, hc.erase k⟩
  | rewrite k v => exact ⟨hp, hc.map _⟩
  | purge k => exact ⟨hp, hc.filter _⟩

theorem run_perm (ops : List Edit) (s t : DB) (h : sameBag s t) :
    sameBag (run s ops) (run t ops) := by
  induction ops generalizing s t with
  | nil => simpa [run] using h
  | cons e es ih =>
      simpa [run] using ih (step s e) (step t e) (step_perm s t e h)

theorem stableRows_of_model (m : Model) : stableRows m := by
  intro s t a b h
  exact run_perm (m.ops a b) s t h

-- frameKeys, proved per model

theorem frameKeys_ParentFirst : frameKeys ParentFirst.model := by
  intro s a b k hk
  simp [ParentFirst.model] at hk
  simp [finish, run, step, ParentFirst.model, bad, hk.1, hk.2]

theorem frameKeys_ChildFirst : frameKeys ChildFirst.model := by
  intro s a b k hk
  simp [ChildFirst.model] at hk
  simp [finish, run, step, ChildFirst.model, bad, hk.1, hk.2]

theorem frameKeys_Reparent : frameKeys Reparent.model := by
  intro s a b k hk
  simp [Reparent.model] at hk
  apply bad_eq_of_contains
  · dsimp [finish, run, step, Reparent.model]
    exact map_rewrite_contains s.children a k b hk.1 hk.2
  · dsimp [finish, run, step, Reparent.model]
    simp [hk.1]

theorem frameKeys_Cascade : frameKeys Cascade.model := by
  intro s a b k hk
  simp [Cascade.model] at hk
  simp [finish, run, step, Cascade.model, bad, hk]

theorem frameKeys_SetNull : frameKeys SetNull.model := by
  intro s a b k hk
  simp [SetNull.model] at hk
  simp [finish, run, step, SetNull.model, bad, hk]

theorem frameKeys_ReplaceParent : frameKeys ReplaceParent.model := by
  intro s a b k hk
  simp [ReplaceParent.model] at hk
  simp [finish, run, step, ReplaceParent.model, bad, hk]

theorem frameKeys_Adopt : frameKeys Adopt.model := by
  intro s a b k hk
  simp [Adopt.model] at hk
  apply bad_eq_of_contains
  · dsimp [finish, run, step, Adopt.model]
    exact map_rewrite_contains s.children a k b hk.1 hk.2
  · dsimp [finish, run, step, Adopt.model]
    simp [hk.1, hk.2]

theorem frameKeys_DropPair : frameKeys DropPair.model := by
  intro s a b k hk
  simp [DropPair.model] at hk
  simp [finish, run, step, DropPair.model, bad, hk]

theorem frameKeys_SwapReferences : frameKeys SwapReferences.model := by
  intro s a b k hk
  simp [SwapReferences.model] at hk
  apply bad_eq_of_contains
  · dsimp [finish, run, step, SwapReferences.model]
    calc
      ((s.children.map (fun x => if x == some a then some b else x)).map
          (fun x => if x == some b then some a else x)).contains (some k)
          = (s.children.map (fun x => if x == some a then some b else x)).contains (some k) :=
            map_rewrite_contains _ b k a hk.2 hk.1
      _ = s.children.contains (some k) := map_rewrite_contains _ a k b hk.1 hk.2
  · dsimp [finish, run, step, SwapReferences.model]

theorem frameKeys_RekeyLate : frameKeys RekeyLate.model := by
  intro s a b k hk
  simp [RekeyLate.model] at hk
  apply bad_eq_of_contains
  · dsimp [finish, run, step, RekeyLate.model]
    exact map_rewrite_contains s.children a k b hk.1 hk.2
  · dsimp [finish, run, step, RekeyLate.model]
    simp [hk.1, hk.2]

end TinylambOpt.ResearchProofs.LayerContracts.ReferenceEdits.Common
