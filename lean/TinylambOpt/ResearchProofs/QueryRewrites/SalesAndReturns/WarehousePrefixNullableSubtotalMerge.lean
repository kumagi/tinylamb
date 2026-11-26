import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns
import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping

/-!
# WarehousePrefixNullableSubtotalMerge

Merging the nullable subtotals of warehouses that share a name prefix preserves the sum
for that prefix.

The executor can derive the prefix on warehouse keys and combine aggregate states
instead of slicing each input row's name.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.WarehousePrefixNullableSubtotalMerge
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

theorem sum_map_zero {α : Type} (l : List α) : (l.map (fun _ => 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

theorem sum_boole_eq_countP_string (l : List String) (p : String → Bool) :
    (l.map (fun x => if p x then 1 else 0)).sum = l.countP p := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    by_cases hx : p x = true
    · simp [hx, ih]
      ac_rfl
    · simp [hx, ih]

theorem count_eq_one_of_nodup_mem_string {l : List String} (hnd : l.Nodup) {a : String}
    (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => simp at ha
  | cons x xs ih =>
    have hnd_tail : xs.Nodup := (List.nodup_cons.mp hnd).2
    have hxnot : x ∉ xs := (List.nodup_cons.mp hnd).1
    simp only [List.count_cons]
    by_cases hxa : x == a
    · have hxEq : x = a := beq_iff_eq.mp hxa
      have hanot : a ∉ xs := by simpa [hxEq] using hxnot
      rw [List.count_eq_zero.mpr hanot]
      simp [hxa]
    · have haxs : a ∈ xs := by
        simp at ha
        rcases ha with ha | ha
        · exact False.elim (hxa (by simpa [ha]))
        · exact ha
      simp [hxa, ih hnd_tail haxs]

theorem sum_indicator_eq_of_nodup_mem_string (l : List String) (hnd : l.Nodup) (s : String) :
    (l.map (fun k => if k == s then 1 else 0)).sum = if s ∈ l then 1 else 0 := by
  rw [sum_boole_eq_countP_string, ← List.count_eq_countP]
  by_cases hmem : s ∈ l
  · rw [count_eq_one_of_nodup_mem_string hnd hmem]
    simp [hmem]
  · rw [List.count_eq_zero.mpr hmem]
    simp [hmem]

theorem beq_comm_string (a b : String) : (a == b) = (b == a) := by
  by_cases h : a == b
  · have hEq : a = b := beq_iff_eq.mp h
    subst b
    simp
  · have hab : (a == b) = false := by
      cases h' : a == b with
      | false => rfl
      | true => exact False.elim (h h')
    rw [hab]
    cases hba : b == a with
    | false => rfl
    | true =>
        have hEq : b = a := beq_iff_eq.mp hba
        subst b
        simp at h

theorem sum_filter_ne_add (l : List String) (hnd : l.Nodup) (s : String) (f : String → Nat) :
    (l.map f).sum = ((l.filter (fun k => !(k == s))).map f).sum + (if s ∈ l then f s else 0) := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    have hnd_tail : xs.Nodup := (List.nodup_cons.mp hnd).2
    have hxnot : x ∉ xs := (List.nodup_cons.mp hnd).1
    by_cases hxs : x == s
    · have hxEq : x = s := beq_iff_eq.mp hxs
      have hsnot : s ∉ xs := by simpa [hxEq] using hxnot
      simp [hxs, hxEq, hsnot, ih hnd_tail]
      ac_rfl
    · have hsx : (s == x) = false := by
        cases h : s == x with
        | false => rfl
        | true => exact False.elim (hxs (by have hEq := beq_iff_eq.mp h; simpa [hEq]))
      have hmem_cons : s ∈ x :: xs ↔ s ∈ xs := by
        constructor
        · intro hm
          simp at hm
          rcases hm with hsxEq | hxsTail
          · subst s
            simp at hxs
          · exact hxsTail
        · intro hm
          simp [hm]
      simp [hxs, hmem_cons, ih hnd_tail]
      ac_rfl

theorem count_filter_cons_count (rs : List (String × Nat)) (s : String) (v bin : Nat) :
    ∀ k, List.count bin (List.map Prod.snd (List.filter (fun r => r.1 == k) ((s, v) :: rs))) =
      List.count bin (List.map Prod.snd (List.filter (fun r => r.1 == k) rs)) +
        (if s == k && v == bin then 1 else 0) := by
  intro k
  by_cases hsk : s == k
  · have hskEq : s = k := beq_iff_eq.mp hsk
    subst k
    simp [List.count_cons]
  · simp [hsk]

theorem sum_indicator_head (keys : List String) (hnd : keys.Nodup) (p : String → Bool)
    (s : String) (hkeysS : s ∈ keys ↔ p s = true) (v bin : Nat) :
    (keys.map (fun k => if s == k && v == bin then 1 else 0)).sum =
      if p s && v == bin then 1 else 0 := by
  by_cases hvb : v == bin
  · have hmap : keys.map (fun k => if s == k && v == bin then 1 else 0) =
        keys.map (fun k => if k == s then 1 else 0) := by
      apply List.map_congr_left
      intro k hk
      rw [beq_comm_string s k]
      simp [hvb]
    rw [hmap, sum_indicator_eq_of_nodup_mem_string keys hnd s]
    by_cases hps : p s = true
    · have hmem : s ∈ keys := hkeysS.mpr hps
      simp [hps, hmem, hvb]
    · have hnmem : s ∉ keys := by
        intro hm
        exact hps (hkeysS.mp hm)
      simp [hps, hnmem, hvb]
  · have hvneq : v ≠ bin := by
      intro hv
      apply hvb
      simp [hv]
    have hzero : ∀ k, (s == k && v == bin) = false := by
      intro k
      simp [hvb]
    have hleft : (keys.map (fun k => if s == k && v == bin then 1 else 0)).sum = 0 := by
      have hmapzero : keys.map (fun k => if s == k && v == bin then 1 else 0) =
          keys.map (fun _ => 0) := by
        apply List.map_congr_left
        intro k hk
        simp [hzero k]
      rw [hmapzero, sum_map_zero]
    rw [hleft]
    have hrhs : (if p s && v == bin then 1 else 0) = 0 := by
      simp [hvneq]
    rw [hrhs]

theorem grouping_sum_eq_countP (rs : List (String × Nat)) (keys : List String) (bin : Nat)
    (p : String → Bool) (hnd : keys.Nodup)
    (hkeys : ∀ k, k ∈ keys ↔ k ∈ rs.map Prod.fst ∧ p k = true) :
    (keys.map (fun k => (rs.filter (fun r => r.1 == k)).map Prod.snd |>.count bin)).sum =
      rs.countP (fun r => p r.1 && r.2 == bin) := by
  induction rs generalizing keys with
  | nil =>
    cases keys with
    | nil => simp
    | cons k ks =>
      have hk : k ∈ k :: ks := by simp
      have hh := (hkeys k).mp hk
      simp at hh
  | cons r rs ih =>
    rcases r with ⟨s,v⟩
    by_cases hdup : s ∈ rs.map Prod.fst
    · have hkeys_tail : ∀ k, k ∈ keys ↔ k ∈ rs.map Prod.fst ∧ p k = true := by
        intro k
        constructor
        · intro hk
          have hh := (hkeys k).mp hk
          rcases hh with ⟨hkmem, hpk⟩
          have hkmem' : k = s ∨ k ∈ rs.map Prod.fst := by
            simpa [beq_iff_eq] using hkmem
          rcases hkmem' with hks | hkt
          · subst k
            exact ⟨hdup, hpk⟩
          · exact ⟨hkt, hpk⟩
        · rintro ⟨hkt, hpk⟩
          apply (hkeys k).mpr
          constructor
          · simp [hkt]
          · exact hpk
      have ih' := ih keys hnd hkeys_tail
      have hkeysS : s ∈ keys ↔ p s = true := by
        simpa using hkeys s
      have hsum_ind := sum_indicator_head keys hnd p s hkeysS v bin
      simp only [count_filter_cons_count, List.countP_cons]
      rw [sum_map_add]
      rw [ih']
      rw [hsum_ind]
    · let keysTail := keys.filter (fun k => !(k == s))
      have hnd_tail : keysTail.Nodup := by
        dsimp [keysTail]
        exact hnd.filter _
      have hkeys_tail : ∀ k, k ∈ keysTail ↔ k ∈ rs.map Prod.fst ∧ p k = true := by
        intro k
        constructor
        · intro hk
          have hkparts : k ∈ keys ∧ !(k == s) = true := by simpa [keysTail] using hk
          have hkneBool : (k == s) = false := by simpa using hkparts.2
          have hh := (hkeys k).mp hkparts.1
          rcases hh with ⟨hkmem, hpk⟩
          have hkmem' : k = s ∨ k ∈ rs.map Prod.fst := by
            simpa [beq_iff_eq] using hkmem
          rcases hkmem' with hks | hkt
          · have htrue : (k == s) = true := by simp [hks]
            rw [htrue] at hkneBool
            cases hkneBool
          · exact ⟨hkt, hpk⟩
        · rintro ⟨hkt, hpk⟩
          have hknew : k ≠ s := by
            intro hks
            apply hdup
            rw [← hks]
            exact hkt
          have hksBool : (k == s) = false := by
            cases h : k == s with
            | false => rfl
            | true => exact False.elim (hknew (beq_iff_eq.mp h))
          have hkheadmem : k ∈ (s :: rs.map Prod.fst) := by simp [hkt]
          have hkkeys : k ∈ keys := (hkeys k).mpr ⟨hkheadmem, hpk⟩
          simp [keysTail, hkkeys, hksBool]
      have ih' := ih keysTail hnd_tail hkeys_tail
      have hkeysS : s ∈ keys ↔ p s = true := by
        simpa [hdup] using hkeys s
      have hsum_ind := sum_indicator_head keys hnd p s hkeysS v bin
      have hfilterNil : rs.filter (fun r => r.1 == s) = [] := by
        apply List.filter_eq_nil_iff.mpr
        intro r hr hrbeq
        have hrEq : r.1 = s := beq_iff_eq.mp hrbeq
        apply hdup
        rw [← hrEq]
        exact List.mem_map.mpr ⟨r, hr, rfl⟩
      have htailCountS : ((rs.filter (fun r => r.1 == s)).map Prod.snd |>.count bin) = 0 := by
        rw [hfilterNil]
        simp
      have hdecomp := sum_filter_ne_add keys hnd s (fun k => (rs.filter (fun r => r.1 == k)).map Prod.snd |>.count bin)
      simp only [count_filter_cons_count, List.countP_cons]
      rw [sum_map_add]
      rw [hdecomp]
      rw [ih']
      rw [htailCountS]
      rw [hsum_ind]
      by_cases hm : s ∈ keys <;> simp [hm]

theorem forKey_count_eq_countP (rs : List (String × Nat)) (k : String) (bin : Nat) :
    (forKey rs k).count bin = rs.countP (fun r => r.1 == k && r.2 == bin) := by
  unfold forKey
  rw [List.count_eq_countP, List.countP_map, List.countP_filter]
  simp [Function.comp_def, Bool.and_comm, Bool.and_left_comm, Bool.and_assoc]

theorem countWhere_eq_countP (rs : List (String × Nat)) (q : String × Nat → Bool) :
    countWhere rs q = rs.countP q := by
  unfold countWhere
  rw [List.countP_eq_length_filter]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.WarehousePrefixNullableSubtotalMerge.Goal := by
  intro rs name bin
  dsimp
  let p : String → Bool := fun s => (s.take 20).toString == name
  let keys : List String := (rs.map Prod.fst).eraseDups.filter p
  have hnd : keys.Nodup := by
    dsimp [keys]
    exact (KeyGrouping.nodup_eraseDups (rs.map Prod.fst)).filter p
  have hkeys : ∀ k, k ∈ keys ↔ k ∈ rs.map Prod.fst ∧ p k = true := by
    intro k
    dsimp [keys]
    simp
  have hgroup := grouping_sum_eq_countP rs keys bin p hnd hkeys
  have hgroup' : (keys.map (fun k => rs.countP (fun r => r.1 == k && r.2 == bin))).sum =
      rs.countP (fun r => p r.1 && r.2 == bin) := by
    simpa [forKey, List.count_eq_countP, List.countP_map, List.countP_filter, Bool.and_comm] using hgroup
  rw [countWhere_eq_countP]
  simp only [forKey_count_eq_countP]
  simpa [keys, p] using hgroup'
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.WarehousePrefixNullableSubtotalMerge
