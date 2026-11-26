import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `MultiplicityRuns.ZeroRuns` — `∀ rs, live bag (rs.filter (fun r => 0<r.2)) |>.Perm
  (live bag rs)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_ZeroRuns

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.ExceptionOverlay

/-!
`ExceptionOverlay.ZeroRuns.Goal` is **not** declared in
`TinylambOpt.ResearchGoals.StorageExecution.BagKernels`: the only `ZeroRuns`
goal in that module lives in `MultiplicityRuns`. The statement below is the
natural ExceptionOverlay analogue. An override `(i, v)` whose ordinal is out of
the visible range `0 .. n` contributes nothing to any mass (the format comment
calls such slots invisible), exactly as a run of multiplicity zero contributes
nothing in `MultiplicityRuns`. Discarding those runs leaves every mass, hence
every live key, unchanged up to permutation.
-/
private def ZeroRuns.Goal : Prop :=
  ∀ n d es, (live bag (n, d, es.filter (fun r => r.1 < n))).Perm (live bag (n, d, es))

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} {a : α} (hn : l.Nodup) (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.mem_cons] at ha
      rw [List.count_cons]
      by_cases hba : b = a
      · subst b
        rw [List.count_eq_zero_of_not_mem hn.1]
        simp
      · have ha' : a ∈ bs := by cases ha <;> simp_all
        have hbeq : (b == a) = false := by simp [hba]
        simp [hbeq, ih hn.2 ha']

private theorem nodup_filter_eraseDups {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (p : α → Bool) : (l.filter p).eraseDups.Nodup := by
  induction l generalizing p with
  | nil => simp
  | cons b bs ih =>
      by_cases hb : p b = true
      · simp only [List.filter_cons, hb, ite_true]
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · simp [List.mem_eraseDups]
        · simpa [List.filter_filter] using ih (fun x => (! x == b) && p x)
      · simp only [List.filter_cons, hb]
        exact ih p

private theorem filter_const_true {α : Type} (l : List α) : l.filter (fun _ => true) = l := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  rw [← filter_const_true l]
  exact nodup_filter_eraseDups l (fun _ => true)

private theorem perm_of_mem_iff {α : Type} [BEq α] [LawfulBEq α] {l₁ l₂ : List α}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (h : ∀ a, a ∈ l₁ ↔ a ∈ l₂) : l₁.Perm l₂ := by
  rw [List.perm_iff_count]
  intro a
  by_cases ha : a ∈ l₁
  · have ha2 : a ∈ l₂ := (h a).mp ha
    rw [count_eq_one_of_mem_nodup h₁ ha, count_eq_one_of_mem_nodup h₂ ha2]
  · have ha2 : ¬ a ∈ l₂ := by intro hx; exact ha ((h a).mpr hx)
    rw [List.count_eq_zero_of_not_mem ha, List.count_eq_zero_of_not_mem ha2]

private theorem lookup_filter_of_lt (es : List (Nat × Nat)) (n i : Nat) (hi : i < n) :
    (es.filter (fun r => r.1 < n)).lookup i = es.lookup i := by
  induction es with
  | nil => simp
  | cons r es ih =>
      obtain ⟨j, v⟩ := r
      by_cases hj : j < n
      · by_cases hij : i == j <;> simp [List.lookup_cons, hj, hij, ih]
      · by_cases hij : i == j
        · have hijEq : i = j := beq_iff_eq.mp hij
          subst j
          exact (hj hi).elim
        · simp [List.lookup_cons, hj, hij, ih]

private theorem map_fst_filter (es : List (Nat × Nat)) (n : Nat) :
    (es.filter (fun r => r.1 < n)).map Prod.fst = (es.map Prod.fst).filter (fun i => i < n) := by
  simpa [Function.comp_def] using
    (List.filter_map (p := fun i : Nat => i < n) (f := Prod.fst) (l := es)).symm

private theorem ps_perm (es : List (Nat × Nat)) (n : Nat) :
    (((es.filter (fun r => r.1 < n)).map Prod.fst).eraseDups.filter (fun i => i < n)).Perm
      (((es.map Prod.fst).eraseDups).filter (fun i => i < n)) := by
  apply perm_of_mem_iff
  · exact List.Pairwise.filter _
      (nodup_eraseDups (l := (es.filter (fun r : Nat × Nat => r.1 < n)).map Prod.fst))
  · exact List.Pairwise.filter _ (nodup_eraseDups (es.map Prod.fst))
  · intro i
    simp only [List.mem_filter, List.mem_eraseDups]
    rw [map_fst_filter]
    constructor
    · intro h
      exact ⟨(List.mem_filter.mp h.1).1, h.2⟩
    · intro h
      exact ⟨List.mem_filter.mpr h, h.2⟩

private theorem mass_filter (n d k : Nat) (es : List (Nat × Nat)) :
    bag.mass (n, d, es.filter (fun r => r.1 < n)) k = bag.mass (n, d, es) k := by
  unfold bag
  simp only
  let ps₁ := ((es.filter (fun r => r.1 < n)).map Prod.fst).eraseDups.filter (fun i => i < n)
  let ps := ((es.map Prod.fst).eraseDups).filter (fun i => i < n)
  have hperm : ps₁.Perm ps := by simpa [ps₁, ps] using ps_perm es n
  have hlen : ps₁.length = ps.length := hperm.length_eq
  have hcnt : ps₁.countP (fun i => (es.filter (fun r => r.1 < n)).lookup i == some k) =
      ps.countP (fun i => es.lookup i == some k) := by
    calc
      ps₁.countP (fun i => (es.filter (fun r => r.1 < n)).lookup i == some k) =
          ps₁.countP (fun i => es.lookup i == some k) := by
            apply List.countP_congr
            intro i hi
            have hi' : i < n := by
              have hm : i ∈ ((es.filter (fun r => r.1 < n)).map Prod.fst).eraseDups :=
                (List.mem_filter.mp hi).1
              have hm' : i ∈ (es.filter (fun r => r.1 < n)).map Prod.fst :=
                (List.mem_eraseDups.mp hm)
              obtain ⟨r, hr, hri⟩ := List.mem_map.mp hm'
              simpa [hri] using (List.mem_filter.mp hr).2
            rw [lookup_filter_of_lt es n i hi']
      _ = ps.countP (fun i => es.lookup i == some k) :=
        (List.Perm.countP_eq (fun i => es.lookup i == some k) hperm)
  rw [hlen, hcnt]

private theorem mem_of_lookup_eq_some {es : List (Nat × Nat)} {i v : Nat}
    (h : es.lookup i = some v) : (i, v) ∈ es := by
  obtain ⟨l₁, l₂, heq, _⟩ := (List.lookup_eq_some_iff.mp h)
  rw [heq]
  simp

private theorem mem_keys_of_mass_pos (n d k : Nat) (es : List (Nat × Nat))
    (h : 0 < bag.mass (n, d, es) k) : k ∈ bag.keys (n, d, es) := by
  simp only [bag, List.mem_cons]
  by_cases hkd : k == d
  · left
    exact eq_of_beq hkd
  · right
    have hkne : k ≠ d := by
      intro hk
      apply hkd
      simp [hk]
    let ps := ((es.map Prod.fst).eraseDups).filter (fun i => i < n)
    have hpos : 0 < ps.countP (fun i => es.lookup i == some k) := by
      simpa [bag, ps, hkne] using h
    have hex : ∃ i ∈ ps, (es.lookup i == some k) = true := by
      by_cases hex : ∃ i ∈ ps, (es.lookup i == some k) = true
      · exact hex
      · have hz : ps.countP (fun i => es.lookup i == some k) = 0 := by
          rw [List.countP_eq_zero]
          intro i hi hp
          exact hex ⟨i, hi, hp⟩
        have hzero : bag.mass (n, d, es) k = 0 := by simp [bag, ps, hkne, hz]
        omega
    obtain ⟨i, hi, hlk⟩ := hex
    have hl : es.lookup i = some k := by simpa using hlk
    exact List.mem_map.mpr ⟨(i, k), mem_of_lookup_eq_some hl, rfl⟩

private theorem mem_live_iff_mass_pos (n d k : Nat) (es : List (Nat × Nat)) :
    k ∈ live bag (n, d, es) ↔ 0 < bag.mass (n, d, es) k := by
  unfold live
  simp only [List.mem_filter, List.mem_eraseDups]
  constructor
  · intro h
    exact by simpa using h.2
  · intro h
    exact ⟨mem_keys_of_mass_pos n d k es h, by simpa using h⟩

private theorem nodup_live (n d : Nat) (es : List (Nat × Nat)) : (live bag (n, d, es)).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys (n, d, es)))

#eval live bag (5, 3, [(1, 7), (1, 9), (12, 4)])
#eval live bag (5, 3, [(1, 7), (1, 9), (12, 4)].filter (fun r => r.1 < 5))
#eval live bag (3, 3, [(0, 7), (0, 9), (5, 3), (2, 4), (2, 2)])
#eval live bag (3, 3, [(0, 7), (0, 9), (5, 3), (2, 4), (2, 2)].filter (fun r => r.1 < 3))

example : (live bag (5, 3, ([(1, 7), (1, 9), (12, 4)] : List (Nat × Nat)).filter (fun r => r.1 < 5))).Perm
    (live bag (5, 3, [(1, 7), (1, 9), (12, 4)])) := by decide
example : (live bag (3, 3, ([(0, 7), (0, 9), (5, 3), (2, 4), (2, 2)] : List (Nat × Nat)).filter (fun r => r.1 < 3))).Perm
    (live bag (3, 3, [(0, 7), (0, 9), (5, 3), (2, 4), (2, 2)])) := by decide

theorem sound_ZeroRuns : ZeroRuns.Goal := by
  intro n d es
  let es' := es.filter (fun r => r.1 < n)
  rw [List.perm_iff_count]
  intro k
  have hm : bag.mass (n, d, es') k = bag.mass (n, d, es) k := by
    simpa [es'] using mass_filter n d k es
  by_cases h : 0 < bag.mass (n, d, es) k
  · have h' : 0 < bag.mass (n, d, es') k := by simpa [hm] using h
    have hl : (live bag (n, d, es')).count k = 1 :=
      count_eq_one_of_mem_nodup (nodup_live n d es')
        ((mem_live_iff_mass_pos n d k es').2 h')
    have hr : (live bag (n, d, es)).count k = 1 :=
      count_eq_one_of_mem_nodup (nodup_live n d es) ((mem_live_iff_mass_pos n d k es).2 h)
    rw [hl, hr]
  · have h' : ¬ 0 < bag.mass (n, d, es') k := by
      intro hk
      exact h (by simpa [hm] using hk)
    have hl : (live bag (n, d, es')).count k = 0 :=
      List.count_eq_zero_of_not_mem (by
        intro hk
        exact h' ((mem_live_iff_mass_pos n d k es').1 hk))
    have hr : (live bag (n, d, es)).count k = 0 :=
      List.count_eq_zero_of_not_mem (by
        intro hk
        exact h ((mem_live_iff_mass_pos n d k es).1 hk))
    rw [hl, hr]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_ExceptionOverlay_ZeroRuns
