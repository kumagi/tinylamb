import TinylambOpt.ResearchGoals.November26.Tpcds
namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q29
open TinylambOpt.ResearchGoals.BenchmarkRematch
set_option maxHeartbeats 0
set_option maxRecDepth 10000

theorem sum_perm {α : Type} (f : α → Int) : ∀ {l₁ l₂ : List α}, l₁.Perm l₂ →
    (l₁.map f).sum = (l₂.map f).sum
  | _, _, List.Perm.nil => rfl
  | _, _, List.Perm.cons x h => by simpa using sum_perm f h
  | _, _, List.Perm.swap x y l => by simp [Int.add_comm, Int.add_left_comm]
  | _, _, List.Perm.trans h1 h2 => by rw [sum_perm f h1, sum_perm f h2]

theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => .nil
  | a :: l => by
      rw [List.eraseDups_cons]
      constructor
      · intro h hmem
        rw [List.mem_eraseDups, List.mem_filter] at hmem
        intro heq
        rw [← heq] at hmem
        simp at hmem
      · exact nodup_eraseDups (l.filter (fun b => !b == a))
termination_by l => l.length
decreasing_by
  have := List.length_filter_le (fun b => !b == a) l
  simp
  omega

theorem sum_if_eq_of_mem {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α)
    (c : α → Nat) (hnd : l.Nodup) :
    (l.map (fun k => if k == a then c k else 0)).sum = if a ∈ l then c a else 0 := by
  induction l with
  | nil => rfl
  | cons k ks ih =>
      have hnd' : ks.Nodup := (List.nodup_cons.mp hnd).2
      have hknot : k ∉ ks := (List.nodup_cons.mp hnd).1
      by_cases hka : k == a
      · have hkeq : k = a := by simpa using hka
        have hnot : a ∉ ks := by
          intro h
          exact hknot (hkeq ▸ h)
        have hih := ih hnd'
        simp only [List.map_cons, List.sum_cons]
        rw [hih]
        simp [hka, hkeq, hnot]
      · have hka' : (k == a) = false := by simpa using hka
        by_cases hmem : a ∈ ks
        · have hmcons : a ∈ k :: ks := by simp [hmem]
          simp [ih hnd', hmem, hmcons, hka']
        · have hmcons : a ∉ k :: ks := by
            intro hm
            simp only [List.mem_cons] at hm
            rcases hm with heq | h
            · have hne : a ≠ k := by intro h'; exact hka (by simpa [h'])
              exact hne heq
            · exact hmem h
          simp [ih hnd', hmem, hmcons, hka']

theorem sum_if_eq_zero_of_not_mem {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α)
    (c : α → Nat) (hnot : a ∉ l) :
    (l.map (fun k => if k == a then c k else 0)).sum = 0 := by
  induction l with
  | nil => rfl
  | cons k ks ih =>
      have hnot' : a ∉ ks := by intro h; exact hnot (by simp [h])
      have hkne : k ≠ a := by intro h; exact hnot (by simp [h])
      have hka : (k == a) = false := by simpa [hkne]
      simp [hka, ih hnot']

theorem count_group {α : Type} [BEq α] [LawfulBEq α] (xs : List α) (a : α) :
    (xs.eraseDups.flatMap (fun k => List.replicate (xs.count k) k)).count a = xs.count a := by
  rw [List.count_flatMap]
  rw [show (xs.eraseDups.map (List.count a ∘ fun k => List.replicate (xs.count k) k)) =
      xs.eraseDups.map (fun k => if k == a then xs.count k else 0) by
    apply List.map_congr_left
    intro k hk
    change (List.replicate (xs.count k) k).count a = if k == a then xs.count k else 0
    rw [List.count_replicate]]
  by_cases ha : a ∈ xs
  · rw [sum_if_eq_of_mem xs.eraseDups a (fun k => xs.count k) (nodup_eraseDups xs)]
    simp [List.mem_eraseDups.mpr ha]
  · rw [sum_if_eq_zero_of_not_mem xs.eraseDups a (fun k => xs.count k) (by simpa using ha)]
    exact (List.count_eq_zero.mpr ha).symm

theorem group_perm {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    xs.Perm (xs.eraseDups.flatMap (fun k => List.replicate (xs.count k) k)) := by
  rw [List.perm_iff_count]
  intro a
  exact (count_group xs a).symm

theorem map_flatMap_replicate {α β : Type} (xs : List α) (f : α → Nat) (g : α → β) :
    (xs.flatMap (fun k => List.replicate (f k) k)).map g =
      xs.flatMap (fun k => List.replicate (f k) (g k)) := by
  induction xs with
  | nil => rfl
  | cons k ks ih => simp [ih]

theorem sum_replicate_nat_int (n : Nat) (r : Int) : (List.replicate n r).sum = (n : Int) * r := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.replicate_succ, ih, Int.natCast_add, Int.add_mul]; omega

theorem sum_flatten_int : ∀ xs : List (List Int), xs.flatten.sum = (xs.map List.sum).sum
  | [] => rfl
  | x :: xs => by simp [sum_flatten_int xs]

theorem group_sum_int {α : Type} [BEq α] [LawfulBEq α] (xs : List α) (g : α → Int) :
    (xs.map g).sum = ((xs.eraseDups.map (fun k => (xs.count k : Int) * g k)).sum) := by
  have hp := (group_perm xs).map g
  rw [map_flatMap_replicate] at hp
  have hsum : (xs.map g).sum =
      (xs.eraseDups.flatMap (fun k => List.replicate (xs.count k) (g k))).sum := by
    simpa [Function.comp_def] using (sum_perm (fun z : Int => z) hp)
  rw [hsum]
  change (xs.eraseDups.map (fun k => List.replicate (xs.count k) (g k))).flatten.sum = _
  rw [sum_flatten_int, List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro k hk
  simp only [Function.comp_apply]
  rw [sum_replicate_nat_int]

-- end basic machinery

theorem fm_flatMap {α : Type} (xs : List α) (cat : α → List (Option Int)) :
    (xs.flatMap cat).filterMap id = xs.flatMap (fun k => (cat k).filterMap id) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

theorem nsum_view (Z : List (Option Int)) :
    nsum Z = if (Z.filterMap id).isEmpty then none else some (Z.filterMap id).sum := rfl

theorem fm_nsum_map_mul {α : Type} (l : List α) (cat : α → List (Option Int)) (cnt : α → Nat) :
    (l.map (fun k => (nsum (cat k)).map (fun v => v * (cnt k : Int)))).filterMap id =
      (l.filter (fun k => !((cat k).filterMap id).isEmpty = true)).map
        (fun k => ((cat k).filterMap id).sum * (cnt k : Int)) := by
  simp only [nsum_view]
  induction l with
  | nil => rfl
  | cons k l ih =>
      rw [List.map_cons]
      by_cases h : ((cat k).filterMap id).isEmpty = true
      · simp only [h, if_true, Option.map_none, List.filterMap_cons]
        rw [ih]
        rw [List.filter_cons]
        simp [h]
      · simp only [h, if_false, Option.map_some, List.filterMap_cons, List.map_cons]
        rw [ih]
        rw [List.filter_cons]
        simp [h]

theorem sum_filter_good {α : Type} (l : List α) (cat : α → List (Option Int)) (cnt : α → Nat) :
    ((l.filter (fun k => !((cat k).filterMap id).isEmpty = true)).map
      (fun k => ((cat k).filterMap id).sum * (cnt k : Int))).sum =
    (l.map (fun k => ((cat k).filterMap id).sum * (cnt k : Int))).sum := by
  induction l with
  | nil => rfl
  | cons k l ih =>
      by_cases h : ((cat k).filterMap id).isEmpty = true
      · have hnil : (cat k).filterMap id = [] := List.isEmpty_iff.mp h
        rw [List.filter_cons]
        simp only [h, decide_true, Bool.not_true, Bool.false_eq_true, ite_false]
        rw [ih]
        simp only [List.map_cons, List.sum_cons]
        simp [hnil]
      · rw [List.filter_cons]
        simp only [h, Bool.false_eq_true, decide_false, Bool.not_false, ite_true,
          List.map_cons, List.sum_cons]
        rw [ih]

theorem fm_flatMap_empty_iff {α : Type} (xs : List α) (cat : α → List (Option Int)) :
    (xs.flatMap (fun k => (cat k).filterMap id)).isEmpty = true ↔
      ∀ k ∈ xs, ((cat k).filterMap id).isEmpty = true := by
  rw [List.isEmpty_iff]
  constructor
  · intro h k hk
    by_cases hne : ((cat k).filterMap id).isEmpty = true
    · simpa using hne
    · have hnonnil : (cat k).filterMap id ≠ [] := by
        intro hnil
        rw [hnil] at hne
        simp at hne
      rcases List.exists_mem_of_ne_nil ((cat k).filterMap id) hnonnil with ⟨z, hz⟩
      have hzflat : z ∈ xs.flatMap (fun k => (cat k).filterMap id) :=
        List.mem_flatMap.mpr ⟨k, hk, hz⟩
      rw [h] at hzflat
      simpa using hzflat
  · intro h
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro z hz
    rcases List.mem_flatMap.mp hz with ⟨k, hk, hzk⟩
    have hnil : (cat k).filterMap id = [] := List.isEmpty_iff.mp (h k hk)
    rw [hnil] at hzk
    simpa using hzk

theorem fm_rhs_empty_iff {α : Type} [BEq α] [LawfulBEq α] (xs : List α)
    (cat : α → List (Option Int)) :
    ((xs.eraseDups.filter (fun k => !((cat k).filterMap id).isEmpty = true)).map
      (fun k => ((cat k).filterMap id).sum * (xs.count k : Int))).isEmpty = true ↔
      ∀ k ∈ xs, ((cat k).filterMap id).isEmpty = true := by
  rw [List.isEmpty_iff, List.map_eq_nil_iff]
  constructor
  · intro h k hk
    by_cases hne : ((cat k).filterMap id).isEmpty = true
    · simpa using hne
    · have hgood : !((cat k).filterMap id).isEmpty = true := by simp [hne]
      have hmem : k ∈ xs.eraseDups := List.mem_eraseDups.mpr hk
      have hmemf : k ∈ xs.eraseDups.filter (fun j => !((cat j).filterMap id).isEmpty = true) := by
        rw [List.mem_filter]
        exact ⟨hmem, hgood⟩
      exfalso
      exact (List.eq_nil_iff_forall_not_mem.mp h k) hmemf
  · intro h
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro k hk
    rw [List.mem_filter] at hk
    have hp := h k (List.mem_eraseDups.mp hk.1)
    have hb : (!decide (((cat k).filterMap id).isEmpty = true)) = false := by
      simp only [hp, decide_true, Bool.not_true]
    rw [hb] at hk
    cases hk.2

theorem ed_filter {α : Type} [BEq α] [LawfulBEq α] (q : α → Bool) : ∀ l : List α,
    (l.filter q).eraseDups = l.eraseDups.filter q
  | [] => rfl
  | a :: l => by
    have ih := ed_filter q (l.filter (fun b => !b == a))
    have hcomm : (l.filter (fun b => !b == a)).filter q = (l.filter q).filter (fun b => !b == a) := by
      rw [List.filter_filter, List.filter_filter]
      apply List.filter_congr
      intro x _
      exact Bool.and_comm _ _
    rw [List.eraseDups_cons]
    by_cases hq : q a = true
    · rw [List.filter_cons_of_pos hq, List.filter_cons_of_pos hq, List.eraseDups_cons, ← ih, hcomm]
    · have hq' : q a = false := by simpa using hq
      rw [List.filter_cons_of_neg hq, List.filter_cons_of_neg hq, ← ih]
      have : (l.filter (fun b => !b == a)).filter q = l.filter q := by
        rw [List.filter_filter]
        apply List.filter_congr
        intro x hx
        by_cases hxa : x = a
        · subst hxa; simp [hq']
        · simp [hxa]
      rw [this]
termination_by l => l.length
decreasing_by
  have := List.length_filter_le (fun b => !b == a) l
  simp
  omega

theorem sum_flatMap_filterMap {α : Type} (xs : List α) (cat : α → List (Option Int)) :
    (xs.flatMap (fun k => (cat k).filterMap id)).sum =
      ((xs.filter (fun k => !((cat k).filterMap id).isEmpty = true)).map
        (fun k => ((cat k).filterMap id).sum)).sum := by
  induction xs with
  | nil => rfl
  | cons k xs ih =>
      rw [List.flatMap_cons, List.sum_append, List.filter_cons]
      by_cases h : ((cat k).filterMap id).isEmpty = true
      · simp only [h, decide_true, Bool.not_true, Bool.false_eq_true, ite_false]
        rw [ih]
        have hnil : (cat k).filterMap id = [] := List.isEmpty_iff.mp h
        simp [hnil]
      · simp only [h, Bool.false_eq_true, decide_false, Bool.not_false, ite_true,
          List.map_cons, List.sum_cons]
        rw [ih]

theorem nsum_group_general {α : Type} [BEq α] [LawfulBEq α] (xs : List α)
    (cat : α → List (Option Int)) :
    nsum (xs.flatMap cat) =
      nsum ((frequency xs).map (fun (k, n) => (nsum (cat k)).map (fun v => v * (n : Int)))) := by
  rw [nsum_view, nsum_view]
  rw [fm_flatMap]
  unfold frequency
  rw [List.map_map]
  change (if (xs.flatMap (fun k => (cat k).filterMap id)).isEmpty = true then none
      else some (xs.flatMap (fun k => (cat k).filterMap id)).sum) =
    if (List.filterMap id (xs.eraseDups.map
        (fun k => (nsum (cat k)).map (fun v => v * (xs.count k : Int))))).isEmpty = true
      then none
      else some (List.filterMap id (xs.eraseDups.map
        (fun k => (nsum (cat k)).map (fun v => v * (xs.count k : Int))))).sum
  rw [fm_nsum_map_mul]
  have hemp : (xs.flatMap (fun k => (cat k).filterMap id)).isEmpty = true ↔
      ((xs.eraseDups.filter (fun k => !((cat k).filterMap id).isEmpty = true)).map
        (fun k => ((cat k).filterMap id).sum * (xs.count k : Int))).isEmpty = true :=
    (fm_flatMap_empty_iff xs cat).trans (fm_rhs_empty_iff xs cat).symm
  have hsum : (xs.flatMap (fun k => (cat k).filterMap id)).sum =
      ((xs.eraseDups.filter (fun k => !((cat k).filterMap id).isEmpty = true)).map
        (fun k => ((cat k).filterMap id).sum * (xs.count k : Int))).sum := by
    rw [show (xs.eraseDups.filter (fun k => !((cat k).filterMap id).isEmpty = true)) =
        (xs.filter (fun k => !((cat k).filterMap id).isEmpty = true)).eraseDups by
      rw [ed_filter]]
    rw [show ((xs.filter (fun k => !((cat k).filterMap id).isEmpty = true)).eraseDups).map
        (fun k => ((cat k).filterMap id).sum * (xs.count k : Int)) =
        ((xs.filter (fun k => !((cat k).filterMap id).isEmpty = true)).eraseDups).map
        (fun k => ((cat k).filterMap id).sum *
          ((xs.filter (fun k => !((cat k).filterMap id).isEmpty = true)).count k : Int)) by
      apply List.map_congr_left
      intro k hk
      have hkf : k ∈ xs.filter (fun k => !((cat k).filterMap id).isEmpty = true) :=
        List.mem_eraseDups.mp hk
      have hgood : (!decide (((cat k).filterMap id).isEmpty = true)) = true :=
        (List.mem_filter.mp hkf).2
      rw [List.count_filter
        (p := fun k => !decide (((cat k).filterMap id).isEmpty = true)) (a := k) hgood]]
    rw [sum_flatMap_filterMap]
    have hg := group_sum_int (xs.filter (fun k => !((cat k).filterMap id).isEmpty = true))
      (fun k => ((cat k).filterMap id).sum)
    rw [hg]
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    rw [Int.mul_comm]
  by_cases hL : (xs.flatMap (fun k => (cat k).filterMap id)).isEmpty = true
  · have hR : ((xs.eraseDups.filter (fun k => !((cat k).filterMap id).isEmpty = true)).map
        (fun k => ((cat k).filterMap id).sum * (xs.count k : Int))).isEmpty = true := hemp.mp hL
    rw [hL, hR, hsum]
  · have hR : ¬ (((xs.eraseDups.filter (fun k => !((cat k).filterMap id).isEmpty = true)).map
        (fun k => ((cat k).filterMap id).sum * (xs.count k : Int))).isEmpty = true) := by
      intro hR
      exact hL (hemp.mpr hR)
    have hL' : ((xs.flatMap (fun k => (cat k).filterMap id)).isEmpty = true) = False := by
      simpa using hL
    have hR' : (((xs.eraseDups.filter (fun k => !((cat k).filterMap id).isEmpty = true)).map
        (fun k => ((cat k).filterMap id).sum * (xs.count k : Int))).isEmpty = true) = False := by
      simpa using hR
    simp only [hL', hR', hsum, ite_false]

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q29.Goal := by
  intro matchedTickets catalog
  exact nsum_group_general matchedTickets catalog
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q29
