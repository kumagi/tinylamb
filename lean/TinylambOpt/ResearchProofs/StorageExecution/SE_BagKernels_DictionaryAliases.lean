import TinylambOpt.ResearchProofs.StorageExecution.SE_BagKernels_Common

set_option maxHeartbeats 0

open TinylambOpt.ResearchProofs.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels

namespace DictionaryAliasesProofs

open DictionaryAliases

theorem sum_map_const_p {β : Type} (l : List β) (c : Nat) :
    (l.map (fun _ => c)).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons a as ih =>
    rw [List.map_cons, List.sum_cons, List.length_cons, ih, Nat.succ_mul]
    omega

theorem sum_add_map_p {β : Type} (l : List β) (f g : β → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a as ih =>
    rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons,
      List.map_cons, List.sum_cons, ih]
    omega

theorem zipIdx_append : ∀ (a b : List Nat) (s : Nat),
    (a ++ b).zipIdx s = a.zipIdx s ++ b.zipIdx (s + a.length) := by
  intro a b; induction a with
  | nil => intro s; rfl
  | cons z t ih =>
    intro s
    rw [List.cons_append, List.zipIdx_cons, List.zipIdx_cons, ih (s + 1)]
    rw [show s + 1 + t.length = s + (t.length + 1) from by omega]
    rw [List.length_cons]
    exact rfl

theorem succ_beq (x j : Nat) : (x + 1 == j + 1) = (x == j) := by
  cases h1 : x + 1 == j + 1 with
  | true =>
    have hx : x = j := by
      have h2 : x + 1 = j + 1 := Nat.eq_of_beq_eq_true h1
      omega
    rw [← hx, beq_tt (rfl : x = x)]
  | false =>
    have hne : ¬(x = j) := by
      intro h
      subst h
      simp at h1
    have hb : (x == j) = false := by
      cases h2 : x == j with
      | true => exact absurd (Nat.eq_of_beq_eq_true h2) hne
      | false => exact rfl
    rw [hb]

theorem succ_beq2 (x j : Nat) : (j + 1 == x + 1) = (j == x) := by
  rw [beq_comm (j + 1) (x + 1), succ_beq x j, beq_comm x j]

theorem hzero (x : Nat) : (0 == x + 1) = false := by
  cases h : 0 == x + 1 with
  | true => exact absurd (Nat.eq_of_beq_eq_true h) (by omega)
  | false => exact h

theorem filter_map_sum_congr (k s : Nat) (g : Nat × Nat → Nat) :
    ∀ (l : List (Nat × Nat)),
      (((l.map (fun e => (e.1, e.2 + s))).filter (fun e => e.1 == k)).map g).sum
        = ((l.filter (fun e => e.1 == k)).map (fun e => g (e.1, e.2 + s))).sum := by
  intro l; induction l with
  | nil => rfl
  | cons x t ih =>
    have hb : (((x.1, x.2 + s).1 == k) : Bool) = (x.1 == k) := rfl
    rw [List.map_cons, List.filter_cons, hb]
    by_cases hz : (x.1 == k) = true
    · rw [if_pos hz, List.filter_cons, if_pos hz, List.map_cons, List.map_cons,
        List.sum_cons, List.sum_cons, ih]
    · rw [if_neg hz, List.filter_cons, if_neg hz, ih]

theorem dict_single : ∀ (k i : Nat) (d : List Nat),
    ((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => if i == e.2 then 1 else 0)).sum
      = if (d[i]? == some k) = true then 1 else 0 := by
  intro k i d; induction d generalizing k i with
  | nil => cases i <;> simp
  | cons z t ih =>
    have hz1 : t.zipIdx (0 + 1) = (t.zipIdx).map (fun e => (e.1, e.2 + 1)) := by
      rw [Nat.zero_add]
      exact by simpa using zipIdx_add t 0 1
    have hp : ((z, 0).1 == k) = (z == k) := rfl
    rw [List.zipIdx_cons, hz1, List.filter_cons, hp]
    by_cases hzk : (z == k) = true
    · rw [if_pos hzk, List.map_cons, List.sum_cons,
        filter_map_sum_congr k 1 (fun e => if i == e.2 then 1 else 0) t.zipIdx]
      cases i with
      | zero =>
        rw [show (if (0 : Nat) == (z, 0).2 then 1 else 0) = 1 from rfl]
        rw [show (fun e : Nat × Nat => (if (0 : Nat) == (e.fst, e.snd + 1).snd then 1 else 0))
              = (fun _ => 0) from by
          funext e
          rw [hzero, if_neg (by decide)]]
        rw [sum_map_const_p, Nat.mul_zero, Nat.add_zero]
        have hr : ((z :: t)[0]? : Option Nat) = some z := rfl
        rw [hr, show (some z == some k) = (z == k) from rfl, if_pos hzk]
      | succ j =>
        rw [show (if j + 1 == (z, 0).2 then 1 else 0) = 0 from rfl, Nat.zero_add]
        rw [show (fun e : Nat × Nat => (if j + 1 == (e.fst, e.snd + 1).snd then 1 else 0))
              = (fun e : Nat × Nat => (if j == e.2 then 1 else 0)) from by
          funext e
          rw [succ_beq2 e.2 j]]
        rw [ih k j]
        rw [show ((z :: t)[j + 1]? : Option Nat) = t[j]? from rfl]
    · rw [if_neg hzk,
        filter_map_sum_congr k 1 (fun e => if i == e.2 then 1 else 0) t.zipIdx]
      cases i with
      | zero =>
        rw [show (fun e : Nat × Nat => (if (0 : Nat) == (e.fst, e.snd + 1).snd then 1 else 0))
              = (fun _ => 0) from by
          funext e
          rw [hzero, if_neg (by decide)]]
        rw [sum_map_const_p, Nat.mul_zero]
        have hr : ((z :: t)[0]? : Option Nat) = some z := rfl
        rw [hr, show (some z == some k) = (z == k) from rfl, if_neg hzk]
      | succ j =>
        rw [show (fun e : Nat × Nat => (if j + 1 == (e.fst, e.snd + 1).snd then 1 else 0))
              = (fun e : Nat × Nat => (if j == e.2 then 1 else 0)) from by
          funext e
          rw [succ_beq2 e.2 j]]
        rw [ih k j]
        rw [show ((z :: t)[j + 1]? : Option Nat) = t[j]? from rfl]

theorem get?_mem (d : List Nat) : ∀ (i : Nat) (k : Nat), d[i]? = some k → k ∈ d := by
  induction d with
  | nil => intro i k h; exact absurd h (by simp)
  | cons z t ih =>
    intro i k h
    cases i with
    | zero =>
      rw [show ((z :: t)[0]? : Option Nat) = some z from rfl] at h
      cases h
      exact (by simp : z ∈ z :: t)
    | succ j =>
      rw [show ((z :: t)[j + 1]? : Option Nat) = t[j]? from rfl] at h
      exact List.mem_cons_of_mem z (ih j k h)

theorem get?_append_index (d ext : List Nat) :
    ∀ (i : Nat), i < d.length → (d ++ ext)[i]? = d[i]? := by
  induction d with
  | nil => intro i h; exact absurd h (Nat.not_lt_zero i)
  | cons z t ih =>
    intro i h
    cases i with
    | zero =>
      show ((z :: t) ++ ext)[0]? = ((z :: t)[0]? : Option Nat)
      rw [List.cons_append]
      rfl
    | succ j =>
      have hj : j < t.length := by simpa using h
      show (z :: (t ++ ext))[j + 1]? = ((z :: t)[j + 1]? : Option Nat)
      rw [show ((z :: (t ++ ext))[j + 1]? : Option Nat) = (t ++ ext)[j]? from rfl,
        show ((z :: t)[j + 1]? : Option Nat) = t[j]? from rfl, ih j hj]

theorem mass_eq_dict (d : List Nat) : ∀ (ids : List Nat) (k : Nat),
    (ids.filterMap (fun i => d[i]?)).count k
      = ((d.zipIdx.filter (fun e => e.1 == k)).map (fun e => ids.count e.2)).sum := by
  intro ids; induction ids with
  | nil =>
    intro k
    rw [List.filterMap_nil, List.count_nil]
    rw [show (fun e : Nat × Nat => ([] : List Nat).count e.2) = (fun _ => 0) from by
      funext e
      rfl]
    rw [sum_map_const_p, Nat.mul_zero]
  | cons i t ih =>
    intro k
    cases hdi : d[i]? with
    | none =>
      rw [List.filterMap_cons_none (f := fun j => d[j]?) (h := hdi), ih]
      rw [show (fun e : Nat × Nat => (i :: t).count e.2)
            = (fun e : Nat × Nat => t.count e.2 + (if i == e.2 then 1 else 0)) from by
        funext e
        rw [List.count_cons]]
      rw [sum_add_map_p, dict_single, hdi]
      rw [show ((none : Option Nat) == some k) = false from rfl,
        if_neg (by decide : ¬((false : Bool) = true))]
      rw [Nat.add_zero]
    | some v =>
      rw [List.filterMap_cons_some (f := fun j => d[j]?) (h := hdi)]
      rw [List.count_cons, ih]
      rw [show (fun e : Nat × Nat => (i :: t).count e.2)
            = (fun e : Nat × Nat => t.count e.2 + (if i == e.2 then 1 else 0)) from by
        funext e
        rw [List.count_cons]]
      rw [sum_add_map_p, dict_single, hdi]
      rw [show (some v == some k) = (v == k) from rfl]

theorem wf : WellFormed bag := by
  refine' ⟨_, _⟩
  · intro p k
    obtain ⟨d, ids⟩ := p
    exact mass_eq_dict d ids k
  · intro p k hk
    obtain ⟨d, ids⟩ := p
    rw [show bag.keys (d, ids) = d from rfl]
    rw [show bag.expand (d, ids) = ids.filterMap (fun i => d[i]?) from rfl] at hk
    obtain ⟨i, hi, hf⟩ := List.mem_filterMap.mp hk
    exact get?_mem d i k hf

theorem sound_DistinctOutput : DistinctOutput.Goal := wf_distinctOutput bag wf
theorem sound_FilteredMoment : FilteredMoment.Goal := wf_filteredMoment bag wf
theorem sound_OrderStatistic : OrderStatistic.Goal := wf_orderStatistic bag wf
theorem sound_JoinFanout : JoinFanout.Goal := wf_joinFanout bag wf
theorem sound_AntiRevenue : AntiRevenue.Goal := wf_antiRevenue bag wf
theorem sound_SelfPairs : SelfPairs.Goal := wf_selfPairs bag wf
theorem sound_IntersectAll : IntersectAll.Goal := wf_intersectAll bag wf
theorem sound_OuterWeight : OuterWeight.Goal := wf_outerWeight bag wf

theorem sound_ManyToOne : ManyToOne.Goal := by
  intro d ids v
  show (((d ++ [v]).zipIdx.filter (fun e => e.1 == v)).map (fun e => ids.count e.2)).sum
      = bag.mass (d, ids) v + ids.count d.length
  rw [zipIdx_append d [v] 0]
  rw [Nat.zero_add]
  rw [show [v].zipIdx d.length = [(v, d.length)] from by
    rw [List.zipIdx_cons, List.zipIdx_nil]]
  rw [List.filter_append, List.map_append, List.sum_append]
  rw [List.filter_cons]
  have hp : ((v, d.length).1 == v) = ((v : Nat) == v) := rfl
  have hv : ((v : Nat) == v) = true := beq_tt rfl
  rw [hp, hv, if_pos rfl, List.map_cons, List.sum_cons,
    show (List.filter (fun e => e.fst == v) []) = ([] : List (Nat × Nat)) from rfl,
    List.map_nil, List.sum_nil, Nat.add_zero]
  dsimp only
  have hm2 : bag.mass (d, ids) v
      = (((d.zipIdx 0).filter (fun e => e.1 == v)).map (fun e => ids.count e.2)).sum := rfl
  rw [hm2]

theorem filterMap_congr_of_bound (d ext : List Nat) :
    ∀ (ids : List Nat), (∀ i ∈ ids, i < d.length) →
      ids.filterMap (fun i => (d ++ ext)[i]?) = ids.filterMap (fun i => d[i]?) := by
  intro ids; induction ids with
  | nil => intro _; rfl
  | cons a t ih =>
    intro h
    have ha : a < d.length := h a (by simp)
    have ht : ∀ i ∈ t, i < d.length := by
      intro x hx
      exact h x (List.mem_cons_of_mem a hx)
    have h1 : (fun i => (d ++ ext)[i]?) a = d[a]? := get?_append_index d ext a ha
    cases h2 : d[a]? with
    | none =>
      have hne : (fun i => (d ++ ext)[i]?) a = none := by rw [h1, h2]
      rw [List.filterMap_cons_none (f := fun i => (d ++ ext)[i]?) (h := hne),
        List.filterMap_cons_none (f := fun i => d[i]?) (h := h2)]
      exact ih ht
    | some v =>
      have hso : (fun i => (d ++ ext)[i]?) a = some v := by rw [h1, h2]
      rw [List.filterMap_cons_some (f := fun i => (d ++ ext)[i]?) (h := hso),
        List.filterMap_cons_some (f := fun i => d[i]?) (h := h2)]
      exact congrArg (fun l => v :: l) (ih ht)

theorem sound_UnusedSuffix : UnusedSuffix.Goal := by
  intro d ids ext h
  have hexpd : ∀ (dd : List Nat) (ss : List Nat),
      bag.expand (dd, ss) = ss.filterMap (fun i => dd[i]?) := fun _ _ => rfl
  rw [weighted_eq bag wf (d ++ ext, ids) id, weighted_eq bag wf (d, ids) id,
    hexpd, hexpd, filterMap_congr_of_bound d ext ids h]

end DictionaryAliasesProofs
