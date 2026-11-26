import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

/-!
Proof contribution for ResourcePolicies_VersionVacuum_AdvancingFloorComposition. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.VersionVacuum.Goal_AdvancingFloorComposition

open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

set_option maxHeartbeats 0
set_option maxRecDepth 10000
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

namespace VacuumHelpers
theorem not_le_of_bool_false {a b : Nat} (h : decide (a ≤ b) = false) : b < a := by
  by_cases hle : a ≤ b
  · simp [hle] at h
  · omega
theorem find_some_prefix {vs : List Version} {s : Nat} {v : Version}
    (h : winner vs s = some v) :
    ∃ as bs, vs = as ++ v :: bs ∧ ∀ u ∈ as, s < u.stamp := by
  unfold winner at h
  rw [List.find?_eq_some_iff_append] at h
  rcases h with ⟨hv, as, bs, hvs, hpref⟩
  refine ⟨as, bs, hvs, ?_⟩
  intro u hu
  have hp : decide (u.stamp ≤ s) = false := by simpa using hpref u hu
  exact not_le_of_bool_false hp
theorem find_none_all {vs : List Version} {s : Nat} (h : winner vs s = none) :
    ∀ u ∈ vs, s < u.stamp := by
  unfold winner at h
  rw [List.find?_eq_none] at h
  intro u hu
  have hp : decide (u.stamp ≤ s) = false := by simpa using h u hu
  exact not_le_of_bool_false hp
theorem winner_filter_of_prefix {vs : List Version} {s : Nat} {v : Version}
    (p : Version → Bool)
    (hvle : v.stamp ≤ s) (hpv : p v = true)
    (hvs : ∃ as bs, vs = as ++ v :: bs ∧ ∀ u ∈ as, s < u.stamp) :
    winner (vs.filter p) s = some v := by
  rcases hvs with ⟨as, bs, rfl, hpref⟩
  unfold winner
  rw [List.filter_append, List.filter_cons]
  rw [hpv]
  simp only [↓reduceIte]
  rw [List.find?_eq_some_iff_append]
  refine ⟨?_, as.filter p, bs.filter p, rfl, ?_⟩
  · simp [hvle]
  · intro u hu
    have hmem : u ∈ as := (List.mem_filter.mp hu).1
    have hgt := hpref u hmem
    simp [show ¬ u.stamp ≤ s by omega]
theorem winner_filter_eq_none_of_all_late {vs : List Version} {s : Nat}
    (p : Version → Bool) (halt : ∀ u ∈ vs, s < u.stamp) :
    winner (vs.filter p) s = none := by
  unfold winner
  rw [List.find?_eq_none]
  intro u hu
  have hmem : u ∈ vs := (List.mem_filter.mp hu).1
  have hgt := halt u hmem
  simp [show ¬ u.stamp ≤ s by omega]
end VacuumHelpers
open VacuumHelpers

theorem sparse_winner {vs : List Version} {pins : List Nat} {s : Nat} (hs : s ∈ pins) :
    winner (sparse vs pins) s = winner vs s := by
  unfold sparse
  cases hopt : winner vs s with
  | none =>
      have halt := find_none_all hopt
      exact winner_filter_eq_none_of_all_late _ (fun u hu => halt u hu)
  | some v =>
      rcases find_some_prefix hopt with ⟨as, bs, hvs, hlate⟩
      have hvle_bool : decide (v.stamp ≤ s) = true := by
        simpa [winner] using (List.find?_some (p := fun v : Version => decide (v.stamp ≤ s)) hopt)
      have hvle : v.stamp ≤ s := by simpa using hvle_bool
      have hpv : pins.any (fun t => winner vs t == some v) = true := by
        simp only [List.any_eq_true]
        exact ⟨s, hs, by simp [hopt]⟩
      have hw : winner (vs.filter (fun v => pins.any (fun s => winner vs s == some v))) s = some v :=
        winner_filter_of_prefix _ hvle hpv ⟨as, bs, hvs, hlate⟩
      exact hw

theorem winner_of_prefix {vs : List Version} {s : Nat} {v : Version}
    {as bs : List Version} (hvs : vs = as ++ v :: bs)
    (hvle : v.stamp ≤ s) (hlate : ∀ u ∈ as, s < u.stamp) :
    winner vs s = some v := by
  unfold winner
  rw [hvs, List.find?_eq_some_iff_append]
  refine ⟨?_, as, bs, rfl, ?_⟩
  · simp [hvle]
  · intro u hu
    have hgt := hlate u hu
    simp [show ¬ u.stamp ≤ s by omega]

theorem winner_dense_eq {vs : List Version} {f s : Nat}
    (hord : ordered vs) (hfs : f ≤ s) : winner (dense vs f) s = winner vs s := by
  cases hopt : winner vs s with
  | none =>
      have halt := find_none_all hopt
      have hfilter : vs.filter (fun v => f < v.stamp) = vs := by
        apply List.filter_eq_self.mpr
        intro u hu
        have hgt := halt u hu
        simp [show f < u.stamp by omega]
      have hwf : (winner vs f).toList = [] := by
        cases hwf : winner vs f with
        | none => simp [hwf]
        | some w =>
            have hwmem : w ∈ vs := List.mem_of_find?_eq_some (by simpa [winner] using hwf)
            have hwle_bool : decide (w.stamp ≤ f) = true := by
              simpa [winner] using (List.find?_some (p := fun v : Version => decide (v.stamp ≤ f)) hwf)
            have hwle : w.stamp ≤ f := by simpa using hwle_bool
            have hgt := halt w hwmem
            omega
      unfold dense
      simp [hfilter, hwf, hopt]
  | some v =>
      rcases find_some_prefix hopt with ⟨as, bs, hvs, hlate⟩
      have hvle_bool : decide (v.stamp ≤ s) = true := by
        simpa [winner] using (List.find?_some (p := fun v : Version => decide (v.stamp ≤ s)) hopt)
      have hvle : v.stamp ≤ s := by simpa using hvle_bool
      have htailord : ordered (v :: bs) := by
        unfold ordered at hord ⊢
        exact List.Pairwise.sublist (List.sublist_append_right as (v :: bs)) (by simpa [hvs] using hord)
      have hfilt_as : as.filter (fun u : Version => f < u.stamp) = as := by
        apply List.filter_eq_self.mpr
        intro u hu
        have hgt := hlate u hu
        simp [show f < u.stamp by omega]
      by_cases hfv : f < v.stamp
      · have hfilt : vs.filter (fun u : Version => f < u.stamp) =
            as ++ v :: bs.filter (fun u : Version => f < u.stamp) := by
          rw [hvs, List.filter_append, List.filter_cons, hfilt_as]
          simp [hfv]
        have hdense : dense vs f =
            as ++ v :: (bs.filter (fun u : Version => f < u.stamp) ++ (winner vs f).toList) := by
          unfold dense
          simp [hfilt]
        rw [hdense]
        unfold winner
        rw [List.find?_eq_some_iff_append]
        refine ⟨?_, as, bs.filter (fun u : Version => f < u.stamp) ++ (winner vs f).toList, ?_, ?_⟩
        · simp [hvle]
        · rfl
        · intro u hu
          have hgt := hlate u hu
          simp [show ¬ u.stamp ≤ s by omega]
      · have htail_late : ∀ u ∈ bs, u.stamp < v.stamp := by
          unfold ordered at htailord
          intro u hu
          exact List.rel_of_pairwise_cons htailord hu
        have hfilt_bs : bs.filter (fun u : Version => f < u.stamp) = [] := by
          apply List.filter_eq_nil_iff.mpr
          intro u hu
          have hlt := htail_late u hu
          simp [show ¬ f < u.stamp by omega]
        have hfilt : vs.filter (fun u : Version => f < u.stamp) = as := by
          rw [hvs, List.filter_append, List.filter_cons, hfilt_as, hfilt_bs]
          simp [show ¬ f < v.stamp by omega]
        have hwf : winner vs f = some v := by
          exact winner_of_prefix hvs (by omega) (fun u hu => by
            have hgt := hlate u hu
            omega)
        have hdense : dense vs f = as ++ [v] := by
          unfold dense
          rw [hfilt, hwf]
          simp
        rw [hdense]
        unfold winner
        rw [List.find?_eq_some_iff_append]
        refine ⟨?_, as, [], by simp, ?_⟩
        · simp [hvle]
        · intro u hu
          have hgt := hlate u hu
          simp [show ¬ u.stamp ≤ s by omega]

theorem sweep_inj_le {n stride start i j : Nat} (hn : 0 < n) (hcop : Nat.gcd stride n = 1)
    (hi : i < n) (hj : j < n) (hij : i ≤ j)
    (heq : (start + i * stride) % n = (start + j * stride) % n) : i = j := by
  have hmod : ((start + j * stride) - (start + i * stride)) % n = 0 := by
    exact Nat.sub_mod_eq_zero_of_mod_eq heq.symm
  have hdvd : n ∣ ((start + j * stride) - (start + i * stride)) :=
    Nat.dvd_of_mod_eq_zero hmod
  have hdiff : (start + j * stride) - (start + i * stride) = (j - i) * stride := by
    rw [Nat.add_sub_add_left, Nat.mul_sub_right_distrib]
  rw [hdiff] at hdvd
  have hcop' : Nat.Coprime n stride := by
    rw [Nat.Coprime, Nat.gcd_comm]
    exact hcop
  have hdvd2 : n ∣ j - i := hcop'.dvd_of_dvd_mul_right hdvd
  have hji : j - i = 0 := by
    by_cases hp : j - i = 0
    · exact hp
    · have hle : n ≤ j - i := Nat.le_of_dvd (by omega) hdvd2
      omega
  omega

theorem sweep_inj {n stride start i j : Nat} (hn : 0 < n) (hcop : Nat.gcd stride n = 1)
    (hi : i < n) (hj : j < n)
    (heq : (start + i * stride) % n = (start + j * stride) % n) : i = j := by
  by_cases hij : i ≤ j
  · exact sweep_inj_le hn hcop hi hj hij heq
  · have hji : j ≤ i := Nat.le_of_not_ge hij
    exact (sweep_inj_le hn hcop hj hi hji heq.symm).symm

theorem sweep_nodup {n stride start : Nat} (hn : 0 < n) (hcop : Nat.gcd stride n = 1) :
    (visits n stride start n).Nodup := by
  unfold visits
  rw [List.nodup_iff_pairwise_ne]
  have hp : (List.range n).Pairwise
      (fun a b : Nat => a ∈ List.range n ∧ b ∈ List.range n ∧ a ≠ b) :=
    (List.Pairwise.and_mem (l := List.range n) (R := fun a b : Nat => a ≠ b)).1
      (List.nodup_iff_pairwise_ne.mp (List.nodup_range : (List.range n).Nodup))
  exact List.Pairwise.map (f := fun i => (start + i * stride) % n)
    (S := fun a b : Nat => a ≠ b)
    (fun a b hab heq => hab.2.2 (sweep_inj hn hcop
      (List.mem_range.mp hab.1) (List.mem_range.mp hab.2.1) heq)) hp

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum.AdvancingFloorComposition.Goal := by
  intro vs a b hord hab
  have hfilts : (vs.filter (fun u : Version => a < u.stamp)).filter
        (fun u : Version => b < u.stamp) = vs.filter (fun u : Version => b < u.stamp) := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro u hu
    by_cases hbu : b < u.stamp
    · have : a < u.stamp := by omega
      simp [hbu, this]
    · simp [hbu]
  have hwin_a_filt : ((winner vs a).toList.filter
        (fun u : Version => b < u.stamp)) = [] := by
    cases hwa : winner vs a with
    | none => simp [hwa]
    | some w =>
        have hwle_bool : decide (w.stamp ≤ a) = true := by
          simpa [winner] using (List.find?_some (p := fun v : Version => decide (v.stamp ≤ a)) hwa)
        have hwle : w.stamp ≤ a := by simpa using hwle_bool
        simp [hwa, show ¬ b < w.stamp by omega]
  have hwin_dense : winner (dense vs a) b = winner vs b := by
    cases hopt : winner vs b with
    | none =>
        have halt := find_none_all hopt
        have hfa : vs.filter (fun u : Version => a < u.stamp) = vs := by
          apply List.filter_eq_self.mpr
          intro u hu
          have hgt : b < u.stamp := halt u hu
          simp [Nat.lt_of_le_of_lt hab hgt]
        have hwa : (winner vs a).toList = [] := by
          cases hwa : winner vs a with
          | none => simp [hwa]
          | some w =>
              have hwmem : w ∈ vs := List.mem_of_find?_eq_some (by simpa [winner] using hwa)
              have hwle_bool : decide (w.stamp ≤ a) = true := by
                simpa [winner] using (List.find?_some (p := fun v : Version => decide (v.stamp ≤ a)) hwa)
              have hwle : w.stamp ≤ a := by simpa using hwle_bool
              have hgt := halt w hwmem
              omega
        unfold dense
        simp [hfa, hwa, hopt]
    | some v =>
        rcases find_some_prefix hopt with ⟨as, bs, hvs, hlate⟩
        have hvle_bool : decide (v.stamp ≤ b) = true := by
          simpa [winner] using (List.find?_some (p := fun v : Version => decide (v.stamp ≤ b)) hopt)
        have hvle_b : v.stamp ≤ b := by simpa using hvle_bool
        have hfas : as.filter (fun u : Version => a < u.stamp) = as := by
          apply List.filter_eq_self.mpr
          intro u hu
          have hgt : b < u.stamp := hlate u hu
          simp [Nat.lt_of_le_of_lt hab hgt]
        have htailord : ordered (v :: bs) := by
          unfold ordered at hord ⊢
          exact List.Pairwise.sublist (List.sublist_append_right as (v :: bs)) (by simpa [hvs] using hord)
        by_cases hav : a < v.stamp
        · have hfa : vs.filter (fun u : Version => a < u.stamp) =
              as ++ v :: bs.filter (fun u : Version => a < u.stamp) := by
            rw [hvs, List.filter_append, List.filter_cons, hfas]
            simp [hav]
          unfold dense
          rw [hfa]
          unfold winner
          rw [List.find?_eq_some_iff_append]
          refine ⟨?_, as, bs.filter (fun u : Version => a < u.stamp) ++ (winner vs a).toList, ?_, ?_⟩
          · simp [hvle_b]
          · simp [winner]
          · intro u hu
            have hgt := hlate u hu
            simp [show ¬ u.stamp ≤ b by omega]
        · have hva : v.stamp ≤ a := by omega
          have hbs : bs.filter (fun u : Version => a < u.stamp) = [] := by
            apply List.filter_eq_nil_iff.mpr
            intro u hu
            unfold ordered at htailord
            have hlt := List.rel_of_pairwise_cons htailord hu
            simp [show ¬ a < u.stamp by omega]
          have hfa : vs.filter (fun u : Version => a < u.stamp) = as := by
            rw [hvs, List.filter_append, List.filter_cons, hfas, hbs]
            simp [hav]
          have hwa : winner vs a = some v := by
            exact winner_of_prefix hvs hva (fun u hu => by
              have hgt := hlate u hu
              omega)
          unfold dense
          rw [hfa, hwa]
          simp
          unfold winner
          rw [List.find?_eq_some_iff_append]
          refine ⟨?_, as, [], by simp, ?_⟩
          · simp [hvle_b]
          · intro u hu
            have hgt := hlate u hu
            simp [show ¬ u.stamp ≤ b by omega]
  change (dense vs a).filter (fun u : Version => b < u.stamp) ++
      (winner (dense vs a) b).toList =
    (vs.filter (fun u : Version => b < u.stamp)) ++ (winner vs b).toList
  rw [hwin_dense]
  congr 1
  unfold dense
  rw [List.filter_append, hfilts, hwin_a_filt]
  simp
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.VersionVacuum.Goal_AdvancingFloorComposition
