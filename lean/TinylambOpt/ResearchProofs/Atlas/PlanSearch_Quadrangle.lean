import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `AlphabeticMerge.Quadrangle` — `∀ a b c, b ≠ [] → best (a++b) + best (b++c) ≤ best
  (a++b++c) + best b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_Quadrangle

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.AlphabeticMerge

private theorem minGetD_le_of_mem (xs : List Nat) (a : Nat) (ha : a ∈ xs) :
    xs.min?.getD 0 ≤ a := by
  cases h : xs.min? with
  | none =>
      have hnil : xs = [] := List.min?_eq_none_iff.mp h
      simp [hnil] at ha
  | some m =>
      have hm : m ≤ a := (List.min?_eq_some_iff.mp h).2 a ha
      simpa [h] using hm

private theorem minGetD_eq_of_mem (xs : List Nat) (a : Nat) (ha : a ∈ xs)
    (hmin : ∀ b ∈ xs, a ≤ b) : xs.min?.getD 0 = a := by
  have h : xs.min? = some a := (List.min?_eq_some_iff).2 ⟨ha, hmin⟩
  simp [h]

private theorem minGetD_map_mono {α : Type} (xs : List α) (f g : α → Nat)
    (h : ∀ a ∈ xs, f a ≤ g a) :
    (xs.map f).min?.getD 0 ≤ (xs.map g).min?.getD 0 := by
  cases hm : (xs.map g).min? with
  | none =>
      have hnil : xs.map g = [] := List.min?_eq_none_iff.mp hm
      cases xs with
      | nil => simp
      | cons a as => simp at hnil
  | some m =>
      have hmem : m ∈ xs.map g := List.min?_mem hm
      obtain ⟨a, ha, rfl⟩ := List.mem_map.mp hmem
      exact Nat.le_trans (minGetD_le_of_mem _ _ (List.mem_map_of_mem ha)) (by simpa [hm] using h a ha)

/-! The fuel in `dp` never binds once it is at least the list length: each split
reduces both the fuel and the length of every recursive list by the same amount.
Hence `best` is the unconstrained optimum. -/

private theorem dp_fuel_eq (ws : List Nat) (f g : Nat)
    (hf : ws.length ≤ f) (hg : ws.length ≤ g) : dp f ws = dp g ws := by
  induction f generalizing g ws with
  | zero =>
      cases ws with
      | nil =>
          cases g with
          | zero => rfl
          | succ g => simp [dp]
      | cons w ws =>
          simp at hf
  | succ f ih =>
      cases g with
      | zero =>
          cases ws with
          | nil => simp [dp]
          | cons w ws => simp at hg
      | succ g =>
          by_cases hsmall : ws.length ≤ 1
          · simp [dp, hsmall]
          · have hlen : 2 ≤ ws.length := by omega
            simp [dp, hsmall]
            congr 2
            apply List.map_congr_left
            intro i hi
            have hi' : i < ws.length - 1 := by simpa using hi
            have htk_f : (ws.take (i + 1)).length ≤ f := by
              rw [List.length_take]
              omega
            have hdk_f : (ws.drop (i + 1)).length ≤ f := by
              rw [List.length_drop]
              omega
            have htk_g : (ws.take (i + 1)).length ≤ g := by
              rw [List.length_take]
              omega
            have hdk_g : (ws.drop (i + 1)).length ≤ g := by
              rw [List.length_drop]
              omega
            rw [ih (ws.take (i + 1)) g htk_f htk_g]
            rw [ih (ws.drop (i + 1)) g hdk_f hdk_g]

private theorem dp_fuel_mono (ws : List Nat) {f g : Nat} (h : f ≤ g) :
    dp f ws ≤ dp g ws := by
  induction g generalizing f ws with
  | zero =>
      have : f = 0 := by omega
      subst f
      exact Nat.le_refl _
  | succ g ih =>
      cases f with
      | zero => exact Nat.zero_le _
      | succ f =>
          by_cases hs : ws.length ≤ 1
          · simp [dp, hs]
          · simp [dp, hs]
            apply minGetD_map_mono
            intro i hi
            exact Nat.add_le_add (ih (ws.take (i + 1)) (by omega))
              (ih (ws.drop (i + 1)) (by omega))

private theorem dp_step (ws : List Nat) (h : 2 ≤ ws.length) :
    dp ws.length ws = ws.sum +
      ((List.range (ws.length - 1)).map (fun i =>
        dp (ws.length - 1) (ws.take (i + 1)) +
        dp (ws.length - 1) (ws.drop (i + 1)))).min?.getD 0 := by
  have hL : ws.length = (ws.length - 1) + 1 := by omega
  rw [hL]
  simp [dp, show ¬ws.length ≤ 1 by omega]

private theorem best_small (ws : List Nat) (h : ws.length ≤ 1) : best ws = 0 := by
  unfold best
  rw [dp_fuel_eq ws ws.length 1 (by omega) h]
  simp [dp, h]

private theorem best_le_split (u v : List Nat) (hu : u ≠ []) (hv : v ≠ []) :
    best (u ++ v) ≤ (u ++ v).sum + best u + best v := by
  have hlen : 2 ≤ (u ++ v).length := by
    simp only [List.length_append]
    have hu1 : 1 ≤ u.length := Nat.succ_le_of_lt (List.length_pos_iff.mpr hu)
    have hv1 : 1 ≤ v.length := Nat.succ_le_of_lt (List.length_pos_iff.mpr hv)
    omega
  let i := u.length - 1
  have hu1 : 1 ≤ u.length := Nat.succ_le_of_lt (List.length_pos_iff.mpr hu)
  have hv1 : 1 ≤ v.length := Nat.succ_le_of_lt (List.length_pos_iff.mpr hv)
  have hi : i ∈ List.range ((u ++ v).length - 1) := by
    simp only [List.mem_range]
    simp [i]
    omega
  have htake : (u ++ v).take (i + 1) = u := by
    simp [i, Nat.sub_add_cancel hu1]
  have hdrop : (u ++ v).drop (i + 1) = v := by
    simp [i, Nat.sub_add_cancel hu1]
  have hle := minGetD_le_of_mem
      ((List.range ((u ++ v).length - 1)).map
        (fun j => dp ((u ++ v).length - 1) ((u ++ v).take (j + 1)) +
          dp ((u ++ v).length - 1) ((u ++ v).drop (j + 1))))
      (dp ((u ++ v).length - 1) u + dp ((u ++ v).length - 1) v)
      (List.mem_map.mpr ⟨i, hi, by simp [htake, hdrop]⟩)
  have hu_fuel : u.length ≤ (u ++ v).length - 1 := by
    simp only [List.length_append]
    omega
  have hv_fuel : v.length ≤ (u ++ v).length - 1 := by
    simp only [List.length_append]
    omega
  have hdp1 : dp ((u ++ v).length - 1) u = dp u.length u := by
    apply dp_fuel_eq
    · exact hu_fuel
    · exact Nat.le_refl _
  have hdp2 : dp ((u ++ v).length - 1) v = dp v.length v := by
    apply dp_fuel_eq
    · exact hv_fuel
    · exact Nat.le_refl _
  unfold best
  rw [dp_step (u ++ v) hlen]
  rw [← hdp1, ← hdp2]
  omega

private theorem best_opt_split (ws : List Nat) (h : 2 ≤ ws.length) :
    ∃ u v : List Nat, u ≠ [] ∧ v ≠ [] ∧ ws = u ++ v ∧
      best ws = ws.sum + best u + best v := by
  let f := fun j => dp (ws.length - 1) (ws.take (j + 1)) +
    dp (ws.length - 1) (ws.drop (j + 1))
  let J := (List.range (ws.length - 1)).map f
  have hJne : J ≠ [] := by
    simp [J]
    omega
  have hJsome : ∃ m, J.min? = some m := by
    cases hs : J.min? with
    | none =>
        have : J = [] := List.min?_eq_none_iff.mp hs
        exact False.elim (hJne this)
    | some m => exact ⟨m, rfl⟩
  obtain ⟨m, hm⟩ := hJsome
  have hm_mem : m ∈ J := List.min?_mem hm
  obtain ⟨i, hi, rfl⟩ := List.mem_map.mp hm_mem
  have hi' : i < ws.length - 1 := by simpa [J] using hi
  have htk_len : (ws.take (i + 1)).length ≤ ws.length - 1 := by
    rw [List.length_take]
    omega
  have hdk_len : (ws.drop (i + 1)).length ≤ ws.length - 1 := by
    rw [List.length_drop]
    omega
  refine ⟨ws.take (i + 1), ws.drop (i + 1), ?_, ?_, (List.take_append_drop (i + 1) ws).symm, ?_⟩
  · apply List.length_pos_iff.mp
    rw [List.length_take]
    omega
  · apply List.length_pos_iff.mp
    rw [List.length_drop]
    omega
  · unfold best
    rw [dp_step ws h]
    change ws.sum + J.min?.getD 0 = ws.sum +
      dp (ws.take (i + 1)).length (ws.take (i + 1)) +
      dp (ws.drop (i + 1)).length (ws.drop (i + 1))
    have htk_eq : dp (ws.length - 1) (ws.take (i + 1)) =
        dp (ws.take (i + 1)).length (ws.take (i + 1)) := by
      apply dp_fuel_eq
      · exact htk_len
      · exact Nat.le_refl _
    have hdk_eq : dp (ws.length - 1) (ws.drop (i + 1)) =
        dp (ws.drop (i + 1)).length (ws.drop (i + 1)) := by
      apply dp_fuel_eq
      · exact hdk_len
      · exact Nat.le_refl _
    have hmin : J.min?.getD 0 = dp (ws.length - 1) (ws.take (i + 1)) +
        dp (ws.length - 1) (ws.drop (i + 1)) := by
      simp [hm, f]
    rw [hmin, htk_eq, hdk_eq]
    omega

/-- `seg xs lo hi` is the half-open interval `[lo, hi)` of `xs`. -/
private def seg (xs : List Nat) (lo hi : Nat) : List Nat := (xs.drop lo).take (hi - lo)
private def cst (xs : List Nat) (lo hi : Nat) : Nat := best (seg xs lo hi)

private theorem seg_append (xs : List Nat) (lo m hi : Nat) (hlm : lo ≤ m) (hmh : m ≤ hi) :
    seg xs lo m ++ seg xs m hi = seg xs lo hi := by
  unfold seg
  rw [show m = lo + (m - lo) by omega]
  rw [← List.drop_drop]
  rw [show lo + (m - lo) - lo = m - lo by omega]
  rw [show hi - (lo + (m - lo)) = hi - m by omega]
  rw [← List.take_add]
  rw [show (m - lo) + (hi - m) = hi - lo by omega]

private theorem seg_length (xs : List Nat) (lo hi : Nat) (hle : hi ≤ xs.length) :
    (seg xs lo hi).length = hi - lo := by
  unfold seg
  rw [List.length_take, List.length_drop]
  rw [Nat.min_eq_left]
  omega

private theorem seg_ne_nil (xs : List Nat) (lo hi : Nat) (hlt : lo < hi) (hle : hi ≤ xs.length) :
    seg xs lo hi ≠ [] := by
  apply List.length_pos_iff.mp
  rw [seg_length xs lo hi hle]
  omega

private theorem seg_sum_append (xs : List Nat) (lo m hi : Nat) (hlm : lo ≤ m) (hmh : m ≤ hi) :
    (seg xs lo hi).sum = (seg xs lo m).sum + (seg xs m hi).sum := by
  rw [← seg_append xs lo m hi hlm hmh, List.sum_append]

private theorem cst_small (xs : List Nat) (lo hi : Nat) (h : hi ≤ lo + 1) :
    cst xs lo hi = 0 := by
  unfold cst
  apply best_small
  unfold seg
  rw [List.length_take]
  omega

private theorem cst_le_split (xs : List Nat) (lo s hi : Nat)
    (hls : lo < s) (hsh : s < hi) (hle : hi ≤ xs.length) :
    cst xs lo hi ≤ (seg xs lo hi).sum + cst xs lo s + cst xs s hi := by
  have hseg := seg_append xs lo s hi (by omega) (by omega)
  have hne1 := seg_ne_nil xs lo s hls (by omega)
  have hne2 := seg_ne_nil xs s hi hsh hle
  unfold cst
  rw [← hseg, List.sum_append]
  simpa [List.sum_append] using best_le_split (seg xs lo s) (seg xs s hi) hne1 hne2

private theorem cst_opt_split (xs : List Nat) (lo hi : Nat) (h : lo + 2 ≤ hi)
    (hle : hi ≤ xs.length) :
    ∃ s, lo < s ∧ s < hi ∧
      cst xs lo hi = (seg xs lo hi).sum + cst xs lo s + cst xs s hi := by
  have hs2 : 2 ≤ (seg xs lo hi).length := by
    rw [seg_length xs lo hi hle]
    omega
  obtain ⟨u, v, hu, hv, huv, heq⟩ := best_opt_split (seg xs lo hi) hs2
  have hulen : u.length + v.length = hi - lo := by
    have := congrArg List.length huv
    rw [List.length_append, seg_length xs lo hi hle] at this
    exact this.symm
  let s := lo + u.length
  have hsl : lo < s := by
    have : 0 < u.length := List.length_pos_iff.mpr hu
    simp [s]
    omega
  have hsh : s < hi := by
    have : 0 < v.length := List.length_pos_iff.mpr hv
    simp [s]
    omega
  have hlen1 : (seg xs lo s).length = u.length := by
    rw [seg_length xs lo s (by omega)]
    simp [s]
  have happ : seg xs lo s ++ seg xs s hi = u ++ v :=
    (seg_append xs lo s hi (by omega) (by omega)).trans huv
  have hseg1 : seg xs lo s = u := by
    have := congrArg (fun l : List Nat => l.take u.length) happ
    simpa [hlen1, List.take_append_of_le_length] using this
  have happ' : u ++ seg xs s hi = u ++ v := by simpa [hseg1] using happ
  have hseg2 : seg xs s hi = v := List.append_cancel_left happ'
  refine ⟨s, hsl, hsh, ?_⟩
  unfold cst
  rw [hseg1, hseg2]
  exact heq

private theorem weight_qi (xs : List Nat) (lo lo' hi hi' : Nat)
    (hll : lo ≤ lo') (hlh : lo' ≤ hi) (hhh : hi ≤ hi') (_hle : hi' ≤ xs.length) :
    (seg xs lo hi).sum + (seg xs lo' hi').sum ≤
      (seg xs lo' hi).sum + (seg xs lo hi').sum := by
  let A := (seg xs lo lo').sum
  let B := (seg xs lo' hi).sum
  let C := (seg xs hi hi').sum
  have h1 : (seg xs lo hi).sum = A + B := by
    exact seg_sum_append xs lo lo' hi hll hlh
  have h2 : (seg xs lo' hi').sum = B + C := by
    exact seg_sum_append xs lo' hi hi' hlh hhh
  have h3 : (seg xs lo hi').sum = A + B + C := by
    rw [seg_sum_append xs lo hi hi' (by omega) hhh,
        seg_sum_append xs lo lo' hi hll hlh]
  rw [h1, h2, h3]
  omega

private theorem weight_mono_right (xs : List Nat) (lo hi hi' : Nat)
    (hlh : lo ≤ hi) (hhh : hi ≤ hi') (_hle : hi' ≤ xs.length) :
    (seg xs lo hi).sum ≤ (seg xs lo hi').sum := by
  rw [seg_sum_append xs lo hi hi' hlh hhh]
  omega

private theorem weight_mono_left (xs : List Nat) (lo lo' hi : Nat)
    (hll : lo ≤ lo') (hlh : lo' ≤ hi) :
    (seg xs lo' hi).sum ≤ (seg xs lo hi).sum := by
  rw [seg_sum_append xs lo lo' hi hll hlh]
  omega

/-- Yao's lemma.  Induct on the length `hi' - lo` of the long side.  The optimal
split of the long interval and of the two shorter intervals are compared; every
induction hypothesis is applied to a strictly shorter long side. -/
private theorem qi_measure (xs : List Nat) (n : Nat) :
    ∀ lo lo' hi hi' : Nat, lo ≤ lo' → lo' < hi → hi ≤ hi' → hi' ≤ xs.length →
      hi' - lo ≤ n →
      cst xs lo hi + cst xs lo' hi' ≤ cst xs lo' hi + cst xs lo hi' := by
  induction n with
  | zero =>
      intro lo lo' hi hi' hll hlt hhh hle hn
      omega
  | succ n ih =>
      intro lo lo' hi hi' hll hlt hhh hle hn
      by_cases hdec : hi' - lo ≤ n
      · exact ih lo lo' hi hi' hll hlt hhh hle hdec
      · have hmeas : hi' - lo = n + 1 := by omega
        by_cases hlo_eq : lo = lo'
        · subst lo'
          omega
        have hlo_lt : lo < lo' := by omega
        by_cases hA : lo' + 2 ≤ hi
        · -- Case B: the middle interval has at least two elements.
          obtain ⟨y, hly, hyh, hy_eq⟩ := cst_opt_split xs lo' hi hA (by omega)
          have hlo_hi' : lo + 2 ≤ hi' := by omega
          obtain ⟨z, hlz, hzh', hz_eq⟩ := cst_opt_split xs lo hi' hlo_hi' hle
          by_cases hzy : z < y
          · have hub1 := cst_le_split xs lo z hi (by omega) (by omega) (by omega)
            have hub2 := cst_le_split xs lo' y hi' (by omega) (by omega) hle
            have hmeas2 : hi' - z ≤ n := by omega
            have hih := ih z y hi hi' (by omega) (by omega) (by omega) hle hmeas2
            have hw := weight_qi xs lo lo' hi hi' hll (by omega) hhh hle
            omega
          · have hyz : y ≤ z := by omega
            have hub1 := cst_le_split xs lo y hi (by omega) (by omega) (by omega)
            have hub2 := cst_le_split xs lo' z hi' (by omega) (by omega) hle
            have hmeas2 : z - lo ≤ n := by omega
            have hih := ih lo lo' y z hll (by omega) (by omega) (by omega) hmeas2
            have hw := weight_qi xs lo lo' hi hi' hll (by omega) hhh hle
            omega
        · -- Case A: the middle interval is a single element, so hi = lo' + 1.
          have hi_succ : hi = lo' + 1 := by omega
          have hcst_lo_hi : cst xs lo' hi = 0 := cst_small xs lo' hi (by omega)
          have hlo_hi' : lo + 2 ≤ hi' := by omega
          obtain ⟨z, hlz, hzh', hz_eq⟩ := cst_opt_split xs lo hi' hlo_hi' hle
          by_cases hzle : z ≤ lo'
          · by_cases hzeq : z = lo'
            · subst z
              have hub := cst_le_split xs lo lo' hi hlo_lt (by omega) (by omega)
              have hw := weight_mono_right xs lo hi hi' (by omega) (by omega) hle
              omega
            · have hzlt : z < lo' := by omega
              have hub := cst_le_split xs lo z hi (by omega) (by omega) (by omega)
              have hmeas2 : hi' - z ≤ n := by omega
              have hih := ih z lo' hi hi' (by omega) hlt (by omega) hle hmeas2
              have hw := weight_mono_right xs lo hi hi' (by omega) (by omega) hle
              omega
          · have hzge : hi ≤ z := by omega
            have hub2 := cst_le_split xs lo' z hi' (by omega) (by omega) hle
            by_cases hzeq : z = hi
            · subst z
              have hw := weight_mono_left xs lo lo' hi' (by omega) (by omega)
              omega
            · have hzgt : hi < z := by omega
              have hmeas2 : z - lo ≤ n := by omega
              have hih := ih lo lo' hi z hll (by omega) (by omega) (by omega) hmeas2
              have hw := weight_mono_left xs lo lo' hi' (by omega) (by omega)
              omega

theorem sound_Quadrangle : AlphabeticMerge.Quadrangle.Goal := by
  intro a b c hb
  by_cases ha : a = []
  · subst a
    simp
    omega
  by_cases hc : c = []
  · subst c
    simp
  let xs := a ++ b ++ c
  let lo := 0
  let lo' := a.length
  let hi := a.length + b.length
  let hi' := xs.length
  have hll : lo ≤ lo' := by simp [lo, lo']
  have hlt : lo' < hi := by
    have hbpos : 0 < b.length := List.length_pos_iff.mpr hb
    simp [lo', hi]
    omega
  have hhh : hi ≤ hi' := by
    simp [hi, hi', xs]
  have hle : hi' ≤ xs.length := by omega
  have hq := qi_measure xs (hi' - lo) lo lo' hi hi' hll hlt hhh hle (by omega)
  have hs1 : seg xs 0 hi = a ++ b := by
    unfold seg
    simp only [List.drop_zero, Nat.sub_zero]
    dsimp [xs, hi]
    change List.take (a.length + b.length) (a ++ b ++ c) = a ++ b
    rw [List.take_add]
    have hdrop : List.drop a.length (a ++ b ++ c) = b ++ c := by
      simp
    rw [hdrop]
    have htakea : List.take a.length (a ++ b ++ c) = a := by simp
    have htakeb : List.take b.length (b ++ c) = b := by simp
    rw [htakea, htakeb]
  have hs2 : seg xs lo' hi = b := by
    unfold seg
    have hsub : hi - lo' = b.length := by simp [hi, lo']
    rw [hsub]
    have hdrop : List.drop lo' xs = b ++ c := by simp [xs, lo']
    rw [hdrop]
    simp
  have hs3 : seg xs lo' hi' = b ++ c := by
    unfold seg
    have hsub : hi' - lo' = (b ++ c).length := by
      simp [hi', lo', xs]
      omega
    rw [hsub]
    have hdrop : List.drop lo' xs = b ++ c := by simp [xs, lo']
    rw [hdrop]
    exact List.take_length
  have hs4 : seg xs 0 hi' = a ++ b ++ c := by
    unfold seg
    simp only [List.drop_zero, Nat.sub_zero]
    change List.take xs.length xs = xs
    exact List.take_length
  unfold cst at hq
  rw [hs1, hs2, hs3, hs4] at hq
  omega

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_Quadrangle
