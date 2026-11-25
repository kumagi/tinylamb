import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_FlushDebt

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Exchanges

private theorem packets_zero (cap : Nat) (hc : 0 < cap) : packets cap 0 = 0 := by
  unfold packets
  rw [show 0 + cap - 1 = cap - 1 by omega]
  exact Nat.div_eq_of_lt (by omega)

private theorem packets_add_le_plus_one (cap n m : Nat) (hc : 0 < cap) :
    packets cap n + packets cap m ≤ packets cap (n + m) + 1 := by
  unfold packets
  let A := n + cap - 1
  let B := m + cap - 1
  let X := n + m + cap - 1
  change A / cap + B / cap ≤ X / cap + 1
  have hAB : A + B ≤ X + cap := by omega
  have hdiv : (A + B) / cap ≤ (X + cap) / cap := Nat.div_le_div_right hAB
  have hXcap : (X + cap) / cap = X / cap + 1 := by
    simpa using (Nat.add_mul_div_left X 1 hc : (X + cap * 1) / cap = X / cap + 1)
  rw [hXcap] at hdiv
  have hsum : A / cap + B / cap ≤ (A + B) / cap := Nat.div_add_div_le_add_div
  omega

private theorem volume_append (a b : List Bucket) (d : Nat) :
    volume (a ++ b) d = volume a d + volume b d := by
  simp [volume, List.filter_append, List.map_append, List.sum_append]

private theorem volume_zero_of_not_mem {xs : List Bucket} {d : Nat}
    (h : d ∉ xs.map Prod.fst) : volume xs d = 0 := by
  induction xs with
  | nil => simp [volume]
  | cons p ps ih =>
      simp only [List.map_cons, List.mem_cons] at h
      have hd : d ≠ p.1 := by intro he; exact h (Or.inl he)
      have hrs : d ∉ ps.map Prod.fst := by intro hk; exact h (Or.inr hk)
      have ih' := ih hrs
      have hp : p.1 ≠ d := by intro h'; exact hd h'.symm
      have hbeq : (p.1 == d) = false := by
        apply Bool.eq_false_of_ne_true
        intro htrue
        exact hp (beq_iff_eq.mp htrue)
      simpa [volume, hbeq] using ih'

private theorem nodup_eraseDups_aux (n : Nat) : ∀ l : List Nat, l.length = n → l.eraseDups.Nodup := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro l hlen
      cases l with
      | nil => simp
      | cons a as =>
          rw [List.eraseDups_cons, List.nodup_cons]
          constructor
          · intro hm
            have hmem : a ∈ as.filter (fun b => !b == a) := List.mem_eraseDups.mp hm
            have hbad := (List.mem_filter.mp hmem).2
            simp at hbad
          · have hle := List.length_filter_le (fun b => !b == a) as
            simp at hlen
            have hlt : (as.filter (fun b => !b == a)).length < n := by omega
            exact ih (as.filter (fun b => !b == a)).length hlt
              (as.filter (fun b => !b == a)) rfl

private theorem nodup_eraseDups (l : List Nat) : l.eraseDups.Nodup :=
  nodup_eraseDups_aux l.length l rfl

private theorem sum_le_sum_of_mem {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.add_le_add (h x (by simp))
        (ih (by intro y hy; exact h y (by simp [hy])))

private theorem sum_add_bool_one {α : Type} (l : List α) (p : α → Bool) (f : α → Nat) :
    (l.map (fun x => f x + if p x then 1 else 0)).sum =
      (l.map f).sum + (l.filter p).length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : p x <;> simp [hx, ih] <;> omega

private theorem sum_add_bool_fun {α : Type} (l : List α) (p : α → Bool)
    (f g : α → Nat) :
    (l.map (fun x => f x + if p x then g x else 0)).sum =
      (l.map f).sum + ((l.filter p).map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : p x <;> simp [hx, ih] <;> omega

theorem sound_Exchanges_FlushDebt : FlushDebt.Goal := by
  intro cap a b hc
  let ad := a.map Prod.fst
  let bd := b.map Prod.fst
  let DA := ad.eraseDups
  let DB := bd.eraseDups
  let R := bd.removeAll ad
  let fa : Nat → Nat := fun d => packets cap (volume a d)
  let fb : Nat → Nat := fun d => packets cap (volume b d)
  let fab : Nat → Nat := fun d => packets cap (volume (a ++ b) d)
  have htraffic_a : traffic cap a = (DA.map fa).sum := by rfl
  have htraffic_b : traffic cap b = (DB.map fb).sum := by rfl
  have htraffic_ab : traffic cap (a ++ b) =
      (DA.map fab).sum + (R.eraseDups.map fab).sum := by
    unfold traffic
    have hmap : (a ++ b).map Prod.fst = ad ++ bd := by simp [ad, bd]
    rw [hmap, List.eraseDups_append]
    change ((DA ++ R.eraseDups).map fun d => packets cap (volume (a ++ b) d)).sum =
      ((DA.map fun d => packets cap (volume (a ++ b) d)).sum +
       (R.eraseDups.map fun d => packets cap (volume (a ++ b) d)).sum)
    simp [List.map_append, List.sum_append]
  have hfab_rest : (R.eraseDups.map fab).sum = (R.eraseDups.map fb).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro d hd
    have hdR : d ∈ R := List.mem_eraseDups.mp hd
    have hnot : d ∉ ad := by
      have hh : d ∈ bd ∧ d ∉ ad := by simpa [R, List.removeAll] using hdR
      exact hh.2
    have hva : volume a d = 0 := volume_zero_of_not_mem hnot
    change packets cap (volume (a ++ b) d) = packets cap (volume b d)
    rw [volume_append, hva]
    simp
  have hnodup_right : ((DA.filter (fun d => DB.contains d)) ++ R.eraseDups).Nodup := by
    rw [List.nodup_append]
    constructor
    · simpa [List.nodup_iff_pairwise_ne] using
        (List.Pairwise.filter (fun d => DB.contains d) (nodup_eraseDups ad))
    constructor
    · exact nodup_eraseDups R
    · intro x hx y hy hxy
      have hxDA : x ∈ DA := (List.mem_filter.mp hx).1
      have had : x ∈ ad := by simpa [DA] using (List.mem_eraseDups.mpr hxDA)
      have hyR : y ∈ R := List.mem_eraseDups.mp hy
      have hynot : y ∉ ad := by
        have hh : y ∈ bd ∧ y ∉ ad := by simpa [R, List.removeAll] using hyR
        exact hh.2
      subst y
      exact hynot had
  have hDBperm : DB.Perm ((DA.filter (fun d => DB.contains d)) ++ R.eraseDups) := by
    rw [List.perm_ext_iff_of_nodup (nodup_eraseDups bd) hnodup_right]
    intro d
    constructor
    · intro hdDB
      have hdbd : d ∈ bd := List.mem_eraseDups.mp hdDB
      by_cases had : d ∈ ad
      · exact List.mem_append.mpr (Or.inl
          (List.mem_filter.mpr ⟨by simpa [DA] using (List.mem_eraseDups.mpr had),
            by simpa [DB] using hdDB⟩))
      · exact List.mem_append.mpr (Or.inr (by
          apply List.mem_eraseDups.mpr
          have hh : d ∈ bd ∧ d ∉ ad := ⟨hdbd, had⟩
          simpa [R, List.removeAll] using hh))
    · intro h
      rcases List.mem_append.mp h with hleft | hright
      · exact (by simpa [DB] using (List.mem_filter.mp hleft).2)
      · have hdR : d ∈ R := List.mem_eraseDups.mp hright
        have hh : d ∈ bd ∧ d ∉ ad := by simpa [R, List.removeAll] using hdR
        exact List.mem_eraseDups.mpr hh.1
  have hcontains : ∀ d : Nat, (bd.contains d) = (DB.contains d) := by
    intro d
    by_cases h : d ∈ bd
    · have hDB : d ∈ DB := by simpa [DB] using h
      simp [h, hDB]
    · have hDB : d ∉ DB := by intro hm; exact h (List.mem_eraseDups.mp hm)
      simp [h, hDB]
  have hcommon : (a.map Prod.fst).eraseDups.countP (fun d => (b.map Prod.fst).contains d) =
      (DA.filter (fun d => DB.contains d)).length := by
    dsimp [DA, ad, bd]
    rw [List.countP_eq_length_filter]
    apply congrArg List.length
    apply List.filter_congr
    intro d hd
    exact hcontains d
  have hpoint : ∀ d ∈ DA,
      fa d + (if DB.contains d then fb d else 0) ≤
        fab d + (if DB.contains d then 1 else 0) := by
    intro d hdDA
    have hvab : volume (a ++ b) d = volume a d + volume b d := volume_append a b d
    by_cases hDB : (DB.contains d) = true
    · have hmemDB : d ∈ DB := by simpa using hDB
      dsimp [fa, fb, fab]
      rw [hvab]
      simpa [hmemDB] using packets_add_le_plus_one cap (volume a d) (volume b d) hc
    · have hnotbd : d ∉ bd := by
        intro hbdmem
        apply hDB
        simpa [DB] using (List.mem_eraseDups.mpr hbdmem)
      have hvzero : volume b d = 0 := volume_zero_of_not_mem hnotbd
      dsimp [fa, fb, fab]
      rw [hvab, hvzero]
      simp [packets_zero cap hc]
  have hsumDA : (DA.map fa).sum + ((DA.filter (fun d => DB.contains d)).map fb).sum ≤
      (DA.map fab).sum + (DA.filter (fun d => DB.contains d)).length := by
    rw [← sum_add_bool_fun DA (fun d => DB.contains d) fa fb]
    rw [← sum_add_bool_one DA (fun d => DB.contains d) fab]
    exact sum_le_sum_of_mem DA _ _ hpoint
  have htraffic_b_split : traffic cap b =
      ((DA.filter (fun d => DB.contains d)).map fb).sum + (R.eraseDups.map fb).sum := by
    rw [htraffic_b]
    have hp := (hDBperm.map fb).sum_nat
    rw [hp]
    simp [List.map_append, List.sum_append]
  have hmain : traffic cap a + traffic cap b ≤
      traffic cap (a ++ b) + (DA.filter (fun d => DB.contains d)).length := by
    rw [htraffic_a, htraffic_b_split, htraffic_ab, hfab_rest]
    omega
  rw [hcommon]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_Exchanges_FlushDebt
