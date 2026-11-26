import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `PatchedOctet.BoundedRaw` — A forced frame cap still limits worst-case expansion to
  raw cells and per-frame overhead.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_BoundedRaw

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.PatchedOctet

/-! Local copies of the wire-size lemmas needed for the ULEB charge. -/

private theorem width_eq (n : Nat) (hn : n ≠ 0) : width n = n.log2 + 1 := by
  unfold width
  simp [hn]

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst hn
    simp [width]
  · rw [width_eq n hn]
    exact Nat.lt_log2_self

private theorem width_le_iff (n W : Nat) (hn : n ≠ 0) : width n ≤ W ↔ n < 2 ^ W := by
  rw [width_eq n hn]
  constructor
  · intro h
    exact (Nat.log2_lt hn).mp (by omega)
  · intro h
    have hh := (Nat.log2_lt hn).mpr h
    omega

private theorem width_mono {a b : Nat} (hab : a ≤ b) : width a ≤ width b := by
  by_cases ha : a = 0
  · subst a
    simp [width]
  · have hlt : a < 2 ^ width b := Nat.lt_of_le_of_lt hab (lt_two_pow_width b)
    exact (width_le_iff a (width b) ha).mpr hlt

private theorem ubytes_le_iff (n k : Nat) (hk : 0 < k) : ubytes n ≤ k ↔ n < 128 ^ k := by
  unfold ubytes
  constructor
  · intro h
    have hw7 : (width n + 6) / 7 ≤ k := (Nat.max_le.mp h).2
    have hw : width n ≤ 7 * k := by
      have hh := (Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)).mp hw7
      omega
    calc n < 2 ^ width n := lt_two_pow_width n
      _ ≤ 2 ^ (7 * k) := Nat.pow_le_pow_right (by decide : 0 < 2) hw
      _ = 128 ^ k := by
        have h128 : (128 : Nat) = 2 ^ 7 := by decide
        rw [h128, ← Nat.pow_mul]
  · intro h
    apply Nat.max_le.mpr
    constructor
    · omega
    · have hw : width n ≤ 7 * k := by
        by_cases hn : n = 0
        · subst hn
          simp [width]
        · have hlt : n < 2 ^ (7 * k) := by
            have h128 : (128 : Nat) = 2 ^ 7 := by decide
            rw [h128, ← Nat.pow_mul] at h
            exact h
          exact (width_le_iff n (7 * k) hn).mpr hlt
      rw [Nat.div_le_iff_le_mul_add_pred (by decide : 0 < 7)]
      omega

private theorem ubytes_mono {a b : Nat} (hab : a ≤ b) : ubytes a ≤ ubytes b := by
  unfold ubytes
  apply Nat.max_le.mpr
  constructor
  · exact Nat.le_max_left 1 ((width b + 6) / 7)
  · exact Nat.le_trans (Nat.div_le_div_right (Nat.add_le_add_right (width_mono hab) 6))
      (Nat.le_max_right 1 ((width b + 6) / 7))

private theorem uleb_len_pos (n : Nat) : 0 < (uleb n).length := by
  unfold uleb
  split <;> simp

private theorem uleb_len_succ (n : Nat) (h : ¬ n < 128) :
    (uleb n).length = 1 + (uleb (n / 128)).length := by
  rw [uleb]
  simp [h]
  omega

private theorem uleb_len_le_one (n : Nat) : (uleb n).length ≤ 1 ↔ n < 128 := by
  by_cases hn : n < 128
  · have h1 : uleb n = [n] := by rw [uleb]; simp [hn]
    rw [h1]
    simp [hn]
  · have hlen : 2 ≤ (uleb n).length := by
      rw [uleb_len_succ n hn]
      have := uleb_len_pos (n / 128)
      omega
    constructor
    · intro h; omega
    · intro h; omega

private theorem uleb_len_le_succ (n k : Nat) :
    (uleb n).length ≤ k + 1 ↔ n < 128 ^ (k + 1) := by
  induction k generalizing n with
  | zero =>
      simpa using uleb_len_le_one n
  | succ k ih =>
      by_cases hn : n < 128
      · have h1 : uleb n = [n] := by rw [uleb]; simp [hn]
        have hlen1 : (uleb n).length = 1 := by rw [h1]; simp
        constructor
        · intro _
          calc n < 128 := hn
            _ ≤ 128 ^ (k + 1 + 1) := by
              have hk1 : 1 ≤ k + 1 + 1 := by omega
              simpa using Nat.pow_le_pow_right (by decide : 0 < 128) hk1
        · intro _
          rw [hlen1]
          omega
      · rw [uleb_len_succ n hn]
        have hih := ih (n / 128)
        constructor
        · intro h
          have hd : (uleb (n / 128)).length ≤ k + 1 := by omega
          have hdiv : n / 128 < 128 ^ (k + 1) := hih.mp hd
          rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)] at hdiv
          calc n < 128 ^ (k + 1) * 128 := hdiv
            _ = 128 ^ (k + 1 + 1) := (Nat.pow_succ 128 (k + 1)).symm
        · intro h
          have hdiv : n / 128 < 128 ^ (k + 1) := by
            rw [Nat.div_lt_iff_lt_mul (by decide : 0 < 128)]
            calc n < 128 ^ (k + 1 + 1) := h
              _ = 128 ^ (k + 1) * 128 := by rw [Nat.pow_succ]
          have hd := hih.mpr hdiv
          omega

private theorem uleb_len_le_ubytes (n : Nat) : (uleb n).length ≤ ubytes n := by
  have hubpos : 0 < ubytes n := by unfold ubytes; omega
  obtain ⟨j, hj⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hubpos)
  rw [hj]
  exact (uleb_len_le_succ n j).mpr ((ubytes_le_iff n (j + 1) (by omega)).mp (by omega))

/-! A partition of `xs` into few pages of at most `cap` rows exists in `parts`. -/

private theorem parts_sum_length (xs : List Nat) : ∀ ps, ps ∈ parts xs → xs.length = (ps.map List.length).sum := by
  induction xs with
  | nil =>
      intro ps h
      simp [parts] at h
      subst ps
      simp
  | cons x xs ih =>
      intro ps h
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨qs, hqs, hmem⟩
      have hsum := ih qs hqs
      cases qs with
      | nil =>
          simp at hmem
          subst ps
          simp [hsum]
      | cons a rest =>
          simp at hmem
          rcases hmem with hps | hps
          · subst ps
            simp [hsum] <;> omega
          · subst ps
            simp [hsum] <;> omega

private theorem mod_succ_of_lt (n cap : Nat) (hcap : 0 < cap) (hnpos : 0 < n)
    (h : (n - 1) % cap + 1 < cap) : n % cap = (n - 1) % cap + 1 := by
  have hn : n - 1 + 1 = n := Nat.sub_add_cancel (by omega)
  have hc1 : 1 < cap := by omega
  calc
    n % cap = ((n - 1) + 1) % cap := by rw [hn]
    _ = ((n - 1) % cap + 1 % cap) % cap := Nat.add_mod _ _ _
    _ = ((n - 1) % cap + 1) % cap := by rw [Nat.mod_eq_of_lt hc1]
    _ = (n - 1) % cap + 1 := Nat.mod_eq_of_lt h

private theorem mod_zero_of_pred (n cap : Nat) (hcap : 0 < cap) (hn : 0 < n)
    (h : (n - 1) % cap + 1 = cap) : n % cap = 0 := by
  by_cases hc1 : cap = 1
  · subst cap
    exact Nat.mod_one n
  · have hc1' : 1 < cap := by omega
    have hn' : n - 1 + 1 = n := Nat.sub_add_cancel (by omega)
    have hp : (n - 1) % cap = cap - 1 := by omega
    calc
      n % cap = ((n - 1) + 1) % cap := by rw [hn']
      _ = ((n - 1) % cap + 1 % cap) % cap := Nat.add_mod _ _ _
      _ = ((cap - 1) + 1) % cap := by rw [hp, Nat.mod_eq_of_lt hc1']
      _ = cap % cap := by rw [Nat.sub_add_cancel (by omega)]
      _ = 0 := Nat.mod_self cap

private theorem pageLimit_succ_of_mod (n cap : Nat) (hcap : 0 < cap) (hmod : n % cap = 0) :
    pageLimit (n + 1) cap = pageLimit n cap + 1 := by
  have hdiv : cap * (n / cap) = n := by
    have h := Nat.div_add_mod n cap
    omega
  have hdiv' : (n / cap) * cap = n := by rw [Nat.mul_comm, hdiv]
  have h1 : (n + cap - 1) / cap = n / cap := by
    apply Nat.div_eq_of_lt_le
    · omega
    · rw [Nat.succ_mul, hdiv']
      omega
  have h2 : (n + 1 + cap - 1) / cap = n / cap + 1 := by
    apply Nat.div_eq_of_lt_le
    · rw [Nat.succ_mul, hdiv']
      omega
    · rw [Nat.succ_mul, Nat.succ_mul, hdiv']
      omega
  unfold pageLimit
  rw [h1, h2]

private theorem exists_good (cap : Nat) (hcap : 0 < cap) :
    ∀ xs, ∃ ps ∈ parts xs,
      (∀ p ∈ ps, p.length ≤ cap) ∧
      ps.length ≤ pageLimit xs.length cap ∧
      (xs ≠ [] → (ps.head?.getD []).length = (xs.length - 1) % cap + 1) := by
  intro xs
  induction xs with
  | nil =>
      refine ⟨[], ?_, ?_, ?_, ?_⟩
      · simp [parts]
      · simp
      · simp [pageLimit]
      · intro h
        contradiction
  | cons x xs ih =>
      by_cases hxs : xs = []
      · subst xs
        refine ⟨[[x]], ?_, ?_, ?_, ?_⟩
        · simp [parts]
        · intro p hp
          simp only [List.mem_singleton] at hp
          subst p
          exact hcap
        · simp [pageLimit, Nat.div_self hcap]
        · intro _
          simp
      · rcases ih with ⟨ps, hpsmem, hpsle, hpscnt, hpshead⟩
        have hhead := hpshead hxs
        cases ps with
        | nil =>
            have hsum := parts_sum_length xs [] hpsmem
            have hsum0 : xs.length = 0 := by simpa using hsum
            have hxn : 0 < xs.length := List.length_pos_iff.mpr hxs
            exfalso
            omega
        | cons a rest =>
            simp only [List.length_cons] at hpscnt
            have hxn : 0 < xs.length := List.length_pos_iff.mpr hxs
            have hflen : a.length = (xs.length - 1) % cap + 1 := by simpa using hhead
            by_cases hfull : a.length = cap
            · have hmod : xs.length % cap = 0 :=
                mod_zero_of_pred xs.length cap hcap hxn (by omega)
              have hstep := pageLimit_succ_of_mod xs.length cap hcap hmod
              refine ⟨[x] :: a :: rest, ?_, ?_, ?_, ?_⟩
              · simp only [parts, List.mem_flatMap]
                refine ⟨a :: rest, hpsmem, ?_⟩
                simp
              · intro p hp
                simp only [List.mem_cons] at hp
                rcases hp with rfl | hp
                · exact hcap
                · exact hpsle p (by simp [hp])
              · simp only [List.length_cons]
                omega
              · intro _
                simp [hmod]
            · have hale : a.length ≤ cap := hpsle a (by simp)
              have hlt : a.length < cap := by omega
              refine ⟨(x :: a) :: rest, ?_, ?_, ?_, ?_⟩
              · simp only [parts, List.mem_flatMap]
                refine ⟨a :: rest, hpsmem, ?_⟩
                simp
              · intro p hp
                simp only [List.mem_cons] at hp
                rcases hp with rfl | hp
                · simp only [List.length_cons]
                  omega
                · exact hpsle p (by simp [hp])
              · simp only [List.length_cons]
                have hmono : pageLimit xs.length cap ≤ pageLimit (xs.length + 1) cap := by
                  unfold pageLimit
                  exact Nat.div_le_div_right (by omega)
                omega
              · intro _
                simp
                have hmod := mod_succ_of_lt xs.length cap hcap hxn (by omega)
                omega

/-! The byte charge of a page is at most its raw length. -/

private theorem little_len (n k : Nat) : (little n k).length = k := by
  simp [little]

private theorem fixed_len (p : List Nat) : (fixed p).length = 4 * p.length := by
  induction p with
  | nil => simp [fixed]
  | cons x xs ih =>
      rw [show fixed (x::xs) = little x 4 ++ fixed xs by rfl]
      simp [little_len, ih, Nat.succ_mul]
      omega

private theorem chosen_le_raw (f : List Nat → Bytes) (p : List Nat) :
    chosen f p ≤ 1 + (uleb p.length).length + 4 * p.length := by
  unfold chosen
  apply Nat.le_trans (Nat.min_le_right _ _)
  simp [raw, fixed_len]
  omega

private theorem charge_le_bound (ps : List (List Nat)) (cap : Nat) :
    (∀ p ∈ ps, p.length ≤ cap) →
    charge write 0 ps ≤ (ps.map List.length).sum * 4 + ps.length * (1 + ubytes cap) := by
  unfold charge
  induction ps with
  | nil => simp
  | cons p ps ih =>
      intro hall
      have hp : p.length ≤ cap := hall p (by simp)
      have htail : ∀ q ∈ ps, q.length ≤ cap := by
        intro q hq
        exact hall q (by simp [hq])
      have hi := ih htail
      have hc := chosen_le_raw write p
      have huleb : (uleb p.length).length ≤ ubytes cap :=
        Nat.le_trans (uleb_len_le_ubytes p.length) (ubytes_mono hp)
      simp only [List.map_cons, List.sum_cons, List.length_cons, Nat.zero_add] at hi ⊢
      have hmul1 : (p.length + (ps.map List.length).sum) * 4 =
          4 * p.length + (ps.map List.length).sum * 4 := by omega
      have hmul2 : (ps.length + 1) * (1 + ubytes cap) =
          ps.length * (1 + ubytes cap) + (1 + ubytes cap) := by
        rw [Nat.add_mul]
        simp
      rw [hmul1, hmul2]
      omega

theorem sound_BoundedRaw : PatchedOctet.BoundedRaw.Goal := by
  intro xs cap s hcap hb
  rcases exists_good cap hcap xs with ⟨ps, hpsmem, hpsle, hpscnt, _⟩
  have hfilter : ps ∈ (parts xs).filter (fun qs => qs.all (fun p => p.length ≤ cap)) := by
    simp only [List.mem_filter]
    refine ⟨hpsmem, ?_⟩
    exact List.all_eq_true.mpr (by intro p hp; simpa using (hpsle p hp))
  have hs_le : s ≤ charge write 0 ps := by
    unfold budget at hb
    have hmin := (List.min?_eq_some_iff.mp hb).2
    exact hmin (charge write 0 ps) (List.mem_map.mpr ⟨ps, hfilter, rfl⟩)
  have hsum : xs.length = (ps.map List.length).sum := parts_sum_length xs ps hpsmem
  have hcharge := charge_le_bound ps cap hpsle
  rw [← hsum] at hcharge
  have hbound : charge write 0 ps ≤ xs.length * 4 + pageLimit xs.length cap * (1 + ubytes cap) := by
    calc
      charge write 0 ps ≤ xs.length * 4 + ps.length * (1 + ubytes cap) := hcharge
      _ ≤ xs.length * 4 + pageLimit xs.length cap * (1 + ubytes cap) := by
        exact Nat.add_le_add_left (Nat.mul_le_mul_right _ hpscnt) _
  exact Nat.le_trans hs_le hbound

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_BoundedRaw
