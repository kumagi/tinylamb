import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SparsePlanes_BoundedRaw

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SparsePlanes

-- 真偽確認（小さい入力）:
--   budget write [] 1 = some 0, 右辺 = 0 なので空入力は反例にならない。
#eval budget write ([] : List Nat) 1
#eval ([] : List Nat).length * 4 + pageLimit 0 1 * (1 + ubytes 1)

private def gen (k n : Nat) : List (List Nat) :=
  match n with
  | 0 => [[]]
  | n + 1 => (gen k n).flatMap (fun xs => (List.range k).map (fun x => x::xs))

-- 長さ 3 以下・値 0..2・cap 1..4 の全入力で予算が上限を超えないことを確認:
#eval (gen 3 4).flatMap (fun xs =>
  (List.range 4).filterMap (fun c =>
    let cap := c + 1
    match budget write xs cap with
    | some s =>
        let rhs := xs.length*4 + pageLimit xs.length cap*(1+ubytes cap)
        if s > rhs then some (xs, cap, s, rhs) else none
    | none => none))

/-! ## Wire の長さ算術 -/

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
    · intro h
      omega
    · intro h
      omega

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

private theorem uleb_len_le_iff (n k : Nat) (hk : 0 < k) :
    (uleb n).length ≤ k ↔ n < 128 ^ k := by
  obtain ⟨j, rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hk)
  exact uleb_len_le_succ n j

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

private theorem ubytes_pos (n : Nat) : 0 < ubytes n := Nat.le_max_left _ _

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

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n := by
  have hlenpos : 0 < (uleb n).length := uleb_len_pos n
  have hubpos : 0 < ubytes n := ubytes_pos n
  apply Nat.le_antisymm
  · exact (uleb_len_le_iff n (ubytes n) hubpos).mpr
      ((ubytes_le_iff n (ubytes n) hubpos).mp (Nat.le_refl _))
  · exact (ubytes_le_iff n (uleb n).length hlenpos).mpr
      ((uleb_len_le_iff n (uleb n).length hlenpos).mp (Nat.le_refl _))

private theorem ubytes_lt_pow (b : Nat) : b < 128 ^ ubytes b :=
  (ubytes_le_iff b (ubytes b) (ubytes_pos b)).mp (Nat.le_refl _)

private theorem ubytes_mono {a b : Nat} (hab : a ≤ b) : ubytes a ≤ ubytes b :=
  (ubytes_le_iff a (ubytes b) (ubytes_pos b)).mpr
    (Nat.lt_of_le_of_lt hab (ubytes_lt_pow b))

private theorem sum_four (xs : List Nat) : (xs.map (fun _ => 4)).sum = 4 * xs.length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp [ih, Nat.mul_add, Nat.add_comm]

private theorem raw_len (xs : List Nat) : (raw xs).length = 1 + ubytes xs.length + 4*xs.length := by
  simp [raw, fixed, little, uleb_len_eq_ubytes, List.length_flatMap, sum_four]
  omega

private theorem chosen_le_raw_bound (f : Writer) (cap : Nat) {p : List Nat} (hple : p.length ≤ cap) :
    chosen f p ≤ 1 + ubytes cap + 4*p.length := by
  unfold chosen
  rw [raw_len]
  have h1 : min (frame f p).length (raw p).length ≤ (raw p).length := Nat.min_le_right _ _
  rw [raw_len] at h1
  have h2 := ubytes_mono hple
  have hA : 1 + ubytes p.length + 4*p.length ≤ 1 + ubytes cap + 4*p.length := by omega
  exact Nat.le_trans h1 hA

/-! ## pageLimit の再帰式 -/

private theorem pageLimit_succ (n cap : Nat) (hcap : 0 < cap) (hn : 0 < n) :
    pageLimit n cap = 1 + pageLimit (n - cap) cap := by
  unfold pageLimit
  by_cases hle : cap ≤ n
  · calc
      (n + cap - 1) / cap = ((n - cap + cap - 1) + cap) / cap := by
        congr 1
        omega
      _ = (n - cap + cap - 1) / cap + 1 := by
        simpa [Nat.mul_one] using (Nat.add_mul_div_left (n - cap + cap - 1) 1 hcap)
      _ = 1 + (n - cap + cap - 1) / cap := by omega
  · have hnlt : n < cap := by omega
    have hleft : (n + cap - 1) / cap = 1 := by
      have hrewrite : n + cap - 1 = (n - 1) + cap := by omega
      rw [hrewrite]
      rw [show ((n - 1) + cap) / cap = (n - 1) / cap + 1 by
        simpa [Nat.mul_one] using (Nat.add_mul_div_left (n - 1) 1 hcap)]
      have hzero : (n - 1) / cap = 0 := Nat.div_eq_of_lt (by omega)
      omega
    have hright : (n - cap + cap - 1) / cap = 0 := by
      have hz : n - cap = 0 := by omega
      rw [hz]
      simpa using Nat.div_eq_of_lt (Nat.sub_lt hcap (by decide : 0 < 1))
    omega

/-! ## 区分分割の構成 -/

private def goChunks (cap : Nat) (hcap : 0 < cap) : List Nat → List (List Nat)
  | [] => []
  | x :: xs => (x :: xs).take cap :: goChunks cap hcap ((x :: xs).drop cap)
termination_by xs => xs.length
decreasing_by
  rw [List.length_drop]
  have : 0 < (x :: xs).length := by simp
  omega

private theorem drop_lt_self {cap : Nat} (hcap : 0 < cap) (x : Nat) (xs : List Nat) :
    ((x :: xs).drop cap).length < (x :: xs).length := by
  simp only [List.length_drop, List.length_cons]
  exact Nat.sub_lt (by omega : 0 < xs.length + 1) hcap

private theorem goChunks_props (cap : Nat) (hcap : 0 < cap) :
    ∀ xs : List Nat,
      (goChunks cap hcap xs).flatten = xs ∧
      (goChunks cap hcap xs).length = pageLimit xs.length cap ∧
      (∀ p ∈ goChunks cap hcap xs, p.length ≤ cap) := by
  intro xs
  refine (Nat.strongRecOn xs.length (motive := fun n => ∀ xs : List Nat, xs.length = n →
      (goChunks cap hcap xs).flatten = xs ∧
      (goChunks cap hcap xs).length = pageLimit xs.length cap ∧
      (∀ p ∈ goChunks cap hcap xs, p.length ≤ cap)) ?_) xs rfl
  intro n ih xs hlen
  subst n
  cases xs with
  | nil =>
      have hz : (cap - 1) / cap = 0 := Nat.div_eq_of_lt (Nat.sub_lt hcap (by decide : 0 < 1))
      simp [goChunks, pageLimit, hz]
  | cons x xs' =>
    have hlt := drop_lt_self hcap x xs'
    have hrec := ih _ hlt ((x :: xs').drop cap) rfl
    rcases hrec with ⟨hflat, hlenrec, hleall⟩
    constructor
    · simp [goChunks, hflat, List.take_append_drop]
    constructor
    · have hdroplen : ((x :: xs').drop cap).length = (x :: xs').length - cap := by
        simp [List.length_drop]
      rw [show (goChunks cap hcap (x :: xs')).length =
          (goChunks cap hcap ((x :: xs').drop cap)).length + 1 by simp [goChunks]]
      rw [hlenrec, hdroplen]
      rw [pageLimit_succ (x :: xs').length cap hcap (by simp)]
      omega
    · intro p hp
      simp only [goChunks, List.mem_cons] at hp
      rcases hp with rfl | hp
      · simpa [List.length_take] using (Nat.min_le_left cap (xs'.length + 1))
      · exact hleall p hp

private theorem parts_single_ne (xs : List Nat) (hne : xs ≠ []) : [xs] ∈ parts xs := by
  induction xs with
  | nil => contradiction
  | cons x xs ih =>
    by_cases htail : xs = []
    · subst xs
      simp [parts]
    · simp only [parts, List.mem_flatMap]
      refine ⟨[xs], ih htail, ?_⟩
      simp

private theorem parts_append (a b : List Nat) (pa pb : List (List Nat))
    (ha : pa ∈ parts a) (hb : pb ∈ parts b) : pa ++ pb ∈ parts (a ++ b) := by
  induction a generalizing pa with
  | nil =>
      simp [parts] at ha
      subst pa
      simpa using hb
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at ha
      rcases ha with ⟨ps', hps', hmem⟩
      have hrec : ps' ++ pb ∈ parts (xs ++ b) := ih ps' hps'
      simp only [List.cons_append, parts, List.mem_flatMap]
      refine ⟨ps' ++ pb, hrec, ?_⟩
      match ps', hmem with
      | [], hmem =>
          simp at hmem
          subst hmem
          cases pb <;> simp
      | a1 :: rest, hmem =>
          simp only [List.mem_cons, List.not_mem_nil, or_false] at hmem
          rcases hmem with rfl | rfl <;> cases pb <;> simp_all

private theorem goChunks_mem_parts (cap : Nat) (hcap : 0 < cap) :
    ∀ xs : List Nat, goChunks cap hcap xs ∈ parts xs := by
  intro xs
  refine (Nat.strongRecOn xs.length (motive := fun n => ∀ xs : List Nat, xs.length = n →
      goChunks cap hcap xs ∈ parts xs) ?_) xs rfl
  intro n ih xs hlen
  subst n
  cases xs with
  | nil => simp [goChunks, parts]
  | cons x xs' =>
    have hlt := drop_lt_self hcap x xs'
    have hrec := ih _ hlt ((x :: xs').drop cap) rfl
    have htake_ne : (x :: xs').take cap ≠ [] := by
      intro hn
      have hlen : ((x :: xs').take cap).length = 0 := by simp [hn]
      have hpos : 0 < ((x :: xs').take cap).length := by
        simp [List.length_take]
        omega
      omega
    have hsingle := parts_single_ne ((x :: xs').take cap) htake_ne
    have happ := parts_append ((x :: xs').take cap) ((x :: xs').drop cap)
      _ _ hsingle hrec
    simpa [goChunks, List.take_append_drop] using happ

/-! ## 課金の上界 -/

private theorem sum_bound_aux (ps : List (List Nat)) (cap : Nat) :
    (ps.map (fun p => 1 + ubytes cap + 4*p.length)).sum =
      ps.length * (1 + ubytes cap) + 4 * (ps.map List.length).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      simp [Nat.add_mul, Nat.mul_add, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

private theorem sum_chosen_le (f : Writer) (cap : Nat) (ps : List (List Nat))
    (hle : ∀ p ∈ ps, p.length ≤ cap) :
    (ps.map (fun p => chosen f p)).sum ≤
      (ps.map (fun p => 1 + ubytes cap + 4*p.length)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons]
      have hp := chosen_le_raw_bound f cap (hle p (by simp))
      have hrest := ih (fun q hq => hle q (by simp [hq]))
      omega

private theorem charge_le_bound (f : Writer) (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    charge f 0 (goChunks cap hcap xs) ≤
      4*xs.length + pageLimit xs.length cap*(1+ubytes cap) := by
  have hprops := goChunks_props cap hcap xs
  have hsum := sum_chosen_le f cap (goChunks cap hcap xs) hprops.2.2
  unfold charge
  simp only [Nat.zero_add]
  have hs := hsum
  rw [sum_bound_aux] at hs
  have hflat : (goChunks cap hcap xs).flatten = xs := hprops.1
  have hlen : (goChunks cap hcap xs).length = pageLimit xs.length cap := hprops.2.1
  rw [hlen] at hs
  have hsumlen : ((goChunks cap hcap xs).map List.length).sum = xs.length := by
    have := congrArg List.length hflat
    simpa [List.length_flatten] using this
  rw [hsumlen] at hs
  omega

private theorem min?_getD_le_mem_nat (l : List Nat) (x : Nat) (hx : x ∈ l) :
    l.min?.getD 0 ≤ x := by
  cases l with
  | nil => simp at hx
  | cons y ys =>
      have hs : (y :: ys).min? ≠ none := by
        intro hnone
        have : (y :: ys) = [] := List.min?_eq_none_iff.mp hnone
        simp at this
      rcases hmn : (y :: ys).min? with _ | M
      · simp [hmn] at hs
      · exact ((List.le_min?_iff hmn).1 (Nat.le_refl _)) x hx

private theorem all_le_true (ps : List (List Nat)) (cap : Nat)
    (h : ∀ p ∈ ps, p.length ≤ cap) :
    ps.all (fun p => p.length ≤ cap) = true := by
  induction ps with
  | nil => rfl
  | cons p ps ih =>
      simp [h p (by simp), ih (fun q hq => h q (by simp [hq]))]

theorem sound_BoundedRaw : SparsePlanes.BoundedRaw.Goal := by
  intro xs cap s hcap hbudget
  let ps := goChunks cap hcap xs
  have hprops := goChunks_props cap hcap xs
  have hmem : ps ∈ parts xs := by
    exact goChunks_mem_parts cap hcap xs
  have hfilter : ps ∈ (parts xs).filter
      (fun qs => qs.all (fun p => p.length ≤ cap)) := by
    simp only [List.mem_filter]
    exact ⟨hmem, all_le_true ps cap hprops.2.2⟩
  let l := ((parts xs).filter (fun qs => qs.all (fun p => p.length ≤ cap))).map
    (charge write 0)
  have hchargemem : charge write 0 ps ∈ l := by
    exact List.mem_map.mpr ⟨ps, hfilter, rfl⟩
  have hgetd : l.min?.getD 0 = s := by
    unfold budget at hbudget
    change l.min? = some s at hbudget
    rw [hbudget]
    rfl
  have hs_le : s ≤ charge write 0 ps := by
    have := min?_getD_le_mem_nat l (charge write 0 ps) hchargemem
    rw [hgetd] at this
    exact this
  have hbound := charge_le_bound write cap hcap xs
  exact Nat.le_trans (by simpa [ps] using hs_le)
    (by simpa [Nat.mul_comm] using hbound)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SparsePlanes_BoundedRaw
