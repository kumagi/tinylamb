import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SharedDivisor_BoundedRaw

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SharedDivisor

private theorem log2_div128 (n : Nat) (hn : 128 ≤ n) : n.log2 = (n / 128).log2 + 7 := by
  have hn0 : n ≠ 0 := by omega
  have hd0 : n / 128 ≠ 0 := by
    apply Nat.ne_of_gt
    exact Nat.div_pos hn (by decide : 0 < 128)
  apply (Nat.log2_eq_iff hn0).2
  constructor
  · have hlo : 2 ^ (n / 128).log2 ≤ n / 128 := (Nat.le_log2 hd0).1 (Nat.le_refl _)
    calc
      2 ^ ((n / 128).log2 + 7) = 2 ^ (n / 128).log2 * 2 ^ 7 := by rw [Nat.pow_add]
      _ = 2 ^ (n / 128).log2 * 128 := by
        have h : 2 ^ 7 = 128 := by decide
        rw [h]
      _ ≤ (n / 128) * 128 := Nat.mul_le_mul_right 128 hlo
      _ ≤ n := Nat.div_mul_le_self n 128
  · have hhi : n / 128 < 2 ^ ((n / 128).log2 + 1) := (Nat.log2_lt hd0).1 (Nat.lt_succ_self _)
    have hdecomp0 := Nat.div_add_mod n 128
    have hdecomp : n = (n / 128) * 128 + n % 128 := by omega
    have hmod : n % 128 < 128 := Nat.mod_lt n (by decide)
    calc
      n = (n / 128) * 128 + n % 128 := hdecomp
      _ < ((n / 128) + 1) * 128 := by omega
      _ ≤ 2 ^ ((n / 128).log2 + 1) * 128 := Nat.mul_le_mul_right 128 (Nat.succ_le_of_lt hhi)
      _ = 2 ^ ((n / 128).log2 + 7 + 1) := by
        have hpow : 2 ^ ((n / 128).log2 + 7 + 1) = 2 ^ ((n / 128).log2 + 1) * 128 := by
          rw [show (n / 128).log2 + 7 + 1 = ((n / 128).log2 + 1) + 7 by omega]
          rw [Nat.pow_add]
        exact hpow.symm

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      rw [uleb.eq_1]
      by_cases hn : n < 128
      · rw [ite_eq_left hn]
        simp only [List.length_singleton]
        unfold ubytes width
        by_cases hz : n = 0
        · simp [hz]
        · have hlog : n.log2 < 7 := (Nat.log2_lt hz).2 (by simpa using hn)
          simp [hz]
          omega
      · have hlt : n / 128 < n := Nat.div_lt_self (by omega) (by decide)
        rw [ite_eq_right hn]
        rw [List.length_cons]
        rw [ih (n / 128) hlt]
        unfold ubytes width
        have hz0 : n / 128 ≠ 0 := by
          intro hz
          exact hn (Nat.div_eq_zero_iff.mp hz |>.resolve_left (by decide))
        have hzn : n ≠ 0 := by omega
        have hlog : n.log2 = (n / 128).log2 + 7 := log2_div128 n (by omega)
        simp [hz0, hzn, hlog]
        omega

private theorem width_mono (a b : Nat) (h : a ≤ b) : width a ≤ width b := by
  unfold width
  by_cases ha : a = 0
  · simp [ha]
  · have hb : b ≠ 0 := by omega
    simp [ha, hb]
    have hpow : 2 ^ a.log2 ≤ a := (Nat.le_log2 ha).1 (Nat.le_refl _)
    exact (Nat.le_log2 hb).2 (Nat.le_trans hpow h)

private theorem ubytes_mono (a b : Nat) (h : a ≤ b) : ubytes a ≤ ubytes b := by
  unfold ubytes
  have hw := width_mono a b h
  omega

private theorem sum_const_four (xs : List Nat) : (xs.map (fun _ => 4)).sum = 4 * xs.length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      omega

private theorem raw_len (xs : List Nat) :
    (raw xs).length = 1 + ubytes xs.length + 4 * xs.length := by
  simp [raw, fixed, little, uleb_len_eq_ubytes, List.length_flatMap, sum_const_four]
  omega

private theorem chosen_le_raw (f : Writer) (xs : List Nat) :
    chosen f xs ≤ (raw xs).length := by
  unfold chosen
  exact Nat.min_le_right _ _

private theorem parts_singleton : ∀ xs : List Nat, xs ≠ [] → [xs] ∈ parts xs
  | [], h => (h rfl).elim
  | x :: xs, _ => by
      cases xs with
      | nil => simp [parts]
      | cons y ys =>
          rw [parts]
          rw [List.mem_flatMap]
          refine ⟨[y :: ys], parts_singleton (y :: ys) (by simp), ?_⟩
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

private theorem pageLimit_succ (n cap : Nat) (hcap : 0 < cap) (hn : 0 < n) :
    pageLimit n cap = 1 + pageLimit (n - cap) cap := by
  unfold pageLimit
  by_cases hle : cap ≤ n
  · have hnum : n + cap - 1 = (n - cap + cap - 1) + cap * 1 := by omega
    rw [hnum]
    rw [Nat.add_mul_div_left (n - cap + cap - 1) 1 hcap]
    omega
  · have hlt : n < cap := Nat.lt_of_not_ge hle
    have hnsub : n - cap = 0 := Nat.sub_eq_zero_of_le (Nat.le_of_lt hlt)
    rw [hnsub]
    have h0 : (0 + cap - 1) / cap = 0 := by
      apply Nat.div_eq_of_lt_le <;> omega
    rw [h0]
    have hq : (n + cap - 1) / cap = 1 := by
      apply Nat.div_eq_of_lt_le <;> omega
    rw [hq]

private def chunks (cap : Nat) (hcap : 0 < cap) : List Nat → List (List Nat)
  | [] => []
  | x :: xs => (x :: xs).take cap :: chunks cap hcap ((x :: xs).drop cap)
termination_by xs => xs.length
decreasing_by
  rw [List.length_drop]
  simp only [List.length_cons]
  omega

private theorem chunks_blocks_le (cap : Nat) (hcap : 0 < cap) : ∀ xs,
    ∀ p ∈ chunks cap hcap xs, p.length ≤ cap
  | [] => by simp [chunks]
  | x :: xs => by
      intro p hp
      simp only [chunks, List.mem_cons] at hp
      rcases hp with rfl | hp
      · simp [List.length_take]
        omega
      · exact chunks_blocks_le cap hcap ((x :: xs).drop cap) p hp
termination_by xs => xs.length
decreasing_by
  rw [List.length_drop]
  simp only [List.length_cons]
  omega

private theorem chunks_flatten (cap : Nat) (hcap : 0 < cap) : ∀ xs,
    (chunks cap hcap xs).flatten = xs
  | [] => by simp [chunks]
  | x :: xs => by
      rw [chunks, List.flatten_cons]
      rw [chunks_flatten cap hcap ((x :: xs).drop cap)]
      rw [List.take_append_drop]
termination_by xs => xs.length
decreasing_by
  rw [List.length_drop]
  simp only [List.length_cons]
  omega

private theorem chunks_length_sum (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    ((chunks cap hcap xs).map List.length).sum = xs.length := by
  have hf := chunks_flatten cap hcap xs
  have hc := congrArg List.length hf
  simpa [List.length_flatten] using hc

private theorem chunks_length (cap : Nat) (hcap : 0 < cap) : ∀ xs,
    (chunks cap hcap xs).length = pageLimit xs.length cap
  | [] => by
      simp [chunks, pageLimit]
      symm
      apply Nat.div_eq_of_lt_le <;> omega
  | x :: xs => by
      rw [chunks, List.length_cons]
      rw [chunks_length cap hcap ((x :: xs).drop cap)]
      rw [pageLimit_succ (x :: xs).length cap hcap (by simp)]
      rw [List.length_drop]
      omega
termination_by xs => xs.length
decreasing_by
  rw [List.length_drop]
  simp only [List.length_cons]
  omega

private theorem chunks_mem_parts (cap : Nat) (hcap : 0 < cap) : ∀ xs,
    chunks cap hcap xs ∈ parts xs
  | [] => by simp [chunks, parts]
  | x :: xs => by
      let a := (x :: xs).take cap
      let ys := (x :: xs).drop cap
      have ha_ne : a ≠ [] := by
        simp [a]
        omega
      have hsing : [a] ∈ parts a := parts_singleton a ha_ne
      have hrec : chunks cap hcap ys ∈ parts ys := chunks_mem_parts cap hcap ys
      have happ := parts_append a ys [a] (chunks cap hcap ys) hsing hrec
      simpa [a, ys, chunks] using happ
termination_by xs => xs.length
decreasing_by
  rw [List.length_drop]
  simp only [List.length_cons]
  omega

private theorem sum_chosen_bound (f : Writer) (ps : List (List Nat)) (cap : Nat)
    (h : ∀ p ∈ ps, p.length ≤ cap) :
    (ps.map (fun p => chosen f p)).sum ≤
      ps.length * (1 + ubytes cap) + 4 * (ps.map List.length).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hp_len : p.length ≤ cap := h p (by simp)
      have hchosen : chosen f p ≤ (raw p).length := chosen_le_raw f p
      have hraw : (raw p).length = 1 + ubytes p.length + 4 * p.length := raw_len p
      have hub : ubytes p.length ≤ ubytes cap := ubytes_mono p.length cap hp_len
      have ih' := ih (fun q hq => h q (by simp [hq]))
      rw [Nat.add_mul, Nat.one_mul]
      omega

private theorem charge_chunks_le (f : Writer) (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    charge f 0 (chunks cap hcap xs) ≤
      4 * xs.length + pageLimit xs.length cap * (1 + ubytes cap) := by
  unfold charge
  simp only [Nat.zero_add]
  have hbound := sum_chosen_bound f (chunks cap hcap xs) cap (chunks_blocks_le cap hcap xs)
  rw [chunks_length_sum cap hcap xs] at hbound
  rw [chunks_length cap hcap xs] at hbound
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

theorem sound_BoundedRaw : SharedDivisor.BoundedRaw.Goal := by
  intro xs cap s hcap hbudget
  unfold budget at hbudget
  let ps := chunks cap hcap xs
  let L := (parts xs).filter (fun ps => ps.all (fun p => p.length ≤ cap))
  have hps_mem : ps ∈ parts xs := chunks_mem_parts cap hcap xs
  have hps_all : ∀ p ∈ ps, p.length ≤ cap := chunks_blocks_le cap hcap xs
  have hps_in_L : ps ∈ L := by
    simp only [L, List.mem_filter]
    refine ⟨hps_mem, List.all_eq_true.mpr ?_⟩
    intro p hp
    simp [hps_all p hp]
  have hx_in_map : charge write 0 ps ∈ L.map (charge write 0) :=
    List.mem_map.mpr ⟨ps, hps_in_L, rfl⟩
  have hs_le_charge : s ≤ charge write 0 ps := by
    have hgetd : (L.map (charge write 0)).min?.getD 0 = s := by
      rw [hbudget]
      rfl
    have hle := min?_getD_le_mem_nat (L.map (charge write 0)) (charge write 0 ps) hx_in_map
    rwa [hgetd] at hle
  have hch := charge_chunks_le write cap hcap xs
  simpa [ps, Nat.mul_comm] using Nat.le_trans hs_le_charge hch

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SharedDivisor_BoundedRaw
