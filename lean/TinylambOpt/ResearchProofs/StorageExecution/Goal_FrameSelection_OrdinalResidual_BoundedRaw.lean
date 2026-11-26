import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_BoundedRaw

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.OrdinalResidual

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem width_mono {a b : Nat} (h : a ≤ b) : width a ≤ width b := by
  unfold width
  by_cases ha : a == 0
  · simp [ha]
  · have ha' : a ≠ 0 := by simpa using ha
    have hb' : b ≠ 0 := by omega
    simp [ha, hb']
    have hblt : b < 2 ^ (b.log2 + 1) := (Nat.log2_lt hb').mp (Nat.lt_succ_self _)
    have halt : a < 2 ^ (b.log2 + 1) := by omega
    have hlog : a.log2 < b.log2 + 1 := (Nat.log2_lt ha').mpr halt
    omega

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  unfold ubytes
  have hw := width_mono h
  omega

/-- Specification of `List.min?` on natural numbers. -/
private theorem minSpec : ∀ (l : List Nat),
    (l.min? = none ↔ l = []) ∧
    (∀ m, l.min? = some m → m ∈ l ∧ ∀ x ∈ l, m ≤ x) := by
  intro l
  induction l with
  | nil => simp [List.min?]
  | cons x xs ih =>
      constructor
      · constructor
        · intro h
          cases hxs : xs.min? with
          | none =>
              rw [List.min?_cons] at h
              simp [hxs] at h
          | some y =>
              rw [List.min?_cons] at h
              simp [hxs] at h
        · intro h
          simp at h
      · intro m hm
        cases hxs : xs.min? with
        | none =>
            rw [List.min?_cons] at hm
            simp [hxs] at hm
            subst m
            have hxsempty : xs = [] := ih.1.mp hxs
            subst xs
            simp
        | some y =>
            rw [List.min?_cons] at hm
            simp [hxs] at hm
            subst m
            have hymem : y ∈ xs := (ih.2 y hxs).1
            have hyle : ∀ z ∈ xs, y ≤ z := (ih.2 y hxs).2
            by_cases hxy : x ≤ y
            · rw [Nat.min_eq_left hxy]
              constructor
              · simp
              · intro z hz
                simp only [List.mem_cons] at hz
                rcases hz with rfl | hz
                · omega
                · exact Nat.le_trans hxy (hyle z hz)
            · have hyx : y ≤ x := by omega
              rw [Nat.min_eq_right hyx]
              constructor
              · exact List.mem_cons_of_mem x hymem
              · intro z hz
                simp only [List.mem_cons] at hz
                rcases hz with rfl | hz
                · exact hyx
                · exact hyle z hz

/-- Greedy right-to-left packing: fill the current rightmost block while it has
    room, otherwise start a new block on the left. -/
private def chunk : Nat → List Nat → List (List Nat)
  | _, [] => []
  | cap, x :: xs =>
      match chunk cap xs with
      | [] => [[x]]
      | p :: ps => if p.length < cap then (x :: p) :: ps else [x] :: p :: ps

private theorem chunk_flatten (cap : Nat) : ∀ xs, (chunk cap xs).flatten = xs := by
  intro xs
  induction xs with
  | nil => simp [chunk]
  | cons x xs ih =>
      cases h : chunk cap xs with
      | nil =>
          have hnil : xs = [] := by simpa [h] using ih
          subst xs
          simp [chunk]
      | cons p ps =>
          have ih' : (p :: ps).flatten = xs := by simpa [h] using ih
          by_cases hlt : p.length < cap
          · rw [show chunk cap (x :: xs) = (x :: p) :: ps by simp [chunk, h, hlt]]
            change (x :: p) ++ ps.flatten = x :: xs
            simpa [List.flatten_cons] using congrArg (fun l => x :: l) ih'
          · rw [show chunk cap (x :: xs) = [x] :: p :: ps by simp [chunk, h, hlt]]
            change [x] ++ (p :: ps).flatten = x :: xs
            simpa [List.flatten_cons] using congrArg (fun l => x :: l) ih'

private theorem chunk_mem_parts (cap : Nat) : ∀ xs, chunk cap xs ∈ parts xs := by
  intro xs
  induction xs with
  | nil => simp [chunk, parts]
  | cons x xs ih =>
      rw [parts]
      simp only [List.mem_flatMap]
      cases h : chunk cap xs with
      | nil =>
          have ih' : ([] : List (List Nat)) ∈ parts xs := by simpa [h] using ih
          refine ⟨[], ih', ?_⟩
          simp [chunk, h]
      | cons p ps =>
          have ih' : p :: ps ∈ parts xs := by simpa [h] using ih
          refine ⟨p :: ps, ih', ?_⟩
          by_cases hlt : p.length < cap <;> simp [chunk, h, hlt]

private theorem chunk_le_cap {cap : Nat} (hcap : 0 < cap) :
    ∀ xs, ∀ p ∈ chunk cap xs, p.length ≤ cap := by
  intro xs
  induction xs with
  | nil => simp [chunk]
  | cons x xs ih =>
      cases h : chunk cap xs with
      | nil =>
          intro q hq
          simp [chunk, h] at hq
          subst q
          change 1 ≤ cap
          omega
      | cons head tail =>
          have ih' : ∀ r ∈ head :: tail, r.length ≤ cap := by simpa [h] using ih
          by_cases hlt : head.length < cap
          · intro r hr
            rw [show chunk cap (x :: xs) = (x :: head) :: tail by simp [chunk, h, hlt]] at hr
            simp only [List.mem_cons] at hr
            rcases hr with hr | hr
            · subst r
              simp only [List.length_cons]
              omega
            · exact ih' r (by simp [hr])
          · intro r hr
            rw [show chunk cap (x :: xs) = [x] :: head :: tail by simp [chunk, h, hlt]] at hr
            simp only [List.mem_cons] at hr
            rcases hr with hr | hr
            · subst r
              change 1 ≤ cap
              omega
            · rcases hr with hr | hr
              · subst r
                exact ih' head (by simp)
              · exact ih' r (by simp [hr])

private theorem chunk_tail_full {cap : Nat} (hcap : 0 < cap) :
    ∀ xs, match chunk cap xs with
      | [] => True
      | p :: ps => ∀ q ∈ ps, q.length = cap := by
  intro xs
  induction xs with
  | nil => simp [chunk]
  | cons x xs ih =>
      cases h : chunk cap xs with
      | nil => simp [chunk, h]
      | cons p ps =>
          have ih' : ∀ q ∈ ps, q.length = cap := by
            have ht := ih
            simpa [h] using ht
          have hple : p.length ≤ cap := chunk_le_cap hcap xs p (by simp [h])
          by_cases hlt : p.length < cap
          · rw [show chunk cap (x :: xs) = (x :: p) :: ps by simp [chunk, h, hlt]]
            change ∀ q ∈ ps, q.length = cap
            exact ih'
          · have hpfull : p.length = cap := by omega
            rw [show chunk cap (x :: xs) = [x] :: p :: ps by simp [chunk, h, hlt]]
            change ∀ q ∈ p :: ps, q.length = cap
            intro q hq
            simp only [List.mem_cons] at hq
            rcases hq with rfl | hq
            · exact hpfull
            · exact ih' q hq

private theorem flat_length_of_full (cap : Nat) (p : List Nat) (rest : List (List Nat))
    (hfull : ∀ q ∈ rest, q.length = cap) :
    (p :: rest).flatten.length = p.length + rest.length * cap := by
  rw [List.length_flatten]
  induction rest with
  | nil => simp
  | cons q qs ih =>
      have hq : q.length = cap := hfull q (by simp)
      have hqs : ∀ r ∈ qs, r.length = cap := fun r hr => hfull r (by simp [hr])
      have ih' := ih hqs
      simp only [List.map_cons, List.sum_cons] at ih' ⊢
      simp only [List.length_cons]
      rw [Nat.succ_mul, hq]
      omega

private theorem chunk_count_lower (cap : Nat) (hcap : 0 < cap) : ∀ xs,
    chunk cap xs = [] ∨ xs.length > ((chunk cap xs).length - 1) * cap := by
  intro xs
  induction xs with
  | nil => simp [chunk]
  | cons x xs ih =>
      cases h : chunk cap xs with
      | nil =>
          right
          simp [chunk, h, hcap]
      | cons p ps =>
          have htail : ∀ q ∈ ps, q.length = cap := by
            have ht := chunk_tail_full hcap xs
            simpa [h] using ht
          by_cases hlt : p.length < cap
          · right
            have hflat : ((x :: p) :: ps).flatten = x :: xs := by
              have hf := chunk_flatten cap (x :: xs)
              simpa [chunk, h, hlt] using hf
            have hlen := flat_length_of_full cap (x :: p) ps htail
            have hn : xs.length + 1 = p.length + 1 + ps.length * cap := by
              have hlen2 := congrArg List.length hflat
              rw [hlen] at hlen2
              simp only [List.length_cons] at hlen2
              omega
            rw [show chunk cap (x :: xs) = (x :: p) :: ps by simp [chunk, h, hlt]]
            simp only [List.length_cons, Nat.add_sub_cancel]
            omega
          · right
            have hple : p.length ≤ cap := chunk_le_cap hcap xs p (by simp [h])
            have hpfull : p.length = cap := by omega
            have htail2 : ∀ q ∈ p :: ps, q.length = cap := by
              intro q hq
              simp only [List.mem_cons] at hq
              rcases hq with rfl | hq
              · exact hpfull
              · exact htail q hq
            have hflat : ([x] :: p :: ps).flatten = x :: xs := by
              have hf := chunk_flatten cap (x :: xs)
              simpa [chunk, h, hlt] using hf
            have hlen := flat_length_of_full cap [x] (p :: ps) htail2
            have hn : xs.length + 1 = 1 + (p :: ps).length * cap := by
              have hlen2 := congrArg List.length hflat
              rw [hlen] at hlen2
              simpa using hlen2.symm
            rw [show chunk cap (x :: xs) = [x] :: p :: ps by simp [chunk, h, hlt]]
            simp only [List.length_cons, Nat.add_sub_cancel]
            simp only [List.length_cons] at hn
            rw [Nat.succ_mul] at hn ⊢
            omega

private theorem chunk_count_le_pageLimit (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    (chunk cap xs).length ≤ pageLimit xs.length cap := by
  unfold pageLimit
  rcases chunk_count_lower cap hcap xs with hzero | hgt
  · simp [hzero]
  · cases hk : (chunk cap xs).length with
    | zero => simp
    | succ k =>
        have hgt' : xs.length > k * cap := by simpa [hk] using hgt
        apply (Nat.le_div_iff_mul_le hcap).mpr
        rw [Nat.succ_mul]
        omega

private theorem sum_const_map (xs : List Nat) (k : Nat) :
    (xs.map (fun _ => k)).sum = xs.length * k := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      change k + (List.map (fun _ => k) xs).sum = (xs.length + 1) * k
      rw [Nat.succ_mul]
      omega

private theorem raw_len (xs : List Nat) : (raw xs).length = 1 + ubytes xs.length + xs.length * 4 := by
  simp [raw, fixed, uleb_len_eq_ubytes, List.length_flatMap, little, sum_const_map]
  omega

private theorem sum_map_le {α : Type} (f g : α → Nat) (l : List α)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have hxs := ih (fun y hy => h y (by simp [hy]))
      omega

private theorem const_sum (ps : List (List Nat)) (b : Nat) :
    (ps.map (fun p => b + p.length * 4)).sum = ps.length * b + ps.flatten.length * 4 := by
  rw [List.length_flatten]
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons] at ih ⊢
      simp only [List.length_cons]
      rw [Nat.succ_mul, Nat.add_mul]
      omega

private theorem charge_chunk_bound (cap : Nat) (hcap : 0 < cap) (xs : List Nat) :
    charge write 0 (chunk cap xs) ≤
      xs.length * 4 + pageLimit xs.length cap * (1 + ubytes cap) := by
  unfold charge
  simp only [Nat.zero_add]
  have heach : ∀ p ∈ chunk cap xs,
      chosen write p ≤ (1 + ubytes cap) + p.length * 4 := by
    intro p hp
    have hraw : chosen write p ≤ (raw p).length := Nat.min_le_right _ _
    rw [raw_len] at hraw
    have hub : ubytes p.length ≤ ubytes cap :=
      ubytes_mono (chunk_le_cap hcap xs p hp)
    omega
  have hs1 := sum_map_le (fun p => chosen write p)
      (fun p => (1 + ubytes cap) + p.length * 4) (chunk cap xs) heach
  rw [const_sum] at hs1
  rw [chunk_flatten] at hs1
  have hk := chunk_count_le_pageLimit cap hcap xs
  have hkmul : (chunk cap xs).length * (1 + ubytes cap) ≤
      pageLimit xs.length cap * (1 + ubytes cap) := Nat.mul_le_mul_right _ hk
  omega

/-- A forced frame cap still limits worst-case expansion: take the greedily
    packed frame partition, whose block headers are bounded by `ubytes cap`. -/
theorem sound_BoundedRaw : OrdinalResidual.BoundedRaw.Goal := by
  intro xs cap s hcap hbudget
  unfold budget at hbudget
  let ps := chunk cap xs
  have hp_mem : ps ∈ parts xs := by
    simpa [ps] using chunk_mem_parts cap xs
  have hp_le : ∀ p ∈ ps, p.length ≤ cap := by
    simpa [ps] using chunk_le_cap hcap xs
  have hp_all : ps.all (fun p => p.length ≤ cap) = true := by
    exact List.all_eq_true.mpr (fun p hp => by simpa using hp_le p hp)
  have hmem : charge write 0 ps ∈
      ((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ cap))).map (charge write 0) := by
    exact List.mem_map.mpr ⟨ps, by simp [hp_mem, hp_all], rfl⟩
  have hs_le : s ≤ charge write 0 ps :=
    ((minSpec _).2 s hbudget).2 (charge write 0 ps) hmem
  exact Nat.le_trans hs_le (by simpa [ps] using charge_chunk_bound cap hcap xs)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_OrdinalResidual_BoundedRaw
