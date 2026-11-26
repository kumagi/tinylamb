import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_BoundedRaw

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.ConstantExceptions

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem ubytes_mono {a b : Nat} (h : a ≤ b) : ubytes a ≤ ubytes b := by
  have hu : 0 < ubytes b := by
    unfold ubytes
    omega
  have hbnd : b < 128 ^ ubytes b :=
    (TinylambOpt.ResearchProofs.Compression.Wire.sound_WidthCliff b (ubytes b) hu).mp (Nat.le_refl _)
  exact (TinylambOpt.ResearchProofs.Compression.Wire.sound_WidthCliff a (ubytes b) hu).mpr
    (Nat.lt_of_le_of_lt h hbnd)

private theorem sum_const {α : Type} (l : List α) (c : Nat) :
    (l.map (fun _ => c)).sum = l.length * c := by
  induction l with
  | nil => simp
  | cons x l ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih, Nat.succ_mul]
      omega

private theorem little_length (n k : Nat) : (little n k).length = k := by
  simp [little]

private theorem fixed_length (xs : List Nat) : (fixed xs).length = xs.length * 4 := by
  simpa [fixed, little_length] using (sum_const xs 4)

private theorem raw_length (xs : List Nat) : (raw xs).length = 1 + ubytes xs.length + xs.length * 4 := by
  simp [raw, fixed_length, uleb_len_eq_ubytes]
  omega

private theorem chosen_le_raw (f : Writer) (xs : List Nat) : chosen f xs ≤ (raw xs).length := by
  unfold chosen
  exact Nat.min_le_right _ _

private theorem sum_map_le_map {α : Type} (l : List α) (g h : α → Nat)
    (hle : ∀ x ∈ l, g x ≤ h x) : (l.map g).sum ≤ (l.map h).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := hle x (by simp)
      have ih' := ih (fun y hy => hle y (by simp [hy]))
      omega

private theorem sum_overhead_decomp (ps : List (List Nat)) (cap : Nat) :
    (ps.map (fun p => 1 + ubytes cap + p.length * 4)).sum =
    (ps.map (fun _ => 1 + ubytes cap)).sum + (ps.map (fun p => p.length * 4)).sum := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem charge_le_overhead (f : Writer) (cap : Nat) (ps : List (List Nat))
    (hall : ∀ p ∈ ps, p.length ≤ cap) :
    charge f 0 ps ≤ (ps.map (fun _ => 1 + ubytes cap)).sum +
      (ps.map (fun p => p.length * 4)).sum := by
  unfold charge
  simp only [Nat.zero_add]
  have h1 : (ps.map (fun p => chosen f p)).sum ≤ (ps.map (fun p => (raw p).length)).sum := by
    apply sum_map_le_map
    intro p hp
    exact chosen_le_raw f p
  have h2 : (ps.map (fun p => (raw p).length)).sum ≤
      (ps.map (fun p => 1 + ubytes cap + p.length * 4)).sum := by
    apply sum_map_le_map
    intro p hp
    rw [raw_length]
    have hmu : ubytes p.length ≤ ubytes cap := ubytes_mono (hall p hp)
    omega
  have h3 := sum_overhead_decomp ps cap
  omega

private theorem sum_lengths_mul_four (ps : List (List Nat)) :
    (ps.map (fun p => p.length * 4)).sum = ps.flatten.length * 4 := by
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.flatten_cons]
      rw [ih, List.length_append, Nat.add_mul]

private theorem parts_flatten (xs : List Nat) (ps : List (List Nat))
    (h : ps ∈ parts xs) : ps.flatten = xs := by
  induction xs generalizing ps with
  | nil =>
      simp [parts] at h
      simp [h]
  | cons x xs ih =>
      simp only [parts, List.mem_flatMap] at h
      rcases h with ⟨ps', hps', hmem⟩
      have hflat := ih ps' hps'
      cases ps' with
      | nil =>
          dsimp only at hmem
          have hsing : ps = [[x]] := by simpa using hmem
          subst ps
          simp [hflat]
      | cons a rest =>
          dsimp only at hmem
          have hcases : ps = [x] :: a :: rest ∨ ps = (x :: a) :: rest := by
            simpa only [List.mem_cons, List.not_mem_nil, or_false] using hmem
          rcases hcases with rfl | rfl <;>
            simpa [List.flatten_cons] using congrArg (List.cons x) hflat

private theorem parts_prepend (xs : List Nat) (k : Nat) (tail : List (List Nat))
    (hk : 0 < k) (hklen : k ≤ xs.length) (htail : tail ∈ parts (xs.drop k)) :
    xs.take k :: tail ∈ parts xs := by
  induction k generalizing xs with
  | zero => omega
  | succ k ih =>
      cases xs with
      | nil => simp at hklen
      | cons x xs =>
          simp only [List.length_cons] at hklen
          cases k with
          | zero =>
              cases tail with
              | nil =>
                  have hnil : xs = [] := by simpa using parts_flatten xs [] htail
                  subst xs
                  simp [parts]
              | cons a rest =>
                  rw [parts]
                  exact List.mem_flatMap.mpr ⟨a :: rest, htail, by simp⟩
          | succ k' =>
              rw [List.take_succ_cons]
              rw [List.drop_succ_cons] at htail
              rw [parts]
              exact List.mem_flatMap.mpr ⟨xs.take (k' + 1) :: tail,
                ih xs (by omega) (by omega) htail, by simp⟩

/-! Canonical partition into chunks of size `cap`: one chunk per
`pageLimit` page.  It is enumerated by `parts`, every chunk is nonempty and
bounded by `cap`, and the chunk count is exactly `pageLimit`. -/

private def canon : List Nat → Nat → List (List Nat)
  | [], _ => []
  | x :: ys, 0 => [x :: ys]
  | x :: ys, cap + 1 =>
      (x :: ys).take (min (cap + 1) (x :: ys).length) ::
        canon ((x :: ys).drop (min (cap + 1) (x :: ys).length)) (cap + 1)
termination_by xs _ => xs.length
decreasing_by
  simp [List.length_drop]
  have _hp : 0 < min (cap + 1) (List.length (x :: ys)) :=
    (Nat.lt_min).mpr ⟨by omega, by simp⟩
  omega

private theorem canon_mem_parts (xs : List Nat) (c : Nat) : canon xs (c + 1) ∈ parts xs := by
  have main : ∀ n, ∀ ys : List Nat, ys.length = n → canon ys (c + 1) ∈ parts ys := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro ys hlen
        cases ys with
        | nil => simp [canon, parts]
        | cons x zs =>
            have hkpos : 0 < min (c + 1) (x :: zs).length :=
              (Nat.lt_min).mpr ⟨by omega, by simp⟩
            have hklen : min (c + 1) (x :: zs).length ≤ (x :: zs).length := Nat.min_le_right _ _
            have hdrop_lt : ((x :: zs).drop (min (c + 1) (x :: zs).length)).length < n := by
              simp [List.length_drop, hlen]
              omega
            have htail : canon ((x :: zs).drop (min (c + 1) (x :: zs).length)) (c + 1) ∈
                parts ((x :: zs).drop (min (c + 1) (x :: zs).length)) :=
              ih _ hdrop_lt _ rfl
            rw [canon]
            exact parts_prepend (x :: zs) (min (c + 1) (x :: zs).length)
              (canon ((x :: zs).drop (min (c + 1) (x :: zs).length)) (c + 1))
              hkpos hklen htail
  exact main xs.length xs rfl

private theorem canon_parts_ok (xs : List Nat) (c : Nat) :
    ∀ p ∈ canon xs (c + 1), 0 < p.length ∧ p.length ≤ c + 1 := by
  have main : ∀ n, ∀ ys : List Nat, ys.length = n →
      ∀ p ∈ canon ys (c + 1), 0 < p.length ∧ p.length ≤ c + 1 := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro ys hlen
        cases ys with
        | nil => simp [canon]
        | cons x zs =>
            have hkpos : 0 < min (c + 1) (x :: zs).length :=
              (Nat.lt_min).mpr ⟨by omega, by simp⟩
            have hklen : min (c + 1) (x :: zs).length ≤ (x :: zs).length := Nat.min_le_right _ _
            have hdrop_lt : ((x :: zs).drop (min (c + 1) (x :: zs).length)).length < n := by
              simp [List.length_drop, hlen]
              omega
            intro p hp
            rw [canon] at hp
            simp only [List.mem_cons] at hp
            rcases hp with rfl | hp
            · rw [List.length_take]
              have hmm : min (min (c + 1) (x :: zs).length) (x :: zs).length =
                  min (c + 1) (x :: zs).length := by
                simp [Nat.min_assoc]
              rw [hmm]
              exact ⟨hkpos, Nat.min_le_left _ _⟩
            · exact ih _ hdrop_lt _ rfl p hp
  exact main xs.length xs rfl

private theorem canon_length (xs : List Nat) (c : Nat) :
    (canon xs (c + 1)).length = pageLimit xs.length (c + 1) := by
  have main : ∀ n, ∀ ys : List Nat, ys.length = n →
      (canon ys (c + 1)).length = pageLimit n (c + 1) := by
    intro n
    induction n using Nat.strongRecOn with
    | ind n ih =>
        intro ys hlen
        cases ys with
        | nil =>
            have hn0 : n = 0 := by simpa using hlen.symm
            subst n
            simp [canon, pageLimit, Nat.div_eq_of_lt (by omega : c < c + 1)]
        | cons x zs =>
            have hkpos : 0 < min (c + 1) (x :: zs).length :=
              (Nat.lt_min).mpr ⟨by omega, by simp⟩
            have hklen : min (c + 1) (x :: zs).length ≤ (x :: zs).length := Nat.min_le_right _ _
            have hdrop_lt : ((x :: zs).drop (min (c + 1) (x :: zs).length)).length < n := by
              simp [List.length_drop, hlen]
              omega
            have htail := ih _ hdrop_lt ((x :: zs).drop (min (c + 1) (x :: zs).length)) rfl
            rw [canon]
            change Nat.succ ((canon ((x :: zs).drop (min (c + 1) (x :: zs).length)) (c + 1)).length) =
              pageLimit n (c + 1)
            rw [htail, hlen]
            have hn_eq : n = zs.length + 1 := by simpa using hlen.symm
            rw [hn_eq]
            simp only [List.length_drop, List.length_cons]
            unfold pageLimit
            by_cases hle : zs.length + 1 ≤ c + 1
            · have hmin : min (c + 1) (zs.length + 1) = zs.length + 1 := Nat.min_eq_right hle
              rw [hmin]
              rw [show zs.length + 1 - (zs.length + 1) = 0 by omega]
              rw [show 0 + (c + 1) - 1 = c by omega]
              rw [Nat.div_eq_of_lt (by omega : c < c + 1)]
              simp
              rw [show zs.length + 1 + c = zs.length + c + 1 by omega]
              rw [Nat.div_eq_ite]
              simp [show c + 1 ≤ zs.length + c + 1 by omega,
                show zs.length + c + 1 - (c + 1) = zs.length by omega,
                Nat.div_eq_of_lt (by omega : zs.length < c + 1)]
            · have hmin : min (c + 1) (zs.length + 1) = c + 1 := Nat.min_eq_left (by omega)
              rw [hmin]
              rw [show zs.length + 1 - (c + 1) = zs.length - c by omega]
              rw [show (zs.length - c) + (c + 1) - 1 = (zs.length - c + (c + 1) - 1) by omega]
              rw [show zs.length + 1 + (c + 1) - 1 = (zs.length - c + (c + 1) - 1) + (c + 1) by omega]
              rw [Nat.add_div_right _ (by omega : 0 < c + 1)]
  exact main xs.length xs rfl

private theorem boundedRaw_holds (f : Writer) : boundedRaw f := by
  intro xs cap s hcap hb
  cases cap with
  | zero => omega
  | succ c =>
      let L : List Nat := ((parts xs).filter (fun ps => ps.all (fun p => p.length ≤ c + 1))).map (charge f 0)
      have hb' : L.min? = some s := by
        simpa [budget, L] using hb
      let pstar := canon xs (c + 1)
      have hp_mem : pstar ∈ parts xs := by
        simpa [pstar] using canon_mem_parts xs c
      have hp_ok : ∀ p ∈ pstar, p.length ≤ c + 1 := by
        intro p hp
        exact (canon_parts_ok xs c p (by simpa [pstar] using hp)).2
      have hp_filter : pstar ∈ (parts xs).filter (fun ps => ps.all (fun p => p.length ≤ c + 1)) := by
        rw [List.mem_filter]
        constructor
        · exact hp_mem
        · rw [List.all_eq_true]
          intro p hp
          have hle := hp_ok p hp
          simp [hle]
      have hc_mem : charge f 0 pstar ∈ L := by
        exact List.mem_map.mpr ⟨pstar, hp_filter, rfl⟩
      have hs_le : s ≤ charge f 0 pstar := (List.min?_eq_some_iff.mp hb').2 _ hc_mem
      have hcharge : charge f 0 pstar ≤
          (pstar.map (fun _ => 1 + ubytes (c + 1))).sum +
          (pstar.map (fun p => p.length * 4)).sum :=
        charge_le_overhead f (c + 1) pstar hp_ok
      have hconst : (pstar.map (fun _ => 1 + ubytes (c + 1))).sum =
          pstar.length * (1 + ubytes (c + 1)) := sum_const pstar (1 + ubytes (c + 1))
      have hlen4 : (pstar.map (fun p => p.length * 4)).sum = xs.length * 4 := by
        rw [sum_lengths_mul_four, parts_flatten xs pstar hp_mem]
      have hclen : pstar.length = pageLimit xs.length (c + 1) := by
        simpa [pstar] using canon_length xs c
      have hbound : charge f 0 pstar ≤
          xs.length * 4 + pageLimit xs.length (c + 1) * (1 + ubytes (c + 1)) := by
        calc
          charge f 0 pstar ≤ (pstar.map (fun _ => 1 + ubytes (c + 1))).sum +
              (pstar.map (fun p => p.length * 4)).sum := hcharge
          _ = pstar.length * (1 + ubytes (c + 1)) + xs.length * 4 := by rw [hconst, hlen4]
          _ = xs.length * 4 + pageLimit xs.length (c + 1) * (1 + ubytes (c + 1)) := by
            rw [hclen]
            omega
      omega

theorem sound_BoundedRaw : BoundedRaw.Goal := boundedRaw_holds write

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_ConstantExceptions_BoundedRaw
