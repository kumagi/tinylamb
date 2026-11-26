import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

set_option maxHeartbeats 0

/-! Shared lemmas for the WAL group-commit model. They characterize a contiguous
durable cut by the prefix of covered bytes, which lets scans resume at an already
durable cut without re-proving coverage of the skipped bytes. These facts justify
reusing one fsync acknowledgement across several waiting clients. -/
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

private theorem tw_length (p : Nat → Bool) (n k : Nat) (hk : k ≤ n) :
    k ≤ ((List.range n).takeWhile p).length ↔ ∀ j, j < k → p j = true := by
  induction n generalizing k with
  | zero =>
      have hk0 : k = 0 := by omega
      subst k
      simp
  | succ n ih =>
      rw [List.range_succ, List.takeWhile_append]
      simp only [List.length_range]
      by_cases hlen : ((List.range n).takeWhile p).length = n
      · simp [hlen]
        by_cases hkn : k ≤ n
        · constructor
          · intro h j hj
            exact (ih k hkn).mp (by omega) j hj
          · intro hp
            omega
        · have hkeq : k = n + 1 := by omega
          subst k
          constructor
          · intro h j hj
            by_cases hjn : j < n
            · exact (ih n (by omega)).mp (by omega) j hjn
            · have hjeq : j = n := by omega
              subst j
              by_cases hpn : p n = true
              · exact hpn
              · have hpf : p n = false := by simpa using hpn
                simp [hpf] at h
                omega
          · intro hp
            have hpn : p n = true := hp n (by omega)
            simp [hpn]
      · simp [hlen]
        classical
        have hle : ((List.range n).takeWhile p).length ≤ (List.range n).length :=
          (List.takeWhile_prefix (p := p) (l := List.range n)).length_le
        simp at hle
        by_cases hkn : k ≤ n
        · exact ih k hkn
        · have hkeq : k = n + 1 := by omega
          subst k
          have hnle : ¬ n ≤ ((List.range n).takeWhile p).length := by omega
          have hnotall : ¬ ∀ j, j < n → p j = true := by
            intro hall
            exact hnle ((ih n (by omega)).mpr hall)
          obtain ⟨j, hjbad⟩ := (Classical.not_forall.mp hnotall)
          have hjpair : j < n ∧ p j ≠ true := (Classical.not_imp.mp hjbad)
          obtain ⟨hjlt, hjfalse⟩ := hjpair
          constructor
          · intro hh
            omega
          · intro hall
            have hnle' : n ≤ ((List.range n).takeWhile p).length :=
              (ih n (by omega)).mpr (fun j hj => hall j (by omega))
            exact False.elim (hnle hnle')
/-- A durable cut is never beyond the scan limit. -/
theorem prefixEnd_le_limit (ws : List Write) (base limit : Nat) (h : base ≤ limit) :
    prefixEnd ws base limit ≤ limit := by
  unfold prefixEnd
  have hle : ((List.range (limit - base)).takeWhile (fun i => cover ws (base + i))).length ≤
      (List.range (limit - base)).length :=
    (List.takeWhile_prefix (p := fun i => cover ws (base + i)) (l := List.range (limit - base))).length_le
  simp at hle
  omega

/-- The cut reaches `t` precisely when every byte from `base` up to `t` is covered. -/
theorem prefixEnd_iff (ws : List Write) (base limit t : Nat) (hbt : base ≤ t) (htl : t ≤ limit) :
    t ≤ prefixEnd ws base limit ↔ ∀ i, base ≤ i → i < t → cover ws i = true := by
  unfold prefixEnd
  have hiff := tw_length (p := fun i => cover ws (base + i))
    (n := limit - base) (k := t - base) (by omega)
  constructor
  · intro hle i hi hit
    have hkle : t - base ≤ ((List.range (limit - base)).takeWhile (fun i => cover ws (base + i))).length := by omega
    have hp := (hiff.mp hkle) (i - base) (by omega)
    have hi_eq : base + (i - base) = i := by omega
    simpa [hi_eq] using hp
  · intro hcov
    have hkle : t - base ≤ ((List.range (limit - base)).takeWhile (fun i => cover ws (base + i))).length :=
      hiff.mpr (by
        intro j hj
        have := hcov (base + j) (by omega) (by omega)
        simpa using this)
    omega

/-- Every byte inside an established durable cut is covered. -/
theorem cover_of_lt_prefixEnd (ws : List Write) (base limit i : Nat)
    (hi : base ≤ i) (hlt : i < prefixEnd ws base limit) : cover ws i = true := by
  have hle_limit : base ≤ limit := by
    by_cases hbl : base ≤ limit
    · exact hbl
    · unfold prefixEnd at hlt
      have hlen : ((List.range (limit - base)).takeWhile (fun j => cover ws (base + j))).length ≤
          (List.range (limit - base)).length :=
        (List.takeWhile_prefix (p := fun j => cover ws (base + j)) (l := List.range (limit - base))).length_le
      simp at hlen
      omega
  have hp_le : prefixEnd ws base limit ≤ limit := prefixEnd_le_limit ws base limit hle_limit
  have hcut := (prefixEnd_iff ws base limit (prefixEnd ws base limit) (by omega) hp_le).mp (by omega)
  exact hcut i hi hlt

theorem eraseDups_nodup : ∀ l : List Nat, l.eraseDups.Nodup := by
  intro l
  induction hlen : l.length using Nat.strongRecOn generalizing l with
  | ind n ih =>
      cases l with
      | nil => simp
      | cons x xs =>
          rw [List.eraseDups_cons, List.nodup_cons]
          constructor
          · simp
          · have hle : (List.filter (fun b => !b == x) xs).length ≤ xs.length :=
              List.Sublist.length_le List.filter_sublist
            simp at hlen hle
            exact ih (List.filter (fun b => !b == x) xs).length (by omega)
              (List.filter (fun b => !b == x) xs) rfl

theorem partition_length (xs : List Nat) (x : Nat) (hxnot : x ∉ xs) :
    (xs.filter (fun y => y < x)).length + (xs.filter (fun y => x < y)).length = xs.length := by
  induction xs with
  | nil => simp
  | cons y ys ih =>
      have hxnot_ys : x ∉ ys := by intro h; exact hxnot (by simp [h])
      have ih' := ih hxnot_ys
      by_cases hylt : y < x
      · have hygt : ¬ x < y := by omega
        simp [hylt, hygt]
        omega
      · have hyneq : y ≠ x := by intro h; exact hxnot (by simp [h])
        have hygt : x < y := by omega
        simp [hylt, hygt]
        omega

theorem nodup_map_sub (l : List Nat) (x : Nat) (hnd : l.Nodup) (hgt : ∀ y ∈ l, x < y) :
    (l.map (fun y => y - (x+1))).Nodup := by
  induction l with
  | nil => simp
  | cons y ys ih =>
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        obtain ⟨z, hzmem, hz⟩ := List.mem_map.mp hmem
        have hzgt := hgt z (by simp [hzmem])
        have hygt := hgt y (by simp)
        have hzdecomp : z = (z - (x+1)) + (x+1) := by omega
        have hydecomp : y = (y - (x+1)) + (x+1) := by omega
        have hzy : z = y := by omega
        have hymem : y ∈ ys := by
          subst z
          exact hzmem
        exact (List.nodup_cons.mp hnd).1 (by simp [hymem])
      · exact ih (List.nodup_cons.mp hnd).2 (fun z hz => hgt z (by simp [hz]))

theorem nodup_lt_length : ∀ N, ∀ (l : List Nat), l.Nodup → (∀ x ∈ l, x < N) → l.length ≤ N := by
  intro N
  induction N using Nat.strongRecOn with
  | ind N ih =>
      intro l hnd hb
      cases l with
      | nil => simp
      | cons x xs =>
          have hxN : x < N := hb x (by simp)
          have hndxs : xs.Nodup := (List.nodup_cons.mp hnd).2
          have hxnot : x ∉ xs := (List.nodup_cons.mp hnd).1
          have hbxs : ∀ y ∈ xs, y < N := by intro y hy; exact hb y (by simp [hy])
          let lo := xs.filter (fun y => y < x)
          let hi := xs.filter (fun y => x < y)
          have hndlo : lo.Nodup := List.Nodup.sublist List.filter_sublist hndxs
          have hndhi : hi.Nodup := List.Nodup.sublist List.filter_sublist hndxs
          have hblo : ∀ y ∈ lo, y < x := by intro y hy; simp [lo] at hy; exact hy.2
          have hhi_gt : ∀ y ∈ hi, x < y := by intro y hy; simp [hi] at hy; exact hy.2
          have hhi_lt : ∀ y ∈ hi, y < N := by
            intro y hy
            have hyx : y ∈ xs ∧ x < y := by simpa [hi] using hy
            exact hbxs y hyx.1
          have hlo_le : lo.length ≤ x := ih x hxN lo hndlo hblo
          have hmapnd : (hi.map (fun y => y - (x+1))).Nodup := nodup_map_sub hi x hndhi hhi_gt
          have hmapb : ∀ z ∈ hi.map (fun y => y - (x+1)), z < N - (x+1) := by
            intro z hz
            obtain ⟨y, hy, hyz⟩ := List.mem_map.mp hz
            have hylt := hhi_lt y hy
            have hygt := hhi_gt y hy
            omega
          have hhi_le : (hi.map (fun y => y - (x+1))).length ≤ N - (x+1) :=
            ih (N - (x+1)) (by omega) (hi.map (fun y => y - (x+1))) hmapnd hmapb
          have hpart : lo.length + hi.length = xs.length := by
            simpa [lo, hi] using partition_length xs x hxnot
          simp only [List.length_map] at hhi_le
          simp only [List.length_cons]
          omega


/-- Distinct end offsets drawn from `(d, t]` cannot outnumber the interval length. -/
theorem distinct_stops_credit (ws : List Waiter) (durable tip : Nat)
    (hb : ∀ w ∈ ws, durable < w.stop ∧ w.stop ≤ tip) :
    (ws.map Waiter.stop).eraseDups.length ≤ tip - durable := by
  have hbd : ∀ x ∈ (ws.map Waiter.stop).eraseDups, durable < x ∧ x ≤ tip := by
    intro x hx
    obtain ⟨w, hw, hwx⟩ := List.mem_map.mp (List.mem_eraseDups.mp hx)
    have := hb w hw
    simpa [hwx] using this
  have hmapnd : ((ws.map Waiter.stop).eraseDups.map (fun x => x - (durable+1))).Nodup :=
    nodup_map_sub (ws.map Waiter.stop).eraseDups durable (eraseDups_nodup _) (fun y hy => (hbd y hy).1)
  have hmapb : ∀ z ∈ (ws.map Waiter.stop).eraseDups.map (fun x => x - (durable+1)),
      z < tip - durable := by
    intro z hz
    obtain ⟨y, hy, hyz⟩ := List.mem_map.mp hz
    have hylo := (hbd y hy).1
    have hyhi := (hbd y hy).2
    omega
  have hle := nodup_lt_length (tip-durable)
    ((ws.map Waiter.stop).eraseDups.map (fun x => x - (durable+1))) hmapnd hmapb
  simpa using hle

end TinylambOpt.ResearchProofs.ResourcePolicies.Common_CommitBatches
