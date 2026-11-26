import TinylambOpt.ResearchGoals.ClickstreamLayout.Storage

/-!
These models describe encoded clickstream columns and the predicates and aggregates that
can consume them.

Proved

- `RunSidecar` — Can clustered compaction replace full-width physical ordinals with one
  label per logical row? Require exact inversion and the minimum number of labels
  among representations with contiguous, strictly ascending lanes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickstreamLayout.Storage_RunSidecar

open TinylambOpt.ResearchGoals.ClickstreamLayout.Storage.RunSidecar
open TinylambOpt.ResearchGoals.ClickstreamLayout.Storage

theorem runs_cons (x : Nat) (xs : List Nat) : runs (x :: xs) =
    match runs xs with
    | (y :: ys) :: rest => if x < y then (x :: y :: ys) :: rest else [x] :: (y :: ys) :: rest
    | rs => [x] :: rs := rfl

theorem runs_flatten : ∀ ids : List Nat, (runs ids).flatten = ids := by
  intro ids
  induction ids with
  | nil => rfl
  | cons x xs ih =>
    cases h : runs xs with
    | nil =>
      have hxs : xs = [] := by simpa [h] using ih.symm
      subst xs
      simp [runs]
    | cons r rest =>
      cases r with
      | nil =>
        have ih' : rest.flatten = xs := by simpa [h] using ih
        rw [runs_cons, h]
        simp [ih']
      | cons y ys =>
        have ih' : ((y :: ys) :: rest).flatten = xs := by simpa [h] using ih
        rw [runs_cons, h]
        rw [← ih']
        simp only [List.flatten_cons]
        by_cases hxy : x < y <;> simp [hxy]

theorem runs_head_eq : ∀ (x : Nat) (xs : List Nat), ∃ r rest, runs (x :: xs) = (x :: r) :: rest := by
  intro x xs
  induction xs generalizing x with
  | nil => exact ⟨[], [], rfl⟩
  | cons x' xs' ih =>
    obtain ⟨r, rest, h⟩ := ih x'
    rw [runs_cons, h]
    by_cases hx : x < x'
    · exact ⟨x' :: r, rest, by simp [hx]⟩
    · exact ⟨[], (x' :: r) :: rest, by simp [hx]⟩

theorem runs_pairwise : ∀ (ids : List Nat), ids.Nodup →
    ∀ r ∈ runs ids, r.Pairwise (· < ·) := by
  intro ids hn
  induction ids with
  | nil => simp [runs]
  | cons x xs ih =>
    have hnxs : xs.Nodup := (List.nodup_cons.mp hn).2
    cases xs with
    | nil => simp [runs]
    | cons y ys =>
      obtain ⟨r, rest, h⟩ := runs_head_eq y ys
      have hp : (y :: r).Pairwise (· < ·) := ih hnxs (y :: r) (by
        rw [h]
        simp)
      rw [runs_cons, h]
      change ∀ l : List Nat, l ∈ (if x < y then (x :: y :: r) :: rest else [x] :: (y :: r) :: rest) →
        l.Pairwise (· < ·)
      by_cases hxy : x < y
      · rw [if_pos hxy]
        intro l hl
        simp only [List.mem_cons] at hl
        rcases hl with rfl | hl
        · rw [List.pairwise_cons]
          constructor
          · intro z hz
            simp only [List.mem_cons] at hz
            rcases hz with rfl | hz
            · exact hxy
            · have hyz : y < z := (List.pairwise_cons.mp hp).1 z hz
              omega
          · exact hp
        · exact ih hnxs l (by rw [h]; exact List.mem_cons_of_mem _ hl)
      · rw [if_neg hxy]
        intro l hl
        simp only [List.mem_cons] at hl
        rcases hl with rfl | hl
        · simp
        · rcases hl with rfl | hl
          · exact ih hnxs (y :: r) (by rw [h]; simp)
          · exact ih hnxs l (by rw [h]; exact List.mem_cons_of_mem _ hl)

theorem runs_cons_len_le (x : Nat) (xs : List Nat) :
    (runs (x :: xs)).length ≤ (runs xs).length + 1 := by
  cases xs with
  | nil => simp [runs]
  | cons z zs =>
    obtain ⟨r, rest, h⟩ := runs_head_eq z zs
    rw [runs_cons, h]
    by_cases hx : x < z <;> simp [hx] <;> omega

theorem runs_cons_len_eq_of_lt (x z : Nat) (zs : List Nat) (hx : x < z) :
    (runs (x :: z :: zs)).length = (runs (z :: zs)).length := by
  obtain ⟨r, rest, h⟩ := runs_head_eq z zs
  rw [runs_cons, h]
  simp [hx]

theorem runs_minimal : ∀ (other : List (List Nat)) (ids : List Nat), other.flatten = ids →
    (∀ lane ∈ other, lane.Pairwise (· < ·)) → (runs ids).length ≤ other.length := by
  intro other
  induction other with
  | nil =>
    intro ids hflat hp
    have hids : ids = [] := by simpa using hflat.symm
    subst ids
    simp [runs]
  | cons lane rest ih =>
    intro ids hflat hp
    cases lane with
    | nil =>
      have hrest : rest.flatten = ids := by simpa using hflat
      have hih := ih ids hrest (by
        intro l hl
        exact hp l (List.mem_cons_of_mem _ hl))
      simp only [List.length_cons]
      omega
    | cons y ys =>
      have hp_rest : ∀ l ∈ rest, l.Pairwise (· < ·) := by
        intro l hl
        exact hp l (List.mem_cons_of_mem _ hl)
      have hbase : (runs rest.flatten).length ≤ rest.length := ih rest.flatten rfl hp_rest
      have hrec : ∀ (y : Nat) (ys : List Nat), (y :: ys).Pairwise (· < ·) →
          (runs ((y :: ys) ++ rest.flatten)).length ≤ 1 + rest.length := by
        intro y ys hpair
        induction ys generalizing y with
        | nil =>
          have hle := runs_cons_len_le y rest.flatten
          have hgoal : (runs (y :: rest.flatten)).length ≤ rest.length + 1 :=
            Nat.le_trans hle (Nat.succ_le_succ hbase)
          simpa [Nat.add_comm] using hgoal
        | cons z zs ihys =>
          have hyz : y < z := (List.pairwise_cons.mp hpair).1 z (by simp)
          have hpairz : (z :: zs).Pairwise (· < ·) := (List.pairwise_cons.mp hpair).2
          have heq := runs_cons_len_eq_of_lt y z (zs ++ rest.flatten) hyz
          have hih := ihys z hpairz
          rw [show (y :: z :: zs) ++ rest.flatten = y :: z :: (zs ++ rest.flatten) by simp]
          rw [heq]
          exact hih
      have hp_lane : (y :: ys).Pairwise (· < ·) := hp (y :: ys) (by simp)
      have hids : ids = (y :: ys) ++ rest.flatten := by simpa using hflat.symm
      rw [hids]
      simpa [Nat.add_comm] using hrec y ys hp_lane

theorem range_pairwise_lt : ∀ n : Nat, (List.range n).Pairwise (· < ·) := by
  intro n
  induction n with
  | zero => simp
  | succ n ih =>
    rw [List.range_succ, List.pairwise_append]
    refine ⟨ih, by simp, ?_⟩
    intro a ha b hb
    simp at hb
    subst b
    exact List.mem_range.mp ha

theorem range_succ_cons : ∀ n : Nat, List.range (n + 1) = 0 :: (List.range n).map (fun i => i + 1) := by
  intro n
  apply List.ext_getElem?
  intro i
  cases i with
  | zero => simp
  | succ i =>
    by_cases hi : i < n
    · rw [List.getElem?_range (by omega)]
      simp only [List.getElem?_cons_succ]
      rw [List.getElem?_map]
      rw [List.getElem?_range hi]
      simp
    · have h1 : (List.range (n + 1))[i + 1]? = none := by
        rw [List.getElem?_eq_none_iff]
        simp only [List.length_range]
        omega
      have h2 : (List.range n)[i]? = none := by
        rw [List.getElem?_eq_none_iff]
        simp only [List.length_range]
        omega
      simp [h1, h2]

theorem map_range_succ_cons (g : Nat → Nat) (n : Nat) :
    (List.range (n + 1)).map g = g 0 :: ((List.range n).map (fun i => g (i + 1))) := by
  rw [range_succ_cons]
  simp

theorem filterMap_congr {α β : Type} (f g : α → Option β) : ∀ l : List α,
    (∀ x ∈ l, f x = g x) → l.filterMap f = l.filterMap g := by
  intro l h
  induction l with
  | nil => simp
  | cons a l ih =>
    simp only [List.filterMap_cons]
    rw [h a (by simp), ih (by intro x hx; exact h x (by simp [hx]))]

theorem flatMap_congr_left {α β : Type} (f g : α → List β) : ∀ l : List α,
    (∀ x ∈ l, f x = g x) → l.flatMap f = l.flatMap g := by
  intro l h
  induction l with
  | nil => simp
  | cons a l ih =>
    simp only [List.flatMap_cons]
    rw [h a (by simp), ih (by intro x hx; exact h x (by simp [hx]))]

theorem filterMap_self_eq_filter (p : Nat → Bool) (l : List Nat) :
    l.filterMap (fun i => if p i then some i else none) = l.filter p := by
  induction l with
  | nil => simp
  | cons a l ih =>
    by_cases h : p a <;> simp [h, ih]

theorem zip_filterMap_range (g : Nat → Nat) (j s n : Nat) :
    (((List.range n).map g).zipIdx s).filterMap (fun (t, i) => if t == j then some i else none) =
      (List.range n).filterMap (fun i => if g i == j then some (s + i) else none) := by
  induction n generalizing s g with
  | zero => simp
  | succ n ih =>
    rw [map_range_succ_cons]
    rw [List.zipIdx_cons]
    simp only [List.filterMap_cons]
    rw [ih (fun i => g (i + 1)) (s + 1)]
    rw [range_succ_cons]
    simp only [List.filterMap_cons]
    rw [List.filterMap_map]
    by_cases hg : g 0 == j
    · simp [hg]
      apply filterMap_congr
      intro i hi
      simp only [Function.comp_apply]
      by_cases h : g (i + 1) = j
      · rw [if_pos h, if_pos h]
        congr 1
        omega
      · rw [if_neg h, if_neg h]
    · simp [hg]
      apply filterMap_congr
      intro i hi
      simp only [Function.comp_apply]
      by_cases h : g (i + 1) = j
      · rw [if_pos h, if_pos h]
        congr 1
        omega
      · rw [if_neg h, if_neg h]

theorem filter_range_mem_eq {l : List Nat} {n : Nat}
    (hp : l.Pairwise (· < ·)) (hb : ∀ x ∈ l, x < n) :
    (List.range n).filter (fun x => decide (x ∈ l)) = l := by
  have hndl : l.Nodup := hp.imp (fun {a b} h => Nat.ne_of_lt h)
  have hndr : (List.range n).Nodup := List.nodup_range
  have hndf : ((List.range n).filter (fun x => decide (x ∈ l))).Nodup := by
    rw [List.nodup_iff_pairwise_ne] at hndr ⊢
    exact List.Pairwise.filter _ hndr
  have hperm : ((List.range n).filter (fun x => decide (x ∈ l))).Perm l := by
    apply (List.perm_ext_iff_of_nodup hndf hndl).2
    intro x
    constructor
    · intro hx
      exact by simpa using (List.mem_filter.mp hx).2
    · intro hx
      exact List.mem_filter.mpr ⟨List.mem_range.mpr (hb x hx), by simp [hx]⟩
  apply @List.Perm.eq_of_pairwise Nat (fun a b : Nat => a ≤ b) ((List.range n).filter (fun x => decide (x ∈ l))) l (by intro a b ha hb hab hba; omega)
  · exact List.Pairwise.filter _ ((range_pairwise_lt n).imp (fun {a b} h => Nat.le_of_lt h))
  · exact hp.imp (fun {a b} h => Nat.le_of_lt h)
  · exact hperm

theorem findIdx_cons_eq_zero (lane : List Nat) (rest : List (List Nat)) (i : Nat) :
    (lane :: rest).findIdx (fun r => r.contains i) = 0 ↔ lane.contains i := by
  rw [List.findIdx_cons]
  by_cases h : lane.contains i <;> simp [h]

theorem group_eq_lane : ∀ (rs : List (List Nat)) (n : Nat),
    rs.flatten.Nodup →
    (∀ lane ∈ rs, lane.Pairwise (· < ·)) →
    (∀ i ∈ rs.flatten, i < n) →
    ∀ j, j < rs.length →
      (labels rs n).zipIdx.filterMap (fun (t, i) => if t == j then some i else none) =
        rs.getD j [] := by
  intro rs n hnd hp hb j hj
  induction rs generalizing j with
  | nil => simp at hj
  | cons lane rest ih =>
    have hndall : (lane ++ rest.flatten).Nodup := by simpa using hnd
    have hnd_lane : lane.Nodup := (List.nodup_append.mp hndall).1
    have hnd_rest : rest.flatten.Nodup := (List.nodup_append.mp hndall).2.1
    have hdisj : ∀ a ∈ lane, ∀ b ∈ rest.flatten, a ≠ b := (List.nodup_append.mp hndall).2.2
    have hb_lane : ∀ i ∈ lane, i < n := by
      intro i hi
      exact hb i (by simp [hi])
    have hb_rest : ∀ i ∈ rest.flatten, i < n := by
      intro i hi
      exact hb i (by simp [hi])
    have hp_rest : ∀ l ∈ rest, l.Pairwise (· < ·) := by
      intro l hl
      exact hp l (by simp [hl])
    have hp_lane : lane.Pairwise (· < ·) := hp lane (by simp)
    cases j with
    | zero =>
      simp only [List.getD_cons_zero]
      rw [show labels (lane :: rest) n = (List.range n).map
          (fun i => (lane :: rest).findIdx (fun r => r.contains i)) by rfl]
      rw [zip_filterMap_range]
      simp only [Nat.zero_add]
      rw [filterMap_self_eq_filter]
      rw [show (List.range n).filter (fun i => (lane :: rest).findIdx (fun r => r.contains i) == 0) =
              (List.range n).filter (fun i => decide (i ∈ lane)) by
        apply List.filter_congr
        intro i hi
        by_cases hmem : i ∈ lane
        · have hbmem : lane.contains i = true := by simp [hmem]
          have hf : (lane :: rest).findIdx (fun r => r.contains i) = 0 :=
            (findIdx_cons_eq_zero lane rest i).2 hbmem
          have hf' : (lane :: rest).findIdx (fun r => decide (i ∈ r)) = 0 := by
            change (lane :: rest).findIdx (fun r => r.contains i) = 0
            exact hf
          simp [hf', hmem]
        · have hbmem : lane.contains i = false := by simp [hmem]
          have hf : (lane :: rest).findIdx (fun r => r.contains i) ≠ 0 := by
            intro hzero
            have : lane.contains i := (findIdx_cons_eq_zero lane rest i).1 hzero
            exact hmem (by simpa using this)
          have hf' : (lane :: rest).findIdx (fun r => decide (i ∈ r)) ≠ 0 := by
            intro hzero
            apply hf
            change (lane :: rest).findIdx (fun r => r.contains i) = 0
            exact hzero
          simp [hf', hmem]
      ]
      exact filter_range_mem_eq hp_lane hb_lane
    | succ j' =>
      have hj' : j' < rest.length := by simpa using hj
      have ih' := ih hnd_rest hp_rest hb_rest j' hj'
      simp only [List.getD_cons_succ]
      rw [show labels (lane :: rest) n = (List.range n).map
          (fun i => (lane :: rest).findIdx (fun r => r.contains i)) by rfl]
      rw [zip_filterMap_range]
      rw [show (List.range n).filterMap
              (fun i => if (lane :: rest).findIdx (fun r => r.contains i) == j' + 1
                        then some (0 + i) else none) =
            (List.range n).filterMap
              (fun i => if rest.findIdx (fun r => r.contains i) == j' then some (0 + i) else none) by
        apply filterMap_congr
        intro i hi
        by_cases hmem : i ∈ lane
        · have hbmem : lane.contains i = true := by simp [hmem]
          have hf0 : (lane :: rest).findIdx (fun r => r.contains i) = 0 :=
            (findIdx_cons_eq_zero lane rest i).2 hbmem
          have hnmem : i ∉ rest.flatten := by
            intro hir
            exact hdisj i hmem i hir rfl
          have hnmem' : ∀ r ∈ rest, r.contains i = false := by
            intro r hr
            by_cases hc : r.contains i
            · exfalso
              apply hnmem
              apply List.mem_flatten.mpr
              exact ⟨r, hr, by simpa using hc⟩
            · simpa using hc
          have hfre : rest.findIdx (fun r => r.contains i) = rest.length :=
            List.findIdx_eq_length_of_false hnmem'
          have hf0' : (lane :: rest).findIdx (fun r => decide (i ∈ r)) = 0 := by
            change (lane :: rest).findIdx (fun r => r.contains i) = 0
            exact hf0
          have hfre' : rest.findIdx (fun r => decide (i ∈ r)) = rest.length := by
            change rest.findIdx (fun r => r.contains i) = rest.length
            exact hfre
          rw [hf0, hfre]
          have hnej : ¬ j' + 1 = 0 := by omega
          have hnej' : j' ≠ rest.length := by omega
          have hnej'' : ¬ rest.length = j' := by omega
          simp [hnej, hnej', hnej'']
        · have hbmem : lane.contains i = false := by simp [hmem]
          have hf1 : (lane :: rest).findIdx (fun r => r.contains i) =
              rest.findIdx (fun r => r.contains i) + 1 := by
            rw [List.findIdx_cons]
            have hbtrue : ¬ lane.contains i = true := by simpa [hbmem] using hmem
            rw [if_neg hbtrue]
          rw [hf1]
          by_cases h : rest.findIdx (fun r => r.contains i) = j'
          · simp [h]
          · have hne : rest.findIdx (fun r => r.contains i) + 1 ≠ j' + 1 := by omega
            simp [h, hne]
      ]
      rw [show (List.range n).filterMap
              (fun i => if rest.findIdx (fun r => r.contains i) == j' then some (0 + i) else none) =
            (labels rest n).zipIdx.filterMap (fun (t, i) => if t == j' then some i else none) by
        rw [show labels rest n = (List.range n).map
            (fun i => rest.findIdx (fun r => r.contains i)) by rfl]
        rw [zip_filterMap_range]
      ]
      exact ih'

theorem flatMap_getD : ∀ rs : List (List Nat),
    (List.range rs.length).flatMap (fun j => rs.getD j []) = rs.flatten := by
  intro rs
  induction rs with
  | nil => simp
  | cons r rest ih =>
    rw [show (r :: rest).length = rest.length + 1 by simp]
    rw [range_succ_cons]
    simp only [List.flatMap_cons, List.getD_cons_zero, List.flatMap_map]
    rw [show List.flatMap (fun a => (r :: rest).getD (a + 1) []) (List.range rest.length) =
        List.flatMap (fun j => rest.getD j []) (List.range rest.length) by
      apply flatMap_congr_left
      intro j hj
      rw [List.getD_cons_succ]]
    rw [ih]
    simp

theorem recover_labels_eq_flatten (rs : List (List Nat)) (n : Nat)
    (hnd : rs.flatten.Nodup) (hp : ∀ lane ∈ rs, lane.Pairwise (· < ·))
    (hb : ∀ i ∈ rs.flatten, i < n) :
    recover (labels rs n) rs.length = rs.flatten := by
  unfold recover
  rw [show (List.range rs.length).flatMap (fun j =>
        (labels rs n).zipIdx.filterMap (fun (t, i) => if t == j then some i else none)) =
      (List.range rs.length).flatMap (fun j => rs.getD j []) by
    apply flatMap_congr_left
    intro j hj
    exact group_eq_lane rs n hnd hp hb j (List.mem_range.mp hj)]
  exact flatMap_getD rs

theorem sound : RunSidecar.Goal := by
  intro ids hp
  dsimp only
  have hnd_ids : ids.Nodup := hp.symm.nodup List.nodup_range
  have hb_ids : ∀ i ∈ ids, i < ids.length := by
    intro i hi
    exact List.mem_range.mp (hp.mem_iff.mp hi)
  have hflatten : (runs ids).flatten = ids := runs_flatten ids
  have hpairs : ∀ lane ∈ runs ids, lane.Pairwise (· < ·) := by
    intro lane hlane
    exact runs_pairwise ids hnd_ids lane hlane
  have hnd_rs : (runs ids).flatten.Nodup := by rw [hflatten]; exact hnd_ids
  have hb_rs : ∀ i ∈ (runs ids).flatten, i < ids.length := by
    rw [hflatten]
    exact hb_ids
  constructor
  · have hrec := recover_labels_eq_flatten (runs ids) ids.length hnd_rs hpairs hb_rs
    rw [hflatten] at hrec
    exact hrec
  · intro other hother hpair
    exact runs_minimal other ids hother hpair

end TinylambOpt.ResearchProofs.ClickstreamLayout.Storage_RunSidecar
