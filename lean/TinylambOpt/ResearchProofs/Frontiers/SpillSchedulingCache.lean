import TinylambOpt.ResearchGoals.Frontiers.SpillScheduling

/-!
The modeled contracts identify when an incremental SQL operator can advance its frontier
while preserving its outputs.

Proved

- `OfflineCache` — `∀ t cap k, (∃ state, legal t cap state ∧ k ≤ saved t state) ↔ ∃
  chosen : List Arc, chosen.Nodup ∧ (∀ e ∈ chosen, e ∈ arcs t) ∧ fits t cap chosen ∧
  k ≤ chosen.length`
- `StackDistance` — `∀ trace cap, lru cap trace [] = trace.zipIdx.countP (fun (p,i) =>
  (reuseDistance (trace.take i) p).any (· < cap))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingCache

open TinylambOpt.ResearchGoals.Frontiers.SpillScheduling

private theorem nodup_eraseDups : ∀ l : List Nat, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      rw [List.nodup_cons]
      constructor
      · intro h
        rw [List.mem_eraseDups] at h
        simp at h
      · exact nodup_eraseDups (as.filter fun b => !b == a)
termination_by l => l.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le (fun b : Nat => !b == a) as)

private theorem filter_eraseDups_comm (p : Nat) : ∀ l : List Nat,
    (l.filter (fun q => ! q == p)).eraseDups = l.eraseDups.filter (fun q => ! q == p)
  | [] => by simp
  | a :: as => by
      by_cases h : a == p
      · have ha : a = p := LawfulBEq.eq_of_beq h
        subst p
        simp [List.eraseDups_cons]
        symm
        apply List.filter_eq_self.mpr
        intro x hx
        rw [List.mem_eraseDups] at hx
        simp at hx
        simpa [hx.2]
      · have hf1 : (as.filter (fun a_1 => !a_1 == a && !a_1 == p)) =
            (as.filter (fun b => ! b == a)).filter (fun q => ! q == p) := by
          ext x
          simp [Bool.and_comm]
        have hih := filter_eraseDups_comm p (as.filter fun b => ! b == a)
        rw [List.filter_cons_of_pos (p := fun q : Nat => ! q == p) (by simpa [h])]
        rw [List.eraseDups_cons]
        rw [List.eraseDups_cons]
        rw [List.filter_cons_of_pos (p := fun q : Nat => ! q == p) (by simpa [h])]
        congr 1
        rw [List.filter_filter, hf1, hih]
termination_by l => l.length
decreasing_by
  exact Nat.lt_succ_of_le (List.length_filter_le (fun b : Nat => !b == a) as)

private theorem eraseDups_cons_eq (l : List Nat) (p : Nat) :
    (p :: l).eraseDups = p :: l.eraseDups.erase p := by
  rw [List.eraseDups_cons]
  rw [(nodup_eraseDups l).erase_eq_filter p]
  change p :: (List.filter (fun q : Nat => !q == p) l).eraseDups =
    p :: List.filter (fun q : Nat => !q == p) l.eraseDups
  rw [← filter_eraseDups_comm p l]

private theorem take_erase_take (l : List Nat) (p : Nat) (hnd : l.Nodup) :
    ∀ m : Nat, ((l.take (m + 1)).erase p).take m = (l.erase p).take m
  | 0 => by simp
  | m + 1 => by
      cases l with
      | nil => simp
      | cons a as =>
          have hndas : as.Nodup := (List.nodup_cons.mp hnd).2
          by_cases h : a = p
          · subst a
            simp [List.erase_cons_head, List.take_take]
          · have hbeq : ¬ (a == p) = true := by
              intro hh
              exact h (LawfulBEq.eq_of_beq hh)
            simp [List.erase_cons_tail hbeq, take_erase_take as p hndas m]

private theorem cons_erase_take_of_nodup (l : List Nat) (p : Nat) (hnd : l.Nodup) :
    ∀ n : Nat, (p :: (l.take n).erase p).take n = (p :: l.erase p).take n
  | 0 => by simp
  | m + 1 => by
      rw [show (p :: (l.take (m + 1)).erase p).take (m + 1) =
              p :: (((l.take (m + 1)).erase p).take m) by simp]
      rw [show (p :: l.erase p).take (m + 1) = p :: ((l.erase p).take m) by simp]
      rw [take_erase_take l p hnd m]

/-! Decomposition of a list at the first occurrence of p. -/

private theorem decomp_of_mem : ∀ (l : List Nat) (p : Nat), p ∈ l →
    ∃ pre suf : List Nat, l = pre ++ p :: suf ∧ (∀ q ∈ pre, q ≠ p)
  | [], _, h => by simp at h
  | a :: as, p, h => by
      by_cases hap : a = p
      · subst a
        exact ⟨[], as, rfl, by simp⟩
      · have hp : p ∈ as := by
          simp only [List.mem_cons] at h
          rcases h with hpa | hpa
          · exact (hap hpa.symm).elim
          · exact hpa
        obtain ⟨pre, suf, hl, hnp⟩ := decomp_of_mem as p hp
        exact ⟨a :: pre, suf, by simp [hl], by
          intro q hq
          simp only [List.mem_cons] at hq
          rcases hq with rfl | hq
          · exact hap
          · exact hnp q hq⟩

private theorem takeWhile_of_decomp {l pre suf : List Nat} {p : Nat}
    (hl : l = pre ++ p :: suf) (hnp : ∀ q ∈ pre, q ≠ p) :
    l.takeWhile (fun q => q != p) = pre := by
  subst l
  induction pre with
  | nil => simp
  | cons a pre ih =>
      have hne : a != p := by simpa [bne] using hnp a (by simp)
      simp [hne, ih (fun q hq => hnp q (by simp [hq]))]

private theorem eraseDups_decomp {pre suf : List Nat} {p : Nat}
    (hnp : ∀ q ∈ pre, q ≠ p) :
    ∃ T : List Nat, (pre ++ p :: suf).eraseDups = pre.eraseDups ++ p :: T := by
  rw [List.eraseDups_append]
  have hc : ¬ pre.contains p = true := by
    intro hc
    have hpmem : p ∈ pre := List.contains_iff_mem.mp hc
    exact hnp p hpmem rfl
  have hnotmem : p ∉ pre := by
    intro h
    exact hnp p h rfl
  have hcr : (p :: suf).removeAll pre = p :: suf.removeAll pre := by
    rw [List.cons_removeAll]
    simp [hnotmem]
  rw [hcr, List.eraseDups_cons]
  refine ⟨(suf.removeAll pre).filter (fun q => ! q == p) |>.eraseDups, rfl⟩

private theorem mem_take_eraseDups_iff (past : Trace) (p : Nat) (cap : Nat) :
    ((past.reverse.eraseDups).take cap).contains p ↔
      (reuseDistance past p).any (fun d => d < cap) := by
  by_cases hp : p ∈ past
  · have hpr : p ∈ past.reverse := by simpa using hp
    obtain ⟨pre, suf, hl, hnp⟩ := decomp_of_mem past.reverse p hpr
    have htw : past.reverse.takeWhile (fun q => q != p) = pre :=
      takeWhile_of_decomp hl hnp
    have hrd : reuseDistance past p = some pre.eraseDups.length := by
      simp [reuseDistance, hp, htw]
    obtain ⟨T, hded⟩ := eraseDups_decomp (pre := pre) (suf := suf) (p := p) hnp
    have hded' : past.reverse.eraseDups = pre.eraseDups ++ p :: T := by
      rw [hl]
      exact hded
    rw [hrd]
    simp only [Option.any_some]
    constructor
    · intro hmem
      have hmem' : p ∈ (past.reverse.eraseDups).take cap := List.contains_iff_mem.mp hmem
      obtain ⟨i, hi⟩ := (List.mem_iff_getElem?).mp hmem'
      have hdi : i < cap := by
        have hlen : (past.reverse.eraseDups.take cap).length ≤ cap := by
          rw [List.length_take]
          exact Nat.min_le_left _ _
        have hi_lt : i < (past.reverse.eraseDups.take cap).length := by
          exact (List.getElem?_eq_some_iff.mp hi).1
        omega
      have hdedi : (past.reverse.eraseDups)[i]? = some p := by
        simpa [List.getElem?_take, hdi] using hi
      have hdidx : (pre.eraseDups ++ p :: T)[i]? = some p := by
        simpa [hded'] using hdedi
      have hnd : (pre.eraseDups ++ p :: T).Nodup := by
        rw [← hded']
        exact nodup_eraseDups _
      have hidxp : (pre.eraseDups ++ p :: T)[pre.eraseDups.length]? = some p := by
        rw [List.getElem?_append_right]
        · simp
        · exact Nat.le_refl _
      have heq : i = pre.eraseDups.length := by
        have hlt : pre.eraseDups.length < (pre.eraseDups ++ p :: T).length := by simp
        exact ((List.Nodup.getElem?_inj hlt hnd).1 (by rw [hdidx, hidxp])).symm
      apply decide_eq_true
      omega
    · intro hd
      have hdlt : pre.eraseDups.length < cap := of_decide_eq_true hd
      have hidxp : (past.reverse.eraseDups)[pre.eraseDups.length]? = some p := by
        rw [hded']
        rw [List.getElem?_append_right]
        · simp
        · exact Nat.le_refl _
      apply List.contains_iff_mem.mpr
      apply (List.mem_iff_getElem?).mpr
      refine ⟨pre.eraseDups.length, ?_⟩
      rw [List.getElem?_take]
      simp [hdlt, hidxp]
  · have hpr : p ∉ past.reverse := by simpa using hp
    simp [reuseDistance, hp]
    intro hmem
    have hmem' : p ∈ past.reverse.eraseDups := List.mem_of_mem_take hmem
    rw [List.mem_eraseDups] at hmem'
    exact hpr hmem'

/-! LRU simulation and counting hits. -/

private def cacheStep (cap : Nat) (cache : List Nat) : Trace → List Nat
  | [] => cache
  | p :: ps => cacheStep cap ((p :: cache.erase p).take cap) ps

private theorem cacheStep_eq (cap : Nat) : ∀ (past : Trace) (cache : List Nat) (future : Trace),
    cache = (past.reverse.eraseDups).take cap →
    cacheStep cap cache future = ((future.reverse ++ past.reverse).eraseDups).take cap
  | past, cache, [], h => by simp [cacheStep, h]
  | past, cache, p :: ps, h => by
      have hc' : (p :: cache.erase p).take cap =
          (((past ++ [p]).reverse).eraseDups).take cap := by
        rw [show (past ++ [p]).reverse = p :: past.reverse by simp [List.reverse_append]]
        rw [h, eraseDups_cons_eq]
        exact cons_erase_take_of_nodup (past.reverse.eraseDups) p (nodup_eraseDups _) cap
      rw [cacheStep]
      have hih := cacheStep_eq cap (past ++ [p]) ((p :: cache.erase p).take cap) ps hc'
      simpa [List.reverse_append, List.append_assoc] using hih

private theorem foldCache_eq (cap : Nat) (trace : Trace) :
    cacheStep cap [] trace = (trace.reverse.eraseDups).take cap := by
  simpa using cacheStep_eq cap [] [] trace (by simp)

private def hitCount (cap : Nat) (cache : List Nat) : Trace → Nat
  | [] => 0
  | p :: ps => (if cache.contains p then 1 else 0) +
      hitCount cap ((p :: cache.erase p).take cap) ps

private theorem lru_eq_hitCount (cap : Nat) (trace : Trace) (cache : List Nat) :
    lru cap trace cache = hitCount cap cache trace := by
  induction trace generalizing cache with
  | nil => simp [lru, hitCount]
  | cons p ps ih => simp [lru, hitCount, ih]

private theorem zipIdx_countP_succ (ps : Trace) (f : Nat → Nat → Bool) : ∀ n : Nat,
    (ps.zipIdx (n + 1)).countP (fun x => f x.1 x.2) =
      (ps.zipIdx n).countP (fun x => f x.1 (x.2 + 1)) := by
  induction ps with
  | nil => intro n; simp [List.zipIdx]
  | cons a as ih =>
      intro n
      simp [List.zipIdx, List.countP_cons, ih (n + 1), Nat.add_assoc]

private theorem hitCount_eq_count (cap : Nat) : ∀ (past : Trace) (cache : List Nat) (future : Trace),
    cache = (past.reverse.eraseDups).take cap →
    hitCount cap cache future =
      future.zipIdx.countP
        (fun (p, j) => (reuseDistance (past ++ future.take j) p).any (fun d => d < cap))
  | past, cache, [], h => by simp [hitCount]
  | past, cache, p :: ps, h => by
      have hhit : (if cache.contains p then 1 else 0) =
          (if (reuseDistance past p).any (fun d => d < cap) then 1 else 0) := by
        rw [h]
        by_cases hh : ((past.reverse.eraseDups).take cap).contains p = true
        · have ht := (mem_take_eraseDups_iff past p cap).mp hh
          have hm : p ∈ (past.reverse.eraseDups).take cap := List.contains_iff_mem.mp hh
          simp [ht, hm]
        · have ht : ¬ (reuseDistance past p).any (fun d => d < cap) = true := by
            intro ht
            exact hh ((mem_take_eraseDups_iff past p cap).mpr ht)
          have hm : p ∉ (past.reverse.eraseDups).take cap := by
            intro hm
            exact hh (show ((past.reverse.eraseDups).take cap).contains p from
              List.contains_iff_mem.mpr hm)
          simp [ht, hm]
      have hc' : (p :: cache.erase p).take cap =
          (((past ++ [p]).reverse).eraseDups).take cap := by
        rw [show (past ++ [p]).reverse = p :: past.reverse by simp [List.reverse_append]]
        rw [h, eraseDups_cons_eq]
        exact cons_erase_take_of_nodup (past.reverse.eraseDups) p (nodup_eraseDups _) cap
      rw [hitCount, hhit]
      rw [List.zipIdx_cons]
      simp only [List.countP_cons, Nat.zero_add]
      -- the head term is the same on both sides
      have hhead :
          (if (reuseDistance (past ++ (p :: ps).take 0) p).any (fun d => d < cap) then 1 else 0) =
          (if (reuseDistance past p).any (fun d => d < cap) then 1 else 0) := by
        simp
      rw [hhead]
      rw [Nat.add_comm]
      congr 1
      have hih := hitCount_eq_count cap (past ++ [p]) ((p :: cache.erase p).take cap) ps hc'
      rw [hih]
      rw [show ps.zipIdx 1 = ps.zipIdx (0 + 1) by rfl]
      have hshift := zipIdx_countP_succ ps
        (fun q j => (reuseDistance (past ++ (p :: ps).take j) q).any (fun d => d < cap)) 0
      rw [hshift]
      congr 1
      funext x
      simp [List.append_assoc]

theorem StackDistance_sound : StackDistance.Goal := by
  intro trace cap
  rw [lru_eq_hitCount]
  simpa using hitCount_eq_count cap [] [] trace (by simp)

open TinylambOpt.ResearchGoals.Frontiers.SpillScheduling

private theorem zipIdx_nodup : ∀ (t : Trace) (n : Nat), (t.zipIdx n).Nodup
  | [], _ => by simp [List.zipIdx]
  | a :: as, n => by
      simp only [List.zipIdx_cons, List.nodup_cons]
      constructor
      · intro hmem
        obtain ⟨hlo, hhi, heq⟩ := List.mem_zipIdx hmem
        omega
      · exact zipIdx_nodup as (n + 1)

private theorem nodup_filter_pairs {l : List (Nat × Nat)} (h : l.Nodup) (p : Nat × Nat → Bool) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha : a ∉ as := (List.nodup_cons.mp h).1
      have hnd : as.Nodup := (List.nodup_cons.mp h).2
      by_cases hp : p a = true
      · simp [List.filter_cons_of_pos hp, List.nodup_cons, ha, ih hnd]
      · have hp' : ¬ p a := by simpa using hp
        simp [List.filter_cons_of_neg hp', ih hnd]

private theorem nodup_map_pairs {l : List (Nat × Nat)} (f : Nat × Nat → Arc)
    (hinj : ∀ x ∈ l, ∀ y ∈ l, f x = f y → x = y) (h : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha : a ∉ as := (List.nodup_cons.mp h).1
      have hnd : as.Nodup := (List.nodup_cons.mp h).2
      have hinj' : ∀ x ∈ as, ∀ y ∈ as, f x = f y → x = y := by
        intro x hx y hy hxy
        exact hinj x (by simp [hx]) y (by simp [hy]) hxy
      have hndmap := ih hinj' hnd
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        obtain ⟨x, hx, hfx⟩ := List.mem_map.mp hmem
        have hxa : x = a := hinj x (by simp [hx]) a (by simp) (by simpa using hfx)
        subst x
        exact ha hx
      · exact hndmap

private theorem getElem?_of_zipIdx_mem {t : Trace} {p i : Nat} (h : (p, i) ∈ t.zipIdx) :
    t[i]? = some p := by
  obtain ⟨hlo, hhi, heq⟩ := List.mem_zipIdx h
  have heq' : p = t[i] := by simpa using heq
  have hi : i < t.length := by simpa using hhi
  rw [heq']
  exact List.getElem?_eq_getElem hi

private theorem mem_arcs_spec {t : Trace} {a b : Nat} (h : (a, b) ∈ arcs t) :
    a < t.length ∧ b < t.length ∧ a < b ∧ t[a]? = t[b]? ∧ t[b]? ≠ none ∧
      (∀ m, a < m → m < b → t[m]? ≠ t[a]?) := by
  rw [arcs] at h
  obtain ⟨x, hx, hfx⟩ := List.mem_filterMap.mp h
  rcases x with ⟨p, i⟩
  have hpi : t[i]? = some p := getElem?_of_zipIdx_mem hx
  obtain ⟨hlo, hhi, heq⟩ := List.mem_zipIdx hx
  have hilen : i < t.length := by simpa using hhi
  obtain ⟨k, hk, hkb⟩ := Option.map_eq_some_iff.mp hfx
  have hai : i = a := congrArg Prod.fst hkb
  have hbi : i + 1 + k = b := congrArg Prod.snd hkb
  subst a
  subst b
  obtain ⟨hklt, hkval, hkfirst⟩ := List.idxOf?_eq_some_iff.mp hk
  have hbval : t[i + 1 + k]? = some p := by
    have hkd : (t.drop (i + 1))[k]? = some p :=
      List.getElem?_eq_some_iff.mpr ⟨hklt, hkval⟩
    simpa [List.getElem?_drop, Nat.add_assoc] using hkd
  have hb_lt : i + 1 + k < t.length := (List.getElem?_eq_some_iff.mp hbval).1
  refine ⟨by omega, hb_lt, by omega, ?_, ?_, ?_⟩
  · rw [hpi, hbval]
  · rw [hbval]; simp
  · intro m ham hmb
    have hklt' : m - (i + 1) < k := by omega
    have hdropne : (t.drop (i + 1))[m - (i + 1)] ≠ p := hkfirst _ hklt'
    have hmopt : t[m]? ≠ some p := by
      intro hm
      have hdm : (t.drop (i + 1))[m - (i + 1)]? = some p := by
        rw [List.getElem?_drop]
        have hsum : i + 1 + (m - (i + 1)) = m := by omega
        simpa [hsum] using hm
      exact hdropne ((List.getElem?_eq_some_iff.mp hdm).2)
    rw [hpi]
    exact hmopt

private theorem arcs_unique_start {t : Trace} {a b b' : Nat}
    (h1 : (a, b) ∈ arcs t) (h2 : (a, b') ∈ arcs t) : b = b' := by
  obtain ⟨ha1, hb1, hab1, hval1, hne1, hn1⟩ := mem_arcs_spec h1
  obtain ⟨ha2, hb2, hab2, hval2, hne2, hn2⟩ := mem_arcs_spec h2
  apply Nat.le_antisymm
  · by_cases hbb : b ≤ b'
    · exact hbb
    · have hbb' : b' < b := by omega
      have hneq := hn1 b' (by omega) hbb'
      exact (hneq hval2.symm).elim
  · by_cases hbb : b' ≤ b
    · exact hbb
    · have hbb' : b < b' := by omega
      have hneq := hn2 b (by omega) hbb'
      exact (hneq hval1.symm).elim

private theorem arcs_unique_end {t : Trace} {a a' b : Nat}
    (h1 : (a, b) ∈ arcs t) (h2 : (a', b) ∈ arcs t) : a = a' := by
  obtain ⟨ha1, hb1, hab1, hval1, hne1, hn1⟩ := mem_arcs_spec h1
  obtain ⟨ha2, hb2, hab2, hval2, hne2, hn2⟩ := mem_arcs_spec h2
  apply Nat.le_antisymm
  · by_cases haa : a ≤ a'
    · exact haa
    · have haa' : a' < a := by omega
      have hneq := hn2 a haa' (by omega)
      exact (hneq (hval1.trans hval2.symm)).elim
  · by_cases haa : a' ≤ a
    · exact haa
    · have haa' : a < a' := by omega
      have hneq := hn1 a' haa' (by omega)
      exact (hneq (hval2.trans hval1.symm)).elim

/-! Last previous access of a page before a given index. -/

private def lastOcc (t : Trace) : Nat → Nat → Option Nat
  | 0, _ => none
  | i + 1, p => if t[i]? = some p then some i else lastOcc t i p

private theorem lastOcc_spec {t : Trace} : ∀ (i p j : Nat), lastOcc t i p = some j →
    j < i ∧ t[j]? = some p ∧ (∀ m, j < m → m < i → t[m]? ≠ some p)
  | 0, _, _, h => by simp [lastOcc] at h
  | i + 1, p, j, h => by
      by_cases hi : t[i]? = some p
      · simp [lastOcc, hi] at h
        subst j
        refine ⟨by omega, hi, ?_⟩
        intro m hm1 hm2
        omega
      · simp [lastOcc, hi] at h
        obtain ⟨hji, hjp, hnone⟩ := lastOcc_spec i p j h
        refine ⟨by omega, hjp, ?_⟩
        intro m hjm hmi
        by_cases hm : m = i
        · subst m
          simpa using hi
        · exact hnone m hjm (by omega)

private theorem lastOcc_exists_of_state {t : Trace} {cap : Nat} {state : Nat → List Nat}
    (hlegal : legal t cap state) : ∀ i, i ≤ t.length → ∀ p, p ∈ state i →
      ∃ j, lastOcc t i p = some j
  | 0, hi, p, hp => by
      have h0 := hlegal.1
      rw [h0] at hp
      simp at hp
  | i + 1, hi, p, hp => by
      have hi_lt : i < t.length := by omega
      have hstep := (hlegal.2 i hi_lt).2 p hp
      rcases hstep with hp_mem | hp_acc
      · by_cases hacc : t[i]? = some p
        · exact ⟨i, by simp [lastOcc, hacc]⟩
        · obtain ⟨j, hj⟩ := lastOcc_exists_of_state hlegal i (by omega) p hp_mem
          exact ⟨j, by simp [lastOcc, hacc, hj]⟩
      · exact ⟨i, by simp [lastOcc, hp_acc]⟩

private theorem idxOf_next_of_lastOcc {t : Trace} {i p j : Nat}
    (hlo : lastOcc t i p = some j) (hpi : t[i]? = some p) :
    (t.drop (j + 1)).idxOf? p = some (i - (j + 1)) := by
  obtain ⟨hji, hjp, hnone⟩ := lastOcc_spec i p j hlo
  have hopt : (t.drop (j + 1))[i - (j + 1)]? = some p := by
    rw [List.getElem?_drop]
    have hsum : j + 1 + (i - (j + 1)) = i := by omega
    simpa [hsum] using hpi
  have hidxlt : i - (j + 1) < (t.drop (j + 1)).length := by
    have hne : (t.drop (j + 1))[i - (j + 1)]? ≠ none := by rw [hopt]; simp
    have hnotge : ¬ (t.drop (j + 1)).length ≤ i - (j + 1) := by
      intro hge
      exact hne (List.getElem?_eq_none hge)
    omega
  apply List.idxOf?_eq_some_iff.mpr
  refine ⟨hidxlt, (List.getElem?_eq_some_iff.mp hopt).2, ?_⟩
  intro k' hk'
  intro hval
  have hdm : (t.drop (j + 1))[k']? = some p := by
    exact List.getElem?_eq_some_iff.mpr ⟨by omega, hval⟩
  rw [List.getElem?_drop] at hdm
  have hm : j + 1 + k' < i := by omega
  exact hnone (j + 1 + k') (by omega) hm hdm

private theorem arc_of_hit {t : Trace} {cap : Nat} {state : Nat → List Nat}
    (hlegal : legal t cap state) {p i : Nat}
    (hpi : t[i]? = some p) (hhit : p ∈ state i) :
    let j := (lastOcc t i p).getD 0
    (j, i) ∈ arcs t := by
  have hi_lt : i < t.length := (List.getElem?_eq_some_iff.mp hpi).1
  obtain ⟨j, hj⟩ := lastOcc_exists_of_state hlegal i (Nat.le_of_lt hi_lt) p hhit
  dsimp only
  rw [show (lastOcc t i p).getD 0 = j by simp [hj]]
  have hidx := idxOf_next_of_lastOcc hj hpi
  rw [arcs]
  apply List.mem_filterMap.mpr
  refine ⟨(p, j), ?_, ?_⟩
  · apply (List.mem_iff_getElem?).mpr
    refine ⟨j, ?_⟩
    rw [List.getElem?_zipIdx]
    obtain ⟨_, hjp, _⟩ := lastOcc_spec i p j hj
    simp [hjp]
  · simp only
    rw [hidx]
    have hend : j + 1 + (i - (j + 1)) = i := by
      obtain ⟨hji, _, _⟩ := lastOcc_spec i p j hj
      omega
    simp [hend]

/-! Backward propagation of residency. -/

private theorem state_mono {t : Trace} {cap : Nat} {state : Nat → List Nat}
    (hlegal : legal t cap state) :
    ∀ i p m, i ≤ m → m ≤ t.length → p ∈ state m →
      (∀ k, i ≤ k → k < m → t[k]? ≠ some p) → p ∈ state i := by
  intro i p m
  induction m generalizing i p with
  | zero =>
      intro him hml hpm hno
      have hi0 : i = 0 := by omega
      subst i
      exact hpm
  | succ m ih =>
      intro him hml hpm hno
      by_cases hi : i = m + 1
      · subst i
        exact hpm
      · have him' : i ≤ m := by omega
        have hmlt : m < t.length := by omega
        have htm : t[m]? ≠ some p := hno m him' (by omega)
        have hstep := (hlegal.2 m hmlt).2 p hpm
        have hpm' : p ∈ state m := by
          rcases hstep with hmem | hacc
          · exact hmem
          · exact (htm hacc).elim
        exact ih i p him' (by omega) hpm' (by
          intro k hik hkm
          exact hno k hik (by omega))

private theorem mem_state_of_arc_hit {t : Trace} {cap : Nat} {state : Nat → List Nat}
    (hlegal : legal t cap state) {p a b i : Nat}
    (harc : (a, b) ∈ arcs t) (hpa : t[a]? = some p)
    (hhit : p ∈ state b) (hcova : a ≤ i) (hicovb : i < b) : p ∈ state (i + 1) := by
  obtain ⟨ha_lt, hb_lt, hab, hval, hne, hno⟩ := mem_arcs_spec harc
  apply state_mono hlegal (i + 1) p b
  · omega
  · exact Nat.le_of_lt hb_lt
  · exact hhit
  · intro k hik hkb
    have hak : a < k := by omega
    rw [← hpa]
    exact hno k hak hkb

/-! Hits and arcs derived from a legal state. -/

private def hits (t : Trace) (state : Nat → List Nat) : List (Nat × Nat) :=
  t.zipIdx.filter (fun (p, i) => (state i).contains p)

private def chosenOf (t : Trace) (state : Nat → List Nat) : List Arc :=
  (hits t state).map (fun x => ((lastOcc t x.2 x.1).getD 0, x.2))

private theorem hits_length (t : Trace) (state : Nat → List Nat) :
    (hits t state).length = saved t state := by
  rw [saved, List.countP_eq_length_filter]
  rfl

private theorem chosenOf_length (t : Trace) (state : Nat → List Nat) :
    (chosenOf t state).length = saved t state := by
  rw [chosenOf, List.length_map, hits_length]

private theorem chosenOf_nodup {t : Trace} {cap : Nat} {state : Nat → List Nat}
    (_hlegal : legal t cap state) : (chosenOf t state).Nodup := by
  unfold chosenOf
  apply nodup_map_pairs
  · intro x hx y hy hxy
    have hxzip : x ∈ t.zipIdx := (List.mem_filter.mp hx).1
    have hyzip : y ∈ t.zipIdx := (List.mem_filter.mp hy).1
    have hsnd : x.2 = y.2 := by
      injection hxy with hfst hsnd
    rcases x with ⟨px, ix⟩
    rcases y with ⟨py, iy⟩
    simp only at hsnd
    subst iy
    have hpx := getElem?_of_zipIdx_mem hxzip
    have hpy := getElem?_of_zipIdx_mem hyzip
    have hpeq : px = py := by
      rw [hpx] at hpy
      cases hpy
      rfl
    subst py
    rfl
  · exact nodup_filter_pairs (zipIdx_nodup t 0) _

private theorem chosenOf_mem_arcs {t : Trace} {cap : Nat} {state : Nat → List Nat}
    (hlegal : legal t cap state) : ∀ e ∈ chosenOf t state, e ∈ arcs t := by
  intro e he
  obtain ⟨x, hx, rfl⟩ := List.mem_map.mp he
  have hxzip : x ∈ t.zipIdx := (List.mem_filter.mp hx).1
  have hxhit : (state x.2).contains x.1 = true := (List.mem_filter.mp hx).2
  exact arc_of_hit hlegal (getElem?_of_zipIdx_mem hxzip) (List.contains_iff_mem.mp hxhit)

private theorem nodup_map_generic {α β : Type} [DecidableEq β] {l : List α} (f : α → β)
    (hinj : ∀ x ∈ l, ∀ y ∈ l, f x = f y → x = y) (h : l.Nodup) : (l.map f).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha : a ∉ as := (List.nodup_cons.mp h).1
      have hnd : as.Nodup := (List.nodup_cons.mp h).2
      have hinj' : ∀ x ∈ as, ∀ y ∈ as, f x = f y → x = y := by
        intro x hx y hy hxy
        exact hinj x (by simp [hx]) y (by simp [hy]) hxy
      rw [List.map_cons, List.nodup_cons]
      constructor
      · intro hmem
        obtain ⟨x, hx, hfx⟩ := List.mem_map.mp hmem
        have hxa : x = a := hinj x (by simp [hx]) a (by simp) (by simpa using hfx)
        subst x
        exact ha hx
      · exact ih hinj' hnd

private theorem cover_of_bool {a i b : Nat} (h : (a ≤ i && i < b) = true) : a ≤ i ∧ i < b := by
  exact of_decide_eq_true (by simpa using h)

private def coverPages (t : Trace) (chosen : List Arc) (i : Nat) : List Nat :=
  (chosen.filter (fun (a, b) => a ≤ i && i < b)).map (fun e => (t[e.1]?).getD 0)

private theorem coverPages_length (t : Trace) (chosen : List Arc) (i : Nat) :
    (coverPages t chosen i).length = chosen.countP (fun (a, b) => a ≤ i && i < b) := by
  rw [coverPages, List.length_map, List.countP_eq_length_filter]

private theorem coverPages_nodup {t : Trace} {chosen : List Arc} {i : Nat}
    (hnd : chosen.Nodup) (hsub : ∀ e ∈ chosen, e ∈ arcs t) :
    (coverPages t chosen i).Nodup := by
  unfold coverPages
  apply nodup_map_generic
  · intro x hx y hy hval
    have hxf : x ∈ chosen.filter (fun (a, b) => a ≤ i && i < b) := hx
    have hyf : y ∈ chosen.filter (fun (a, b) => a ≤ i && i < b) := hy
    have hxc : x ∈ chosen := (List.mem_filter.mp hx).1
    have hyc : y ∈ chosen := (List.mem_filter.mp hy).1
    have hxa := hsub x hxc
    have hya := hsub y hyc
    obtain ⟨hxa_lt, hxb_lt, hxab, hxval, hxne, hxno⟩ := mem_arcs_spec hxa
    obtain ⟨hya_lt, hyb_lt, hyab, hyval, hyne, hyno⟩ := mem_arcs_spec hya
    have hxopt : t[x.1]? ≠ none := by rw [hxval]; exact hxne
    have hyopt : t[y.1]? ≠ none := by rw [hyval]; exact hyne
    have hopts : t[x.1]? = t[y.1]? := by
      cases hox : t[x.1]? with
      | none => exact (hxopt hox).elim
      | some px =>
          cases hoy : t[y.1]? with
          | none => exact (hyopt hoy).elim
          | some py =>
              rw [hox, hoy] at hval
              have hpxy : px = py := by simpa using hval
              rw [hpxy]
    apply Prod.ext
    · by_cases hle : x.1 ≤ y.1
      · by_cases heq : x.1 = y.1
        · exact heq
        · have hlt : x.1 < y.1 := by omega
          have hxcover := (List.mem_filter.mp hxf).2
          have hycover := (List.mem_filter.mp hyf).2
          have hy_le_i : y.1 ≤ i := (cover_of_bool hycover).1
          have hi_lt_xb : i < x.2 := (cover_of_bool hxcover).2
          have hneq := hxno y.1 hlt (by omega)
          exact (hneq (by rw [hopts])).elim
      · have hlt : y.1 < x.1 := by omega
        have hxcover := (List.mem_filter.mp hxf).2
        have hycover := (List.mem_filter.mp hyf).2
        have hx_le_i : x.1 ≤ i := (cover_of_bool hxcover).1
        have hi_lt_yb : i < y.2 := (cover_of_bool hycover).2
        have hneq := hyno x.1 hlt (by omega)
        exact (hneq (by rw [← hopts])).elim
    · by_cases hfst : x.1 = y.1
      · exact arcs_unique_start hxa (by simpa [hfst] using hya)
      · -- distinct starts cannot share an end while both cover i
        have hneq : x.2 ≠ y.2 := by
          intro hend
          by_cases hle : x.1 < y.1
          · have hxcover := (List.mem_filter.mp hxf).2
            have hycover := (List.mem_filter.mp hyf).2
            have hy_le_i : y.1 ≤ i := (cover_of_bool hycover).1
            have hi_lt_xb : i < x.2 := (cover_of_bool hxcover).2
            have hneq' := hxno y.1 hle (by omega)
            exact hneq' (by rw [hopts])
          · have hgt : y.1 < x.1 := by omega
            have hxcover := (List.mem_filter.mp hxf).2
            have hycover := (List.mem_filter.mp hyf).2
            have hx_le_i : x.1 ≤ i := (cover_of_bool hxcover).1
            have hi_lt_yb : i < y.2 := (cover_of_bool hycover).2
            have hneq' := hyno x.1 hgt (by omega)
            exact hneq' (by rw [← hopts])
        -- if ends differ, the two arcs cannot both cover i with distinct starts and equal page
        by_cases hle : x.1 < y.1
        · have hxcover := (List.mem_filter.mp hxf).2
          have hycover := (List.mem_filter.mp hyf).2
          have hy_le_i : y.1 ≤ i := (cover_of_bool hycover).1
          have hi_lt_xb : i < x.2 := (cover_of_bool hxcover).2
          have hneq' := hxno y.1 hle (by omega)
          exact (hneq' (by rw [hopts])).elim
        · have hgt : y.1 < x.1 := by omega
          have hxcover := (List.mem_filter.mp hxf).2
          have hycover := (List.mem_filter.mp hyf).2
          have hx_le_i : x.1 ≤ i := (cover_of_bool hxcover).1
          have hi_lt_yb : i < y.2 := (cover_of_bool hycover).2
          have hneq' := hyno x.1 hgt (by omega)
          exact (hneq' (by rw [← hopts])).elim
  · exact nodup_filter_pairs hnd _

private theorem coverPages_subset {t : Trace} {cap : Nat} {state : Nat → List Nat}
    (hlegal : legal t cap state) (i : Nat) (_hi_lt : i < t.length) :
    coverPages t (chosenOf t state) i ⊆ state (i + 1) := by
  intro p hp
  rcases List.mem_map.mp hp with ⟨e, hef, rfl⟩
  have hecov : ((fun z : Nat × Nat => z.1 ≤ i && i < z.2) e) = true := (List.mem_filter.mp hef).2
  have hechosen : e ∈ chosenOf t state := (List.mem_filter.mp hef).1
  obtain ⟨a, b⟩ := e
  have ha_le : a ≤ i := (cover_of_bool hecov).1
  have hib : i < b := (cover_of_bool hecov).2
  obtain ⟨x, hx, hxmap⟩ := List.mem_map.mp hechosen
  have hxzip : x ∈ t.zipIdx := (List.mem_filter.mp hx).1
  have hxhitb : x.1 ∈ state x.2 := List.contains_iff_mem.mp (List.mem_filter.mp hx).2
  have hpi := getElem?_of_zipIdx_mem hxzip
  have hb_eq : x.2 = b := by
    injection hxmap with hfst hsnd
  have ha_eq : (lastOcc t x.2 x.1).getD 0 = a := by
    injection hxmap with hfst hsnd
  subst b
  have hx_lt : x.2 < t.length := (List.getElem?_eq_some_iff.mp hpi).1
  obtain ⟨j, hj⟩ := lastOcc_exists_of_state hlegal x.2 (Nat.le_of_lt hx_lt) x.1 hxhitb
  have hjD : (lastOcc t x.2 x.1).getD 0 = j := by simp [hj]
  have haj : a = j := by omega
  subst a
  obtain ⟨hji, hjp, _⟩ := lastOcc_spec x.2 x.1 j hj
  have harc0 := arc_of_hit hlegal hpi hxhitb
  have harc : (j, x.2) ∈ arcs t := by simpa [hj] using harc0
  have hres := mem_state_of_arc_hit hlegal harc hjp hxhitb (by omega) hib
  have hpage : (t[(lastOcc t x.2 x.1).getD 0]?).getD 0 = x.1 := by
    simp [hj, hjp]
  simpa [hpage] using hres

private theorem chosenOf_fits {t : Trace} {cap : Nat} {state : Nat → List Nat}
    (hlegal : legal t cap state) : fits t cap (chosenOf t state) := by
  rw [fits, List.all_eq_true]
  intro i hi
  have hi_lt : i < t.length := List.mem_range.mp hi
  rw [← coverPages_length]
  have h1 : (coverPages t (chosenOf t state) i).length ≤ (state (i + 1)).length :=
    List.Nodup.length_le_of_subset
      (coverPages_nodup (chosenOf_nodup hlegal) (chosenOf_mem_arcs hlegal))
      (coverPages_subset hlegal i hi_lt)
  have h2 : (state (i + 1)).length ≤ cap := (hlegal.2 i hi_lt).1
  exact decide_eq_true (Nat.le_trans h1 h2)

private theorem OfflineCache_forward {t : Trace} {cap k : Nat}
    (h : ∃ state, legal t cap state ∧ k ≤ saved t state) :
    ∃ chosen : List Arc, chosen.Nodup ∧ (∀ e ∈ chosen, e ∈ arcs t) ∧
      fits t cap chosen ∧ k ≤ chosen.length := by
  obtain ⟨state, hlegal, hk⟩ := h
  refine ⟨chosenOf t state, chosenOf_nodup hlegal, chosenOf_mem_arcs hlegal,
    chosenOf_fits hlegal, ?_⟩
  rw [chosenOf_length]
  exact hk

private theorem filterMap_length_eq_filter {α : Type} (l : List α) (f : α → Option Nat)
    (g : α → Bool) (h : ∀ x ∈ l, ∃ n, f x = some n) :
    (l.filterMap (fun x => if g x then f x else none)).length = (l.filter g).length := by
  induction l with
  | nil => simp
  | cons a as ih =>
      obtain ⟨n, hn⟩ := h a (by simp)
      have h' : ∀ x ∈ as, ∃ m, f x = some m := by
        intro x hx
        exact h x (by simp [hx])
      by_cases hg : g a = true
      · simp [hg, hn, ih h']
      · have hg' : ¬ g a := by simpa using hg
        simp [hg, ih h']

private def stateOf (t : Trace) (chosen : List Arc) (i : Nat) : List Nat :=
  chosen.filterMap (fun e => if e.1 < i && i ≤ e.2 then t[e.1]? else none)

private theorem stateOf_length {t : Trace} {chosen : List Arc} {i : Nat}
    (hsub : ∀ e ∈ chosen, e ∈ arcs t) :
    (stateOf t chosen (i + 1)).length = chosen.countP (fun (a, b) => a ≤ i && i < b) := by
  unfold stateOf
  rw [filterMap_length_eq_filter]
  · rw [List.countP_eq_length_filter]
    congr 1
  · intro e he
    have hearc := hsub e he
    obtain ⟨ha, hb, hab, hval, hne, hno⟩ := mem_arcs_spec hearc
    have hane : t[e.1]? ≠ none := by rw [hval]; exact hne
    cases hopt : t[e.1]? with
    | none => exact (hane hopt).elim
    | some n => exact ⟨n, rfl⟩

private theorem stateOf_legal {t : Trace} {cap : Nat} {chosen : List Arc}
    (hsub : ∀ e ∈ chosen, e ∈ arcs t) (hfit : fits t cap chosen) :
    legal t cap (stateOf t chosen) := by
  constructor
  · simp [stateOf]
  · intro i hi
    have hi_lt : i < t.length := hi
    constructor
    · rw [stateOf_length hsub]
      exact of_decide_eq_true ((List.all_eq_true.mp hfit) i (List.mem_range.mpr hi_lt))
    · intro p hp
      obtain ⟨e, he, heval⟩ := List.mem_filterMap.mp hp
      have hearc := hsub e he
      by_cases hcov : e.1 < i + 1 && i + 1 ≤ e.2
      · have hpe : t[e.1]? = some p := by simpa [hcov] using heval
        have hc1 : e.1 ≤ i := (cover_of_bool hcov).1
        have hc2 : i < e.2 := (cover_of_bool hcov).2
        by_cases hai : e.1 = i
        · right
          simpa [hai] using hpe
        · left
          apply List.mem_filterMap.mpr
          refine ⟨e, he, ?_⟩
          have hlt : e.1 < i := by omega
          have hle : i ≤ e.2 := by omega
          have hcovb : (e.1 < i && i ≤ e.2) = true := by simp [hlt, hle]
          simp [hcovb, hpe]
      · simp [hcov] at heval

private theorem stateOf_saved {t : Trace} {chosen : List Arc}
    (hnd : chosen.Nodup) (hsub : ∀ e ∈ chosen, e ∈ arcs t) :
    chosen.length ≤ saved t (stateOf t chosen) := by
  rw [saved, List.countP_eq_length_filter]
  have hends_nodup : (chosen.map (fun e : Arc => e.2)).Nodup := by
    apply nodup_map_generic
    · intro x hx y hy hxy
      apply Prod.ext
      · exact arcs_unique_end (hsub x hx) (by simpa [hxy] using hsub y hy)
      · exact hxy
    · exact hnd
  have hsubends : chosen.map (fun e : Arc => e.2) ⊆
      (t.zipIdx.filter (fun (p, j) => (stateOf t chosen j).contains p)).map
        (fun x : Nat × Nat => x.2) := by
    intro b hb
    obtain ⟨e, he, rfl⟩ := List.mem_map.mp hb
    have hearc := hsub e he
    obtain ⟨ha, hb_lt, hab, hval, hne, hno⟩ := mem_arcs_spec hearc
    apply List.mem_map.mpr
    refine ⟨((t[e.2]?).getD 0, e.2), ?_, rfl⟩
    apply List.mem_filter.mpr
    constructor
    · have hsome : t[e.2]? = some ((t[e.2]?).getD 0) := by
        cases hopt : t[e.2]? with
        | none => exact (hne hopt).elim
        | some n => simp
      have hz : (t.zipIdx)[e.2]? = some ((t[e.2]?).getD 0, e.2) := by
        rw [List.getElem?_zipIdx, hsome]
        simp
      exact (List.mem_iff_getElem?).mpr ⟨e.2, hz⟩
    · apply List.contains_iff_mem.mpr
      apply List.mem_filterMap.mpr
      refine ⟨e, he, ?_⟩
      have hcovb : (e.1 < e.2 && e.2 ≤ e.2) = true := by simp [hab]
      have hsome : t[e.2]? = some ((t[e.2]?).getD 0) := by
        cases hopt : t[e.2]? with
        | none => exact (hne hopt).elim
        | some n => simp
      rw [hcovb, hval, hsome]
      rfl
  have hle := List.Nodup.length_le_of_subset hends_nodup hsubends
  simpa using hle

private theorem OfflineCache_backward {t : Trace} {cap k : Nat}
    (h : ∃ chosen : List Arc, chosen.Nodup ∧ (∀ e ∈ chosen, e ∈ arcs t) ∧
      fits t cap chosen ∧ k ≤ chosen.length) :
    ∃ state, legal t cap state ∧ k ≤ saved t state := by
  obtain ⟨chosen, hnd, hsub, hfit, hk⟩ := h
  exact ⟨stateOf t chosen, stateOf_legal hsub hfit,
    Nat.le_trans hk (stateOf_saved hnd hsub)⟩

theorem OfflineCache_sound : OfflineCache.Goal := by
  intro t cap k
  constructor
  · exact OfflineCache_forward
  · exact OfflineCache_backward

end TinylambOpt.ResearchProofs.Frontiers.SpillSchedulingCache
