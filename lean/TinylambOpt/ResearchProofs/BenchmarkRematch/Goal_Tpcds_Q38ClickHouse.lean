import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q38ClickHouse

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem sum_map_zero : ∀ (l : List Nat), (l.map (fun _ => 0)).sum = 0 := by
  intro l
  induction l with
  | nil => rfl
  | cons a l ih => rw [List.map_cons, List.sum_cons, ih]

private theorem sum_map_add {α : Type} :
    ∀ (l : List α) (f g : α → Nat),
      (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  intro l
  induction l with
  | nil => intro f g; rfl
  | cons a l ih =>
    intro f g
    rw [List.map_cons, List.sum_cons, ih f g, List.map_cons, List.map_cons,
      List.sum_cons, List.sum_cons]
    omega

private theorem ite_count (y : Nat) :
    ∀ (n : Nat), 0 < n → y < n →
      ((List.range n).map (fun p => if y = p then 1 else 0)).sum = 1 := by
  intro n
  induction n with
  | zero => intro hn; omega
  | succ m ih =>
    intro hn hy
    rw [List.range_succ, List.map_append, List.sum_append, List.map_cons, List.sum_cons,
      List.map_nil, List.sum_nil, Nat.add_zero]
    by_cases hcm : y = m
    · rw [hcm]
      have hz : (List.range m).map (fun p => if m = p then 1 else 0) =
          (List.range m).map (fun _ => 0) := by
        refine List.map_congr_left (fun p hp => ?_)
        simp only [List.mem_range] at hp
        rw [ite_eq_right (Nat.ne_of_gt hp)]
      rw [hz, sum_map_zero]
      simp
    · have hym : y < m := by omega
      rw [ih (by omega) hym, ite_eq_right hcm]

private theorem bucketSum (F : Nat × Nat → Bool) :
    ∀ (l : List (Nat × Nat)) (n : Nat), 0 < n →
      ((List.range n).map
          (fun p => (l.filter (fun k => (k.2 % n == p) && F k)).length)).sum =
        (l.filter F).length := by
  intro l
  induction l with
  | nil =>
    intro n hn
    simp only [List.filter_nil, List.length_nil, sum_map_zero]
  | cons x l ih =>
    intro n hn
    by_cases hF : F x
    · have key : ∀ p,
          ((x :: l).filter (fun k => (k.2 % n == p) && F k)).length =
            (l.filter (fun k => (k.2 % n == p) && F k)).length +
              if x.2 % n = p then 1 else 0 := by
        intro p
        by_cases hp : x.2 % n = p
        · rw [ite_eq_left hp,
            List.filter_cons_of_pos
              (p := fun k : Nat × Nat => (k.2 % n == p) && F k)
              (show (fun k => (k.2 % n == p) && F k) x = true from by simp [hp, hF]),
            List.length_cons]
        · rw [ite_eq_right hp,
            List.filter_cons_of_neg
              (p := fun k : Nat × Nat => (k.2 % n == p) && F k)
              (show ¬((fun k => (k.2 % n == p) && F k) x = true) from by simp [hp, hF])]
          omega
      rw [show (List.range n).map
              (fun p => ((x :: l).filter (fun k => (k.2 % n == p) && F k)).length) =
            (List.range n).map
              (fun p => (l.filter (fun k => (k.2 % n == p) && F k)).length +
                if x.2 % n = p then 1 else 0) from
          List.map_congr_left (fun p _ => key p)]
      rw [sum_map_add, ih n hn, ite_count (x.2 % n) n hn (Nat.mod_lt x.2 hn)]
      rw [List.filter_cons_of_pos hF, List.length_cons]
    · rw [show (List.range n).map
            (fun p => ((x :: l).filter (fun k => (k.2 % n == p) && F k)).length) =
          (List.range n).map (fun p => (l.filter (fun k => (k.2 % n == p) && F k)).length) from
        List.map_congr_left (fun p _ =>
          congrArg List.length
            (List.filter_cons_of_neg
              (p := fun k : Nat × Nat => (k.2 % n == p) && F k)
              (show ¬((fun k => (k.2 % n == p) && F k) x = true) from by simp [hF])))]
      rw [ih n hn, List.filter_cons_of_neg hF]

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] :
    ∀ (l : List α), l.eraseDups.Nodup := by
  have key : ∀ (n : Nat) (l : List α), l.length ≤ n → l.eraseDups.Nodup := by
    intro n
    induction n with
    | zero =>
      intro l hl
      cases l with
      | nil => exact List.Pairwise.nil
      | cons a t =>
        have h1 : (a :: t).length ≤ 0 := hl
        simp only [List.length_cons] at h1
        omega
    | succ m ih =>
      intro l hl
      cases l with
      | nil => exact List.Pairwise.nil
      | cons a t =>
        rw [List.eraseDups_cons, List.nodup_cons]
        refine ⟨?_, ih (t.filter (fun b => !b == a)) ?_⟩
        · intro hx
          have h2 := (List.mem_filter.mp (List.mem_eraseDups.mp hx)).2
          simp at h2
        · have hlen : (a :: t).length = t.length + 1 := List.length_cons
          have ht : t.length ≤ m := by omega
          exact Nat.le_trans (List.length_filter_le _ _) ht
  intro l
  exact key l.length l (Nat.le_refl _)

private theorem eraseDups_id {α : Type} [BEq α] [LawfulBEq α] (l : List α) (h : l.Nodup) :
    l.eraseDups = l := by
  have key : ∀ (n : Nat) (l : List α), l.length ≤ n → l.Nodup → l.eraseDups = l := by
    intro n
    induction n with
    | zero =>
      intro l hl hn
      cases l with
      | nil => rfl
      | cons a t =>
        have h1 : (a :: t).length ≤ 0 := hl
        simp only [List.length_cons] at h1
        omega
    | succ m ih =>
      intro l hl hn
      cases l with
      | nil => rfl
      | cons a t =>
        obtain ⟨hnin, hNt⟩ := List.nodup_cons.mp hn
        have hlen : (a :: t).length = t.length + 1 := List.length_cons
        have ht : t.length ≤ m := by omega
        rw [List.eraseDups_cons]
        have hfe : t.filter (fun b => !b == a) = t := by
          refine List.filter_eq_self.mpr (fun b hb => ?_)
          cases h : (b == a) with
          | true => exact absurd (beq_iff_eq.mp h) fun he => hnin (he ▸ hb)
          | false => rfl
        rw [hfe, ih t ht hNt]
  exact key l.length l (Nat.le_refl _) h

private theorem eraseDupsFilter :
    ∀ (l : List (Nat × Nat)) (p : (Nat × Nat) → Bool),
      (∀ (x y : Nat × Nat), x = y → p x = p y) → (l.filter p).eraseDups = l.eraseDups.filter p := by
  have key : ∀ (n : Nat) (l : List (Nat × Nat)) (p : (Nat × Nat) → Bool),
      (∀ (x y : Nat × Nat), x = y → p x = p y) → l.length ≤ n →
        (l.filter p).eraseDups = l.eraseDups.filter p := by
    intro n
    induction n with
    | zero =>
      intro l p hp hl
      cases l with
      | nil => rfl
      | cons a t =>
        have h1 : (a :: t).length ≤ 0 := hl
        simp only [List.length_cons] at h1
        omega
    | succ m ih =>
      intro l p hp hl
      cases l with
      | nil => rfl
      | cons a t =>
        have ht : t.length ≤ m := by
          have h2 : (a :: t).length = t.length + 1 := List.length_cons
          omega
        by_cases ha : p a
        · have hq2 : ∀ (x y : Nat × Nat), x = y → (fun b => !b == a) x = (fun b => !b == a) y := by
            intro x y h
            subst h
            rfl
          have hq1 : ∀ (x y : Nat × Nat), x = y →
              (fun b => !b == a && p b) x = (fun b => !b == a && p b) y := by
            intro x y h
            subst h
            rfl
          rw [List.filter_cons_of_pos ha, List.eraseDups_cons, List.eraseDups_cons,
            List.filter_cons_of_pos ha]
          refine congrArg (fun x => a :: x) ?_
          rw [List.filter_filter, ih t (fun b => !b == a && p b) hq1 ht,
            ih t (fun b => !b == a) hq2 ht, List.filter_filter]
          exact List.filter_congr (fun b _ => Bool.and_comm _ _)
        · rw [List.filter_cons_of_neg ha, List.eraseDups_cons, List.filter_cons_of_neg ha]
          have he : t.filter p = (t.filter (fun b => !b == a)).filter p := by
            rw [List.filter_filter]
            refine List.filter_congr (l := t) (fun b hb => ?_)
            by_cases hab : (b == a) = true
            · have pb : p b = false := by
                have h1 : p b = p a := hp b a (beq_iff_eq.mp hab)
                rw [h1]
                exact Bool.eq_false_of_ne_true ha
              simp [pb, hab]
            · simp [hab]
          rw [he]
          exact ih (t.filter (fun b => !b == a)) p hp
            (Nat.le_trans (List.length_filter_le _ _) ht)
  intro l p hp
  exact key l.length l p hp (Nat.le_refl _)

private theorem contains_filter (l : List (Nat × Nat)) (p : (Nat × Nat) → Bool) (k : Nat × Nat)
    (hk : p k = true) : (l.filter p).contains k = l.contains k := by
  apply Bool.eq_iff_iff.mpr
  rw [List.contains_iff_mem, List.contains_iff_mem, List.mem_filter]
  exact ⟨fun h => h.1, fun h => ⟨h, hk⟩⟩

private theorem members_bp (n : Nat) (l c : List (Nat × Nat)) (p : Nat) :
    members (l.filter (fun k => k.2 % n == p)) (c.filter (fun k => k.2 % n == p)) =
      l.eraseDups.filter (fun k => (k.2 % n == p) && c.contains k) := by
  have hbp : ∀ (x y : Nat × Nat), x = y → (fun k => k.2 % n == p) x = (fun k => k.2 % n == p) y := by
    intro x y h
    subst h
    rfl
  simp only [members]
  rw [eraseDupsFilter l (fun k => k.2 % n == p) hbp, List.filter_filter]
  refine List.filter_congr (l := l.eraseDups) (fun k hk => ?_)
  by_cases hb : (k.2 % n == p) = true
  · rw [contains_filter c (fun j => j.2 % n == p) k hb]
    simp [hb]
  · simp [hb]

private theorem members2_bp (n : Nat) (s c w : List (Nat × Nat)) (p : Nat) :
    members (members (s.filter (fun k => k.2 % n == p)) (c.filter (fun k => k.2 % n == p)))
        (w.filter (fun k => k.2 % n == p)) =
      s.eraseDups.filter (fun k => (k.2 % n == p) && (c.contains k && w.contains k)) := by
  rw [members_bp n s c p]
  have hN : (s.eraseDups.filter (fun k => (k.2 % n == p) && c.contains k)).Nodup := by
    refine List.Pairwise.filter (fun k => (k.2 % n == p) && List.contains c k) ?_
    exact nodup_eraseDups s
  simp only [members]
  rw [eraseDups_id _ hN, List.filter_filter]
  refine List.filter_congr (l := s.eraseDups) (fun k hk => ?_)
  by_cases hb : (k.2 % n == p) = true
  · rw [contains_filter w (fun j => j.2 % n == p) k hb, hb, Bool.true_and]
    cases hwc : List.contains w k <;> cases hcc : List.contains c k <;> rfl
  · simp [hb]

theorem sound : Q38ClickHouse.Goal := by
  intro s c w n hn
  show ((List.range n).map (fun p =>
        (members (members (s.filter (fun k => k.2 % n == p)) (c.filter (fun k => k.2 % n == p)))
          (w.filter (fun k => k.2 % n == p))).length)).sum =
      (members (members s c) w).length
  have hN : (s.eraseDups.filter (fun k => List.contains c k)).Nodup := by
    refine List.Pairwise.filter (fun k => List.contains c k) ?_
    exact nodup_eraseDups s
  have hR : members (members s c) w =
      s.eraseDups.filter (fun k => List.contains c k && List.contains w k) := by
    simp only [members]
    rw [eraseDups_id _ hN, List.filter_filter]
    exact List.filter_congr (fun k _ => Bool.and_comm _ _)
  rw [hR]
  refine Eq.trans (congrArg List.sum
    (List.map_congr_left (fun p _ => congrArg List.length (members2_bp n s c w p)))) ?_
  exact bucketSum (fun k => List.contains c k && List.contains w k) s.eraseDups n hn

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q38ClickHouse
