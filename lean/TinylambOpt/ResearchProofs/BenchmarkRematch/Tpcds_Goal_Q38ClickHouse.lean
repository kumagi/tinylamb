import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace Proof_Q38ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : (l : List α) → (l.eraseDups).Nodup
  | [] => by simp [List.Nodup]
  | a :: l => by
      rw [List.eraseDups_cons]
      rw [List.nodup_cons]
      constructor
      · intro hmem
        rw [List.mem_eraseDups] at hmem
        simp at hmem
      · exact nodup_eraseDups (l.filter (fun b => ! b == a))
termination_by l => l.length
decreasing_by
  simp only [List.length_cons]
  have hle := List.length_filter_le (p := fun b => ! b == a) (l := l)
  omega

theorem nodup_filter {α : Type} (p : α → Bool) : ∀ {l : List α}, l.Nodup → (l.filter p).Nodup := by
  intro l
  induction l with
  | nil => intro h; simp [List.Nodup]
  | cons a l ih =>
    intro h
    rw [List.nodup_cons] at h
    simp only [List.filter_cons]
    by_cases hp : p a = true
    · simp only [hp, ↓reduceIte]
      rw [List.nodup_cons]
      have haf : a ∉ l.filter p := by
        intro ha
        exact h.1 ((List.mem_filter.mp ha).1)
      exact ⟨haf, ih h.2⟩
    · simp only [hp, ↓reduceIte]
      exact ih h.2

theorem nodup_members {α : Type} [BEq α] [LawfulBEq α] (a b : List α) : (members a b).Nodup :=
  nodup_filter b.contains (nodup_eraseDups a)

theorem contains_eq_mem {α : Type} [BEq α] [LawfulBEq α] (l : List α) (a : α) :
    l.contains a = true ↔ a ∈ l := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih]

theorem mem_members {α : Type} [BEq α] [LawfulBEq α] (a b : List α) (x : α) :
    x ∈ members a b ↔ x ∈ a ∧ x ∈ b := by
  dsimp only [members]
  simp [contains_eq_mem, List.mem_eraseDups]

theorem length_eq_of_nodup_mem {α : Type} [DecidableEq α] {l₁ l₂ : List α}
    (d₁ : l₁.Nodup) (d₂ : l₂.Nodup) (hm : ∀ a, a ∈ l₁ ↔ a ∈ l₂) : l₁.length = l₂.length := by
  exact ((List.perm_ext_iff_of_nodup d₁ d₂).mpr hm).length_eq

theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
    simp [ih]
    omega

theorem sum_zero_range : ∀ n : Nat, ((List.range n).map (fun _ : Nat => 0)).sum = 0 := by
  intro n
  induction n with
  | zero => simp
  | succ n ih => simp [List.range_succ, ih]

theorem one_at (n k : Nat) (hk : k < n) : ((List.range n).map (fun p => if k = p then 1 else 0)).sum = 1 := by
  induction n with
  | zero => omega
  | succ n ih =>
    rw [List.range_succ, List.map_append, List.sum_append]
    simp only [List.map_cons, List.map_nil, List.sum_cons, List.sum_nil, Nat.add_zero]
    by_cases hkn : k = n
    · subst k
      have hz : (List.range n).map (fun p => if n = p then 1 else 0) =
          (List.range n).map (fun _ : Nat => 0) := by
        apply List.map_congr_left
        intro p hp
        have hnp : n ≠ p := by
          intro h
          subst p
          exact (Nat.lt_irrefl n) (List.mem_range.mp hp)
        simp [hnp]
      rw [hz, sum_zero_range]
      simp
    · have hklt : k < n := by omega
      simp [hkn, ih hklt]

theorem partition_sum {α : Type} (l : List α) (f : α → Nat) (n : Nat) (hn : 0 < n)
    (hf : ∀ x ∈ l, f x < n) :
    ((List.range n).map (fun p => (l.filter (fun x => f x == p)).length)).sum = l.length := by
  induction l with
  | nil =>
    change ((List.range n).map (fun _ : Nat => 0)).sum = 0
    exact sum_zero_range n
  | cons a l ih =>
    have hfa : f a < n := hf a (by simp)
    have hfl : ∀ x ∈ l, f x < n := by
      intro x hx
      exact hf x (by simp [hx])
    have hlen : ∀ p : Nat,
        (List.filter (fun x => f x == p) (a :: l)).length =
          (List.filter (fun x => f x == p) l).length + (if f a = p then 1 else 0) := by
      intro p
      by_cases hp : f a = p
      · have hpb : (f a == p) = true := by simp [hp]
        simp [hp, hpb]
      · have hpb : (f a == p) = false := by simp [hp]
        simp [hp, hpb]
    have hmap : (List.range n).map (fun p => (List.filter (fun x => f x == p) (a :: l)).length) =
        (List.range n).map (fun p => (List.filter (fun x => f x == p) l).length + (if f a = p then 1 else 0)) := by
      apply List.map_congr_left
      intro p hp
      exact hlen p
    rw [hmap, sum_map_add, ih hfl, one_at n (f a) hfa]
    simp

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q38ClickHouse.Goal := by
  intro s c w n hn
  dsimp only
  let M := members (members s c) w
  have hpart : ((List.range n).map (fun p => (M.filter (fun k : Nat × Nat => k.2 % n == p)).length)).sum = M.length := by
    dsimp only [M]
    apply partition_sum (members (members s c) w) (fun k : Nat × Nat => k.2 % n) n hn
    intro k hk
    exact Nat.mod_lt k.2 hn
  have hcount : ∀ p : Nat,
      (members (members (s.filter (fun k : Nat × Nat => k.2 % n == p))
                        (c.filter (fun k : Nat × Nat => k.2 % n == p)))
               (w.filter (fun k : Nat × Nat => k.2 % n == p))).length =
        (M.filter (fun k : Nat × Nat => k.2 % n == p)).length := by
    intro p
    apply length_eq_of_nodup_mem
    · exact nodup_members _ _
    · exact nodup_filter (fun k : Nat × Nat => k.2 % n == p) (nodup_members _ _)
    · intro k
      simp only [M, mem_members, List.mem_filter]
      constructor
      · intro h
        rcases h with ⟨⟨⟨hs, hP⟩, hc, hP'⟩, hw, hP''⟩
        exact ⟨⟨⟨hs, hc⟩, hw⟩, hP⟩
      · intro h
        rcases h with ⟨⟨⟨hs, hc⟩, hw⟩, hP⟩
        exact ⟨⟨⟨hs, hP⟩, hc, hP⟩, hw, hP⟩
  have hmap : ((List.range n).map (fun p =>
        (members (members (s.filter (fun k : Nat × Nat => k.2 % n == p))
                          (c.filter (fun k : Nat × Nat => k.2 % n == p)))
                 (w.filter (fun k : Nat × Nat => k.2 % n == p))).length)) =
      ((List.range n).map (fun p => (M.filter (fun k : Nat × Nat => k.2 % n == p)).length)) := by
    apply List.map_congr_left
    intro p hp
    exact hcount p
  rw [hmap, hpart]

end Proof_Q38ClickHouse

#print axioms Proof_Q38ClickHouse.sound
