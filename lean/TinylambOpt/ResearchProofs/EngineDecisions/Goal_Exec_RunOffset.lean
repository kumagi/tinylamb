import TinylambOpt.ResearchGoals.EngineDecisions.Executor

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Named results include `sound`.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Exec_RunOffset

open TinylambOpt.ResearchGoals.EngineDecisions.Executor.RunOffset

private def keyO (pr : Output) : Nat × Nat := (pr.1, pr.2.1)

private def fullOf (gs : List (List Row × Nat)) : List Output :=
  gs.flatMap fun (rs, p) => (ordered rs).map (p, ·)

private theorem fullOf_cons (rs : List Row) (p : Nat) (t : List (List Row × Nat)) :
    fullOf ((rs, p) :: t) = (ordered rs).map (p, ·) ++ fullOf t := rfl

private theorem len_ordered (rs : List Row) : (ordered rs).length = rs.length := by
  rw [ordered, List.length_mergeSort]

private theorem len_map_ordered (rs : List Row) (p : Nat) :
    ((ordered rs).map (p, ·)).length = rs.length := by
  rw [List.length_map, len_ordered]

private theorem slice_cons (rs : List Row) (p : Nat) (t : List (List Row × Nat))
    (off : Nat) (n : Nat) :
    slice off n ((rs, p) :: t) =
      if n == 0 then [] else
      if rs.length ≤ off then slice (off - rs.length) n t else
        if n ≤ rs.length - off then (takeTies Prod.fst n ((ordered rs).drop off)).map (p, ·)
        else ((ordered rs).drop off).map (p, ·) ++ slice 0 (n - (rs.length - off)) t := rfl

-- Generic facts about takeTies

private theorem takeTies_zero {α β : Type} [BEq β] (key : α → β) (xs : List α) :
    takeTies key 0 xs = [] := by
  have h0 : xs.take 0 = [] := rfl
  simp only [takeTies, h0, List.getLast?_nil]

private theorem takeTies_key_nil {α β : Type} [BEq β] (key : α → β) (k : Nat) :
    takeTies key k [] = [] := by
  have h0 : ([] : List α).take k = [] := by cases k <;> rfl
  simp only [takeTies, h0, List.getLast?_nil]

private theorem takeWhile_congr {α : Type} (f g : α → Bool) (l : List α)
    (h : ∀ x ∈ l, f x = g x) : l.takeWhile f = l.takeWhile g := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    rw [List.takeWhile_cons, List.takeWhile_cons, h x List.mem_cons_self,
      ih (fun y hy => h y (List.mem_cons_of_mem x hy))]

private theorem takeWhile_append_of_forall_not {α : Type} (f : α → Bool) (xs ys : List α)
    (h : ∀ y ∈ ys, f y = false) : (xs ++ ys).takeWhile f = xs.takeWhile f := by
  induction xs with
  | nil =>
    cases ys with
    | nil => rfl
    | cons y ys =>
      rw [List.nil_append, List.takeWhile_cons, h y List.mem_cons_self]
      simp
  | cons x xs ih =>
    rw [List.cons_append, List.takeWhile_cons, ih, List.takeWhile_cons]

private theorem takeTies_singleton {α β : Type} [BEq β] (key : α → β) (k : Nat) (x : α) :
    takeTies key (k + 1) [x] = [x] := by
  have ht : ([x] : List α).take (k + 1) = [x] := by
    cases k with
    | zero => rfl
    | succ k => rw [List.take_succ_cons]; simp
  have hg : ([x] : List α).getLast? = some x := List.getLast?_singleton
  have hd : ([x] : List α).drop (k + 1) = [] := by
    cases k with
    | zero => rfl
    | succ k => rw [List.drop_succ_cons]; exact List.drop_nil
  simp only [takeTies, ht, hg, hd, List.takeWhile_nil, List.append_nil]

private theorem takeTies_succ_cons {α β : Type} [BEq β] (key : α → β) (k : Nat) (x : α)
    (xs : List α) (hk : 1 ≤ k) : takeTies key (k + 1) (x :: xs) = x :: takeTies key k xs := by
  cases xs with
  | nil => rw [takeTies_singleton, takeTies_key_nil]
  | cons y ys =>
    have hkt : (y :: ys).take k ≠ [] := by
      cases k with
      | zero => omega
      | succ k => rw [List.take_succ_cons]; simp
    obtain ⟨l, hl⟩ : ∃ lt, ((y :: ys).take k).getLast? = some lt := by
      cases hgs : ((y :: ys).take k).getLast? with
      | none => exact absurd (List.getLast?_eq_none_iff.mp hgs) hkt
      | some lt => exact ⟨lt, rfl⟩
    simp only [takeTies, List.take_succ_cons, List.drop_succ_cons,
      List.getLast?_cons_of_ne_nil hkt, hl]
    rfl

private theorem takeTies_append_barrier {α β : Type} [BEq β] (key : α → β) (m b : List α)
    (n : Nat) (hn : 0 < n) (hm : n ≤ m.length)
    (hb : ∀ x ∈ m.take n, ∀ y ∈ b, (key y == key x) = false) :
    takeTies key n (m ++ b) = takeTies key n m := by
  have ht : (m ++ b).take n = m.take n := List.take_append_of_le_length hm
  have hd : (m ++ b).drop n = m.drop n ++ b := List.drop_append_of_le_length hm
  obtain ⟨last, hlast⟩ : ∃ l, (m.take n).getLast? = some l := by
    cases hgs : (m.take n).getLast? with
    | none =>
      have hnil : m.take n = [] := List.getLast?_eq_none_iff.mp hgs
      have hlen := congrArg List.length hnil
      rw [List.length_take, Nat.min_eq_left hm, List.length_nil] at hlen
      omega
    | some l => exact ⟨l, rfl⟩
  have hmem : last ∈ m.take n := List.mem_of_getLast? hlast
  have hf : ∀ y ∈ b, (key y == key last) = false := hb last hmem
  have hL : takeTies key n (m ++ b)
      = m.take n ++ (m.drop n ++ b).takeWhile (fun x => key x == key last) := by
    simp only [takeTies, ht, hd, hlast]
  have hR : takeTies key n m = m.take n ++ (m.drop n).takeWhile (fun x => key x == key last) := by
    simp only [takeTies, hlast]
  rw [hL, hR, takeWhile_append_of_forall_not _ _ _ hf]

private theorem mem_of_mem_take {α : Type} : ∀ (l : List α) (n : Nat) (a : α),
    a ∈ l.take n → a ∈ l := by
  intro l
  induction l with
  | nil =>
    intro n a h
    have h0 : ([] : List α).take n = [] := by cases n <;> rfl
    rw [h0] at h
    cases h
  | cons x xs ih =>
    intro n a h
    cases n with
    | zero => cases h
    | succ n =>
      rw [List.take_succ_cons] at h
      cases h with
      | head => exact List.mem_cons_self
      | tail _ h => exact List.mem_cons_of_mem x (ih n _ h)

private theorem takeTies_append_of_length_lt {α β : Type} [BEq β] (key : α → β) :
    ∀ (m b : List α) (n : Nat), m.length < n →
      takeTies key n (m ++ b) = m ++ takeTies key (n - m.length) b := by
  intro m
  induction m with
  | nil => intro b n _; rw [List.nil_append, List.nil_append, List.length_nil, Nat.sub_zero]
  | cons a m ih =>
    intro b n h
    cases n with
    | zero => simp at h
    | succ k =>
      rw [List.length_cons] at h
      have hlt : m.length < k := by omega
      have hk1 : 1 ≤ k := by omega
      have hstep := takeTies_succ_cons key k a (m ++ b) hk1
      have hlen : k - m.length = k + 1 - (m.length + 1) := by omega
      rw [List.cons_append, hstep, ih _ _ hlt, hlen, List.length_cons]
      rfl

-- Interaction of the per-run output key with mapping a run by (p, ·)

private theorem nat_beq_self (p : Nat) : (p == p) = true := by
  cases h : p == p with
  | true => rfl
  | false => exact absurd (h ▸ beq_self_eq_true p) Bool.noConfusion

private theorem beq_false_of_ne {α : Type} [BEq α] [LawfulBEq α] (a b : α) (h : a ≠ b) :
    (a == b) = false := by
  cases hbeq : a == b with
  | true => exact absurd (beq_iff_eq.mp hbeq) h
  | false => rfl

private theorem pair_beq_congr (a b c d : Nat) : ((a, b) == (c, d)) = (a == c && b == d) := rfl

private theorem tie_eq_simp (p : Nat) (a b : Nat) :
    ((p, a) == (p, b)) = (a == b) := by
  rw [pair_beq_congr, nat_beq_self]
  cases h : a == b <;> rfl

private theorem takeTies_map_run (p : Nat) (L : List Row) (n : Nat) :
    takeTies keyO n (L.map (p, ·)) = (takeTies Prod.fst n L).map (p, ·) := by
  cases n with
  | zero =>
    show ([] : List Output) = List.map (fun x => (p, x)) []
    rfl
  | succ n =>
    rw [takeTies, takeTies, ← List.map_take, ← List.map_drop, List.getLast?_map]
    cases hgs : (List.getLast? (List.take (n+1) L)) with
    | none => rfl
    | some l =>
      rw [show Option.map (fun x => (p, x)) (some l) = some (p, l) from rfl]
      simp only []
      rw [List.map_append, List.takeWhile_map]
      refine congrArg ((List.map (fun x => (p, x)) (List.take (n+1) L)) ++ ·) ?_
      refine congrArg (List.map (fun x => (p, x)) ·) ?_
      refine congrArg (List.takeWhile · (List.drop (n+1) L)) ?_
      refine funext fun z => ?_
      show (keyO (p, z) == keyO (p, l)) = (z.1 == l.1)
      rw [show keyO (p, z) = (p, z.1) from rfl, show keyO (p, l) = (p, l.1) from rfl]
      exact tie_eq_simp p z.1 l.1

-- Facts about group lists.

private theorem nodup_snd_zipIdx_go : ∀ (runs : List (List Row)) (i : Nat),
    ((runs.zipIdx i).map Prod.snd).Nodup ∧
      ∀ x ∈ runs.zipIdx i, i ≤ x.2 := by
  intro runs
  induction runs with
  | nil =>
    intro i
    rw [List.zipIdx_nil]
    exact ⟨by rw [List.map_nil]; exact List.nodup_nil, by simp⟩
  | cons r rs ih =>
    intro i
    rw [List.zipIdx_cons, List.map_cons]
    obtain ⟨hn, hb⟩ := ih (i + 1)
    refine ⟨List.nodup_cons.mpr ⟨?_, hn⟩, ?_⟩
    · intro hx
      obtain ⟨p, hp, hpe⟩ := List.mem_map.mp hx
      have hge := hb p hp
      omega
    · intro x hx
      cases List.mem_cons.mp hx with
      | inl h => exact h ▸ Nat.le_refl i
      | inr h =>
        have hge := hb x h
        omega

private theorem nodup_snd_zipIdx (runs : List (List Row)) :
    (runs.zipIdx.map Prod.snd).Nodup := (nodup_snd_zipIdx_go runs 0).1

-- Main induction

private theorem main_lemma : ∀ (gs : List (List Row × Nat)), (gs.map Prod.snd).Nodup →
    ∀ off n, slice off n gs = takeTies keyO n ((fullOf gs).drop off) := by
  intro gs
  induction gs with
  | nil =>
    intro _ off n
    have hnil : (fullOf ([] : List (List Row × Nat))).drop off = [] := by
      show ([] : List Output).drop off = []
      exact List.drop_nil
    rw [show slice off n [] = [] from rfl, hnil, takeTies_key_nil]
  | cons g t ih =>
    obtain ⟨rs, p⟩ := g
    intro hnd off n
    have hnd' : (p :: t.map Prod.snd).Nodup := by simpa [List.map_cons] using hnd
    obtain ⟨hpnot, hndt⟩ := List.nodup_cons.mp hnd'
    cases n with
    | zero =>
      show ([] : List Output) = takeTies keyO 0 ((fullOf ((rs, p) :: t)).drop off)
      exact (takeTies_zero keyO _).symm
    | succ n =>
      rw [slice_cons]
      rw [ite_eq_right (by simp : ¬ (((n + 1 : Nat) == 0) = true))]
      by_cases hle : rs.length ≤ off
      · rw [ite_eq_left hle]
        have hsplit : (fullOf ((rs, p) :: t)).drop off
            = ((ordered rs).map (p, ·)).drop off ++ (fullOf t).drop (off - rs.length) := by
          rw [fullOf_cons, List.drop_append, len_map_ordered]
        rw [hsplit, ← List.map_drop,
          show (ordered rs).drop off = [] from
            List.drop_of_length_le (by rw [len_ordered]; exact hle),
          List.map_nil, List.nil_append]
        rw [ih hndt]
      · have hlt : off < rs.length := by omega
        have hlenL : ((ordered rs).drop off).length = rs.length - off := by
          rw [ordered, List.length_drop, List.length_mergeSort]
        have hlenA : (((ordered rs).drop off).map (p, ·)).length = rs.length - off := by
          rw [List.length_map, hlenL]
        have hsplit : (fullOf ((rs, p) :: t)).drop off
            = ((ordered rs).drop off).map (p, ·) ++ fullOf t := by
          rw [fullOf_cons, List.drop_append, len_map_ordered,
            Nat.sub_eq_zero_of_le (by omega : off ≤ rs.length), List.drop_zero, ← List.map_drop]
        rw [ite_eq_right (by omega : ¬ (rs.length ≤ off)), hsplit]
        by_cases hge : n + 1 ≤ rs.length - off
        · rw [ite_eq_left hge]
          refine ((takeTies_map_run p _ _).symm).trans
            ((takeTies_append_barrier keyO _ _ (n + 1) (by omega)
                (by rw [hlenA]; exact hge) ?_).symm)
          intro x hxm y hy
          obtain ⟨r', hr', hx⟩ := List.mem_map.mp (mem_of_mem_take _ _ _ hxm)
          obtain ⟨q, hqt, hwq⟩ := List.mem_flatMap.mp hy
          obtain ⟨r2, hr2l, hr2⟩ := List.mem_map.mp hwq
          have hx1 : keyO x = (p, r'.1) := by rw [← hx]; rfl
          have hy1 : keyO y = (q.2, r2.1) := by rw [← hr2]; rfl
          have hqn : q.2 ∈ t.map Prod.snd := List.mem_map.mpr ⟨q, hqt, rfl⟩
          have hne : (q.2 == p) = false :=
            beq_false_of_ne _ _ (fun hh => hpnot (hh ▸ hqn))
          rw [hy1, hx1, pair_beq_congr, hne, Bool.false_and]
        · rw [ite_eq_right (by omega : ¬ (n + 1 ≤ rs.length - off)),
            takeTies_append_of_length_lt keyO _ _ _ (by rw [hlenA]; omega),
            hlenA, ← List.drop_zero (l := fullOf t), ← ih hndt]

theorem sound : Goal := by
  intro runs off n
  exact main_lemma runs.zipIdx (nodup_snd_zipIdx runs) off n

end TinylambOpt.ResearchProofs.EngineDecisions.Exec_RunOffset

-- C++ references to this model or its proof.
-- executor/incremental_sort.cpp
