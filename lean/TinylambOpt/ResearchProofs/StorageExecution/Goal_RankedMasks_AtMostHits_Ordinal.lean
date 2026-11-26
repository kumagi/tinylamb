import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_AtMostHits_Ordinal

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits

/-- The rank/ordinal correspondence holds for every deterministic machine;
    `AtMostHits.model` is one instance. -/

private theorem filter_false {α : Type} (xs : List α) :
    xs.filter (fun _ => false) = [] := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [List.filter_cons, ih]

private abbrev hiCount (m : Machine) (n q : Nat) : Nat :=
  ((m.step q true).map (count m n)).getD 0

private theorem count_succ (m : Machine) (n q : Nat) :
    count m (n + 1) q = lowCount m n q + hiCount m n q := by
  simp [count, lowCount, hiCount]

private theorem run_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    run m q (b :: bs) = (m.step q b).bind (fun q' => run m q' bs) := by
  simp [run, List.foldlM]

private theorem legal_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    legal m q (b :: bs) = (((m.step q b).map (fun q' => legal m q' bs)).getD false) := by
  simp [legal, run_cons]
  cases m.step q b <;> simp

private theorem count_eq_filter_length (m : Machine) : ∀ (n q : Nat),
    count m n q = ((words n).filter (legal m q)).length
  | 0, q => by
    cases ha : m.accept q <;> simp [count, words, legal, run, ha]
  | n + 1, q => by
    rw [count_succ]
    simp only [words, List.filter_append, List.filter_map, List.length_append,
      List.length_map, Function.comp_def, legal_cons]
    cases hf : m.step q false with
    | none =>
      cases ht : m.step q true with
      | none => simp [lowCount, hiCount, hf, ht, filter_false]
      | some qt =>
        simp [lowCount, hiCount, hf, ht, filter_false]
        rw [← count_eq_filter_length m n qt]
    | some qf =>
      cases ht : m.step q true with
      | none =>
        simp [lowCount, hiCount, hf, ht, filter_false]
        rw [← count_eq_filter_length m n qf]
      | some qt =>
        simp [lowCount, hiCount, hf, ht]
        rw [← count_eq_filter_length m n qf, ← count_eq_filter_length m n qt]

private theorem lowCount_eq_filter_length (m : Machine) (n q : Nat) :
    lowCount m n q = (((words n).map (false :: ·)).filter (legal m q)).length := by
  rw [List.filter_map]
  simp only [List.length_map, Function.comp_def, legal_cons]
  cases hs : m.step q false with
  | none => simp [lowCount, hs, filter_false]
  | some q' => simp [lowCount, hs, count_eq_filter_length]

private theorem mem_words_of_length : ∀ {xs : List Bool}, xs ∈ words xs.length
  | [] => by simp [words]
  | b :: bs => by
    have ih := mem_words_of_length (xs := bs)
    simp only [List.length_cons, words, List.mem_append, List.mem_map]
    cases b
    · exact Or.inl ⟨bs, ih, rfl⟩
    · exact Or.inr ⟨bs, ih, rfl⟩

private theorem idxOf_cons_map : ∀ (xs : List (List Bool)) (b : Bool) (ys : List Bool),
    ys ∈ xs → (xs.map (b :: ·)).idxOf (b :: ys) = xs.idxOf ys
  | [], b, ys, h => by simp at h
  | z :: zs, b, ys, h => by
    simp only [List.mem_cons] at h
    by_cases hzy : z = ys
    · subst z; simp
    · have hmem : ys ∈ zs := by
        rcases h with heq | hmem
        · exact False.elim (hzy heq.symm)
        · exact hmem
      simp only [List.map_cons, List.idxOf_cons]
      simp [hzy, idxOf_cons_map zs b ys hmem]

private theorem rank_cons (m : Machine) (q : Nat) (b : Bool) (bs : List Bool) :
    rank m q (b :: bs) = (m.step q b).bind
      (fun q' => (rank m q' bs).map
        (fun r => r + if b then lowCount m bs.length q else 0)) := by
  cases hs : m.step q b with
  | none => simp [rank, hs]
  | some q' =>
    cases hr : rank m q' bs with
    | none => simp [rank, hs, hr]
    | some r => simp [rank, hs, hr]

private theorem filter_cons_words (m : Machine) (q : Nat) (b : Bool) (n : Nat) :
    (((words n).map (b :: ·)).filter (legal m q)) =
      ((words n).filter
        (fun xs => ((m.step q b).map (fun q' => legal m q' xs)).getD false)).map
        (b :: ·) := by
  rw [List.filter_map]
  simp only [Function.comp_def, legal_cons]

private theorem ordinal_generic (m : Machine) : ordinal m := by
  intro q xs
  induction xs generalizing q with
  | nil =>
    intro hlegal
    have hacc : m.accept q = true := by simpa [legal, run] using hlegal
    cases ha : m.accept q <;> simp_all [rank, words, legal, run]
  | cons b bs ih =>
    intro hlegal
    have hlen : (b :: bs).length = bs.length + 1 := by simp
    rw [hlen, words, List.filter_append, filter_cons_words, filter_cons_words]
    have hleg' : (((m.step q b).map (fun q' => legal m q' bs)).getD false) = true := by
      simpa [legal_cons] using hlegal
    cases hs : m.step q b with
    | none => simp [hs] at hleg'
    | some q' =>
      simp [hs] at hleg'
      have htail := ih q' hleg'
      have hmem' : bs ∈ (words bs.length).filter (legal m q') :=
        List.mem_filter.mpr ⟨mem_words_of_length, hleg'⟩
      cases b with
      | false =>
        simp only [rank_cons, hs, Bool.false_eq_true, ite_false, Nat.add_zero,
          Option.bind_some, Option.map_some, Option.some.injEq, htail,
          Option.map_some, Option.getD_some]
        have hmem : false :: bs ∈
            ((words bs.length).filter (legal m q')).map (false :: ·) :=
          List.mem_map.mpr ⟨bs, hmem', rfl⟩
        rw [List.idxOf_append, ite_eq_left hmem, idxOf_cons_map _ _ _ hmem']
      | true =>
        simp only [rank_cons, hs, ite_true, Option.bind_some, Option.map_some,
          Option.some.injEq, htail, Option.map_some, Option.getD_some]
        have hleft_len :
            (((words bs.length).filter
              (fun xs => ((m.step q false).map (fun q0 => legal m q0 xs)).getD false)).map
              (false :: ·)).length = lowCount m bs.length q := by
          calc
            (((words bs.length).filter
              (fun xs => ((m.step q false).map (fun q0 => legal m q0 xs)).getD false)).map
              (false :: ·)).length =
                (((words bs.length).map (false :: ·)).filter (legal m q)).length := by
                  rw [filter_cons_words]
            _ = lowCount m bs.length q := (lowCount_eq_filter_length m bs.length q).symm
        have hnot : true :: bs ∉
            ((words bs.length).filter
              (fun xs => ((m.step q false).map (fun q0 => legal m q0 xs)).getD false)).map
              (false :: ·) := by
          simp [List.mem_map]
        rw [List.idxOf_append, ite_eq_right hnot, hleft_len,
          idxOf_cons_map ((words bs.length).filter (legal m q')) true bs hmem']

theorem sound_Ordinal :
    TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.AtMostHits.Ordinal.Goal := by
  exact ordinal_generic AtMostHits.model

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_AtMostHits_Ordinal
