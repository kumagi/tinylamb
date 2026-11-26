import TinylambOpt.ResearchGoals.November26.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Analytics.Goal_L5
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.BenchmarkRematch

abbrev Sale := Nat × Option Int

def addNsum (a b : Option Int) : Option Int :=
  match a, b with
  | none, none => none
  | some x, none => some x
  | none, some y => some y
  | some x, some y => some (x + y)

theorem isEmpty_append {α : Type} (a b : List α) : (a ++ b).isEmpty = (a.isEmpty && b.isEmpty) := by
  cases a <;> simp

theorem sum_eq_zero_of_isEmpty (l : List Int) (h : l.isEmpty = true) : l.sum = 0 := by
  cases l <;> simp at h ⊢

theorem nsum_append (a b : List (Option Int)) : nsum (a ++ b) = addNsum (nsum a) (nsum b) := by
  unfold nsum addNsum
  simp only [List.filterMap_append, isEmpty_append, List.sum_append]
  by_cases ha : (List.filterMap id a).isEmpty = true <;>
    by_cases hb : (List.filterMap id b).isEmpty = true <;>
    simp [ha, hb, sum_eq_zero_of_isEmpty]

theorem nsum_one (o : Option Int) : nsum [o] = o := by
  cases o <;> simp [nsum]

theorem nsum_cons_addNsum (o : Option Int) (xs : List (Option Int)) :
    nsum (o :: xs) = addNsum o (nsum xs) := by
  change nsum ([o] ++ xs) = addNsum o (nsum xs)
  rw [nsum_append, nsum_one]

theorem addNsum_assoc (a b c : Option Int) : addNsum (addNsum a b) c = addNsum a (addNsum b c) := by
  cases a <;> cases b <;> cases c <;> simp [addNsum, Int.add_assoc]

theorem addNsum_left_comm (a b c : Option Int) : addNsum a (addNsum b c) = addNsum b (addNsum a c) := by
  cases a <;> cases b <;> cases c <;> simp [addNsum, Int.add_assoc, Int.add_left_comm, Int.add_comm]

theorem addNsum_none_left (a : Option Int) : addNsum none a = a := by
  cases a <;> rfl

theorem sum_filter_partition (g : Sale → Nat) (t : List Sale) (a : Nat) (p : Nat → Bool) :
    (List.map g (t.filter (fun s => p s.1))).sum =
      (if p a = true then (List.map g (t.filter (fun s => s.1 == a))).sum else 0) +
        (List.map g ((t.filter (fun s => !(s.1 == a))).filter (fun s => p s.1))).sum := by
  induction t with
  | nil => by_cases h : p a = true <;> simp [h]
  | cons s t ih =>
      by_cases hsa : (s.1 == a) = true
      · have heq : s.1 = a := beq_iff_eq.mp hsa
        subst a
        by_cases hp : p s.1 = true <;>
          simp [hp, ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · by_cases hpa : p a = true <;> by_cases hp : p s.1 = true <;>
          simp [hsa, hpa, hp, ih, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

theorem filterMap_id_cons_length {α : Type} (o : Option α) (xs : List (Option α)) :
    ((o :: xs).filterMap id).length = (if o.isSome then 1 else 0) + (xs.filterMap id).length := by
  cases o <;> simp [Nat.add_comm]

theorem filterMap_id_map_snd (xs : List Sale) :
    (xs.map Prod.snd).filterMap id = xs.filterMap Prod.snd := by
  induction xs with
  | nil => simp
  | cons s t ih =>
      rcases s with ⟨_, o⟩
      cases o <;> simp [ih]

theorem some_count_eq_sum (xs : List Sale) :
    (xs.filterMap Prod.snd).length = (xs.map (fun s => if s.2.isSome then 1 else 0)).sum := by
  induction xs with
  | nil => simp
  | cons s t ih =>
      rcases s with ⟨_, o⟩
      cases o <;> simp [ih, Nat.add_comm]

theorem sum_const_one (xs : List Sale) : (xs.map (fun _ => 1)).sum = xs.length := by
  induction xs with
  | nil => simp
  | cons s t ih => simp [ih, Nat.add_comm]

theorem count_filter_partition (t : List Sale) (a : Nat) (p : Nat → Bool) :
    (t.filter (fun s => p s.1)).length =
      (if p a = true then (t.filter (fun s => s.1 == a)).length else 0) +
        ((t.filter (fun s => !(s.1 == a))).filter (fun s => p s.1)).length := by
  rw [← sum_const_one, ← sum_const_one, ← sum_const_one]
  exact sum_filter_partition (fun _ : Sale => 1) t a p

theorem some_count_filter_partition (t : List Sale) (a : Nat) (p : Nat → Bool) :
    (((t.filter (fun s => p s.1)).map Prod.snd).filterMap id).length =
      (if p a = true then (((t.filter (fun s => s.1 == a)).map Prod.snd).filterMap id).length else 0) +
        ((((t.filter (fun s => !(s.1 == a))).filter (fun s => p s.1)).map Prod.snd).filterMap id).length := by
  repeat rw [filterMap_id_map_snd]
  rw [some_count_eq_sum, some_count_eq_sum, some_count_eq_sum]
  exact sum_filter_partition (fun s : Sale => if s.2.isSome then 1 else 0) t a p

theorem filter_map_fst_eq (t : List Sale) (a : Nat) :
    (t.map Prod.fst).filter (fun b => ! b == a) =
      (t.filter (fun s => ! s.1 == a)).map Prod.fst := by
  induction t with
  | nil => simp
  | cons s t ih =>
      by_cases h : (s.1 == a) = true <;> simp [h, ih]

theorem filter_p_eq_filter_ne_filter_p (t : List Sale) (a : Nat) (p : Nat → Bool)
    (h : ¬ p a = true) :
    t.filter (fun s => p s.1) =
      (t.filter (fun s => ! s.1 == a)).filter (fun s => p s.1) := by
  rw [List.filter_filter]
  induction t with
  | nil => simp
  | cons s t ih =>
      by_cases hsa : (s.1 == a) = true
      · have heq : s.1 = a := beq_iff_eq.mp hsa
        subst a
        have hp : p s.1 = false := by
          cases hp : p s.1 <;> simp_all
        simp [hp, ih]
      · have hne : (! s.1 == a) = true := by simpa [hsa]
        by_cases hp : p s.1 = true <;> simp [hsa, hne, hp, ih]

theorem eraseDups_fst_cons (r : Sale) (t : List Sale) :
    ((r :: t).map Prod.fst).eraseDups =
      r.1 :: ((t.filter (fun s => ! s.1 == r.1)).map Prod.fst).eraseDups := by
  change (r.1 :: t.map Prod.fst).eraseDups =
    r.1 :: ((t.filter (fun s => ! s.1 == r.1)).map Prod.fst).eraseDups
  induction t with
  | nil => simp [List.eraseDups_cons]
  | cons s t ih =>
      by_cases h : (s.1 == r.1) = true
      · simp [List.eraseDups_cons, h, ih, filter_map_fst_eq]
      · have hf : (s.1 == r.1) = false := by
          cases hb : s.1 == r.1 <;> simp_all
        simp [List.eraseDups_cons, hf, ih, filter_map_fst_eq]

theorem forKey_cons_eq (r : Sale) (t : List Sale) :
    forKey (r :: t) r.1 = r.2 :: (t.filter (fun s => s.1 == r.1)).map Prod.snd := by
  simp [forKey]

theorem forKey_cons_ne (r : Sale) (t : List Sale) (k : Nat) (h : (k == r.1) = false) :
    forKey (r :: t) k = forKey (t.filter (fun s => ! s.1 == r.1)) k := by
  have hne : r.1 ≠ k := by
    intro heq
    subst heq
    simp at h
  have h' : (r.1 == k) = false := by simpa [beq_iff_eq] using hne
  have hfilter : t.filter (fun s => s.1 == k) =
      (t.filter (fun s => ! s.1 == r.1)).filter (fun s => s.1 == k) := by
    induction t with
    | nil => rfl
    | cons s t ih =>
        by_cases hsk : (s.1 == k) = true
        · have heq : s.1 = k := beq_iff_eq.mp hsk
          have hneq : s.1 ≠ r.1 := by
            intro he
            exact hne (by simpa [heq] using he.symm)
          have hsk_ne : (! (s.1 == r.1)) = true := by simpa [beq_iff_eq] using hneq
          simp [hsk, hsk_ne, ih]
        · simp [hsk, ih]
  unfold forKey
  simp [h', hfilter]

theorem nsum_filter_partition (t : List Sale) (a : Nat) (p : Nat → Bool) :
    nsum ((t.filter (fun s => p s.1)).map Prod.snd) =
      addNsum (if p a then nsum ((t.filter (fun s => s.1 == a)).map Prod.snd) else none)
        (nsum (((t.filter (fun s => ! s.1 == a)).filter (fun s => p s.1)).map Prod.snd)) := by
  induction t with
  | nil => simp [nsum, addNsum]
  | cons s t ih =>
      by_cases hsa : (s.1 == a) = true
      · have heq : s.1 = a := beq_iff_eq.mp hsa
        subst a
        by_cases hp : p s.1 = true
        · simp [hp, nsum_cons_addNsum, ih, addNsum_assoc]
        · simp [hp, ih]
      · by_cases hp : p s.1 = true
        · simp [hsa, hp, nsum_cons_addNsum, ih, addNsum_left_comm]
        · simp [hsa, hp, ih]

theorem mainNsum (p : Nat → Bool) : ∀ sales : List Sale,
    nsum (((sales.map Prod.fst).eraseDups.filter p).map (fun k => nsum (forKey sales k))) =
      nsum (((sales.filter (fun s : Sale => p s.1)).map Prod.snd))
  | [] => by simp [nsum]
  | r :: t => by
      let trest := t.filter (fun s => ! s.1 == r.1)
      let ks := (trest.map Prod.fst).eraseDups
      let tailG := nsum ((ks.filter p).map (fun k => nsum (forKey trest k)))
      let tailR := nsum ((trest.filter (fun s => p s.1)).map Prod.snd)
      have ih := mainNsum p trest
      have htailG : tailG = nsum ((ks.filter p).map (fun k => nsum (forKey (r :: t) k))) := by
        dsimp [tailG, ks, trest]
        apply congrArg nsum
        apply List.map_congr_left
        intro k hk
        have hkE : k ∈ (trest.map Prod.fst).eraseDups ∧ p k = true := List.mem_filter.mp hk
        have hkM : k ∈ trest.map Prod.fst := List.mem_eraseDups.mp hkE.1
        obtain ⟨s, hs, hsk⟩ := List.mem_map.mp hkM
        subst k
        have hne_s : (! s.1 == r.1) = true := (List.mem_filter.mp hs).2
        have hne : (s.1 == r.1) = false := by
          cases hb : s.1 == r.1 <;> simp_all
        rw [forKey_cons_ne r t s.1 hne]
      have htail : tailG = tailR := by
        dsimp [tailG, tailR, ks, trest] at ih ⊢
        exact ih
      have hkeys : ((r :: t).map Prod.fst).eraseDups = r.1 :: ks := by
        simpa [ks, trest] using eraseDups_fst_cons r t
      have hmap_filter : (((r :: t).map Prod.fst).eraseDups.filter p) =
          if p r.1 then r.1 :: (ks.filter p) else (ks.filter p) := by
        rw [hkeys]
        by_cases h : p r.1 = true <;> simp [h]
      rw [hmap_filter]
      by_cases h : p r.1 = true
      · have hpart := nsum_filter_partition t r.1 p
        have hR : nsum ((t.filter (fun s => p s.1)).map Prod.snd) =
            addNsum (nsum ((t.filter (fun s => s.1 == r.1)).map Prod.snd)) tailR := by
          rw [hpart]
          simp [h, tailR, trest]
        simp [h, nsum_cons_addNsum, forKey_cons_eq, htailG.symm, htail, hR, addNsum_assoc]
      · simp only [h, Bool.false_eq_true, if_false]
        rw [htailG.symm, htail]
        have hR : nsum (((r :: t).filter (fun s => p s.1)).map Prod.snd) = tailR := by
          simp [h, nsum_filter_partition t r.1 p, tailR, trest, addNsum_none_left]
        rw [hR]
termination_by sales => sales.length
decreasing_by
  simp [trest]
  exact Nat.lt_of_le_of_lt (List.length_filter_le (fun s : Sale => ! s.1 == r.1) t) (Nat.lt_succ_self t.length)

theorem mainLen (p : Nat → Bool) : ∀ sales : List Sale,
    (((sales.map Prod.fst).eraseDups.filter p).map (fun k => (forKey sales k).length)).sum =
      (((sales.filter (fun s : Sale => p s.1)).map Prod.snd)).length
  | [] => by simp
  | r :: t => by
      let trest := t.filter (fun s => ! s.1 == r.1)
      let ks := (trest.map Prod.fst).eraseDups
      let tailG := ((ks.filter p).map (fun k => (forKey trest k).length)).sum
      let tailR := ((trest.filter (fun s => p s.1)).map Prod.snd).length
      have ih := mainLen p trest
      have htailG : tailG = ((ks.filter p).map (fun k => (forKey (r :: t) k).length)).sum := by
        dsimp [tailG, ks, trest]
        apply congrArg (fun l : List Nat => l.sum)
        apply List.map_congr_left
        intro k hk
        have hkE : k ∈ (trest.map Prod.fst).eraseDups ∧ p k = true := List.mem_filter.mp hk
        have hkM : k ∈ trest.map Prod.fst := List.mem_eraseDups.mp hkE.1
        obtain ⟨s, hs, hsk⟩ := List.mem_map.mp hkM
        subst k
        have hne_s : (! s.1 == r.1) = true := (List.mem_filter.mp hs).2
        have hne : (s.1 == r.1) = false := by
          cases hb : s.1 == r.1 <;> simp_all
        rw [forKey_cons_ne r t s.1 hne]
      have htail : tailG = tailR := by
        dsimp [tailG, tailR, ks, trest] at ih ⊢
        simpa using ih
      have hkeys : ((r :: t).map Prod.fst).eraseDups = r.1 :: ks := by
        simpa [ks, trest] using eraseDups_fst_cons r t
      have hmap_filter : (((r :: t).map Prod.fst).eraseDups.filter p) =
          if p r.1 then r.1 :: (ks.filter p) else (ks.filter p) := by
        rw [hkeys]
        by_cases h : p r.1 = true <;> simp [h]
      rw [hmap_filter]
      by_cases h : p r.1 = true
      · have hpart := count_filter_partition t r.1 p
        have hR : ((t.filter (fun s => p s.1)).map Prod.snd).length =
            ((t.filter (fun s => s.1 == r.1)).map Prod.snd).length + tailR := by
          simpa [List.length_map, h, tailR, trest] using hpart
        simp only [h, Bool.false_eq_true, ↓reduceIte, List.map_cons, List.sum_cons]
        rw [List.filter_cons_of_pos (p := fun s : Sale => p s.1) h]
        rw [List.map_cons, List.length_cons]
        rw [← htailG, htail]
        rw [forKey_cons_eq, List.length_cons]
        rw [hR]
        omega
      · simp only [h, Bool.false_eq_true, ite_false]
        rw [htailG.symm, htail]
        have hfilt := filter_p_eq_filter_ne_filter_p t r.1 p h
        have hR : (((r :: t).filter (fun s => p s.1)).map Prod.snd).length = tailR := by
          simp [h, tailR, trest, hfilt]
        rw [hR]
termination_by sales => sales.length
decreasing_by
  simp [trest]
  exact Nat.lt_of_le_of_lt (List.length_filter_le (fun s : Sale => ! s.1 == r.1) t) (Nat.lt_succ_self t.length)

theorem mainSome (p : Nat → Bool) : ∀ sales : List Sale,
    (((sales.map Prod.fst).eraseDups.filter p).map (fun k => ((forKey sales k).filterMap id).length)).sum =
      ((((sales.filter (fun s : Sale => p s.1)).map Prod.snd)).filterMap id).length
  | [] => by simp
  | r :: t => by
      let trest := t.filter (fun s => ! s.1 == r.1)
      let ks := (trest.map Prod.fst).eraseDups
      let tailG := ((ks.filter p).map (fun k => ((forKey trest k).filterMap id).length)).sum
      let tailR := (((trest.filter (fun s => p s.1)).map Prod.snd).filterMap id).length
      have ih := mainSome p trest
      have htailG : tailG = ((ks.filter p).map (fun k => ((forKey (r :: t) k).filterMap id).length)).sum := by
        dsimp [tailG, ks, trest]
        apply congrArg (fun l : List Nat => l.sum)
        apply List.map_congr_left
        intro k hk
        have hkE : k ∈ (trest.map Prod.fst).eraseDups ∧ p k = true := List.mem_filter.mp hk
        have hkM : k ∈ trest.map Prod.fst := List.mem_eraseDups.mp hkE.1
        obtain ⟨s, hs, hsk⟩ := List.mem_map.mp hkM
        subst k
        have hne_s : (! s.1 == r.1) = true := (List.mem_filter.mp hs).2
        have hne : (s.1 == r.1) = false := by
          cases hb : s.1 == r.1 <;> simp_all
        rw [forKey_cons_ne r t s.1 hne]
      have htail : tailG = tailR := by
        dsimp [tailG, tailR, ks, trest] at ih ⊢
        simpa using ih
      have hkeys : ((r :: t).map Prod.fst).eraseDups = r.1 :: ks := by
        simpa [ks, trest] using eraseDups_fst_cons r t
      have hmap_filter : (((r :: t).map Prod.fst).eraseDups.filter p) =
          if p r.1 then r.1 :: (ks.filter p) else (ks.filter p) := by
        rw [hkeys]
        by_cases h : p r.1 = true <;> simp [h]
      rw [hmap_filter]
      by_cases h : p r.1 = true
      · have hpart := some_count_filter_partition t r.1 p
        have hR : (((t.filter (fun s => p s.1)).map Prod.snd).filterMap id).length =
            (((t.filter (fun s => s.1 == r.1)).map Prod.snd).filterMap id).length + tailR := by
          simpa [h, tailR, trest] using hpart
        simp only [h, Bool.false_eq_true, ↓reduceIte, List.map_cons, List.sum_cons]
        rw [List.filter_cons_of_pos (p := fun s : Sale => p s.1) h]
        rw [← htailG, htail]
        rw [forKey_cons_eq]
        simp only [List.map_cons]
        repeat rw [filterMap_id_cons_length]
        rw [hR]
        omega
      · simp only [h, Bool.false_eq_true, ite_false]
        rw [htailG.symm, htail]
        have hfilt := filter_p_eq_filter_ne_filter_p t r.1 p h
        have hR : ((((r :: t).filter (fun s => p s.1)).map Prod.snd).filterMap id).length = tailR := by
          simp [h, tailR, trest, hfilt]
        rw [hR]
termination_by sales => sales.length
decreasing_by
  simp [trest]
  exact Nat.lt_of_le_of_lt (List.length_filter_le (fun s : Sale => ! s.1 == r.1) t) (Nat.lt_succ_self t.length)

theorem sound : TinylambOpt.ResearchGoals.November26.Analytics.L5.Goal := by
  intro edgeAges country k
  dsimp only
  apply Prod.ext
  · exact (mainLen (fun p => country p == k) edgeAges).symm
  · apply Prod.ext
    · exact (mainNsum (fun p => country p == k) edgeAges).symm
    · exact (mainSome (fun p => country p == k) edgeAges).symm
end TinylambOpt.ResearchProofs.November26.Analytics.Goal_L5
