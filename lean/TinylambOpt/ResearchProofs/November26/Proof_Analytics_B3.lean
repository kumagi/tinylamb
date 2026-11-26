import TinylambOpt.ResearchGoals.November26.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Analytics.Goal_B3
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.BenchmarkRematch

abbrev Key := Nat × Nat
abbrev Sale := Key × Option Int

def addOpt (a b : Option Int) : Option Int :=
  match a, b with
  | none, none => none
  | some x, none => some x
  | none, some y => some y
  | some x, some y => some (x + y)

theorem isEmpty_append {α : Type} (a b : List α) : (a ++ b).isEmpty = (a.isEmpty && b.isEmpty) := by
  cases a <;> simp

theorem sum_eq_zero_of_isEmpty (l : List Int) (h : l.isEmpty = true) : l.sum = 0 := by
  cases l <;> simp at h ⊢

theorem nsum_append (a b : List (Option Int)) : nsum (a ++ b) = addOpt (nsum a) (nsum b) := by
  unfold nsum addOpt
  simp only [List.filterMap_append, isEmpty_append, List.sum_append]
  by_cases ha : (List.filterMap id a).isEmpty = true <;>
    by_cases hb : (List.filterMap id b).isEmpty = true <;>
    simp [ha, hb, sum_eq_zero_of_isEmpty]

theorem nsum_one (o : Option Int) : nsum [o] = o := by
  cases o <;> simp [nsum]

theorem nsum_cons_addOpt (o : Option Int) (xs : List (Option Int)) :
    nsum (o :: xs) = addOpt o (nsum xs) := by
  change nsum ([o] ++ xs) = addOpt o (nsum xs)
  rw [nsum_append, nsum_one]

theorem addOpt_assoc (a b c : Option Int) : addOpt (addOpt a b) c = addOpt a (addOpt b c) := by
  cases a <;> cases b <;> cases c <;> simp [addOpt, Int.add_assoc]

theorem addOpt_left_comm (a b c : Option Int) : addOpt a (addOpt b c) = addOpt b (addOpt a c) := by
  cases a <;> cases b <;> cases c <;> simp [addOpt, Int.add_assoc, Int.add_left_comm, Int.add_comm]

theorem addOpt_none_left (a : Option Int) : addOpt none a = a := by
  cases a <;> rfl

theorem addOpt_none_right (a : Option Int) : addOpt a none = a := by
  cases a <;> rfl

theorem filter_map_fst_not_eq (r : Sale) (t : List Sale) :
    (t.map Prod.fst).filter (fun b => ! b == r.1) =
      (t.filter (fun s => ! s.1 == r.1)).map Prod.fst := by
  induction t with
  | nil => rfl
  | cons s t ih => by_cases h : s.1 == r.1 <;> simp [h, ih]

theorem eraseDups_fst_cons (r : Sale) (t : List Sale) :
    ((r :: t).map Prod.fst).eraseDups =
      r.1 :: ((t.filter (fun s => ! s.1 == r.1)).map Prod.fst).eraseDups := by
  change (r.1 :: t.map Prod.fst).eraseDups =
    r.1 :: ((t.filter (fun s => ! s.1 == r.1)).map Prod.fst).eraseDups
  rw [List.eraseDups_cons]
  congr 1
  exact congrArg List.eraseDups (filter_map_fst_not_eq r t)

theorem forKey_cons_eq (r : Sale) (t : List Sale) :
    forKey (r :: t) r.1 = r.2 :: (t.filter (fun s => s.1 == r.1)).map Prod.snd := by
  simp [forKey]

theorem forKey_cons_ne (r : Sale) (t : List Sale) (k : Key) (h : (k == r.1) = false) :
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
        by_cases hsk : s.1 == k
        · have heq : s.1 = k := beq_iff_eq.mp hsk
          have hneq : s.1 ≠ r.1 := by
            intro he
            exact hne (by simpa [heq] using he.symm)
          have hsk_ne : (! s.1 == r.1) = true := by simpa [beq_iff_eq] using hneq
          simp [hsk, hsk_ne, ih]
        · simp [hsk, ih]
  unfold forKey
  simp [h', hfilter]

theorem nsum_filter_partition (t : List Sale) (a : Key) (p : Key → Bool) :
    nsum ((t.filter (fun s => p s.1)).map Prod.snd) =
      addOpt (if p a then nsum ((t.filter (fun s => s.1 == a)).map Prod.snd) else none)
        (nsum (((t.filter (fun s => ! s.1 == a)).filter (fun s => p s.1)).map Prod.snd)) := by
  induction t with
  | nil => simp [nsum, addOpt]
  | cons s t ih =>
      by_cases hsa : s.1 == a
      · have heq : s.1 = a := beq_iff_eq.mp hsa
        subst a
        by_cases hp : p s.1 = true
        · simp [hp, nsum_cons_addOpt, ih, addOpt_assoc]
        · simp [hp, ih]
      · by_cases hp : p s.1 = true
        · simp [hsa, hp, nsum_cons_addOpt, ih, addOpt_left_comm]
        · simp [hsa, hp, ih]

theorem main (p : Key → Bool) : ∀ sales : List Sale,
    nsum (((sales.map Prod.fst).eraseDups.filter p).map
      (fun k => nsum (forKey sales k))) =
      nsum ((sales.filter (fun s => p s.1)).map Prod.snd)
  | [] => by simp [nsum]
  | r :: t => by
      let trest := t.filter (fun s => ! s.1 == r.1)
      let ks := (trest.map Prod.fst).eraseDups
      let tailG := nsum ((ks.filter p).map (fun k => nsum (forKey trest k)))
      let tailR := nsum ((trest.filter (fun s => p s.1)).map Prod.snd)
      have ih := main p trest
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
            addOpt (nsum ((t.filter (fun s => s.1 == r.1)).map Prod.snd)) tailR := by
          rw [hpart]
          simp [h, tailR, trest]
        simp [h, nsum_cons_addOpt, forKey_cons_eq, htailG.symm, htail, hR, addOpt_assoc]
      · simp only [h, Bool.false_eq_true, ite_false]
        rw [htailG.symm, htail]
        have hR : nsum (((r :: t).filter (fun s => p s.1)).map Prod.snd) = tailR := by
          simp [h, nsum_filter_partition t r.1 p, tailR, trest, addOpt_none_left]
        rw [hR]
termination_by sales => sales.length
decreasing_by
  simp [trest]
  exact Nat.lt_of_le_of_lt (List.length_filter_le (fun s : Sale => ! s.1 == r.1) t) (Nat.lt_succ_self t.length)

theorem sound : TinylambOpt.ResearchGoals.November26.Analytics.B3.Goal := by
  intro sales labels k
  dsimp only
  exact main (fun p => labels p == k) sales


#print axioms TinylambOpt.ResearchProofs.November26.Analytics.Goal_B3.sound
end TinylambOpt.ResearchProofs.November26.Analytics.Goal_B3
