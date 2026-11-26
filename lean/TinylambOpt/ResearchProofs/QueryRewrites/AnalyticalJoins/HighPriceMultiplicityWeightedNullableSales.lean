import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# HighPriceMultiplicityWeightedNullableSales

Summing sales by product and scaling the nullable subtotal by qualifying price-match
multiplicity preserves the modeled annual SUM. The executor can pass partial states
through the price join.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.HighPriceMultiplicityWeightedNullableSales

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (merge merge_assoc merge_comm merge_none_left nsum_append nsum_cons nsum_replicate)


theorem merge_map_mul_distrib (v s rest : Option Int) (c : Int) :
    merge ((merge v s).map (fun x => x * c)) rest =
      merge (v.map (fun x => x * c)) (merge (s.map (fun x => x * c)) rest) := by
  cases v with
  | none =>
      cases s <;> cases rest <;> simp [merge]
  | some a =>
      cases s with
      | none =>
          cases rest <;> simp [merge]
      | some b =>
          cases rest with
          | none => simp [merge, Int.add_mul]
          | some r => simp [merge, Int.add_mul]; omega


theorem nsum_map_congr {α : Type} (l : List α) (f g : α → Option Int)
    (h : ∀ x ∈ l, f x = g x) : nsum (l.map f) = nsum (l.map g) := by
  induction l with
  | nil => simp [nsum]
  | cons a t ih =>
      simp only [List.map_cons]
      rw [nsum_cons, nsum_cons, h a (by simp),
        ih (fun x hx => h x (by simp [hx]))]

theorem filter_ne_eq_self_of_not_mem (a : Nat) (l : List Nat) (h : a ∉ l) :
    l.filter (fun b => !(b == a)) = l := by
  apply (List.filter_eq_self).2
  intro b hb
  have hbne : b ≠ a := by
    intro heq
    subst b
    exact h hb
  simp [hbne]

theorem filter_filter_comm (p q : Nat → Bool) (l : List Nat) :
    (l.filter p).filter q = (l.filter q).filter p := by
  rw [List.filter_filter, List.filter_filter]
  apply congrArg (fun f => l.filter f)
  funext x
  exact Bool.and_comm _ _

theorem filter_eraseDups (p : Nat → Bool) : ∀ (l : List Nat),
    (l.eraseDups).filter p = (l.filter p).eraseDups
  | [] => by simp
  | a :: t => by
      rw [List.eraseDups_cons]
      simp only [List.filter_cons]
      by_cases hp : p a = true
      · simp only [hp, ↓reduceIte]
        have h1 := filter_eraseDups p (t.filter (fun b => !(b == a)))
        rw [h1]
        rw [List.eraseDups_cons]
        rw [← filter_filter_comm p (fun b => !(b == a)) t]
      · simp only [hp, Bool.false_eq_true, ↓reduceIte]
        have h1 := filter_eraseDups p (t.filter (fun b => !(b == a)))
        rw [h1]
        rw [← filter_filter_comm p (fun b => !(b == a)) t]
        have hnotmem : a ∉ t.filter p := by simp [hp]
        exact congrArg List.eraseDups (filter_ne_eq_self_of_not_mem a (t.filter p) hnotmem)
termination_by l => l.length
decreasing_by
  all_goals
    simp_wf
    have hle := List.length_filter_le (fun b => !(b == a)) t
    omega

theorem nsum_map_eq_merge_of_mem (l : List Nat) (g : Nat → Option Int) (k0 : Nat)
    (hmem : k0 ∈ l) (hnodup : l.Nodup) :
    nsum (l.map g) = merge (g k0) (nsum ((l.filter (fun x => !(x == k0))).map g)) := by
  induction l with
  | nil => simp at hmem
  | cons a t ih =>
      simp only [List.map_cons, nsum_cons]
      by_cases hak : (a == k0) = true
      · have haeq : a = k0 := by simpa using hak
        subst a
        simp only [List.filter_cons, Bool.false_eq_true, ↓reduceIte]
        have hknot : k0 ∉ t := by
          have hnd := (List.nodup_cons).1 hnodup
          exact hnd.1
        rw [filter_ne_eq_self_of_not_mem k0 t hknot]
        simp [merge]
      · have hane : a ≠ k0 := by simpa using hak
        have hmemt : k0 ∈ t := by
          simp only [List.mem_cons] at hmem
          rcases hmem with h | h
          · exact False.elim (hane h.symm)
          · exact h
        have hnd_t : t.Nodup := (List.nodup_cons).1 hnodup |>.2
        have ih' := ih hmemt hnd_t
        have hfilt : (a :: t).filter (fun x => !(x == k0)) = a :: (t.filter (fun x => !(x == k0))) := by
          simp [hane]
        rw [hfilt]
        simp only [List.map_cons, nsum_cons]
        rw [ih']
        rw [← merge_assoc, ← merge_assoc, merge_comm (g a) (g k0)]

def groupSum (sales : List (Nat × Option Int)) (k : Nat) : Option Int :=
  nsum ((sales.filter (fun r => r.1 == k)).map Prod.snd)

def keysOf (sales : List (Nat × Option Int)) : List Nat :=
  (sales.map Prod.fst).eraseDups

theorem groupSum_cons (k0 : Nat) (v0 : Option Int) (k : Nat) (t : List (Nat × Option Int)) :
    groupSum ((k0, v0) :: t) k =
      if (k0 == k) = true then merge v0 (groupSum t k) else groupSum t k := by
  unfold groupSum
  simp only [List.filter_cons]
  by_cases h : (k0 == k) = true
  · simp only [h, ↓reduceIte, List.map_cons, nsum_cons]
  · simp [h]

theorem groupSum_eq_none_of_not_key : ∀ (sales : List (Nat × Option Int)) (k : Nat),
    k ∉ sales.map Prod.fst → groupSum sales k = none
  | [], k, _ => by simp [groupSum, nsum]
  | (k0, v0) :: t, k, h => by
      have hne : k ≠ k0 := by
        intro heq
        apply h
        simp [heq]
      have ht : k ∉ t.map Prod.fst := by
        intro hkt
        apply h
        simp [hkt]
      have ih' := groupSum_eq_none_of_not_key t k ht
      rw [groupSum_cons]
      have hne' : k0 ≠ k := by intro hk; exact hne hk.symm
      simp [hne', ih']

theorem keysOf_cons (k0 : Nat) (v0 : Option Int) (t : List (Nat × Option Int)) :
    keysOf ((k0, v0) :: t) = k0 :: (keysOf t).filter (fun x => !(x == k0)) := by
  unfold keysOf
  simp only [List.map_cons, List.eraseDups_cons]
  rw [← filter_eraseDups]

theorem mainB5 : ∀ (sales : List (Nat × Option Int)) (items : List Nat),
    nsum (sales.flatMap (fun (k,v) => List.replicate (items.count k) v)) =
      nsum ((keysOf sales).filter items.contains |>.map
        (fun k => (groupSum sales k).map (fun v => v * (items.count k : Int)))) := by
  intro sales
  induction sales with
  | nil =>
      intro items
      simp [groupSum, nsum]
  | cons r t ih =>
      intro items
      rcases r with ⟨k0, v0⟩
      have hrepeat :
          nsum (List.replicate (items.count k0) v0) =
            if items.count k0 = 0 then none else v0.map (fun v => v * (items.count k0 : Int)) := by
        simpa only [Int.mul_comm] using nsum_replicate (items.count k0) v0
      rw [List.flatMap_cons, nsum_append, ih items, hrepeat]
      by_cases hc0 : items.count k0 = 0
      · have hnotmem : k0 ∉ items := (List.count_eq_zero).1 hc0
        have hnot : items.contains k0 = false := by simp [hnotmem]
        simp [hc0, hnot]
        rw [merge_none_left]
        -- rewrite keys
        rw [keysOf_cons]
        simp only [List.filter_cons, hnot, Bool.false_eq_true, ↓reduceIte]
        rw [filter_filter_comm (fun x => !(x == k0)) items.contains (keysOf t)]
        rw [filter_ne_eq_self_of_not_mem k0 ((keysOf t).filter items.contains) (by
          intro hx
          have hx' := hx
          simp only [List.mem_filter] at hx'
          exact hnotmem (by simpa using hx'.2))]
        apply nsum_map_congr
        intro k hk
        have hkne : k ≠ k0 := by
          intro h
          subst k
          have hk' := hk
          simp only [List.mem_filter] at hk'
          exact hnotmem (by simpa using hk'.2)
        rw [groupSum_cons]
        have hkne' : k0 ≠ k := by intro h; exact hkne h.symm
        simp [hkne']
      · have hmemitems : k0 ∈ items := by
          exact Classical.not_not.mp (fun hn => hc0 ((List.count_eq_zero).2 hn))
        have hc0true : items.contains k0 = true := by simp [hmemitems]
        simp [hc0, hc0true]
        rw [keysOf_cons]
        simp only [List.filter_cons, hc0true, ↓reduceIte]
        rw [← filter_filter_comm items.contains (fun x => !(x == k0)) (keysOf t)]
        simp only [List.map_cons, nsum_cons]
        let K := (keysOf t).filter items.contains
        have hcongr : nsum ((K.filter (fun x => !(x == k0))).map
              (fun k => (groupSum ((k0, v0) :: t) k).map (fun v => v * (items.count k : Int)))) =
            nsum ((K.filter (fun x => !(x == k0))).map
              (fun k => (groupSum t k).map (fun v => v * (items.count k : Int)))) := by
          apply nsum_map_congr
          intro k hk
          have hkne : k ≠ k0 := by
            intro h
            subst k
            simp at hk
          rw [groupSum_cons]
          have hkne' : k0 ≠ k := by intro h; exact hkne h.symm
          simp [hkne']
        rw [hcongr]
        by_cases hk0 : k0 ∈ K
        · have hnodupK : K.Nodup := by
            dsimp [K]
            exact (KeyGrouping.nodup_eraseDups (t.map Prod.fst)).filter items.contains
          have hsplit := nsum_map_eq_merge_of_mem K
            (fun k => (groupSum t k).map (fun v => v * (items.count k : Int))) k0 hk0 hnodupK
          rw [hsplit]
          rw [groupSum_cons]
          simp
          rw [merge_map_mul_distrib]
        · rw [filter_ne_eq_self_of_not_mem k0 K hk0]
          have hk0notkeys : k0 ∉ keysOf t := by
            intro hk
            apply hk0
            dsimp [K]
            simp [hk, hmemitems]
          have hk0notfst : k0 ∉ t.map Prod.fst := by
            intro h
            apply hk0notkeys
            exact (List.mem_eraseDups).2 h
          have hnogroup : groupSum t k0 = none := groupSum_eq_none_of_not_key t k0 hk0notfst
          rw [groupSum_cons]
          simp [hnogroup]
          dsimp [K]
          rw [merge_map_mul_distrib]
          cases h1 : Option.map (fun v => v * (items.count k0 : Int)) v0 <;>
            cases h2 : nsum (List.map (fun k => Option.map (fun v => v * (items.count k : Int)) (groupSum t k))
              (List.filter items.contains (keysOf t))) <;> simp [merge, h1, h2]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.HighPriceMultiplicityWeightedNullableSales.Goal := by
  intro sales items
  dsimp only
  simpa [keysOf, groupSum] using mainB5 sales items

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.HighPriceMultiplicityWeightedNullableSales

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.HighPriceMultiplicityWeightedNullableSales.sound

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
