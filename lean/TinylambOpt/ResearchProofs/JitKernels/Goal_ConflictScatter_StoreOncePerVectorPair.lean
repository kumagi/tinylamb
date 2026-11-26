import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_StoreOncePerVectorPair

open TinylambOpt.ResearchGoals.JitKernels

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ConflictScatter.StoreOncePerVectorPair.Goal := by
  intro a b k
  have happendelta : ∀ (u v : ConflictScatter.Updates) (k : Nat),
      ConflictScatter.delta (u ++ v) k = ConflictScatter.delta u k + ConflictScatter.delta v k := by
    intro u v k
    simp [ConflictScatter.delta, List.filter_append, List.map_append, List.sum_append]
  have hnodup : ∀ (l : List Nat) (p : Nat → Bool), (l.filter p).eraseDups.Nodup := by
    intro l
    induction l with
    | nil => intro p; simp
    | cons x xs ih =>
        intro p
        by_cases hpx : p x
        · rw [List.filter_cons_of_pos hpx, List.eraseDups_cons]
          constructor
          · intro a ha hxa
            have haq : (! a == x) = true := (List.mem_filter.mp (List.mem_eraseDups.mp ha)).2
            have hane : a ≠ x := by
              intro h
              subst a
              simp at haq
            exact hane hxa.symm
          · rw [List.filter_filter]
            exact ih (fun y => ! y == x && p y)
        · rw [List.filter_cons_of_neg hpx]
          exact ih p
  have hfilterTrue : ∀ l : List Nat, l.filter (fun _ => true) = l := by
    intro l
    induction l with
    | nil => simp
    | cons x xs ih => simp [ih]
  have hfilter_single : ∀ (ls : List Nat) (k : Nat), ls.Nodup → k ∈ ls →
      ls.filter (fun x => x == k) = [k] := by
    intro ls k hnd hk
    induction ls generalizing k with
    | nil => simp at hk
    | cons x xs ih =>
        simp only [List.nodup_cons] at hnd
        by_cases hxk : x == k
        · have hx : x = k := beq_iff_eq.mp hxk
          subst k
          have htail : xs.filter (fun y => y == x) = [] := by
            apply List.eq_nil_iff_forall_not_mem.mpr
            intro y hy
            have hyx : y == x := (List.mem_filter.mp hy).2
            exact hnd.1 (by simpa [beq_iff_eq.mp hyx] using hy)
          simp [hxk, htail]
        · have hkxs : k ∈ xs := by
            rcases List.mem_cons.mp hk with h | h
            · exact (hxk (by simpa [h])).elim
            · exact h
          simp [hxk, ih k hnd.2 hkxs]
  have hfilter_none : ∀ (ls : List Nat) (k : Nat), k ∉ ls →
      ls.filter (fun x => x == k) = [] := by
    intro ls k hk
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro x hx
    have hxk : x == k := (List.mem_filter.mp hx).2
    exact hk (by simpa [beq_iff_eq.mp hxk] using hx)
  have hnomem : ∀ (u : ConflictScatter.Updates) (k : Nat), k ∉ ConflictScatter.leaders u →
      ConflictScatter.delta u k = 0 := by
    intro u k hk
    unfold ConflictScatter.delta
    have hnil : u.filter (fun z => z.1 == k) = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro z hz
      have hzu : z ∈ u := (List.mem_filter.mp hz).1
      have hmemmap : z.1 ∈ u.map Prod.fst := List.mem_map.mpr ⟨z, hzu, rfl⟩
      have hmem : z.1 ∈ ConflictScatter.leaders u := by
        simpa [ConflictScatter.leaders] using hmemmap
      have hzk : z.1 = k := beq_iff_eq.mp (List.mem_filter.mp hz).2
      exact hk (by simpa [hzk] using hmem)
    simp [hnil]
  have hcomb : ∀ (u : ConflictScatter.Updates) (k : Nat),
      ConflictScatter.delta (ConflictScatter.combine u) k = ConflictScatter.delta u k := by
    intro u k
    have hmap : ((ConflictScatter.leaders u).map (fun l => (l, ConflictScatter.delta u l))).filter
          (fun z => z.1 == k) =
        ((ConflictScatter.leaders u).filter (fun l => l == k)).map
          (fun l => (l, ConflictScatter.delta u l)) := by
      induction ConflictScatter.leaders u with
      | nil => simp
      | cons l ls ih => by_cases hlk : l == k <;> simp [hlk, ih]
    by_cases hk : k ∈ ConflictScatter.leaders u
    · have hfilt : (ConflictScatter.combine u).filter (fun z => z.1 == k) =
          [(k, ConflictScatter.delta u k)] := by
        unfold ConflictScatter.combine
        have hnd : (ConflictScatter.leaders u).Nodup := by
          have hnd' := hnodup (u.map Prod.fst) (fun _ => true)
          rw [hfilterTrue] at hnd'
          simpa [ConflictScatter.leaders] using hnd'
        rw [hmap, hfilter_single (ConflictScatter.leaders u) k hnd hk]
        simp [hk]
      simp [ConflictScatter.delta, hfilt]
    · have hfilt : (ConflictScatter.combine u).filter (fun z => z.1 == k) = [] := by
        unfold ConflictScatter.combine
        rw [hmap, hfilter_none (ConflictScatter.leaders u) k hk]
        simp
      simp [ConflictScatter.delta, hfilt]
      simpa [ConflictScatter.delta] using (hnomem u k hk).symm
  rw [hcomb (ConflictScatter.combine a ++ ConflictScatter.combine b) k]
  rw [happendelta (ConflictScatter.combine a) (ConflictScatter.combine b) k]
  rw [hcomb a k, hcomb b k]
  rw [← happendelta a b k]

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_StoreOncePerVectorPair
