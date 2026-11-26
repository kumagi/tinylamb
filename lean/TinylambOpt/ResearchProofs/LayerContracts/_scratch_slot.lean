import TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
open TinylambOpt.ResearchGoals.LayerContracts.SlotPlacement

theorem probes_sorted (m : Model) (r : Req) (u : List Nat) :
    (probes m r u).Pairwise (fun x y : Nat => x ≤ y) := by
  unfold probes
  let l := (List.map (m.snap r) (0 :: u.map (fun x => x + 1))).eraseDups
  let le : Nat → Nat → Bool := fun a b => decide (a ≤ b)
  change (l.mergeSort le).Pairwise (fun x y => x ≤ y)
  have hs := List.pairwise_mergeSort (le := le) (by
      intro a b c hab hbc
      simp [le] at hab hbc ⊢
      omega) (by
      intro a b
      simp [le]
      omega) l
  simpa [le] using hs

theorem mem_probes (m : Model) (r : Req) (u : List Nat) (i : Nat) :
    i ∈ probes m r u ↔ i = m.snap r 0 ∨ ∃ x ∈ u, m.snap r (x+1) = i := by
  simp [probes, eq_comm]

theorem find?_eq_some_of_sorted {l : List Nat} {p : Nat → Bool} {a : Nat}
    (hs : l.Pairwise (fun x y : Nat => x ≤ y)) (ha : a ∈ l)
    (hpa : p a = true) (hmin : ∀ b ∈ l, b < a → p b = false) :
    l.find? p = some a := by
  induction l with
  | nil => simp at ha
  | cons hd tl ih =>
      simp only [List.pairwise_cons] at hs
      by_cases hphd : p hd = true
      · by_cases heq : a = hd
        · subst a
          simp [hphd]
        · have hatl : a ∈ tl := by
            simp only [List.mem_cons] at ha
            rcases ha with h | h
            · exact (heq h).elim
            · exact h
          have hda : hd ≤ a := hs.1 a hatl
          have hlt : hd < a := by omega
          have hh := hmin hd (by simp) hlt
          simp [hphd] at hh
      · simp [hphd]
        have hatl' : a ∈ tl := by
          simp only [List.mem_cons] at ha
          rcases ha with h | h
          · subst a
            exact (hphd hpa).elim
          · exact h
        exact ih hs.2 hatl' (by
          intro b hb hblt
          exact hmin b (by simp [hb]) hblt)
