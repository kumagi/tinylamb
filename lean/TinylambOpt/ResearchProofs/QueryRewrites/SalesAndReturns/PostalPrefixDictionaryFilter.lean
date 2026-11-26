import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# PostalPrefixDictionaryFilter

Testing the postcode prefix on each address code's dictionary entry selects the same
encoded rows as applying the predicate to the decoded strings.

A dictionary scan can compute qualifying codes once and avoid repeating substring
extraction on the fact-side path.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.PostalPrefixDictionaryFilter
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem zipIdx_index_ge (zs : List String) (n j : Nat) (hj : j ∈ (zs.zipIdx n).map Prod.snd) : n ≤ j := by
  induction zs generalizing n with
  | nil => simp at hj
  | cons z zs ih =>
      rw [List.zipIdx_cons] at hj
      simp only [List.map_cons, List.mem_cons] at hj
      rcases hj with hj | hj
      · subst hj
        omega
      · have hle := ih (n + 1) hj
        omega

theorem not_mem_tail_index (zs : List String) (n : Nat) :
    (List.map Prod.snd (zs.zipIdx (n + 1))).contains n = false := by
  have hnot : ¬ n ∈ (zs.zipIdx (n + 1)).map Prod.snd := by
    intro h
    have hle := zipIdx_index_ge zs (n + 1) n h
    omega
  simpa using hnot

theorem not_mem_zipIdx_head (zs : List String) (n : Nat) (x : String) : (x, n) ∉ zs.zipIdx (n + 1) := by
  intro h
  have hm : n ∈ (zs.zipIdx (n + 1)).map Prod.snd := by
    rw [List.mem_map]
    exact ⟨(x, n), h, rfl⟩
  have hle := zipIdx_index_ge zs (n + 1) n hm
  omega

theorem good_index_aux (zips allowed : List String) (n i : Nat) :
    (((zips.zipIdx n).filter (fun (z, _) => allowed.contains (z.take 5).toString)).map Prod.snd).contains (n + i) = true ↔
      zips[i]?.any (fun z => allowed.contains (z.take 5).toString) = true := by
  revert n i
  induction zips with
  | nil => intro n i; simp
  | cons z zs ih =>
      intro n i
      cases i with
      | zero =>
          have hnotfiltered :
              (List.map Prod.snd (List.filter (fun (z, _) => allowed.contains (z.take 5).toString)
                (zs.zipIdx (n + 1)))).contains n = false := by
            have hnot : ¬ n ∈ (List.map Prod.snd (List.filter (fun (z, _) => allowed.contains (z.take 5).toString)
                (zs.zipIdx (n + 1)))) := by
              intro hn
              rw [List.mem_map] at hn
              rcases hn with ⟨p, hp, hpn⟩
              rw [List.mem_filter] at hp
              have hbase : p.2 ∈ (zs.zipIdx (n + 1)).map Prod.snd := by
                rw [List.mem_map]
                exact ⟨p, hp.1, rfl⟩
              have hle := zipIdx_index_ge zs (n + 1) p.2 hbase
              omega
            simpa using hnot
          rw [List.zipIdx_cons]
          simp [not_mem_zipIdx_head]
      | succ i =>
          rw [List.zipIdx_cons]
          have h := ih (n + 1) i
          simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using h

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.PostalPrefixDictionaryFilter.Goal := by
  intro zips allowed codes
  dsimp only
  apply List.filter_congr
  intro i hi
  have h := good_index_aux zips allowed 0 i
  exact Bool.eq_iff_iff.mpr (by simpa using h)
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.PostalPrefixDictionaryFilter
