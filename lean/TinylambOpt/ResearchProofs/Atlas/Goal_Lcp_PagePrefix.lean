import TinylambOpt.ResearchGoals.Atlas.TextExecution

/-!
SQL string predicates can use encoded or summarized text when the modeled match,
boundaries, and collation are preserved.

Proved

- `Lcp.PagePrefix` — `∀ (p a b w : Word), a ≤ w → w ≤ b → p.isPrefixOf a → p.isPrefixOf
  b → p.isPrefixOf w`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! PagePrefix for `Lcp`: a word bounded lexicographically between two words
that share a prefix must share that prefix as well.  Truth was checked first
by exhaustive evaluation over all words of length at most two on a two-letter
alphabet; the proof below is generic over arbitrary words. -/

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Lcp_PagePrefix

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.Lcp

/-! Exhaustive truth check on small inputs: every quadruple of words of length
at most two over the alphabet {0,1} satisfying the hypotheses satisfies the
conclusion. -/

private def smallWords : List Word :=
  [[]] ++ [[0]] ++ [[1]] ++ [[0, 0]] ++ [[0, 1]] ++ [[1, 0]] ++ [[1, 1]]

#guard smallWords.all (fun p =>
  smallWords.all (fun a =>
    smallWords.all (fun b =>
      smallWords.all (fun w =>
        !(decide (a ≤ w) && decide (w ≤ b) && p.isPrefixOf a && p.isPrefixOf b) ||
          p.isPrefixOf w))))

private theorem isPrefixOf_true_iff (p a : Word) : p.isPrefixOf a = true ↔ ∃ t, a = p ++ t := by
  constructor
  · intro h
    have hp : List.IsPrefix p a := by simpa using h
    rcases hp with ⟨t, ht⟩
    exact ⟨t, ht.symm⟩
  · rintro ⟨t, rfl⟩
    simp

theorem sound_Lcp_PagePrefix : PagePrefix.Goal := by
  intro p
  induction p with
  | nil =>
      intro a b w haw hwb hpa hpb
      exact (isPrefixOf_true_iff [] w).mpr ⟨w, by simp⟩
  | cons x ps ih =>
      intro a b w haw hwb hpa hpb
      rcases (isPrefixOf_true_iff (x :: ps) a).mp hpa with ⟨ta, rfl⟩
      rcases (isPrefixOf_true_iff (x :: ps) b).mp hpb with ⟨tb, rfl⟩
      cases w with
      | nil =>
          exact False.elim (haw (List.Lex.nil))
      | cons y ws =>
          by_cases hxy : x = y
          · subst y
            have hpsa : ps.isPrefixOf (ps ++ ta) = true :=
              (isPrefixOf_true_iff ps (ps ++ ta)).mpr ⟨ta, rfl⟩
            have hpsb : ps.isPrefixOf (ps ++ tb) = true :=
              (isPrefixOf_true_iff ps (ps ++ tb)).mpr ⟨tb, rfl⟩
            have htaila : ps ++ ta ≤ ws := by
              intro hlt
              exact haw (List.Lex.cons hlt)
            have htailb : ws ≤ ps ++ tb := by
              intro hlt
              exact hwb (List.Lex.cons hlt)
            have ihres := ih (ps ++ ta) (ps ++ tb) ws htaila htailb hpsa hpsb
            rcases (isPrefixOf_true_iff ps ws).mp ihres with ⟨tw, rfl⟩
            exact (isPrefixOf_true_iff (x :: ps) (x :: ps ++ tw)).mpr ⟨tw, rfl⟩
          · have hltgt : x < y ∨ y < x := by omega
            rcases hltgt with hxylt | hyxlt
            · exact False.elim (hwb (List.Lex.rel hxylt))
            · exact False.elim (haw (List.Lex.rel hyxlt))

end TinylambOpt.ResearchProofs.Atlas.Goal_Lcp_PagePrefix
