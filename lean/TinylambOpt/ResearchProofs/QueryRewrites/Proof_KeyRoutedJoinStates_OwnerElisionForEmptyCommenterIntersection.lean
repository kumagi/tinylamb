import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
Proof contribution for QueryRewrites_KeyRoutedJoinStates_OwnerElisionForEmptyCommenterIntersection. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_OwnerElisionForEmptyCommenterIntersection

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-
# OwnerElisionForEmptyCommenterIntersection

`members A qualified` is empty exactly when no commenter in `A` is qualified.
Deduplicating the candidate list does not change that emptiness test, and
filtering each commenter list before flattening gives the same qualified
commenter multiset. Hence an owner whose commenters never intersect the
qualified set contributes no group, whether the lookup is done before or after
the flat map.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites (members)

theorem krs_filter_eraseDups_empty_iff {α : Type} [BEq α] [LawfulBEq α]
    (l q : List α) : l.eraseDups.filter q.contains = [] ↔ l.filter q.contains = [] := by
  constructor
  · intro h
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro x hx
    simp only [List.mem_filter] at hx
    have hxq : x ∈ q := by simpa using hx.2
    have hm : x ∈ l.eraseDups.filter q.contains := by simpa [hx.1, hxq]
    simpa [h] using hm
  · intro h
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro x hx
    simp only [List.mem_filter, List.mem_eraseDups] at hx
    have hxq : x ∈ q := by simpa using hx.2
    have hm : x ∈ l.filter q.contains := by simpa [hx.1, hxq]
    simpa [h] using hm

theorem krs_eraseDups_filter_isEmpty_iff {α : Type} [BEq α] [LawfulBEq α]
    (l q : List α) : (l.eraseDups.filter q.contains).isEmpty = (l.filter q.contains).isEmpty := by
  apply Bool.eq_iff_iff.mpr
  simp [krs_filter_eraseDups_empty_iff]

theorem krs_flatMap_filter (posts : List Nat) (commenters : Nat → List Nat) (qualified : List Nat) :
    posts.flatMap (fun p => (commenters p).filter qualified.contains) =
      (posts.flatMap commenters).filter qualified.contains := by
  induction posts with
  | nil => simp
  | cons p ps ih => simp [ih, List.filter_append]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.OwnerElisionForEmptyCommenterIntersection.Goal := by
  intro posts commenters qualified
  dsimp [members]
  rw [krs_eraseDups_filter_isEmpty_iff]
  rw [krs_flatMap_filter]
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_OwnerElisionForEmptyCommenterIntersection
