import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# NameCodeIntersectionCardinality

Canonical codes for equal names preserve the cardinality of the modeled distinct
intersection on name and date.

The executor can compare compact name codes in the set operation when the code
assignment preserves SQL equality.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.NameCodeIntersectionCardinality
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem eraseDups_map_of_injective {α β : Type} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    (f : α → β) (hf : Function.Injective f) : ∀ l : List α, (l.map f).eraseDups = l.eraseDups.map f
  | [] => rfl
  | a :: l => by
      simp only [List.map_cons]
      rw [List.eraseDups_cons, List.eraseDups_cons]
      have hbe : ∀ x : α, (f x == f a) = (x == a) := by
        intro x
        apply Bool.eq_iff_iff.mpr
        constructor
        · intro h
          have hfx : f x = f a := by simpa using h
          have hxa : x = a := hf hfx
          simp [hxa]
        · intro h
          have hxa : x = a := by simpa using h
          simp [hxa]
      have hfilter : List.filter (fun b => !b == f a) (List.map f l) =
          List.map f (List.filter (fun b => !b == a) l) := by
        rw [List.filter_map]
        apply congrArg (List.map f)
        apply List.filter_congr
        intro x hx
        simp [hbe x]
      rw [hfilter, eraseDups_map_of_injective f hf (List.filter (fun b => !b == a) l)]
      rfl
termination_by l => l.length
decreasing_by
  have := List.length_filter_le (fun b => !b == a) l
  simp
  omega

theorem members_map_of_injective {α β : Type} [BEq α] [LawfulBEq α] [BEq β] [LawfulBEq β]
    (f : α → β) (hf : Function.Injective f) (a b : List α) :
    members (a.map f) (b.map f) = (members a b).map f := by
  unfold members
  rw [eraseDups_map_of_injective f hf a]
  rw [List.filter_map]
  apply congrArg (List.map f)
  apply List.filter_congr
  intro x hx
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro h
    have hm : f x ∈ b.map f := by simpa using h
    rw [List.mem_map] at hm
    rcases hm with ⟨y, hy, hyx⟩
    have hy' : y = x := hf hyx
    subst hy'
    simpa using hy
  · intro h
    have hm : x ∈ b := by simpa using h
    have : f x ∈ b.map f := List.mem_map.mpr ⟨x, hm, rfl⟩
    simpa using this

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.NameCodeIntersectionCardinality.Goal := by
  intro s c w code hcode
  dsimp only
  have hinj : Function.Injective (fun p : String × Nat => (code p.1, p.2)) := by
    intro p q h
    have h1 : code p.1 = code q.1 := by simpa using congrArg Prod.fst h
    have hn : p.1 = q.1 := hcode p.1 q.1 h1
    have h2 : p.2 = q.2 := by simpa using congrArg Prod.snd h
    cases p
    cases q
    simp_all
  rw [members_map_of_injective _ hinj s c]
  rw [members_map_of_injective _ hinj (members s c) w]
  simp
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.NameCodeIntersectionCardinality
