import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q81
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] :
    ∀ xs : List α, xs.eraseDups.Nodup := by
  have key : ∀ (n : Nat) (xs : List α), xs.length ≤ n → xs.eraseDups.Nodup := by
    intro n
    induction n with
    | zero =>
      intro xs hx
      cases xs with
      | nil => simp
      | cons a as => exact absurd (Nat.eq_zero_of_le_zero hx) (by simp)
    | succ n ih =>
      intro xs hx
      cases xs with
      | nil => simp
      | cons a as =>
        have hlen : (as.filter (fun b => !(b == a))).length ≤ n := by
          have hAs : as.length ≤ n := by
            simpa [List.length_cons] using (Nat.succ_le_succ_iff.mp hx)
          exact Nat.le_trans (List.length_filter_le (fun b => !(b == a)) as) hAs
        have htail := ih (as.filter (fun b => !(b == a))) hlen
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · intro ha
          have hf : a ∈ as.filter (fun b => !(b == a)) := List.mem_eraseDups.mp ha
          simpa using (List.mem_filter.mp hf).2
        · exact htail
  intro xs
  exact key xs.length xs (Nat.le_refl _)

/-- In a deduplicated list of keys, the entry for a present key occurs exactly
once. -/
theorem forKey_eraseDups_map {α β : Type} [BEq α] [LawfulBEq α] (f : α → β)
    (xs : List α) (k : α) (hk : k ∈ xs) :
      (((xs.eraseDups.map (fun x => (x, f x))).filter (fun r => r.1 == k)).map Prod.snd) =
        [f k] := by
  have hnodup : xs.eraseDups.Nodup := nodup_eraseDups xs
  have hmem : k ∈ xs.eraseDups := by simpa using hk
  have hcnt : xs.eraseDups.count k = 1 := hnodup.count_of_mem hmem
  let filtered := (xs.eraseDups.map (fun x => (x, f x))).filter (fun r => r.1 == k)
  have hlen : filtered.length = 1 := by
    rw [← List.countP_eq_length_filter]
    rw [List.countP_map]
    change List.countP (fun x : α => x == k) xs.eraseDups = 1
    simpa [List.count_eq_countP] using hcnt
  have hall : ∀ y ∈ filtered, y = (k, f k) := by
    intro y hy
    obtain ⟨p, hp, rfl⟩ := List.mem_map.mp (List.mem_filter.mp hy).1
    have hxeq : p = k := eq_of_beq (List.mem_filter.mp hy).2
    subst p
    rfl
  have hrep : filtered = List.replicate filtered.length (k, f k) := by
    rw [List.eq_replicate_iff]
    exact ⟨rfl, hall⟩
  change filtered.map Prod.snd = [f k]
  rw [hrep, List.map_replicate, hlen]
  rfl

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q81.Goal := by
  intro totals georgia
  dsimp only
  intro r hr hg
  have hrstates : r.1 ∈ (totals.filter (fun x => georgia x.2.1)).map Prod.fst := by
    rw [List.mem_map]
    exact ⟨r, List.mem_filter.mpr ⟨hr, hg⟩, rfl⟩
  exact forKey_eraseDups_map (fun k => (((totals.filter (fun x => x.1 == k)).filterMap
      (fun x => x.2.2)).sum, ((totals.filter (fun x => x.1 == k)).filterMap
      (fun x => x.2.2)).length))
    ((totals.filter (fun x => georgia x.2.1)).map Prod.fst) r.1 hrstates
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q81
