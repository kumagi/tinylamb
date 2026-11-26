import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q09_PerBlockRegionDistinctMergeGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem filter_eraseDups_comm {α : Type} [BEq α] [LawfulBEq α]
    (a : α) (l : List α) :
    (l.eraseDups).filter (fun b => !b == a) = (l.filter (fun b => !b == a)).eraseDups := by
  generalize hn : l.length = n
  revert a l
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro a l hn
      cases l with
      | nil => simp
      | cons b l =>
          have hlen : (List.filter (fun x => !x == b) l).length < n := by
            rw [← hn]
            simp
            have hle := List.length_filter_le (fun x => !x == b) l
            omega
          by_cases hba : b == a
          · have hab : a = b := (beq_iff_eq.mp hba).symm
            subst a
            have hrec := ih (List.filter (fun x => !x == b) l).length hlen b
              (List.filter (fun x => !x == b) l) rfl
            simp [List.eraseDups_cons, hrec]
          · have hne : (!b == a) = true := by simpa using hba
            have hlen2 : (List.filter (fun x => !x == a) l).length < n := by
              rw [← hn]
              simp
              have hle := List.length_filter_le (fun x => !x == a) l
              omega
            have hrec1 := ih (List.filter (fun x => !x == b) l).length hlen a
              (List.filter (fun x => !x == b) l) rfl
            have hrec2 := ih (List.filter (fun x => !x == a) l).length hlen2 b
              (List.filter (fun x => !x == a) l) rfl
            rw [List.eraseDups_cons (a := b) (as := l)]
            simp [hne]
            rw [hrec1]
            rw [List.eraseDups_cons (a := b) (as := List.filter (fun x => !x == a) l)]
            simp [List.filter_filter, Bool.and_comm]

private theorem eraseDups_idem {α : Type} [BEq α] [LawfulBEq α] (p : List α) :
    p.eraseDups.eraseDups = p.eraseDups := by
  generalize hn : p.length = n
  revert p
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro p hn
      cases p with
      | nil => simp
      | cons a p =>
          have hlen : (List.filter (fun x => !x == a) p).length < n := by
            rw [← hn]
            simp
            have hle := List.length_filter_le (fun x => !x == a) p
            omega
          have hrec := ih (List.filter (fun x => !x == a) p).length hlen
            (List.filter (fun x => !x == a) p) rfl
          calc
            ((a :: p).eraseDups).eraseDups
                = (a :: (List.filter (fun x => !x == a) p).eraseDups).eraseDups := by
                    rw [List.eraseDups_cons]
            _ = a :: (List.filter (fun x => !x == a)
                  ((List.filter (fun x => !x == a) p).eraseDups)).eraseDups := by
                    rw [List.eraseDups_cons]
            _ = a :: ((List.filter (fun x => !x == a)
                  (List.filter (fun x => !x == a) p)).eraseDups).eraseDups := by
                    rw [filter_eraseDups_comm]
            _ = a :: (List.filter (fun x => !x == a) p).eraseDups := by simp [hrec]
            _ = (a :: p).eraseDups := by rw [List.eraseDups_cons]

private theorem removeAll_eraseDups {α : Type} [BEq α] [LawfulBEq α]
    (q p : List α) : q.removeAll p.eraseDups = q.removeAll p := by
  simp [List.removeAll, List.mem_eraseDups]

private theorem eraseDups_append_eraseDups {α : Type} [BEq α] [LawfulBEq α]
    (p q : List α) : (p.eraseDups ++ q).eraseDups = (p ++ q).eraseDups := by
  rw [List.eraseDups_append, List.eraseDups_append, eraseDups_idem, removeAll_eraseDups]

private theorem eraseDups_append_of_right {α : Type} [BEq α] [LawfulBEq α]
    (p ys : List α) : (p ++ ys).eraseDups = (p ++ ys.eraseDups).eraseDups := by
  generalize hn : p.length = n
  revert p ys
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro p ys hn
      cases p with
      | nil =>
          simp
          exact (eraseDups_idem ys).symm
      | cons a p =>
          have hlen : (List.filter (fun x => !x == a) p).length < n := by
            rw [← hn]
            simp
            have hle := List.length_filter_le (fun x => !x == a) p
            omega
          have hrec := ih (List.filter (fun x => !x == a) p).length hlen
            (List.filter (fun x => !x == a) p)
            (List.filter (fun x => !x == a) ys) rfl
          rw [List.cons_append, List.cons_append, List.eraseDups_cons, List.eraseDups_cons]
          rw [List.filter_append, List.filter_append]
          rw [filter_eraseDups_comm]
          exact congrArg (fun l => a :: l) hrec

private theorem local_dedup (parts : List (List Nat)) :
    (parts.flatMap List.eraseDups).eraseDups = parts.flatten.eraseDups := by
  induction parts with
  | nil => simp
  | cons p parts ih =>
      rw [List.flatMap_cons, List.flatten_cons]
      rw [eraseDups_append_eraseDups]
      rw [eraseDups_append_of_right]
      rw [ih]
      rw [← eraseDups_append_of_right]

private theorem flatMap_filter_map (blocks : List (List (Nat × Nat))) (reg : Nat) :
    (blocks.flatten.filter (fun r => r.1 == reg)).map Prod.snd =
      (blocks.map (fun b => (b.filter (fun r => r.1 == reg)).map Prod.snd)).flatten := by
  induction blocks with
  | nil => rfl
  | cons b bs ih => simp [List.flatten_cons, List.filter_append, List.map_append, Function.comp_def]

private theorem flatMap_eraseDups_parts (blocks : List (List (Nat × Nat))) (reg : Nat) :
    blocks.flatMap (fun b => ((b.filter (fun r => r.1 == reg)).map Prod.snd).eraseDups) =
      (blocks.map (fun b => (b.filter (fun r => r.1 == reg)).map Prod.snd)).flatMap List.eraseDups := by
  induction blocks with
  | nil => rfl
  | cons b bs ih => simp [ih]

theorem sound : Q09_PerBlockRegionDistinctMergeGoal := by
  intro blocks reg
  let parts := blocks.map (fun b => (b.filter (fun r => r.1 == reg)).map Prod.snd)
  apply congrArg List.length
  calc
    ((blocks.flatten.filter (fun r => r.1 == reg)).map Prod.snd).eraseDups
        = parts.flatten.eraseDups := by
            rw [flatMap_filter_map]
    _ = (parts.flatMap List.eraseDups).eraseDups := (local_dedup parts).symm
    _ = (blocks.flatMap (fun b => ((b.filter (fun r => r.1 == reg)).map Prod.snd).eraseDups)).eraseDups := by
            rw [flatMap_eraseDups_parts]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q09_PerBlockRegionDistinctMergeGoal
