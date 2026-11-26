import TinylambOpt.ResearchGoals.November26.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q08
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpch
open TinylambOpt.ResearchGoals.BenchmarkRematch


theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => by simp
  | a :: l => by
      rw [List.eraseDups_cons]
      refine List.pairwise_cons.mpr ⟨?_, ?_⟩
      · intro b hb
        have hb' : b ∈ l.filter (fun b => !b == a) := by simpa using hb
        simp only [List.mem_filter] at hb'
        intro hba
        subst b
        simp at hb'
      · exact nodup_eraseDups (l.filter (fun b => !b == a))
termination_by l => l.length
decreasing_by
  simp only [List.length_cons]
  exact Nat.lt_succ_of_le (List.length_filter_le (fun b => !b == a) l)

theorem sum_filter_partition (xs : List (Nat × Int)) (p : (Nat × Int) → Bool) :
    ((xs.filter p).map Prod.snd).sum + ((xs.filter (fun x => !p x)).map Prod.snd).sum =
      (xs.map Prod.snd).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true
      · simp [h] at *
        omega
      · have hf : (!p x) = true := by simp [h]
        simp [h, hf] at *
        omega

theorem sum_yes (xs : List (Nat × Int)) (k : Nat) :
    ((xs.filter (fun r => r.1 == k)).map Prod.snd).sum = (forKey xs k).sum := by
  rfl

theorem forKey_filter_not (xs : List (Nat × Int)) (k k' : Nat) (hneq : k ≠ k') :
    forKey (xs.filter (fun r => !(r.1 == k))) k' = forKey xs k' := by
  unfold forKey
  rw [List.filter_filter]
  apply congrArg (fun l : List (Nat × Int) => l.map Prod.snd)
  apply List.filter_congr
  intro r hr
  by_cases he : r.1 == k'
  · have hrk : r.1 = k' := by simpa using he
    have hrnk : r.1 ≠ k := by
      intro h
      apply hneq
      exact h.symm.trans hrk
    simp [he, hrnk]
  · simp [he]

theorem sum_by_keys (ks : List Nat) (xs : List (Nat × Int))
    (hcover : ∀ r ∈ xs, r.1 ∈ ks) (hnd : ks.Nodup) :
    (xs.map Prod.snd).sum = (ks.map fun k => (forKey xs k).sum).sum := by
  induction ks generalizing xs with
  | nil =>
      have hxnil : xs = [] := by
        cases xs with
        | nil => rfl
        | cons r rs =>
            have := hcover r (by simp)
            simp at this
      simp [hxnil, forKey]
  | cons k ks ih =>
      simp only [List.Nodup, List.pairwise_cons] at hnd
      have hknks : k ∉ ks := by
        intro h
        exact hnd.1 k h rfl
      let no := xs.filter (fun r => !(r.1 == k))
      have hnocover : ∀ r ∈ no, r.1 ∈ ks := by
        intro r hr
        have hmem : r ∈ xs := (List.mem_filter.mp hr).1
        have hkeq : !(r.1 == k) = true := (List.mem_filter.mp hr).2
        have hc := hcover r hmem
        rcases List.mem_cons.mp hc with hd | hk
        · have hr1 : r.1 = k := by simpa using hd
          simp [hr1] at hkeq
        · exact hk
      have ih' := ih no hnocover hnd.2
      have hpartition := sum_filter_partition xs (fun r => r.1 == k)
      have hyes := sum_yes xs k
      have hno : (ks.map (fun k' => (forKey no k').sum)).sum =
          (ks.map (fun k' => (forKey xs k').sum)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro k' hk'
        have hne : k ≠ k' := by
          intro h
          subst k'
          exact hknks hk'
        rw [forKey_filter_not xs k k' hne]
      rw [← hpartition, hyes, ih', hno]
      simp

theorem forKey_filter_peru (sales : List (Nat × Int)) (k : Nat) (peru : Nat → Bool)
    (hperu : peru k = true) :
    forKey (sales.filter fun r => peru r.1) k = forKey sales k := by
  unfold forKey
  rw [List.filter_filter]
  apply congrArg (fun l : List (Nat × Int) => l.map Prod.snd)
  apply List.filter_congr
  intro r hr
  by_cases he : r.1 == k
  · have hrk : r.1 = k := by simpa using he
    rw [hrk, hperu]
    simp
  · simp [he]


theorem sound : TinylambOpt.ResearchGoals.November26.Tpch.Q08.Goal := by
  intro sales peru
  let sel := sales.filter (fun r => peru r.1)
  let ks := (sales.map Prod.fst).eraseDups.filter peru
  have hcover : ∀ r ∈ sel, r.1 ∈ ks := by
    intro r hr
    rcases r with ⟨k, p⟩
    have hmem : (k, p) ∈ sales := by simpa using (List.mem_filter.mp hr).1
    have hp : peru k = true := by simpa using (List.mem_filter.mp hr).2
    have hk : k ∈ (sales.map Prod.fst).eraseDups := by
      simpa [List.mem_eraseDups] using (List.mem_map.mpr ⟨(k, p), hmem, rfl⟩ : k ∈ sales.map Prod.fst)
    exact (List.mem_filter.mpr ⟨hk, hp⟩)
  have hnd : ks.Nodup := by
    exact (TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q08.nodup_eraseDups (sales.map Prod.fst)).filter peru
  have hsum := TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q08.sum_by_keys ks sel hcover hnd
  have hkeys : (ks.map (fun k => (forKey sel k).sum)).sum =
      (ks.map (fun k => (forKey sales k).sum)).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro k hk
    have hp : peru k = true := (List.mem_filter.mp hk).2
    rw [TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q08.forKey_filter_peru sales k peru hp]
  simpa [total, sel, ks] using hsum.trans hkeys
end TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q08
