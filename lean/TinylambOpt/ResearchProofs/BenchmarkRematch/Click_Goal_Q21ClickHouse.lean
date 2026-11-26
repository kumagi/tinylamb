import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q21ClickHouse
open Std
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem contains_goo_of_contains_google (s : String) :
    s.contains "google" = true → s.contains "goo" = true := by
  intro h
  rw [String.contains_string_iff] at h ⊢
  exact (by decide : "goo".toList <:+: "google".toList).trans h

theorem any_goo_of_any_google (b : List String) :
    b.any (fun s => s.contains "google") = true →
      b.any (fun s => s.contains "goo") = true := by
  intro h
  rw [List.any_eq_true] at h ⊢
  rcases h with ⟨s, hs, hsg⟩
  exact ⟨s, hs, contains_goo_of_contains_google s hsg⟩

theorem filter_flatten_length {α : Type} (bs : List (List α)) (p : α → Bool) :
    (bs.flatten.filter p).length = (bs.map (fun b => (b.filter p).length)).sum := by
  induction bs with
  | nil => simp
  | cons b bs ih =>
      calc
        ((b :: bs).flatten.filter p).length = (b.filter p).length + (bs.flatten.filter p).length := by simp [List.filter_append]
        _ = (b.filter p).length + (bs.map (fun c => (c.filter p).length)).sum := by rw [ih]
        _ = ((b :: bs).map (fun c => (c.filter p).length)).sum := by simp

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q21ClickHouse.Goal := by
  intro blocks
  let q : List String → Bool := fun b => b.any (fun s => s.contains "goo")
  let p : String → Bool := fun s => s.contains "google"
  change ((blocks.filter q).flatten.filter p).length = (blocks.flatten.filter p).length
  repeat rw [filter_flatten_length]
  induction blocks with
  | nil => simp
  | cons b bs ih =>
      by_cases hb : q b = true
      · simp [hb, ih]
      · have hbgoogle : b.any p = false := by
          by_cases h : b.any p = true
          · exact False.elim (hb (any_goo_of_any_google b h))
          · simpa using h
        have hfilter : b.filter p = [] := by
          rw [List.eq_nil_iff_forall_not_mem]
          intro s hs
          have hsp : s ∈ b.filter p := hs
          have hmem : s ∈ b := (List.mem_filter.mp hsp).1
          have hps : p s = true := (List.mem_filter.mp hsp).2
          have : b.any p = true := List.any_eq_true.mpr ⟨s, hmem, hps⟩
          simp [hbgoogle] at this
        simp [hb, ih, hfilter]
end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Click_Goal_Q21ClickHouse
