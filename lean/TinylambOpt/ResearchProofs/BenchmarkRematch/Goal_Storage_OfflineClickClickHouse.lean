import TinylambOpt.ResearchGoals.BenchmarkRematch.Storage
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_OfflineClickClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Storage
open TinylambOpt.ResearchGoals.Compression.Wire

theorem prefix_lemma : ∀ (u o : Bytes),
    u.take (((u.zip o).takeWhile (fun (a,b) => a == b)).length) =
      o.take (((u.zip o).takeWhile (fun (a,b) => a == b)).length) := by
  intro u
  induction u with
  | nil => intro o; simp
  | cons a u ih =>
    intro o
    cases o with
    | nil => simp
    | cons b o =>
      simp only [List.zip_cons_cons, List.takeWhile_cons]
      by_cases h : a = b
      · subst h
        simp [ih o]
      · simp [h]

theorem len_lemma : ∀ (u o : Bytes),
    ((u.zip o).takeWhile (fun (a,b) => a == b)).length ≤ o.length := by
  intro u
  induction u with
  | nil => intro o; simp
  | cons a u ih =>
    intro o
    cases o with
    | nil => simp
    | cons b o =>
      simp only [List.zip_cons_cons, List.takeWhile_cons]
      by_cases h : a = b
      · subst h
        have := ih o
        simp; omega
      · simp [h]

theorem sound : OfflineClickClickHouse.Goal := by
  intro url original tail
  dsimp only
  have h1 : ∀ n tail, readU (uleb n ++ tail) = some (n,tail) :=
    TinylambOpt.ResearchProofs.Compression.Wire.sound_DelimitedInteger
  generalize hn : ((url.zip original).takeWhile (fun (a,b) => a == b)).length = n
  have hp := prefix_lemma url original
  have hl := len_lemma url original
  rw [hn] at hp hl
  simp only [List.append_assoc]
  rw [h1 _ _]
  simp only [bind, Option.bind_some, pure]
  rw [h1 _ _]
  simp only [Option.bind_some]
  rw [List.take_left, List.drop_left, hp, List.take_append_drop]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_OfflineClickClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Storage_OfflineClickClickHouse.sound
