import TinylambOpt.ResearchGoals.BenchmarkRematch.Storage

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Storage.Goal_AnalyticsClickHouse

open TinylambOpt.ResearchGoals.BenchmarkRematch.Storage
open TinylambOpt.ResearchGoals.Compression.Wire

theorem scanl_add_start (ms : List Nat) (s : Nat) :
    List.scanl (fun a b => a + b) s ms = (List.scanl (fun a b => a + b) 0 ms).map (fun x => s + x) := by
  induction ms generalizing s with
  | nil => simp
  | cons m ms ih =>
      rw [List.scanl_cons, List.scanl_cons]
      simp only [List.map_cons, Nat.zero_add]
      rw [ih (s + m), ih m]
      simp [List.map_map, Function.comp_apply, Nat.add_assoc]

theorem flatMap_congr {α β : Type} (l : List α) (f g : α → List β)
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.flatMap_cons]
      rw [h a (by simp)]
      rw [ih]
      intro b hb
      exact h b (by simp [hb])

theorem mem_zip_pair {α β : Type} (l₁ : List α) (l₂ : List β) (e : α × β)
    (h : e ∈ l₁.zip l₂) : e.1 ∈ l₁ ∧ e.2 ∈ l₂ := by
  induction l₁ generalizing l₂ with
  | nil => simp at h
  | cons a l₁ ih =>
      cases l₂ with
      | nil => simp at h
      | cons b l₂ =>
          simp only [List.zip_cons_cons, List.mem_cons] at h
          cases h with
          | inl he =>
              subst e
              simp
          | inr he =>
              have hh := ih l₂ he
              simp [hh.1, hh.2]

theorem shifted_read_ge (m start len pos sz : Nat) (body : List Nat) (hm : m ≤ start) :
    (if (m + pos) + sz ≤ start ∨ start + len ≤ m + pos then []
     else (body.drop (start - (m + pos))).take (start + len - max (m + pos) start))
    =
    (if pos + sz ≤ start - m ∨ (start - m) + len ≤ pos then []
     else (body.drop ((start - m) - pos)).take ((start - m) + len - max pos (start - m))) := by
  have hc1 : (m + pos) + sz ≤ start ↔ pos + sz ≤ start - m := by omega
  have hc2 : start + len ≤ m + pos ↔ (start - m) + len ≤ pos := by omega
  have hsub : start - (m + pos) = (start - m) - pos := by omega
  have hmax : max (m + pos) start = m + max pos (start - m) := by omega
  by_cases h1 : pos + sz ≤ start - m
  · have h1' : (m + pos) + sz ≤ start := hc1.mpr h1
    simp [h1, h1']
  · have h1' : ¬ (m + pos) + sz ≤ start := by intro h; exact h1 (hc1.mp h)
    by_cases h2 : (start - m) + len ≤ pos
    · have h2' : start + len ≤ m + pos := hc2.mpr h2
      simp [h1, h1', h2, h2']
    · have h2' : ¬ start + len ≤ m + pos := by intro h; exact h2 (hc2.mp h)
      simp [h1, h1', h2, h2', hsub, hmax]
      omega

theorem shifted_read_lt (m start len pos sz : Nat) (body : List Nat)
    (hm : start < m) (hlen : body.length = sz) :
    (if m + pos + sz ≤ start ∨ start + len ≤ m + pos then []
     else (body.drop (start - (m + pos))).take (start + len - max (m + pos) start))
    =
    (if pos + sz ≤ 0 ∨ (start + len - m) ≤ pos then []
     else (body.drop (0 - pos)).take ((start + len - m) - max pos 0)) := by
  have hleft1 : ¬ m + pos + sz ≤ start := by omega
  have hc2 : start + len ≤ m + pos ↔ start + len - m ≤ pos := by omega
  by_cases h2 : start + len - m ≤ pos
  · have h2' : start + len ≤ m + pos := hc2.mpr h2
    simp [hleft1, h2, h2']
  · have h2' : ¬ start + len ≤ m + pos := by intro h; exact h2 (hc2.mp h)
    have h2'' : ¬ start + len ≤ pos + m := by omega
    by_cases hzero : pos + sz ≤ 0
    · have hpos : pos = 0 := by omega
      have hsz : sz = 0 := by omega
      have hb : body = [] := by
        apply List.length_eq_zero_iff.mp
        omega
      simp [hleft1, h2', h2'', hzero, hb]
    · simp [hleft1, h2', h2'', hzero]
      have hsub : start - (m + pos) = 0 := by omega
      have hmax1 : max (m + pos) start = m + pos := by omega
      rw [hsub, hmax1]
      congr 1
      omega

theorem read_index_eq (frames : List Bytes) (enc dec : Bytes → Bytes)
    (hdec : ∀ b ∈ frames, dec (enc b) = b) (start len : Nat) :
    let starts := (frames.map List.length).scanl (·+·) 0
    let index := starts.zip (frames.map (fun b => (b.length, enc b)))
    let readSpan := fun (entry : Nat × Nat × Bytes) =>
      let (pos,sz,body) := entry
      if pos + sz ≤ start || start + len ≤ pos then []
      else ((dec body).drop (start-pos)).take (start+len-max pos start)
    index.flatMap readSpan = (frames.flatten.drop start).take len := by
  induction frames generalizing start len with
  | nil =>
      simp
  | cons b bs ih =>
      dsimp only
      have hdec_b : dec (enc b) = b := hdec b (by simp)
      have hdec_bs : ∀ x ∈ bs, dec (enc x) = x := by
        intro x hx
        exact hdec x (by simp [hx])
      simp only [List.map_cons, List.scanl_cons, Nat.zero_add, List.zip_cons_cons]
      rw [scanl_add_start]
      simp only [List.map_cons, List.flatMap_cons]
      simp [hdec_b]
      by_cases hm : b.length ≤ start
      · have hfirst : (if b.length ≤ start ∨ start = 0 ∧ len = 0 then []
            else (b.drop start).take len) = [] := by simp [hm]
        rw [hfirst]
        simp only [List.nil_append]
        let starts := (bs.map List.length).scanl (·+·) 0
        let entries := bs.map (fun b => (b.length, enc b))
        have htail :
            ((starts.map (fun x => b.length + x)).zip entries).flatMap (fun entry =>
              if entry.1 + entry.2.1 ≤ start ∨ start + len ≤ entry.1 then []
              else ((dec entry.2.2).drop (start - entry.1)).take
                (start + len - max entry.1 start)) =
            (starts.zip entries).flatMap (fun entry =>
              if entry.1 + entry.2.1 ≤ start - b.length ∨ (start - b.length) + len ≤ entry.1 then []
              else ((dec entry.2.2).drop ((start - b.length) - entry.1)).take
                ((start - b.length) + len - max entry.1 (start - b.length))) := by
          rw [List.zip_map_left, List.flatMap_map]
          apply flatMap_congr
          intro e he
          rcases e with ⟨p, q⟩
          rcases q with ⟨sz, body⟩
          simpa [Prod.map] using (shifted_read_ge b.length start len p sz (dec body) hm)
        have hih := ih hdec_bs (start - b.length) len
        dsimp only at hih
        simp only [Bool.or_eq_true, decide_eq_true_eq] at hih
        rw [htail]
        rw [hih]
        have hdrop : (b ++ bs.flatten).drop start = bs.flatten.drop (start - b.length) := by
          rw [show start = b.length + (start - b.length) by omega]
          rw [← List.drop_drop]
          rw [List.drop_append, List.drop_length, Nat.sub_self, List.nil_append]
          simp
        rw [hdrop]
      · have hltm : start < b.length := by omega
        let starts := (bs.map List.length).scanl (·+·) 0
        let entries := bs.map (fun b => (b.length, enc b))
        have htail :
            ((starts.map (fun x => b.length + x)).zip entries).flatMap (fun entry =>
              if entry.1 + entry.2.1 ≤ start ∨ start + len ≤ entry.1 then []
              else ((dec entry.2.2).drop (start - entry.1)).take
                (start + len - max entry.1 start)) =
            (starts.zip entries).flatMap (fun entry =>
              if entry.1 + entry.2.1 ≤ 0 ∨ (start + len - b.length) ≤ entry.1 then []
              else ((dec entry.2.2).drop (0 - entry.1)).take
                ((start + len - b.length) - max entry.1 0)) := by
          rw [List.zip_map_left, List.flatMap_map]
          apply flatMap_congr
          intro e he
          rcases e with ⟨p, q⟩
          rcases q with ⟨sz, body⟩
          have hmem := mem_zip_pair starts entries (p, (sz, body)) he
          rcases List.mem_map.mp hmem.2 with ⟨x, hxbs, hxeq⟩
          have hsz : sz = x.length := (congrArg Prod.fst hxeq).symm
          have hbody : body = enc x := (congrArg Prod.snd hxeq).symm
          have hlen : (dec body).length = sz := by
            rw [hbody, hdec_bs x hxbs, hsz]
          simpa [Prod.map] using (shifted_read_lt b.length start len p sz (dec body) hltm hlen)
        have hih := ih hdec_bs 0 (start + len - b.length)
        dsimp only at hih
        simp only [Bool.or_eq_true, decide_eq_true_eq, Nat.zero_add, List.drop_zero] at hih
        rw [htail]
        rw [hih]
        by_cases hzero : start = 0 ∧ len = 0
        · rcases hzero with ⟨rfl, rfl⟩
          simp
        · have hfirst : (if b.length ≤ start ∨ start = 0 ∧ len = 0 then []
              else (b.drop start).take len) = (b.drop start).take len := by simp [hm, hzero]
          rw [hfirst]
          have hdrop : (b ++ bs.flatten).drop start = b.drop start ++ bs.flatten := by
            rw [List.drop_append]
            have hsub0 : start - b.length = 0 := by omega
            simp [hsub0]
          rw [hdrop, List.take_append]
          congr 1
          have hlenDrop : (b.drop start).length = b.length - start := by simp
          have heq : start + len - b.length = len - (b.length - start) := by
            by_cases hle : len ≤ b.length - start
            · have hle' : start + len ≤ b.length := by omega
              omega
            · have hgt : b.length ≤ start + len := by omega
              omega
          rw [hlenDrop, heq]


theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Storage.AnalyticsClickHouse.Goal := by
  intro frames enc dec start len hdec
  exact read_index_eq frames enc dec hdec start len

end TinylambOpt.ResearchProofs.BenchmarkRematch.Storage.Goal_AnalyticsClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Storage.Goal_AnalyticsClickHouse.sound
