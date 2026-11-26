import TinylambOpt.ResearchGoals.OffsetBitmapGather

/-!
Proved

- `Goal` — `∀ off k (pages : List Page), let r := run off k pages.zipIdx let rows :=
  pages.zipIdx.flatMap fun (p,i) => (selected p).map fun lane => ((i,lane),p.payload
  lane) let slice := (rows.drop off).take k r.values = slice.map Prod.snd ∧ r.reads =
  slice.map Prod.fst ∧ r.opened = (slice.map (fun x => x.1.1)).eraseDups`

The reference model is defined in `TinylambOpt.ResearchGoals.OffsetBitmapGather`.
-/

namespace TinylambOpt.ResearchProofs.OffsetBitmapGather

open TinylambOpt.ResearchGoals.OffsetBitmapGather

/-!
Page-list induction relating `run` to a naive full gather:
`rows pages n` is the flattened list of all selected lanes with their zip indices
(the `rows` let-binding of `Goal` when `n = 0`), and `win pages n off k` is the
window `(rows pages n).drop off |>.take k`.  The three conjuncts state that
`run` produces exactly the window's payloads, exactly the window's read addresses,
and the first-appearance page list of the window.
-/

private def rows (pages : List Page) (n : Nat) : List (Addr × Int) :=
  (pages.zipIdx n).flatMap fun (p,i) => (selected p).map fun lane => ((i,lane), p.payload lane)

private def win (pages : List Page) (n off k : Nat) : List (Addr × Int) :=
  ((rows pages n).drop off).take k

private theorem mem_of_take {α : Type} : ∀ {l : List α} {i : Nat} {a : α}, a ∈ l.take i → a ∈ l := by
  intro l
  induction l with
  | nil => intro i a h; cases i <;> cases h
  | cons x xs ih =>
    intro i a h
    cases i with
    | zero => cases h
    | succ i =>
      rw [List.take_succ_cons] at h
      rcases List.mem_cons.mp h with rfl | h2
      · exact List.mem_cons_self
      · exact List.mem_cons_of_mem x (ih h2)

private theorem map_fst_fst {α : Type} (n : Nat) (q : Nat → α) :
    ∀ (l : List Nat), (l.map fun lane => ((n,lane), q lane)).map (fun x => x.1.1)
      = List.replicate l.length n := by
  intro l
  induction l with
  | nil => rfl
  | cons x xs ih => rw [List.map_cons, List.map_cons, List.length_cons, List.replicate_succ, ih]

private theorem filter_replicate_false {α : Type} (p : α → Bool) {a : α} (ha : p a = false) :
    ∀ n, (List.replicate n a).filter p = [] := by
  intro n
  induction n with
  | zero => rfl
  | succ n ih =>
    rw [List.replicate_succ, List.filter_cons_of_neg (by simpa using ha), ih]

private theorem filter_true {α : Type} (p : α → Bool) :
    ∀ (l : List α), (∀ x ∈ l, p x = true) → l.filter p = l := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons x xs ih =>
    intro h
    rw [List.filter_cons_of_pos (h x List.mem_cons_self),
      ih (fun y hy => h y (List.mem_cons_of_mem x hy))]

/-- Deduplicating a nonempty block of `i`s followed by entries all `> i` keeps one `i`. -/
private theorem erD {D : List Nat} {i m : Nat} (hm : 0 < m) (h : ∀ x ∈ D, i < x) :
    (List.replicate m i ++ D).eraseDups = i :: D.eraseDups := by
  obtain ⟨m', rfl⟩ := Nat.exists_eq_succ_of_ne_zero (Nat.ne_of_gt hm)
  have pf : (fun b => !b == i) i = false := by simp
  have pd : ∀ x ∈ D, (fun b => !b == i) x = true := by
    intro x hx
    have : ¬ (x = i) := Nat.ne_of_gt (h x hx)
    simpa using this
  rw [List.replicate_succ, List.cons_append, List.eraseDups_cons, List.filter_append,
    filter_replicate_false (p := fun b => !b == i) pf, filter_true (p := fun b => !b == i) D pd,
    List.nil_append]

/-- Every row produced for `pages` starting at zip index `m` carries a page index `≥ m`. -/
private theorem idx_ge : ∀ (pages : List Page) (m : Nat) (x : Addr × Int),
    x ∈ rows pages m → m ≤ x.1.1 := by
  intro pages
  induction pages with
  | nil => intro m x h; cases h
  | cons p ps ih =>
    intro m x h
    simp only [rows, List.zipIdx_cons, List.flatMap_cons] at h
    rcases List.mem_append.mp h with h | h
    · obtain ⟨lane, _, rfl⟩ := List.mem_map.mp h
      exact Nat.le_refl m
    · have := ih (m+1) x h
      omega

private theorem run_spec :
    ∀ (pages : List Page) (off k n : Nat),
      (run off k (pages.zipIdx n)).values = (win pages n off k).map Prod.snd ∧
      (run off k (pages.zipIdx n)).reads = (win pages n off k).map Prod.fst ∧
      (run off k (pages.zipIdx n)).opened = ((win pages n off k).map fun x => x.1.1).eraseDups := by
  intro pages
  induction pages with
  | nil =>
    intro off k n
    simp [run, win, rows]
  | cons p ps ih =>
    intro off k n
    cases k with
    | zero =>
      simp [run, win, rows]
    | succ k =>
      by_cases hle : (selected p).length ≤ off
      · -- the whole page is skipped by OFFSET: no payload read, no `opened` entry
        have hr : run off (k+1) ((p,n) :: ps.zipIdx (n+1))
            = run (off - (selected p).length) (k+1) (ps.zipIdx (n+1)) := by
          simp [run, hle]
        have key : win (p :: ps) n off (k+1)
            = win ps (n+1) (off - (selected p).length) (k+1) := by
          simp only [win, rows, List.zipIdx_cons, List.flatMap_cons]
          rw [List.drop_append, List.drop_of_length_le (by rwa [List.length_map]),
            List.length_map, List.nil_append]
        rw [List.zipIdx_cons, hr, key]
        exact ih (off - (selected p).length) (k+1) (n+1)
      · -- the window starts inside this page
        have hlt : off < (selected p).length := Nat.lt_of_not_le hle
        obtain ⟨h1, h2, h3⟩ :=
          ih 0 (k+1 - (((selected p).drop off).take (k+1)).length) (n+1)
        have hr : run off (k+1) ((p,n) :: ps.zipIdx (n+1))
            = ⟨ (((selected p).drop off).take (k+1)).map p.payload
                  ++ (run 0 (k+1 - (((selected p).drop off).take (k+1)).length) (ps.zipIdx (n+1))).values,
                (((selected p).drop off).take (k+1)).map (fun lane => (n, lane))
                  ++ (run 0 (k+1 - (((selected p).drop off).take (k+1)).length) (ps.zipIdx (n+1))).reads,
                n :: (run 0 (k+1 - (((selected p).drop off).take (k+1)).length) (ps.zipIdx (n+1))).opened ⟩ := by
          simp [run, hle]
        have hsub : k+1 - ((selected p).drop off).length
            = k+1 - (((selected p).drop off).take (k+1)).length := by
          rw [List.length_take]
          omega
        have key : win (p :: ps) n off (k+1)
            = (((selected p).drop off).take (k+1)).map (fun lane => ((n,lane), p.payload lane))
              ++ (rows ps (n+1)).take (k+1 - (((selected p).drop off).take (k+1)).length) := by
          simp only [win, rows, List.zipIdx_cons, List.flatMap_cons]
          rw [List.drop_append, List.length_map, Nat.sub_eq_zero_of_le (Nat.le_of_lt hlt),
            List.drop_zero, List.take_append, ← List.map_drop, List.map_take,
            List.length_map, hsub]
        rw [win] at h1 h2 h3
        rw [List.drop_zero] at h1 h2 h3
        rw [List.zipIdx_cons, hr]
        simp only []
        rw [key, h1, h2, h3]
        refine ⟨?_, ?_, ?_⟩
        · rw [List.map_append, List.map_map]
          rfl
        · rw [List.map_append, List.map_map]
          rfl
        · rw [List.map_append, map_fst_fst n p.payload]
          have hpos : 0 < (((selected p).drop off).take (k+1)).length := by
            rw [List.length_take, List.length_drop]
            omega
          have hy : ∀ y ∈ ((rows ps (n+1)).take
              (k+1 - (((selected p).drop off).take (k+1)).length)).map (fun x => x.1.1), n < y := by
            intro y hyy
            obtain ⟨z, hz, hzy⟩ := List.mem_map.mp hyy
            have hzy' : z.1.1 = y := hzy
            have hz' : z ∈ rows ps (n+1) := mem_of_take hz
            have hle' : n+1 ≤ z.1.1 := idx_ge ps (n+1) z hz'
            omega
          rw [erD hpos hy]

theorem sound : TinylambOpt.ResearchGoals.OffsetBitmapGather.Goal := by
  intro off k pages
  have h := run_spec pages off k 0
  exact ⟨h.1, h.2.1, h.2.2⟩

end TinylambOpt.ResearchProofs.OffsetBitmapGather
