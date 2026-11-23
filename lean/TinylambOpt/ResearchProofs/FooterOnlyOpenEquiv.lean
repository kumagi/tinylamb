import TinylambOpt.ResearchGoals.FooterOnlyOpenEquiv

namespace TinylambOpt.ResearchProofs.FooterOnlyOpenEquiv
open TinylambOpt.ResearchGoals.FooterOnlyOpenEquiv

/-! ## Directory construction: an offset-generalized copy of the `let rec` -/

private def buildDirFrom : Nat → List (List UInt8) → List ChunkEntry
  | _, [] => []
  | pos, c :: rest => ⟨pos, c.length⟩ :: buildDirFrom (pos + c.length) rest

private theorem go_eq_from :
    ∀ chunks off, buildDir.go off chunks = buildDirFrom off chunks := by
  intro chunks
  induction chunks with
  | nil => intro off; rfl
  | cons c rest ih =>
      intro off
      rw [show buildDir.go off (c :: rest)
            = ⟨off, c.length⟩ :: buildDir.go (off + c.length) rest from rfl,
        show buildDirFrom off (c :: rest)
            = ⟨off, c.length⟩ :: buildDirFrom (off + c.length) rest from rfl,
        ih (off + c.length)]

private theorem buildDir_eq_from :
    ∀ chunks, buildDir chunks = buildDirFrom 0 chunks := by
  intro chunks
  induction chunks with
  | nil => rfl
  | cons c rest ih =>
      rw [show buildDir (c :: rest)
            = ⟨0, c.length⟩ :: buildDir.go (0 + c.length) rest from rfl,
        show buildDirFrom 0 (c :: rest)
            = ⟨0, c.length⟩ :: buildDirFrom (0 + c.length) rest from rfl,
        go_eq_from rest (0 + c.length)]

private theorem buildDirFrom_len :
    ∀ (chunks : List (List UInt8)) (off : Nat),
      (buildDirFrom off chunks).length = chunks.length := by
  intro chunks
  induction chunks with
  | nil => intro off; rfl
  | cons c rest ih =>
      intro off
      rw [show buildDirFrom off (c :: rest)
            = ⟨off, c.length⟩ :: buildDirFrom (off + c.length) rest from rfl,
        List.length_cons, ih (off + c.length)]
      rfl

private theorem buildDir_len (chunks : List (List UInt8)) :
    (buildDir chunks).length = chunks.length := by
  rw [buildDir_eq_from, buildDirFrom_len]

/-! ## Exact slicing of a column payload from the serialized buffer.
`pre` holds the chunks laid out before `chunks` (its byte total is `off`,
the position the directory was built from). -/

private theorem slice_correct :
    ∀ (chunks : List (List UInt8)),
      ∀ (off : Nat) (pre : List (List UInt8)) (buf suf : List UInt8),
        (pre.map List.length).sum = off →
        buf = pre.flatten ++ chunks.flatten ++ suf →
        ∀ (i : Nat) (e : ChunkEntry),
          (buildDirFrom off chunks)[i]? = some e →
          ∃ ch, chunks[i]? = some ch ∧ (buf.drop e.offset).take e.len = ch := by
  intro chunks
  induction chunks with
  | nil =>
      intro off pre buf suf _ hb i e h
      rw [show buildDirFrom off ([] : List (List UInt8)) = [] from rfl, List.getElem?_nil] at h
      exact absurd h (by simp)
  | cons c rest ih =>
      intro off pre buf suf hsum hb i e h
      rw [show buildDirFrom off (c :: rest)
            = ⟨off, c.length⟩ :: buildDirFrom (off + c.length) rest from rfl] at h
      cases i with
      | zero =>
          rw [List.getElem?_cons_zero] at h
          have heq := Option.some.inj h
          have heo : e.offset = off := by rw [← heq]
          have hel : e.len = c.length := by rw [← heq]
          refine ⟨c, rfl, ?_⟩
          rw [heo, hel, hb, List.flatten_cons, ← hsum, ← List.length_flatten,
            List.append_assoc pre.flatten (c ++ rest.flatten) suf, List.drop_append_length,
            List.append_assoc c rest.flatten suf, List.take_append_length]
      | succ k =>
          rw [List.getElem?_cons_succ] at h
          have hsum' : (List.map List.length (pre ++ [c])).sum = off + c.length := by
            rw [List.map_append, List.sum_append, List.map_singleton, List.sum_cons,
              List.sum_nil, Nat.add_zero, hsum]
          have hb' : buf = (pre ++ [c]).flatten ++ rest.flatten ++ suf := by
            rw [hb]
            simp only [List.flatten_append, List.flatten_cons, List.flatten_nil,
              List.append_nil, List.append_assoc]
          obtain ⟨ch, hch, hsl⟩ :=
            ih (off + c.length) (pre ++ [c]) buf suf hsum' hb' k e h
          refine ⟨ch, ?_, hsl⟩
          rw [List.getElem?_cons_succ]
          exact hch

/-! ## The read step -/

private theorem lazyReadAll_nil (h : LazyHandle) : lazyReadAll h [] = ([], h.bytesRead) := rfl

private theorem lazyReadAll_cons (h : LazyHandle) (c : Nat) (cs : List Nat) :
    lazyReadAll h (c :: cs)
      = let (res, h') := lazyReadCol h c
        let (rest, tot) := lazyReadAll h' cs
        (res :: rest, tot) := rfl

private theorem lazyReadCol_none (h : LazyHandle) (c : Nat) (hc : h.dir[c]? = none) :
    lazyReadCol h c = (none, h) := by
  unfold lazyReadCol
  rw [hc]

private theorem lazyReadCol_some (h : LazyHandle) (c : Nat) (e : ChunkEntry)
    (he : h.dir[c]? = some e) :
    lazyReadCol h c =
      (some ((h.buf.drop e.offset).take e.len),
        { h with bytesRead := h.bytesRead + e.len }) := by
  unfold lazyReadCol
  rw [he]

/-! ## On-demand reads reproduce the eager chunks, and the byte counter is
exactly the footer plus the sizes of the directory hits. -/

private theorem readAll_spec (chunks : List (List UInt8)) (footer garbage : List UInt8) :
    ∀ (cols : List Nat) (br dataEnd : Nat),
      lazyReadAll ⟨chunks.flatten ++ footer ++ garbage, dataEnd, buildDir chunks, br⟩ cols
        = (cols.map (fun c => chunks[c]?),
           br + ((cols.filterMap (fun c => (buildDir chunks)[c]?)).map ChunkEntry.len).sum) := by
  intro cols
  induction cols with
  | nil =>
      intro br dataEnd
      rw [lazyReadAll_nil]
      simp
  | cons c cs ih =>
      intro br dataEnd
      rw [lazyReadAll_cons]
      cases hdir : (buildDir chunks)[c]? with
      | none =>
          have hcn : chunks[c]? = none := by
            rw [List.getElem?_eq_none_iff] at hdir
            rw [buildDir_len chunks] at hdir
            rw [List.getElem?_eq_none_iff]
            exact hdir
          rw [lazyReadCol_none ⟨chunks.flatten ++ footer ++ garbage, dataEnd,
            buildDir chunks, br⟩ c hdir]
          dsimp only
          rw [ih br dataEnd]
          dsimp only
          simp only [List.map_cons, List.filterMap_cons, hcn, hdir]
      | some e =>
          have heq : (buildDirFrom 0 chunks)[c]? = some e := by
            rw [← buildDir_eq_from chunks]
            exact hdir
          obtain ⟨ch, hch, hsl⟩ :=
            slice_correct chunks 0 [] (chunks.flatten ++ footer ++ garbage) (footer ++ garbage)
              (by simp : (([] : List (List UInt8)).map List.length).sum = 0)
              (by simp : chunks.flatten ++ footer ++ garbage
                  = ([] : List (List UInt8)).flatten ++ chunks.flatten ++ (footer ++ garbage))
              c e heq
          have hs2 : ((chunks.flatten ++ footer ++ garbage).drop e.offset).take e.len = ch := hsl
          rw [lazyReadCol_some ⟨chunks.flatten ++ footer ++ garbage, dataEnd,
            buildDir chunks, br⟩ c e hdir]
          dsimp only
          rw [ih (br + e.len) dataEnd]
          dsimp only
          simp only [List.map_cons, List.filterMap_cons, hch, hdir, hs2, List.sum_cons,
            Nat.add_assoc]

theorem sound : Goal := by
  intro chunks footer garbage cols
  have hall : lazyReadAll (footerOpen chunks footer garbage) cols
      = (cols.map (fun c => chunks[c]?),
         footer.length + ((cols.filterMap
            (fun c => (footerOpen chunks footer garbage).dir[c]?)).map ChunkEntry.len).sum) :=
    readAll_spec chunks footer garbage cols footer.length chunks.flatten.length
  dsimp only
  rw [hall]
  exact ⟨rfl, rfl, rfl⟩

end TinylambOpt.ResearchProofs.FooterOnlyOpenEquiv
