import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_LeaderReducedScatter

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ConflictScatter.LeaderReducedScatter.Goal := by
  have hndFilter : ∀ (l : List Nat) (p : Nat → Bool), (l.filter p).eraseDups.Nodup := by
    intro l
    induction l with
    | nil => intro p; simp
    | cons a l ih =>
        intro p
        by_cases hp : p a = true
        · have ih' := ih (fun b => !b == a && p b)
          simpa [hp, List.eraseDups_cons, List.filter_filter] using ih'
        · simpa [hp] using ih p
  intro us init k
  unfold sequential
  congr 1
  unfold combine
  let L : List Nat := (us.map Prod.fst).eraseDups
  have hnd : L.Nodup := by
    have hfilt : (us.map Prod.fst).filter (fun _ => true) = us.map Prod.fst := by
      induction us.map Prod.fst with
      | nil => rfl
      | cons a l ih => simp [ih]
    dsimp [L]
    rw [← hfilt]
    exact hndFilter (us.map Prod.fst) (fun _ => true)
  have hsumLeaders : ∀ (l : List Nat), l.Nodup →
      (((l.map (fun key => (key, delta us key))).filter (fun u => u.1 == k)).map Prod.snd).sum =
        if k ∈ l then delta us k else 0 := by
    intro l hn
    induction l with
    | nil => simp [delta]
    | cons key l ih =>
        have hnot : key ∉ l := (List.nodup_cons.mp hn).1
        have htail : l.Nodup := (List.nodup_cons.mp hn).2
        specialize ih htail
        by_cases hk : (key == k) = true
        · have hkeq : key = k := by simpa using hk
          have hknot : k ∉ l := by simpa [hkeq] using hnot
          simp [hk, hkeq, hknot, ih]
        · have hkne : key ≠ k := by simpa using hk
          have hkne' : k ≠ key := by intro h; exact hkne h.symm
          simp [hk, hkne', ih]
  have hdeltaNotMem : k ∉ L → delta us k = 0 := by
    intro hk
    unfold delta
    have hf : us.filter (fun u => u.1 == k) = [] := by
      apply List.eq_nil_iff_forall_not_mem.mpr
      intro u hu
      simp only [List.mem_filter] at hu
      have huL : u.1 ∈ L := by
        dsimp [L]
        rw [List.mem_eraseDups]
        exact List.mem_map.mpr ⟨u, hu.1, rfl⟩
      have heq : u.1 = k := by simpa using hu.2
      exact hk (heq ▸ huL)
    simp [hf]
  change delta (L.map (fun key => (key, delta us key))) k = delta us k
  rw [show delta (L.map (fun key => (key, delta us key))) k =
      (((L.map (fun key => (key, delta us key))).filter (fun u => u.1 == k)).map Prod.snd).sum by rfl]
  rw [hsumLeaders L hnd]
  by_cases hk : k ∈ L
  · simp [hk]
  · simp [hk, hdeltaNotMem hk]

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_LeaderReducedScatter
