import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-! Shared list algebra for the conflict-scatter kernels. The lemmas relate the
sum of signed deltas under list concatenation, deduplicated slot leaders, and
zero-delta filtering; each goal imports them so that SQL execution can be shown
to preserve the same per-slot numeric result. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

theorem eraseDups_nodup (l : List Nat) : l.eraseDups.Nodup := by
  match l with
  | [] => simp
  | a :: as =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · simp
      · exact eraseDups_nodup (as.filter fun b => !b == a)
termination_by l.length
decreasing_by
  simp only [List.length_cons]
  have hle := List.length_filter_le (fun b => !b == a) as
  omega

theorem eraseDups_of_nodup (l : List Nat) (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have hnot : a ∉ as := (List.nodup_cons.mp h).1
      have hnd : as.Nodup := (List.nodup_cons.mp h).2
      have hf : (as.filter fun b => !b == a) = as := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hba : b ≠ a := by
          intro heq
          exact hnot (by simpa [heq] using hb)
        simp [hba]
      rw [List.eraseDups_cons, hf, ih hnd]

theorem mem_leaders_iff (us : Updates) (k : Nat) : k ∈ leaders us ↔ k ∈ us.map Prod.fst := by
  simp [leaders]

theorem delta_append (a b : Updates) (k : Nat) : delta (a ++ b) k = delta a k + delta b k := by
  simp [delta, List.filter_append, List.map_append, List.sum_append]

theorem delta_zero_of_not_mem (us : Updates) (k : Nat) (h : k ∉ us.map Prod.fst) : delta us k = 0 := by
  have hf : us.filter (fun u => u.1 == k) = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro u hu
    have hu' := List.mem_filter.mp hu
    have heq : u.1 = k := by simpa using hu'.2
    exact h (List.mem_map.mpr ⟨u, hu'.1, heq⟩)
  simp [delta, hf]

theorem delta_map_key (l : List Nat) (f : Nat → Int) (k : Nat) (hnd : l.Nodup) :
    (((l.map (fun x => (x, f x))).filter (fun u => u.1 == k)).map Prod.snd).sum =
      if k ∈ l then f k else 0 := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have ha_not : a ∉ as := (List.nodup_cons.mp hnd).1
      have has_nd : as.Nodup := (List.nodup_cons.mp hnd).2
      by_cases hak : a == k
      · have heq : a = k := by simpa using hak
        have hnotk : k ∉ as := by simpa [heq] using ha_not
        simp [heq, hnotk, ih has_nd]
      · have hkne : k ≠ a := by
          intro hka
          apply hak
          simp [hka]
        simp [hak, hkne, ih has_nd]

theorem delta_combine (us : Updates) (k : Nat) : delta (combine us) k = delta us k := by
  change List.sum (List.map Prod.snd (List.filter (fun u => u.1 == k) (List.map (fun k => (k, delta us k)) (leaders us)))) = delta us k
  rw [delta_map_key (leaders us) (fun k => delta us k) k]
  · by_cases hk : k ∈ leaders us
    · simp [hk]
    · have hk' : k ∉ us.map Prod.fst := by simpa [leaders] using hk
      simp [hk, delta_zero_of_not_mem us k hk']
  · unfold leaders
    exact eraseDups_nodup _

theorem leaders_combine (us : Updates) : leaders (combine us) = leaders us := by
  have hlead : (leaders us).Nodup := by
    unfold leaders
    exact eraseDups_nodup _
  have hmap : List.map Prod.fst (List.map (fun k => (k, delta us k)) (leaders us)) = leaders us := by
    simp only [List.map_map]
    have hcomp : (Prod.fst ∘ fun k => (k, delta us k)) = id := by
      funext k
      rfl
    rw [hcomp, List.map_id]
  change (List.map Prod.fst (List.map (fun k => (k, delta us k)) (leaders us))).eraseDups = leaders us
  rw [hmap]
  exact eraseDups_of_nodup _ hlead

theorem sum_map_snd_filter_ne_zero (us : Updates) (k : Nat) :
    (((us.filter (fun u => u.1 == k && u.2 != 0)).map Prod.snd).sum) =
      (((us.filter (fun u => u.1 == k)).map Prod.snd).sum) := by
  induction us with
  | nil => simp
  | cons u us ih =>
      by_cases hk : u.1 == k
      · by_cases hz : u.2 != 0
        · simp [hk, hz, ih]
        · have hz0 : u.2 = 0 := by simpa using hz
          simp [hk, hz0, ih]
      · simp [hk, ih]

theorem delta_filter_zero (us : Updates) (k : Nat) :
    delta (us.filter (fun u => u.2 != 0)) k = delta us k := by
  unfold delta
  rw [List.filter_filter]
  exact sum_map_snd_filter_ne_zero us k

theorem sum_nonneg (l : List Int) (h : ∀ x ∈ l, 0 ≤ x) : 0 ≤ l.sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : 0 ≤ x := h x (by simp)
      have htail : ∀ z ∈ xs, 0 ≤ z := by
        intro z hz
        exact h z (by simp [hz])
      have hs : 0 ≤ xs.sum := ih htail
      simp only [List.sum_cons]
      omega

theorem scanl_bounds (l : List Int) (acc lo hi : Int)
    (hnonneg : ∀ x ∈ l, 0 ≤ x) (hlo : lo ≤ acc) (hhi : acc + l.sum ≤ hi) :
    (l.scanl (· + ·) acc).all (fun x => lo ≤ x && x ≤ hi) = true := by
  induction l generalizing acc with
  | nil =>
      have hacc_hi : acc ≤ hi := by simpa using hhi
      simp [hlo, hacc_hi]
  | cons x xs ih =>
      have hx : 0 ≤ x := hnonneg x (by simp)
      have htail : ∀ z ∈ xs, 0 ≤ z := by
        intro z hz
        exact hnonneg z (by simp [hz])
      have hsum_nonneg : 0 ≤ xs.sum := sum_nonneg xs htail
      have hacc_hi : acc ≤ hi := by
        simp only [List.sum_cons] at hhi
        omega
      have hacc' : lo ≤ acc + x := by omega
      have hhi' : acc + x + xs.sum ≤ hi := by
        simp only [List.sum_cons] at hhi
        omega
      have ih' := ih (acc + x) htail hacc' hhi'
      rw [List.scanl_cons]
      rw [List.all_eq_true]
      intro y hy
      simp only [List.mem_cons] at hy
      rcases hy with rfl | hy
      · simp [hlo, hacc_hi]
      · exact (List.all_eq_true.mp ih') y hy

theorem zipIdx_delta_partition (us : Updates) (n k : Nat) :
    delta (((us.zipIdx n).filter (fun u => u.2 % 2 == 0)).map Prod.fst) k +
      delta (((us.zipIdx n).filter (fun u => u.2 % 2 == 1)).map Prod.fst) k =
        delta us k := by
  induction us generalizing n with
  | nil => simp [delta]
  | cons u us ih =>
      rw [List.zipIdx_cons]
      have hmod : n % 2 = 0 ∨ n % 2 = 1 := by omega
      rcases hmod with hn | hn
      · by_cases huk : u.1 == k
        · have hih := ih (n + 1)
          simp [hn, delta, huk] at hih ⊢
          omega
        · have hih := ih (n + 1)
          simp [hn, delta, huk] at hih ⊢
          omega
      · have hn0 : ¬n % 2 = 0 := by omega
        by_cases huk : u.1 == k
        · have hih := ih (n + 1)
          simp [hn, delta, huk] at hih ⊢
          omega
        · have hih := ih (n + 1)
          simp [hn, delta, huk] at hih ⊢
          omega

end TinylambOpt.ResearchProofs.JitKernels.Common_ConflictScatter
