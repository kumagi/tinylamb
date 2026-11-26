import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-!
# Conflict-scatter proof audit

This module audits the SIMD conflict-scatter model used by grouped SQL
execution. It proves the transformations that preserve every signed per-slot
delta, including leader reduction, batch coalescing, monotone range checks,
disjoint-wave commits, zero-delta store elision, and parity-striped merging.
It also refutes four unsafe substitutions with concrete lane lists: last-lane
RMW, endpoint-only checked scatter, truncated conflict ballots, and occupancy
elision for groups whose reduced delta is zero.
-/

namespace TinylambOpt.ResearchProofs.JitKernels.ProofConflictScatter

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-- Appending two update waves adds their contributions to each slot. This
allows SQL execution to split a scatter across batches without changing the
modeled accumulator result. -/
private theorem delta_append (a b : Updates) (k : Nat) :
    delta (a ++ b) k = delta a k + delta b k := by
  simp [delta, List.filter_append, List.map_append, List.sum_append]

/-- A slot absent from the update keys has no contribution. This supports
eliminating branches for keys that cannot be touched by a compiled wave. -/
private theorem delta_eq_zero_of_not_mem_keys {us : Updates} {k : Nat}
    (hk : k ∉ us.map Prod.fst) : delta us k = 0 := by
  induction us with
  | nil => simp [delta]
  | cons u us ih =>
      have hku : k ≠ u.1 := by
        intro h
        apply hk
        simp [h]
      have hkt : k ∉ us.map Prod.fst := by
        intro h
        apply hk
        simp [h]
      simp [delta, hku, ih hkt]

/-- Adding one lane contributes its signed delta exactly when its physical
slot is the queried slot. This is the elementary scatter recurrence. -/
private theorem delta_cons (u : Nat × Int) (us : Updates) (k : Nat) :
    delta (u :: us) k = (if u.1 = k then u.2 else 0) + delta us k := by
  by_cases h : u.1 = k
  · simp [delta, h]
  · simp [delta, h]

/-- `eraseDups` produces a duplicate-free leader list. The proof measures the
recursive filtered tail, which is the part consumed by `eraseDups_cons`. -/
private theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) : l.eraseDups.Nodup := by
  refine Nat.strongRecOn (motive := fun n => ∀ xs : List α, xs.length = n → xs.eraseDups.Nodup)
    l.length ?_ l rfl
  intro n ih xs hx
  cases xs with
  | nil => simp
  | cons a as =>
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro hm
        have hfil : a ∈ as.filter (fun b => !(b == a)) := List.mem_eraseDups.mp hm
        have hc := (List.mem_filter.mp hfil).2
        simp at hc
      · have hlt : (as.filter (fun b => !(b == a))).length < n := by
          rw [← hx]
          exact Nat.lt_succ_of_le (List.length_filter_le _ as)
        exact ih _ hlt (as.filter (fun b => !(b == a))) rfl

/-- The distinct physical slots of an update list are exactly its leaders. -/
private theorem leaders_nodup (us : Updates) : (leaders us).Nodup := by
  unfold leaders
  exact eraseDups_nodup _

/-- Filtering a weighted map over a duplicate-free key list selects either
the matching weight or zero. This justifies one store per elected leader. -/
private theorem sum_map_nodup (f : Nat → Int) (ks : List Nat)
    (hnd : ks.Nodup) (k : Nat) :
    (((ks.map (fun j => (j, f j))).filter (fun u => u.1 == k)).map Prod.snd).sum =
      if k ∈ ks then f k else 0 := by
  induction ks with
  | nil => simp
  | cons a as ih =>
      have hnot : a ∉ as := (List.nodup_cons.mp hnd).1
      have hndas : as.Nodup := (List.nodup_cons.mp hnd).2
      simp only [List.map_cons, List.filter_cons, List.mem_cons]
      by_cases hak : a = k
      · subst a
        have hknot : k ∉ as := by simpa using hnot
        simp [hknot, ih hndas]
      · have hbe : (a == k) = false := by simp [hak]
        have hkne : k ≠ a := by intro he; exact hak he.symm
        simp [hbe, hkne, ih hndas]

/-- Leader membership is exactly membership in the original physical-slot
list, so absent leaders can be handled using the empty-delta lemma. -/
private theorem mem_leaders {us : Updates} {k : Nat} :
    k ∈ leaders us ↔ k ∈ us.map Prod.fst := by
  simp [leaders]

/-- Reducing all matching lanes into one leader preserves the signed delta
for every slot, including slots absent from the update list. -/
private theorem delta_combine (us : Updates) (k : Nat) :
    delta (combine us) k = delta us k := by
  calc
    delta (combine us) k =
        if k ∈ leaders us then delta us k else 0 := by
      change
        ((((leaders us).map (fun j => (j, delta us j))).filter
          (fun u => u.1 == k)).map Prod.snd).sum = _
      exact sum_map_nodup (delta us) (leaders us) (leaders_nodup us) k
    _ = delta us k := by
      by_cases hk : k ∈ leaders us
      · simp [hk]
      · have hk' : k ∉ us.map Prod.fst := by simpa [mem_leaders] using hk
        simp [hk, delta_eq_zero_of_not_mem_keys hk']

/-- **Proved: leader-reduced scatter.** Reducing all lanes for each physical
slot before scattering preserves the sequential SQL accumulator result; the
initial counter is unchanged and each slot receives exactly its original
signed delta. -/
theorem leaderReducedScatter_sound : LeaderReducedScatter.Goal := by
  intro us init k
  simp only [sequential, delta_combine]

/-- **Refuted: last-lane RMW substitution.** For slot `0`, updates
`[(0, 1), (0, 2)]` have last lane `2`, but their sequential delta is `3`.
Replacing the reduction by one native last-writer-wins store loses `1`. -/
theorem lastLaneRmwSubstitution_refuted : ¬ LastLaneRmwSubstitution.Goal := by
  intro h
  have hbad := h [(0, 1), (0, 2)] (fun _ => 0) 0 (by decide)
  norm_num [sequential, lastLane, delta] at hbad

/-- **Proved: store once per vector pair.** Coalescing each vector batch and
then coalescing the pair preserves every slot's total signed delta, so SQL
execution may move the final RMW across the two-batch boundary. -/
theorem storeOncePerVectorPair_sound : StoreOncePerVectorPair.Goal := by
  intro a b k
  simp only [delta_append, delta_combine]

/-- **Refuted: checked scatter from final extrema.** Starting at `1` with
updates `[(0, 2), (0, -3)]`, both endpoints lie in `[0, 1]`, but the prefix
after the first lane is `3`. Endpoint bounds therefore do not certify all
intermediate RMW states. -/
theorem checkedScatterFromFinalExtrema_refuted : ¬ CheckedScatterFromFinalExtrema.Goal := by
  intro h
  have hbad := h [(0, 2), (0, -3)] 0 0 1 1
    (by decide) (by decide) (by decide) (by decide)
  norm_num [check, delta] at hbad

/-- A finite list of nonnegative integers has a nonnegative sum. This helper
supports the monotone prefix bound used by checked scatter. -/
private theorem list_sum_nonneg (vs : List Int) (h : ∀ v ∈ vs, 0 ≤ v) :
    0 ≤ vs.sum := by
  induction vs with
  | nil => simp
  | cons v vs ih =>
      have hv : 0 ≤ v := h v (by simp)
      have htail : ∀ w ∈ vs, 0 ≤ w := by
        intro w hw
        exact h w (by simp [hw])
      simp only [List.sum_cons]
      omega

/-- Every prefix of a nonnegative running sum lies between its initial and
final values. This permits SQL checked scatter to certify all prefixes from
the initial counter and the final reduced sum. -/
private theorem scanl_add_all_bounds (vs : List Int) (lo hi s : Int)
    (hlo : lo ≤ s) (hhi : s + vs.sum ≤ hi)
    (hnonneg : ∀ v ∈ vs, 0 ≤ v) :
    (vs.scanl (· + ·) s).all (fun x => lo ≤ x && x ≤ hi) := by
  induction vs generalizing s with
  | nil => simp [hlo]
  | cons v vs ih =>
      have hv : 0 ≤ v := hnonneg v (by simp)
      have htail : ∀ w ∈ vs, 0 ≤ w := by
        intro w hw
        exact hnonneg w (by simp [hw])
      have hsumtail : 0 ≤ vs.sum := list_sum_nonneg vs htail
      have hhi' : (s + v) + vs.sum ≤ hi := by
        simpa only [List.sum_cons, add_assoc] using hhi
      have hs_le : s ≤ (s + v) + vs.sum := by omega
      have hs_hi : s ≤ hi := le_trans hs_le hhi'
      simp only [List.scanl_cons, List.all_cons]
      constructor
      · simp [hlo, hs_hi]
      · exact ih (s := s + v) (by omega) hhi' htail

/-- **Proved: monotone scatter range certificate.** If every contribution to
the queried slot is nonnegative, all its running prefixes are bounded by the
initial and final values. SQL execution may therefore use those two bounds
without changing the checked result. -/
theorem monotoneScatterRangeCertificate_sound : MonotoneScatterRangeCertificate.Goal := by
  intro us k lo hi s hsign hlo hhi
  unfold check
  apply scanl_add_all_bounds
  · exact hlo
  · simpa only [delta] using hhi
  · intro v hv
    rcases List.mem_map.mp hv with ⟨u, hu, rfl⟩
    have hu' := List.mem_filter.mp hu
    exact hsign u hu'.1 (by simpa using hu'.2)

/-- **Refuted: truncated conflict ballot.** With modulus `2`, the sorted
leaders `[0, 1, 2]` have distinct residues on every adjacent zip pair, yet
slots `0` and `2` collide modulo `2`. Updates `[(0, 1), (1, 2), (2, 4)]`
therefore reduce to `5` in bucket `0`, not the original slot-`2` delta `4`. -/
theorem truncatedConflictBallot_refuted : ¬ TruncatedConflictBallot.Goal := by
  intro h
  have hbad := h [(0, 1), (1, 2), (2, 4)] 2 2
    (by decide) (by decide) (by decide) (by decide)
  norm_num [delta, leaders] at hbad

/-- **Proved: disjoint wave commit.** When the two waves touch disjoint slot
sets, a slot's combined delta is exactly the nonzero side's delta. SQL
execution may publish the two compiled waves independently without losing a
contribution. -/
theorem disjointWaveCommit_sound : DisjointWaveCommit.Goal := by
  intro a b k hdisj
  rw [delta_append]
  by_cases hk : k ∈ leaders a
  · have hkb : k ∉ leaders b := fun h => hdisj k hk h
    have hkeys : k ∉ b.map Prod.fst := by simpa [mem_leaders] using hkb
    rw [if_pos hk, delta_eq_zero_of_not_mem_keys hkeys, add_zero]
  · have hka : k ∉ a.map Prod.fst := by simpa [mem_leaders] using hk
    rw [if_neg hk, delta_eq_zero_of_not_mem_keys hka, zero_add]

/-- Removing zero-valued lanes from an update list preserves every signed
slot delta. This allows the arithmetic store to be skipped without changing
the modeled numeric sum. -/
private theorem delta_filter_nonzero (us : Updates) (k : Nat) :
    delta (us.filter (fun u => u.2 != 0)) k = delta us k := by
  induction us with
  | nil => simp [delta]
  | cons u us ih =>
      by_cases hk : u.1 = k <;> by_cases hz : u.2 = 0 <;>
        simp [delta, hk, hz, ih]

/-- **Proved: zero-delta leader elision.** Filtering zero reduced deltas out
of the leader list preserves every numeric slot sum. SQL execution may omit
the arithmetic store while retaining the correct group totals. -/
theorem zeroDeltaLeaderElision_sound : ZeroDeltaLeaderElision.Goal := by
  intro us k
  rw [delta_filter_nonzero, delta_combine]

/-- **Refuted: zero-delta occupancy elision.** The updates
`[(0, 1), (0, -1)]` occupy slot `0`, but their reduced delta is zero, so the
filtered leader list is empty. Numeric elision is sound, but using the
filtered list to emit SQL groups drops an occupied group. -/
theorem zeroDeltaOccupancyElision_refuted : ¬ ZeroDeltaOccupancyElision.Goal := by
  intro h
  have hbad := h [(0, 1), (0, -1)]
  norm_num [combine, leaders, delta] at hbad

/-- Extract the updates whose ordinal belongs to one parity class. The first
projection retains the original physical slot and signed contribution. -/
private def parityLane (p i : Nat) (us : Updates) : Updates :=
  (((us.zipIdx i).filter (fun u => u.2 % 2 == p)).map Prod.fst)

/-- Splitting `zipIdx` at any starting ordinal into its two parity classes
partitions the original updates, so the two signed slot deltas sum to the
unsplit delta. This permits privatized striped SQL counters to merge once. -/
private theorem delta_parity_zipIdx (us : Updates) (k i : Nat) :
    delta (parityLane 0 i us) k + delta (parityLane 1 i us) k = delta us k := by
  induction us generalizing i with
  | nil => simp [parityLane, delta]
  | cons u us ih =>
      have hp : i % 2 = 0 ∨ i % 2 = 1 := by omega
      rcases hp with hp | hp
      · have hp' : (i + 1) % 2 = 1 := by omega
        simp [parityLane, List.zipIdx_cons, hp, hp', delta_cons, ih (i + 1)]
        omega
      · have hp' : (i + 1) % 2 = 0 := by omega
        simp [parityLane, List.zipIdx_cons, hp, hp', delta_cons, ih (i + 1)]
        omega

/-- **Proved: privatized stripe merge.** Ordinal parity partitions all lanes,
and projecting each parity lane back to its update preserves the physical
slot and signed delta. SQL execution may merge the two private counters once
without changing the sequential total. -/
theorem privatizedStripeMerge_sound : PrivatizedStripeMerge.Goal := by
  intro us k
  change delta (parityLane 0 0 us) k + delta (parityLane 1 0 us) k = delta us k
  exact delta_parity_zipIdx us k 0

end TinylambOpt.ResearchProofs.JitKernels.ProofConflictScatter
