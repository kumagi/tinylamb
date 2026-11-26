import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-! These proposals evaluate predicates and aggregate state on encoded columns. Keys use the scan's snapshot and collation. Ordered folds retain the input order when rounding is observable. -/

namespace TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

/-- A constant base and signed corrections at exceptional lanes suffice for a non-null integer sum. -/
def DefaultLaneSumWithPatches.Goal : Prop := ∀ (ids : List Int) base,
  ids.sum = (ids.length:Int)*base + total (ids.filter (· != base)) (· - base)

/-- Fusing frame reconstruction with the same cast and accumulator step preserves even an order-sensitive average state. -/
def FrameResidualOrderedAverageFold.Goal : Prop := ∀ (deltas : List Int) base (step : Nat → Int → Nat) initial,
  deltas.foldl (fun acc d => step acc (base+d)) initial =
    (deltas.map (base+·)).foldl step initial

/-- For adjacent sorted user slices, only an equal boundary user can occur in both distinct counts. -/
def SegmentedDistinctAcrossRangeSeams.Goal : Prop := ∀ (a b : List Nat), (a++b).Pairwise (· ≤ ·) →
  (a++b).eraseDups.length + (if a.getLast?.isSome && a.getLast? == b.head? then 1 else 0) =
    a.eraseDups.length + b.eraseDups.length

/-- Aliased model codes can share a user set; summing the individual set sizes would double count shared users. -/
def ModelPostingUnionBeforeDistinctUsers.Goal : Prop := ∀ (rows : List (Nat × Nat)) (model : Nat → String) target,
  let codes := (rows.map Prod.fst).eraseDups.filter (fun c => model c == target)
  ((codes.flatMap (forKey rows)).eraseDups).Perm
    (((rows.filter (fun r => model r.1 == target)).map Prod.snd).eraseDups)

/-- A dictionary of observed device/model pairs admits dense ordinals without mixing distinct users between pairs. -/
def DistinctUsersFromObservedPairOrdinals.Goal : Prop := ∀ (rs : List ((Nat × Nat) × Nat)) k,
  let dict := (rs.map Prod.fst).eraseDups
  k ∈ dict → (forKey (rs.map (fun (g,u) => (dict.idxOf g,u))) (dict.idxOf k)).eraseDups =
    (forKey rs k).eraseDups

/-- Any phrase missing from all per-page shortlists has frequency at most the sum of the pages' omitted maxima. -/
def GlobalPhraseFrequencyBoundFromOmittedPageMaxima.Goal : Prop := ∀ (pages : List (List Nat × List Nat)) k,
  (∀ p ∈ pages, ¬ k ∈ p.2) →
  ((pages.flatMap Prod.fst).count k) ≤
    (pages.map (fun (xs,keep) => (((xs.filter (!keep.contains ·)).map xs.count).max?).getD 0)).sum

/-- Within a sorted phrase group, adjacent changes plus its first lane count distinct users, including the empty case. -/
def PhraseUserRunTransitions.Goal : Prop := ∀ (users : List Nat), users.Pairwise (· ≤ ·) →
  users.eraseDups.length = (if users.isEmpty then 0 else 1) +
    countWhere (users.zip (users.drop 1)) (fun (a,b) => a != b)

/-- Counting engine IDs in each phrase posting gives the full composite-key histogram including repeated pairs. -/
def SparsePhraseEngineCounters.Goal : Prop := ∀ (rs : List (Nat × String)) phrase,
  let engines := (rs.filter (fun r => r.2 == phrase)).map Prod.fst
  (frequency engines).map (fun (e,n) => ((e,phrase),n)) =
    (frequency rs).filter (fun r => r.1.2 == phrase)

/-- With each user owned by exactly one partition, locally completed top-ten counts suffice for global top-ten counts. -/
def DisjointOwnerTopFrequencyMerge.Goal : Prop := ∀ (shards : List (List Nat)), shards.Pairwise (fun a b => ∀ k ∈ a, ¬ k ∈ b) →
  let counts := fun (xs : List Nat) => (frequency xs).map Prod.snd
  let top := fun (xs : List Nat) => xs.mergeSort.reverse.take 10
  top (shards.flatMap (fun s => top (counts s))) = top (counts shards.flatten)

/-- A verified top-ten lower bound allows discarding pairs with either marginal below it, retaining all boundary ties. -/
def HierarchicalPairFrequencyScreen.Goal : Prop := ∀ (rs : List (Nat × Nat)) t,
  let viable := fun (p : Nat × Nat) => t ≤ min ((rs.map Prod.fst).count p.1) ((rs.map Prod.snd).count p.2)
  (frequency (rs.filter viable)).filter (fun r => t ≤ r.2) = (frequency rs).filter (fun r => t ≤ r.2)

/-- For a chosen set of groups from the prefix, folding only suffix hits into the prefix counts completes every chosen count. -/
def SeededGroupCompletionFromSuffix.Goal : Prop := ∀ (a b : List (Nat × Nat)), let keep := a.eraseDups.take 10
  keep.map (fun k => a.count k + (b.filter keep.contains).count k) = keep.map (a++b).count

/-- Equality to an absolute signed key can be tested in the residual stream without materializing the base-adjusted values. -/
def ResidualPointProbeBeforeUnpacking.Goal : Prop := ∀ (ds : List Int) base key, ((ds.map (base+·)).filter (· == key)).length = ds.count (key-base)

/-- An order-preserving dictionary decodes the minimum accepted code to the minimum matching URL, including empty groups. -/
def CollationOrderedUrlMinimumCode.Goal : Prop := ∀ (ids : List Nat) (url : Nat → String), (∀ a b, a ≤ b → url a ≤ url b) →
  (ids.min?).map url = (ids.map url).min?

/-- For groups reaching the count boundary after both text predicates, delaying user aggregation leaves their exact distinct sets intact. -/
def LateDistinctOnWinningSearchPhrases.Goal : Prop := ∀ (rs : List (Nat × Nat)) phrase threshold,
  let passes := fun k => decide (threshold ≤ (rs.map Prod.fst).count k)
  passes phrase = true → (forKey (rs.filter (fun r => passes r.1)) phrase).eraseDups = (forKey rs phrase).eraseDups

/-- In individually sorted runs the first ten qualifying order ordinals from each suffice for the global ten. -/
def OrderedRunQualifyingPrefixMerge.Goal : Prop := ∀ (runs : List (List Nat)) (pass : Nat → Bool), (∀ r ∈ runs, r.Pairwise (· ≤ ·)) →
  best 10 (runs.flatMap (fun r => (r.filter pass).take 10)) = best 10 (runs.flatten.filter pass)

/-- The first ten entries of a nonempty-position index select exactly the first ten nonempty phrases in timestamp order. -/
def SparseQualifyingTimestampSeek.Goal : Prop := ∀ (phrases : List String),
  let positions := (phrases.zipIdx.filter (fun r => r.1 != "")).map Prod.snd
  valuesAt phrases (positions.take 10) = (phrases.filter (· != "")).take 10

/-- Once a timestamp boundary is certified, only the remaining quota of secondary keys at that time is needed to complete lexicographic top ten. -/
def LexicographicPrefixRunSelection.Goal : Prop := ∀ (rs : List (Nat × Nat)) time,
  let earlier := rs.filter (fun r => r.1 < time)
  let tied := forKey rs time
  let cmp := fun (a b : Nat × Nat) => decide (a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2))
  earlier.length ≤ 10 → 10 ≤ earlier.length+tied.length →
  earlier.mergeSort cmp ++ (best (10-earlier.length) tied).map (fun k => (time,k)) =
    (rs.mergeSort cmp).take 10

/-- On a counter run bounded by monotone string offsets, the endpoint distance and repeated endpoints recover both average components. -/
def CounterRunOffsetTelescoping.Goal : Prop := ∀ (start : Nat) (ends : List Nat), (start::ends).Pairwise (· ≤ ·) →
  let spans := (start::ends).zip ends
  let lengths := spans.map (fun (a,b) => b-a)
  ((lengths.filter (· > 0)).sum,(lengths.filter (· > 0)).length) =
    ((ends.getLast?).getD start-start,ends.length-countWhere spans (fun r => r.1 == r.2))

/-- Filtering the fact rows by a shortlist of composite keys preserves payload order for every shortlisted key. -/
def DeferredPayloadAcrossCountTies.Goal : Prop := ∀ (rs : List ((Nat × Nat) × (Option Int × Option Int))) (keep : List (Nat × Nat)) k, k ∈ keep →
  forKey (rs.filter (fun r => keep.contains r.1)) k = forKey rs k

/-- All repeated pairs outrank singletons. Once their ranking is fixed the remaining quota can be filled from singleton keys in tie order. -/
def SingletonCompletionAfterDuplicateDetection.Goal : Prop := ∀ (rs : List (Nat × Nat)),
  let keys := rs.eraseDups
  let heavy := keys.filter (fun k => 1 < rs.count k)
  let single := keys.filter (fun k => rs.count k == 1)
  let cmp := fun a b => decide (rs.count b ≤ rs.count a)
  ((heavy.mergeSort cmp).take 10 ++ single.take (10-heavy.length)) = (keys.mergeSort cmp).take 10

/-- Translation by a fixed base preserves frequency and first occurrence order, allowing group counting in the smaller residual domain. -/
def DeltaDomainIpHistogram.Goal : Prop := ∀ (ips : List Int) base,
  (frequency (ips.map (· - base))).map (fun (d,n) => (d+base,n)) = frequency ips

/-- Within a counter/date slice, subtracting the union of rejected rows preserves counts when both exclusion flags are set. -/
def SparseFlagExceptionsSubtractFromUrlCounts.Goal : Prop := ∀ (rs : List (String × Bool × Bool)) u,
  (rs.map Prod.fst).count u - countWhere rs (fun (v,a,b) => v == u && (a || b)) =
    countWhere rs (fun (v,a,b) => v == u && !a && !b)

/-- A certified constant nonempty title contributes the population of the survivor mask without any per-row title lookup. -/
def ConstantTitlePagePopulation.Goal : Prop := ∀ (titles : List (String × Bool)) label, label ≠ "" → (∀ r ∈ titles, r.1 = label) →
  (titles.filterMap (fun (s,b) => if b && s != "" then some s else none)) =
    List.replicate (countWhere titles Prod.snd) label

/-- Discarding all strictly higher counts when they fit inside the offset does not change the selected page of URL keys. -/
def SkipStrictlyHigherRankPrefix.Goal : Prop := ∀ (groups : List (Nat × Nat)) cut,
  let hi := countWhere groups (fun r => cut < r.2)
  let cmp := fun (a b : Nat × Nat) => decide (b.2 ≤ a.2)
  hi ≤ 1000 → (((groups.mergeSort cmp).drop 1000).take 10) =
    ((((groups.filter (fun r => r.2 ≤ cut)).mergeSort cmp).drop (1000-hi)).take 10)

/-- A reserved code for the empty referrer and injective codes for nonempty strings preserve CASE-key equality in a fixed source partition. -/
def ConditionalReferrerKeyInterning.Goal : Prop := ∀ (s a : Nat) (x y : String) (intern : String → Nat), (∀ p q, intern p = intern q → p = q) →
  let text := fun r => if s == 0 && a == 0 then r else ""
  let code := fun r => if text r == "" then 0 else intern (text r)+1
  (code x = code y ↔ text x = text y)

/-- The product of the two observed marginal cardinalities bounds the number of distinct resolution pairs. -/
def ResolutionDomainProductOffsetBound.Goal : Prop := ∀ (dims : List (Nat × Nat)), (dims.map Prod.fst).eraseDups.length * (dims.map Prod.snd).eraseDups.length ≤ 10000 →
  (dims.eraseDups.drop 10000) = []

/-- For a bounded minute domain, selecting occupied slots in increasing order reproduces the sparse sorted page, including gaps. -/
def MinuteOccupancySelectBeforeCounting.Goal : Prop := ∀ (ms : List Nat) bound, (∀ m ∈ ms, m < bound) →
  (((List.range bound).filter ms.contains).drop 1000).take 10 = ranges ms.eraseDups 1000 10

end TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates
