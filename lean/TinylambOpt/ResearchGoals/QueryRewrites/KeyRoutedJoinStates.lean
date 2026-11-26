import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-! These models route qualifying keys and row occurrences before joining or expanding payloads. Integer measures use exact widened arithmetic; an optional value records SQL absence. -/

namespace TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-- For one return/status group, subtracting late rows from its total and non-null count recovers the eligible average state. -/
def ExceptionalShipDatesSubtractFromGroupState.Goal : Prop := ∀ (xs : List (Nat × Option Int)) cutoff,
  let amounts := fun (rs : List (Nat × Option Int)) => rs.filterMap Prod.snd
  let late := amounts (xs.filter (fun r => cutoff < r.1))
  let early := amounts (xs.filter (fun r => r.1 ≤ cutoff))
  ((amounts xs).sum-late.sum,(amounts xs).length-late.length) = (early.sum,early.length)

/-- Filtering line keys by the closure of eligible customers through eligible orders preserves their exact join multiplicities. -/
def SegmentOrderClosureBeforeLineProbe.Goal : Prop := ∀ (customers : List Nat) (orders : List (Nat × Nat)) (lines : List Nat),
  let weight := fun order => ((forKey orders order).map customers.count).sum
  let support := (orders.filter (fun r => customers.contains r.2)).map Prod.fst
  ((lines.filter support.contains).map weight).sum = (lines.map weight).sum

/-- For qualifying lanes, frame-of-reference prices allow one base multiplication and a dot product of residuals with discounts. -/
def DiscountBlockRevenueDotProduct.Goal : Prop := ∀ (lanes : List (Int × Int)) base,
  total lanes (fun (r,d) => (base+r)*d) = base*total lanes Prod.snd + total lanes (fun (r,d) => r*d)

/-- In a fixed year slice the two disjoint trade directions can be gathered separately without duplicating revenue-bearing row IDs. -/
def OppositeNationBitIntersection.Goal : Prop := ∀ (ids : List Nat) (supplier customer : Nat → Option Bool),
  let dir := fun side i => supplier i == some side && customer i == some (!side)
  (ids.filter (fun i => dir true i || dir false i)).Perm ((ids.filter (dir true)) ++ ids.filter (dir false))

/-- A rejected key contributes zero to both volume states; guarding the country lookup by exact join membership preserves both sums. -/
def VolumeNeutralDimensionRejection.Goal : Prop := ∀ (rs : List (Nat × Int)) (factor : Nat → Nat) (peru : Nat → Bool),
  let pair := fun (xs : List (Nat × Int)) =>
    (total xs (fun (k,v) => if peru k then (factor k:Int)*v else 0), total xs (fun (k,v) => (factor k:Int)*v))
  pair (rs.filter (fun r => factor r.1 != 0)) = pair rs

/-- For unique customer keys the zero-order bin is the complement of matched customer keys, including when the order filter removes all rows. -/
def UnmatchedCustomersInZeroOrderBin.Goal : Prop := ∀ (customers orders : List Nat), customers.Nodup →
  (customers.map orders.count).count 0 = customers.length - (members customers orders).length

/-- Intersecting candidate quantity branches with eligible part branches implements disjunction without double counting shared endpoints. -/
def QuantityBandCandidatePartMask.Goal : Prop := ∀ (qty : Nat) (eligible : List Nat),
  let bands : List (Nat × Nat) := [(5,15),(15,25),(29,39)]
  let candidates := (bands.zipIdx.filter (fun ((l,h),_) => l ≤ qty && qty ≤ h)).map Prod.snd
  (members eligible candidates).isEmpty = !(eligible.any (fun i => bands[i]?.any (fun (l,h) => l ≤ qty && qty ≤ h)))

/-- Once a nonempty prefix reaches twice available stock, a nonnegative remainder cannot restore the strict supply comparison. -/
def SupplyThresholdEarlyFailure.Goal : Prop := ∀ (scanned pending : List Nat) available, scanned ≠ [] → 2*available ≤ scanned.sum →
  ¬ (scanned++pending).sum < 2*available

/-- Within a filtered year range, weighted year frequencies simultaneously recover the count and year sum of joined credits. -/
def QualifiedTitleYearHistogram.Goal : Prop := ∀ (credits : List Nat) lo span, (∀ y ∈ credits, lo ≤ y ∧ y < lo+span) →
  let bins := (List.range span).map (fun d => (lo+d,credits.count (lo+d)))
  ((bins.map Prod.snd).sum,(bins.map (fun (y,n) => y*n)).sum) = (credits.length,credits.sum)

/-- Transposing the cast adjacency from person/movie occurrences to movie postings preserves the qualified year histogram for each year. -/
def QualifiedYearCountsFromMoviePersonPostings.Goal : Prop := ∀ (cast : List (Nat × Nat)) (female : Nat → Bool) (movieYear : Nat → Option Nat) y,
  let movies := (cast.map Prod.fst).eraseDups.filter (fun k => movieYear k == some y)
  (movies.map (fun m => countWhere (forKey cast m) female)).sum =
    countWhere cast (fun (m,p) => female p && movieYear m == some y)

/-- An owner with no commenter in the eligible set contributes no group even if the owner has posts and comments. -/
def OwnerElisionForEmptyCommenterIntersection.Goal : Prop := ∀ (posts : List Nat) (commenters : Nat → List Nat) (qualified : List Nat),
  (members (posts.flatMap commenters) qualified).isEmpty =
    ((posts.flatMap (fun p => (commenters p).filter qualified.contains)).isEmpty)

/-- Moving the target-post semijoin before the source-score lookup preserves ordered nullable scores with duplicate qualifying target matches. -/
def TypedLinkEndpointRoleRouting.Goal : Prop := ∀ (links : List (Nat × Nat)) (scores : Nat → List (Option Int)) (targets : List Nat),
  let payload := fun (r : Nat × Nat) => (scores r.1).flatMap (List.replicate (targets.count r.2))
  (links.filter (fun r => targets.contains r.2)).flatMap payload = links.flatMap payload

/-- Disjoint radix partitions of composite keys can independently histogram pairs, preserving every count including tied groups. -/
def PairHistogramByRadixRuns.Goal : Prop := ∀ (rows : List (Nat × Nat)) radix, 0 < radix →
  let lane := fun (p : Nat × Nat) => p.1 % radix
  ((List.range radix).flatMap (fun i => frequency (rows.filter (fun r => lane r == i)))).Perm (frequency rows)

/-- A precomputed predicate on dictionary entries gives the same sequence of non-null average inputs, even with multiple codes for one value. -/
def PredicateDictionarySelectionForAverage.Goal : Prop := ∀ (rows : List (Nat × Option Int)) (dict : List Nat),
  let keep := (dict.zipIdx.filter (fun (v,_) => v < 4)).map Prod.snd
  ((rows.filter (fun r => keep.contains r.1)).filterMap Prod.snd) =
    ((rows.filter (fun r => dict[r.1]?.any (· < 4))).filterMap Prod.snd)

/-- Splitting the build values for one skewed join key into shards preserves both nullable product sums when merging partial states. -/
def SkewSplitWeightedJoinPartial.Goal : Prop := ∀ (probe : List (Option Int)) (shards : List (List (Option Int))),
  let product := fun (ys : List (Option Int)) => probe.flatMap fun x => ys.map (fun y => (x,y))
  ∀ col ∈ ([Prod.fst,Prod.snd] : List ((Option Int × Option Int) → Option Int)),
    nsum (shards.map (fun s => nsum ((product s).map col))) = nsum ((product shards.flatten).map col)

/-- When a forum's memberships are sorted by joining year, a suffix retains all qualifying membership/person multiplicities. -/
def MemberYearThresholdSuffixGather.Goal : Prop := ∀ (memberships : List (Nat × Nat)) (personMatches : Nat → Nat),
  (memberships.map Prod.fst).Pairwise (· ≤ ·) →
  (((memberships.dropWhile (fun r => r.1 < 2010)).map (fun r => personMatches r.2)).sum) =
    (memberships.map (fun (yr,p) => if 2010 ≤ yr then personMatches p else 0)).sum

/-- On an already qualified reply group, null runs contribute to reply count but not to either component of score average. -/
def ScoreNullMaskIndependentReplyCount.Goal : Prop := ∀ (runs : List (Option Int × Nat)),
  let allScores := runs.flatMap (fun (v,n) => List.replicate n v)
  (allScores.length,(allScores.filterMap id).length,(allScores.filterMap id).sum) =
    ((runs.map Prod.snd).sum,((runs.filter (fun r => r.1.isSome)).map Prod.snd).sum,
      total runs (fun (v,n) => v.getD 0*(n:Int)))

/-- Subtracting the first year gives unique bounded slots for the retained years and preserves nullable revenue within each slot. -/
def StarGroupSelectiveYearSlots.Goal : Prop := ∀ (sales : List (Nat × Option Int)) i, i < 6 →
  let inRange := sales.filter (fun r => 2011 ≤ r.1 && r.1 ≤ 2016)
  forKey (inRange.map (fun (y,v) => (y-2011,v))) i = forKey sales (2011+i)

/-- If a page rules out a required substring, the text arm cannot pass and the country arm alone selects the correct rows. -/
def TrigramNegativePageWithCountryExceptions.Goal : Prop := ∀ (page : List (Bool × String)), (∀ r ∈ page, r.2.contains "dat" = false) →
  page.filter (fun r => r.1 || r.2.contains "data") = page.filter Prod.fst

/-- Disjoint, full and mixed price pages can use different paths while returning the same item IDs in physical order. -/
def PriceRangeBlockAcceptance.Goal : Prop := ∀ (pages : List (List (Int × Nat))),
  let readPage := fun (p : List (Int × Nat)) =>
    if (p.map Prod.fst).min?.any (100 < ·) then p
    else if (p.map Prod.fst).max?.any (· ≤ 100) then [] else p.filter (fun r => 100 < r.1)
  (pages.flatMap readPage).map Prod.snd = (pages.flatten.filter (fun r => 100 < r.1)).map Prod.snd

/-- Even overlapping cached group slices can be merged for extrema when their union is exactly the selected row IDs. -/
def CachedExtremaAcrossCoveringMasks.Goal : Prop := ∀ (selected : List Nat) (slices : List (List Nat)) (value : Nat → Option Int),
  (∀ i, i ∈ selected ↔ i ∈ slices.flatten) →
  (((slices.filterMap (fun s => (s.filterMap value).min?)).min?),
    ((slices.filterMap (fun s => (s.filterMap value).max?)).max?)) =
    ((selected.filterMap value).min?,(selected.filterMap value).max?)

/-- A complete unique manifest lets the executor omit the file lookup while preserving row count and nullable identifier sum. -/
def ReferentialManifestJoinElision.Goal : Prop := ∀ (rows : List (Nat × Option Int)) (files : List Nat), files.Nodup →
  (∀ r ∈ rows, r.1 ∈ files) →
  let expanded := rows.flatMap fun (k,id) => List.replicate (files.count k) id
  (expanded.length,nsum expanded) = (rows.length,nsum (rows.map Prod.snd))

end TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates
