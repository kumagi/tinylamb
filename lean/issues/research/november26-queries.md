# 11月26日測定の敗北クエリと新しい Lean 命題

8スレッドの比較で正解照合を通り、相手より中央値が遅かった **83 クエリ**に1件ずつ命題を追加した。対戦相手別には 101 敗で、うち 18 クエリは両者に敗北した。前回と比べて初めて負けたクエリは 15 件、今回も負けたクエリは 68 件、今回両者に負けなかった前回の敗北クエリは 3 件。

入力は after-ed-cb-20261126 の TPC-H SF10（22問）、TPC-DS SF10（99問）、ClickBench 全量（43問）、派生分析5種（26問）、クラウド・BI（tinylamb のある23ケース）。TPC-DS Q44 は結果不一致。Iceberg native の3ケースは tinylamb の測定がない。CostBench と PBench の記録には今回の敗北がなかった。

速度は各ハーネスの保存サンプルの中央値で判定した。通常はウォームアップ1回、計測3回。主要3系列の ClickHouse の記録は前回と全164問で同一。派生分析はハーネスの多重集合照合と数値許容差に従う。表は負けた相手の時間だけを載せ、表示は ms を小数6桁に丸めた。両者に勝つには、次回同じ条件で正解した上で相手の短い方を下回る必要がある。僅差も含み、有意差は判定していない。

「新規」は前回どちらにも負けなかったクエリ。「継続」は前回も少なくとも一方に負けたクエリ。各提出型は SQL 内の特定の演算を扱い、計測項目は仮説が効くかを見分けるために選んだ。整数はオーバーフローのない算術、順序キーは照合順と同点処理を含む。非 NULL・一意性・順序・正確な割り切れ方などの条件を使う経路ではそれを確認し、丸め・評価エラー・適用条件が合わない区画を通常経路に戻す。

測定元は [TPC-H](../../../build/benchmarks/three-db-sf10-20261122/after-ed-cb-20261126/tpch/results.jsonl)、[TPC-DS](../../../build/benchmarks/three-db-sf10-20261122/after-ed-cb-20261126/tpcds/results.jsonl)、[ClickBench](../../../build/benchmarks/three-db-sf10-20261122/after-ed-cb-20261126/clickbench/results.jsonl)、[派生分析](../../../build/benchmarks/three-db-sf10-20261122/after-ed-cb-20261126/analytics/results.jsonl)、[クラウド・BI](../../../build/benchmarks/three-db-sf10-20261122/after-ed-cb-20261126/cloud_bi/measurements.json)。前回の個別案は[11月25日の対応表](benchmark-rematch-ledger.md)にある。

## 実データで確かめた境界

- TPC-DS Q45 の tinylamb 中央値は前回の11.673 msから204.192 msに変化した。今回の3サンプルは191.653、204.192、205.215 ms。SF10 の item で調べると指定10物理キーは9商品IDに対応し、同じ商品IDを持つ履歴を含めると20物理キーになる。DuckDB の参照DBで逆展開してから売上側を絞る SQL は、元の49行と一致した。この fixture では10キーを直に使う近道も最終49行が偶然一致するため、命題では同一商品に複数履歴がある入力と ZIP の OR 条件を含めて検証できるようにした。
- JOB J3 では、今回の分析用 DB を DuckDB 2.0.0 で読み、女性・役柄条件後の325,495行で映画コードを照合した。適格映画IDの重複は0、非ゼロコードは79,291行。コードの和に 1969 × 79,291 を加えた制作年の和は 157,120,459 で、直接 SUM した値と一致した。この方式なら、不適格の印と1970～2010年を1バイトに収められる。
- JOB J5 では同じ DB の aka_title 全行で、元の前方一致と半開区間の判定が食い違った行は0件。この範囲化を使う索引には、ベンチマークと同じ文字列順が必要になる。
- Iceberg SQL base の fixture では96ファイルに各48行、rid はファイルごとの連番。各ファイルを1区間として合成した COUNT は4,608、SUM は10,614,528で期待値と一致する。適用には連番の保証が必要で、欠番は区間を分割し、空入力なら件数を使って SUM の NULL を復元する。[fixture の生成コード](../../../benchmark/comparison/cloud_bi_bench.py#L144)

## TPC-H

| クエリ | 前回比 | tinylamb (ms) | DuckDB (ms) | ClickHouse (ms) | 新命題と計測項目 |
|---|---|---:|---:|---:|---|
| Q1 | 継続 | 80.350944 | 74.611945 | — | [DateBoundBlockSubtotalReuse.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L66)　Q1 は日付上限までの完全ブロックで事前集約を使い、境界だけ走査する。観測量は数量を復号する行数。 |
| Q3 | 継続 | 109.427512 | 49.069211 | — | [NonnegativeRevenueUpperBoundPruning.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L80)　Q3 は売上の非負上界と確定済み10件から、集計不要な注文を判定する。観測量は省略した出荷明細。 |
| Q6 | 新規 | 23.737412 | 23.040670 | — | [DiscountGroupedRevenueSum.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L92)　Q6 は日付・数量を通った行を割引率別に畳み、積を割引率ごとに一度取る。観測量は乗算数。 |
| Q7 | 継続 | 88.555647 | 53.721995 | — | [DirectionalNationKeyPrefilter.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L104)　Q7 は取引先の国を注文キーに伝播し、逆向きの相手国だけで明細を展開する。観測量は展開後の国ペア件数。 |
| Q8 | 継続 | 71.993059 | 55.277969 | — | [SupplierYearVolumePreaggregation.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L118)　Q8 は年・仕入先別に volume を先に足し、仕入先ごとの国を後引きする。観測量は国の照会回数。 |
| Q10 | 新規 | 95.329725 | 84.351831 | — | [TopCustomersBeforeWideAttributeLookup.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L130)　Q10 は顧客の上位20件を決めた後に幅広いコメントと住所を引く。観測量は復元した顧客文字列バイト。 |
| Q19 | 新規 | 75.499501 | 36.093230 | — | [CommonShipmentPredicateFactoring.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L142)　Q19 は品目側のブランド枝に数量区間を付け、共通の航空便条件を一度だけ通す。観測量は明細あたりの条件評価数。 |
| Q20 | 新規 | 120.483495 | 47.438885 | — | [CandidateRestrictedNullableSupplierQuantity.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L156)　Q20 は候補 partsupp のキー集合だけで数量を集約し、該当なしの SUM は NULL のままにする。観測量は年度集約のキー数。 |

## TPC-DS

| クエリ | 前回比 | tinylamb (ms) | DuckDB (ms) | ClickHouse (ms) | 新命題と計測項目 |
|---|---|---:|---:|---:|---|
| Q2 | 継続 | 62.333466 | 36.045959 | 58.184059 | [DateKeyPreaggregationBeforeWeekLookup.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L381)　Q2 は sold_date_sk 別の nullable SUM を先に作って曜日と週を引く。観測量は date_dim へのプローブ数。 |
| Q4 | 継続 | 283.375981 | 275.454309 | — | [PositivePriorYearDenominatorFilter.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L393)　Q4 は前年の分母が正の顧客だけに2年目のチャネル集約を作る。観測量は2年目の顧客状態数。 |
| Q11 | 継続 | 223.092393 | 148.221663 | — | [PositiveDenominatorRatioCrossProduct.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L405)　Q11 は正分母の比率順序を整数の交差積で比較する。観測量は高精度除算へのフォールバック数。 |
| Q14 | 継続 | 470.960223 | 229.047897 | — | [HavingFilteredNullableRollup.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L416)　Q14 は ROLLUP の上位階層に、HAVING を通った明細グループの nullable 状態だけを渡す。観測量は上位階層への投入行数。 |
| Q15 | 継続 | 19.303557 | 14.998800 | — | [PostalPrefixDictionaryFilter.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L428)　Q15 は ZIP の先頭5文字の判定を住所辞書に移し、同じ住所から繰り返す切出しを省く。観測量は ZIP を切り出した回数。 |
| Q16 | 継続 | 14.263404 | 7.170536 | — | [CandidateRestrictedNullAwareReturnAntijoin.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L440)　Q16 は候補注文に属する返品だけで除外表を作り、NULL 注文番号の SQL 等値を保つ。観測量は除外表のキー数。 |
| Q17 | 継続 | 43.759012 | 43.050137 | — | [ShiftedVarianceNumerator.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L452)　Q17 は数量をブロック内の基準値からの差に置き換え、分散の分子を小さい数で累積する。観測量はワイド整数へ昇格した状態数。 |
| Q21 | 継続 | 23.826713 | 9.073514 | — | [EligibleItemRangeBlockPruning.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L463)　Q21 は期間内の価格該当商品だけを対象とする在庫ブロックを商品ID範囲から選ぶ。観測量は復号した在庫ブロック数。 |
| Q25 | 継続 | 40.581138 | 14.438018 | — | [SalesKeyFilterOnReturnJoin.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L475)　Q25 は販売側の (customer,item,ticket) を返品側に伝播してから catalog と照合する。観測量は catalog へ渡す返品キー数。 |
| Q29 | 継続 | 51.473056 | 43.458167 | — | [CachedCatalogQuantityByCustomerItem.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L487)　Q29 は ticket で一致した後の (customer,item) ごとに catalog 数量の非 NULL 和をキャッシュする。観測量は catalog 数量の再走査数。 |
| Q30 | 継続 | 24.149590 | 17.015403 | — | [AverageRatioComparisonByCrossProduct.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L499)　Q30 は州内平均の1.2倍との比較を州合計・非 NULL 人数で行う。観測量は顧客単位の除算数。 |
| Q35 | 継続 | 384.343902 | 55.398459 | 106.609329 | [StoreCustomerRestrictedExistenceMarks.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L512)　Q35 は店舗購入者のうち必要な顧客だけで web・catalog の EXISTS を構築する。観測量は両経路の印の個数。 |
| Q38 | 継続 | 203.833291 | 73.682468 | 182.945677 | [NameCodeIntersectionCardinality.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L526)　Q38 は顧客IDを同姓同名の共通コードへ正規化してから日付との INTERSECT を取る。観測量は氏名比較バイト。 |
| Q39 | 継続 | 96.540049 | 57.583466 | — | [ConstantGroupVarianceRejection.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L538)　Q39 はグループ数量の最小・最大が同じなら変動係数の正のしきい値を通らず、二乗和更新を省ける。観測量は分散計算を省いたグループ数。 |
| Q40 | 継続 | 7.797003 | 5.439728 | — | [OuterJoinBuildRestrictedToSalesKeys.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L549)　Q40 は限定期間の販売キーだけで返品のハッシュ表を作り、返品なしの外部行を保つ。観測量は返品ビルド側の行数。 |
| Q41 | 新規 | 6.544429 | 5.995079 | — | [CorrelatedPositiveCountAsMembership.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L562)　Q41 は相関 COUNT(*) > 0 を適格メーカーの存在表で解決する。観測量は内側 item の行検査数。 |
| Q45 | 新規 | 204.191646 | 25.530650 | 33.336832 | [ExpandPhysicalKeysByLogicalItem.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L575)　Q45 は指定された物理キーに紐づく商品IDから全履歴の物理キーへ逆展開し、半結合を一度にする。観測量は売上側の商品ID文字列照合数。 |
| Q49 | 継続 | 53.194326 | 42.246921 | — | [NullRejectingRefundOuterJoinReduction.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L591)　Q49 は返品額の下限が NULL 延長行を必ず落とすので外部結合を内部結合に変える。観測量は生成した NULL 延長行。 |
| Q57 | 継続 | 60.045563 | 55.419499 | — | [YearPartitionAverageBeforeNeighborLookup.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L604)　Q57 は対象年の窓平均の計算と前後年から読む隣接月を分離する。観測量は平均計算に含めるパーティション数。 |
| Q58 | 継続 | 32.977104 | 28.047571 | — | [PairwiseRatioBoundsFromExtrema.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L618)　Q58 は3経路の売上が互いに許容範囲内かを最小値と最大値で判定する。観測量は売上比の比較回数。 |
| Q59 | 継続 | 129.853965 | 107.414164 | — | [WeekPairMultiplicityProduct.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L631)　Q59 は52週差の週ペアに対応する日付行の積を重みとし、同じ結果行の繰返しを遅延する。観測量は整列前の展開行数。 |
| Q62 | 継続 | 22.796233 | 12.503099 | — | [WarehousePrefixNullableSubtotalMerge.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L644)　Q62 は倉庫名の先頭20文字が同じ倉庫の集約状態を統合する。観測量は部分文字列の評価回数。 |
| Q63 | 継続 | 22.684470 | 21.087638 | — | [OverlappingItemPredicateUnion.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L656)　Q63 はカテゴリ・クラス・ブランドの枝ごとの適格商品を集合和し、重複しても売上を一度だけ加える。観測量は fact 側の文字列述語数。 |
| Q66 | 継続 | 47.648536 | 26.530481 | — | [FunctionalWarehouseAttributesAfterMonthlySum.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L668)　Q66 は倉庫の面積など一定の属性を月別加算の後で補う。観測量は月別グループキー幅。 |
| Q72 | 継続 | 133.515414 | 82.131548 | — | [ReturnMultiplicityAndPromotionCounts.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L680)　Q72 は不要な返品列を復元せず、一致する返品数とプロモーション有無で3つの件数を合成する。観測量は外部結合の生成行。 |
| Q74 | 継続 | 210.927250 | 136.115704 | — | [OrderedTopKAfterPriorYearRatioFilter.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L695)　Q74 は前年の store/web の組に一致する顧客から順に評価し、100件が確定したら残りの比率計算を打ち切る。観測量は評価した顧客数。 |
| Q75 | 継続 | 171.973032 | 106.584096 | — | [ChannelLocalDedupBeforeUnionDistinct.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L707)　Q75 は UNION DISTINCT の完全行をチャネル内でも圧縮してから年別数量を集約する。観測量は大域 DISTINCT に送る行数。 |
| Q79 | 継続 | 56.875463 | 45.425548 | — | [AddressKeyElisionByFunctionalDependency.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L719)　Q79 は同じ ticket・顧客・市の中で住所が一意なときだけ、集約キーから住所を省く。観測量は狭いキーを使えたグループ数。 |
| Q80 | 新規 | 43.098960 | 40.632536 | — | [NullableTotalFromChannelSubtotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L731)　Q80 はチャネル小計から総計を作り、空入力・全 NULL の SUM も再現する。観測量は総計への状態更新数。 |
| Q81 | 継続 | 45.456196 | 31.678591 | — | [StateAverageFromCustomerTotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L742)　Q81 は返品州内の顧客別合計を一度だけ集約し、現住所 GA の候補には州の合計と人数を引く。観測量は州平均の反復走査数。 |
| Q83 | 継続 | 23.915342 | 9.313703 | — | [CommonItemTopKBeforeQuantityLookup.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L757)　Q83 は item_id 順で3チャネルすべてにあるキーを100件見つけてから数量を読む。一意な非 NULL 集約キーが条件。観測量は復元した数量行数。 |
| Q85 | 継続 | 21.633100 | 18.888293 | — | [EqualNonNullDemographicCodes.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L773)　Q85 は2つの人口統計キーを同じ非 NULL 属性コードに正規化し、文字列の二重比較を省く。観測量は属性文字列の比較バイト。 |
| Q86 | 継続 | 12.480976 | 10.782149 | — | [RankFromDistinctValueFrequencies.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L784)　Q86 は順位の比較を売上の相異値とその頻度に集約して同額を一括処理する。観測量は順位比較数。 |
| Q87 | 継続 | 204.608783 | 88.024421 | — | [CandidateOnlyDifferenceMarks.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L796)　Q87 は店頭側に存在する氏名・日付の候補だけを保持し、他チャネルの行で印を消す。観測量は除外側の最大状態数。 |
| Q89 | 新規 | 28.544200 | 23.883732 | — | [MonthlyDeviationBandPruning.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L809)　Q89 は月間売上の区間全体が平均の許容帯に入ったブランド区画をまとめて棄却する。観測量は偏差を計算した月数。 |
| Q90 | 継続 | 9.520718 | 5.374663 | — | [WeightedTimeBandJoinCounts.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L822)　Q90 は同じ時刻・世帯・ページの fact を頻度表にし、両方の時間帯の次元照会を再利用する。観測量は次元表へのプローブ数。 |
| Q99 | 継続 | 49.341182 | 15.831832 | — | [RunLengthShippingDelayHistogram.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L836)　Q99 は同じ販売日と出荷日の run を重み付き遅延バケットに変換する。観測量は日付差の評価回数。 |

## ClickBench

| クエリ | 前回比 | tinylamb (ms) | DuckDB (ms) | ClickHouse (ms) | 新命題と計測項目 |
|---|---|---:|---:|---:|---|
| Q12 | 継続 | 57.671094 | 54.824643 | — | [PartitionedUserBitmapDistinct.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L228)　Q12 は端末グループ内で UserID を固定幅の区画に分け、使用ビット数を足す。観測量は DISTINCT 状態のバイト数。 |
| Q14 | 継続 | 317.057230 | — | 310.305558 | [DistinctTopKPruningByRemainingRows.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L241)　Q14 は頻出候補の DISTINCT 下限と残りの行数上限で検索語を剪定する。観測量は維持した UserID 集合数。 |
| Q16 | 新規 | 156.718966 | — | 153.759833 | [CandidateTopKWithOmittedCountBound.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L253)　Q16 は上位頻度の候補を標本から得て、未収録キーの上界が10位より小さいときだけ候補内で確定する。観測量は全件集計へのフォールバック率。 |
| Q18 | 継続 | 30.825310 | — | 8.659509 | [RunLengthGroupCounts.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L269)　Q18 は辞書コードの連続 run を1回の加算で (UserID, SearchPhrase) ごとに数える。観測量はハッシュ表の更新回数。 |
| Q20 | 継続 | 12.961705 | 6.598683 | 8.318037 | [SortedEqualRangeCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L280)　Q20 は昇順 UserID 区画で等値範囲の両端から該当件数を得る。観測量は比較した ID 数。 |
| Q21 | 継続 | 730.121107 | 351.213967 | 480.338403 | [ChunkedSubstringBoundaryCarry.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L291)　Q21 は URL をフレーズに分割したまま google を照合し、境界に5文字の履歴を渡す。観測量は連結した URL のバイト数。 |
| Q22 | 継続 | 744.159250 | 378.030445 | 579.906182 | [CountTopKBeforePayloadMinimum.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L303)　Q22 は件数で10位候補を絞ってから MIN(URL) を計算する。同点候補は全て残す。観測量は最小文字列の更新数。 |
| Q23 | 継続 | 702.165363 | 337.188880 | 663.477129 | [DictionaryPairPredicateMemoization.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L315)　Q23 は Title と URL の適格辞書コード同士を、出現した組に限って一度検査する。観測量は照合した文字列ペア数。 |
| Q24 | 継続 | 40.667182 | 27.353864 | — | [TimestampFenceBeforeSubstringRead.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L328)　Q24 は EventTime 順序の下限が現在の10位より後のページで URL 自体を読まない。観測量はスキップした LIKE 入力バイト。 |
| Q27 | 新規 | 8.340192 | 8.021081 | — | [TimestampTieBoundaryForSecondarySort.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L340)　Q27 は時刻順の走査を10行目と同時刻の末尾まで進め、検索語で境界内を整列する。観測量は整列した検索語数。 |
| Q28 | 継続 | 186.617655 | — | 96.780356 | [TotalFrequencyBoundsNonemptyHaving.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L352)　Q28 は CounterID の全行頻度が10万以下なら URL が非空でも HAVING を通らない。観測量は調べた URL 長の件数。 |
| Q31 | 継続 | 168.104839 | — | 142.070447 | [SplitIpKeyReconstruction.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L365)　Q31 は低位 IP の配列を上位プレフィックスごとに持ち、元のグループを復元する。観測量は複合キー表の割当量。 |
| Q32 | 継続 | 239.559269 | 225.243625 | 166.242505 | [SingleValueGroupWithConflictFallback.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L377)　Q32 は WatchID ごとの IP を単一スロットに置き、異なる IP のある ID だけ補助表に昇格させる。観測量は補助表へ昇格した WatchID 数。 |
| Q36 | 新規 | 144.662731 | — | 128.114833 | [DerivedIpKeysAfterGrouping.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L389)　Q36 は ClientIP だけでグループ化し、派生した3つの差をグループ確定後に作る。観測量はハッシュ・比較したキーバイト。 |
| Q37 | 継続 | 88.903943 | 16.271442 | 18.775190 | [IntervalCountFromSortedPostings.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L402)　Q37 は CounterID ごとに日付順の URL 出現リストを持ち、7月の件数を2つの境界で取る。観測量は走査した投稿リスト要素数。 |
| Q38 | 継続 | 14.158043 | 4.193866 | 10.228506 | [PageFrequencyBoundsForTitleCandidates.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L414)　Q38 はフィルタ済み辞書の最大頻度で Title の候補をページから選び、全体上界で漏れを判定する。観測量は大域表に登録する Title 数。 |
| Q39 | 継続 | 9.051421 | 4.685466 | — | [OffsetSelectionByCountBuckets.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L426)　Q39 は件数ごとのグループ数から OFFSET が入る順位帯を求め、同数の URL 内だけで残りの OFFSET を処理する。観測量は順位付けする URL 数。 |
| Q40 | 継続 | 38.219033 | 27.561541 | — | [DisjointReferrerBranchKeys.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L441)　Q40 は参照元を使う枝と定数の枝を別々に集約し、完全キーの分離を保証する。観測量は参照元列つきのハッシュ挿入数。 |
| Q42 | 継続 | 1.755336 | 1.572697 | — | [RowCountBoundsDistinctOffset.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L454)　Q42 は解像度列を読む前に URLHash 等の通過行数だけで OFFSET を判定する。観測量は省略した解像度ペアの復号行数。 |
| Q43 | 継続 | 2.097108 | 1.998410 | — | [MinuteTruncationFromBlockBounds.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L465)　Q43 はフィルタ通過後、時刻区画の最小・最大が同じ分なら切捨てを1回で済ませる。観測量は行単位の時刻切捨て回数。 |

## JOB

| クエリ | 前回比 | tinylamb (ms) | DuckDB (ms) | ClickHouse (ms) | 新命題と計測項目 |
|---|---|---:|---:|---:|---|
| J1 | 新規 | 4.994153 | 3.621331 | — | [CompanyWeightedMovieYearTotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L194)　J1 は会社国・会社種別を通る movie_companies を映画別の重みとし、タイトルから件数と年和を引く。観測量はタイトル側との結合行数。 |
| J2 | 新規 | 9.863598 | 8.913433 | — | [RatingAndGenreJoinMultiplicity.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L207)　J2 は評価条件を満たす映画だけを drama の索引で探し、両側の重複度を年別件数に反映する。観測量は映画別に生成した組数。 |
| J3 | 継続 | 22.826939 | 9.868077 | 19.277427 | [OptionalYearByteCodeTotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L219)　J3 は映画ごとに不適格を0、制作年を1..41の1バイトで引き、年の合計と COUNT を同時に復元する。観測量は映画プローブで読んだバイト。 |
| J5 | 新規 | 8.648887 | 7.979258 | — | [AliasPrefixHalfOpenRange.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L235)　J5 は aka-1 の前方一致を同じ照合順の上限開区間に変換してからタイトルを引く。観測量は比較した別名文字列バイト。 |

## STATS-CEB

| クエリ | 前回比 | tinylamb (ms) | DuckDB (ms) | ClickHouse (ms) | 新命題と計測項目 |
|---|---|---:|---:|---:|---|
| C1 | 継続 | 29.397956 | 22.491459 | 26.926804 | [ScoreAndViewCountZonePruning.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L247)　C1 は posts の score と view_count の区画統計で不適格ページを省き、適格 post_id の順を保つ。観測量はコメント・投票照会前に飛ばしたページ数。 |
| C2 | 継続 | 26.027936 | 11.009228 | — | [OwnerCommentFrequencyJoinCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L261)　C2 は適格投稿を owner ごとのキー集合にまとめ、コメント件数を投稿の重複ぶんだけ加算する。観測量は owner 集約前の行数。 |

## H2O

| クエリ | 前回比 | tinylamb (ms) | DuckDB (ms) | ClickHouse (ms) | 新命題と計測項目 |
|---|---|---:|---:|---:|---|
| H2 | 継続 | 18.930624 | 11.787284 | — | [StableDescendingCountBuckets.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L274)　H2 は完成したグループを件数バケットに散布し、件数の降順で同数グループを列挙する。観測量は比較ソートの比較回数。 |
| H4 | 継続 | 23.813435 | 8.311092 | — | [OrderedNullableValuesFromSelectedRuns.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L287)　H4 は選択マスクの連続した通過区間だけを AVG に渡し、浮動小数の入力順も保つ。観測量はマスク判定の回数。 |
| H5 | 継続 | 36.668149 | 13.052272 | — | [BoundedCartesianProductTiles.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L300)　H5 は結合キーごとの積を最大ペア数つきのタイルに分けて偏りを分散する。観測量は最大タイル行数と SUM の部分状態数。 |

## LDBC SNB

| クエリ | 前回比 | tinylamb (ms) | DuckDB (ms) | ClickHouse (ms) | 新命題と計測項目 |
|---|---|---:|---:|---:|---|
| L2 | 継続 | 259.464738 | 198.515320 | 239.747692 | [ForumMembershipSemijoinWeights.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L316)　L2 は年別メッセージに存在する forum だけの会員で年齢結合を行う。観測量は person 側に渡す会員数。 |
| L3 | 新規 | 8.431641 | — | 7.842646 | [ParentReplyCountAndScoreTotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L327)　L3 は正の score の返信を parent_id ごとの COUNT・SUM に圧縮し、親の年へ伝播する。観測量は親メッセージのプローブ数。 |
| L4 | 継続 | 7.855521 | 3.851200 | — | [NullableParentAndYearPositionGather.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L342)　L4 は親 ID の有効ビットと2012年の行マスクを交差させて score だけを順番に集める。観測量は復号した parent_id バイト。 |
| L5 | 継続 | 12.915089 | 4.299626 | 8.605605 | [PlaceGroupedCountryAgeTotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L355)　L5 は同じ場所に住む適格人物の辺をまとめてから国を解決する。観測量は place へのプローブ数。 |

## BigBench

| クエリ | 前回比 | tinylamb (ms) | DuckDB (ms) | ClickHouse (ms) | 新命題と計測項目 |
|---|---|---:|---:|---:|---|
| B3 | 継続 | 17.137879 | 5.133309 | 14.770655 | [PhysicalKeySubtotalRollup.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L370)　B3 は商品・店舗の物理 ID で一次集約し、同じ category・region へ二次集約する。観測量は次元ラベルの照会回数。 |
| B4 | 継続 | 5.084806 | 3.150410 | — | [DictionarySubstringOrFlagSelection.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L383)　B4 は検索語の辞書を走査し、data が部分一致した RID と米国 RID を重複なく合成する。観測量は LIKE の照合数。 |
| B5 | 継続 | 7.360074 | 2.754894 | 6.500071 | [SortedPriceSuffixKeyCounts.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L396)　B5 はソート済み価格索引の境界から対象商品を作り、重複キーの結合件数も保つ。観測量は読んだ価格索引の範囲。 |

## Public BI

| クエリ | 前回比 | tinylamb (ms) | DuckDB (ms) | ClickHouse (ms) | 新命題と計測項目 |
|---|---|---:|---:|---:|---|
| MLB Q108 | 継続 | 3.440224 | 0.907604 | 2.255670 | [PairedExtremaReduction.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/FileRangesAndExtrema.lean#L67)　MLB Q108 は4指標それぞれの MIN/MAX を対ごとの1比較から更新する。観測量は両端を求めるための比較命令数。 |

## Iceberg SQL

| クエリ | 前回比 | tinylamb (ms) | DuckDB (ms) | ClickHouse (ms) | 新命題と計測項目 |
|---|---|---:|---:|---:|---|
| base | 継続 | 0.683276 | 0.564409 | — | [ContiguousFileRowIdTotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/FileRangesAndExtrema.lean#L81)　Iceberg SQL base は各ファイルの連続 RID 区間を等差数列として加算する。観測量は読み出した RID 数と例外区間数。 |

## 再確認

保存済みの両測定系列がある作業ツリーで、以下を実行する。監査は速度の勝敗を生データから再計算し、対応表の数値と全宣言の一対一対応を照合する。

```sh
python3 lean/issues/research/fixtures/november26_queries.py
(cd lean && lake build CheckProofs)
```
