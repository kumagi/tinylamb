# 敗北と Lean 命題の対応表

11月25日の速度比較と保存サイズの比較に対応する。測定条件と対象外のケースは[調査記録](benchmark-rematch.md)を参照。

## TPC-H

| 種目 | 相手 | tinylamb (ms) | 相手 (ms) | 同点までの短縮 (ms) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| Q1 | DuckDB | 78.208496 | 74.063063 | 4.145433 | [SharedDiscountedPriceSums.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L17)　Q1 は日付を満たす行の割引後価格を一度計算して2つの SUM に配る。計測は割引積の評価回数。 |
| Q3 | DuckDB | 93.12986 | 51.105658 | 42.024202 | [MergeOfLocalRevenueTopK.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L27)　Q3 は注文単位の売上を確定したワーカーで上位10件だけを merge に渡す。計測は最終ソート入力数。 |
| Q7 | DuckDB | 55.542668 | 54.87313 | 0.669538 | [TwoNationCodeDirectionalMatch.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L35)　Q7 は国を2値コードに落として逆向きの2組だけを通す。計測は nation 間の文字列比較回数。 |
| Q8 | DuckDB | 61.925843 | 55.305476 | 6.620367 | [NationPartitionedYearVolume.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L44)　Q8 は年ごとに PERU／その他の2状態を作り、分母をその和から得る。計測は volume の再読回数。 |
| Q15 | DuckDB | 34.412722 | 34.059057 | 0.353665 | [GlobalMaximizersFromLocalTies.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/RevenueAndShipments.lean#L52)　Q15 は supplier 別集約の局所最大と同値の全候補を残す。計測は最大値確定後の詳細行プローブ数。 |

## TPC-DS

| 種目 | 相手 | tinylamb (ms) | 相手 (ms) | 同点までの短縮 (ms) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| Q1 | DuckDB | 14.04435 | 13.054059 | 0.990291 | [EligibleStoreCustomerReturnTotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L19)　Q1 は TN 店舗だけで顧客返品額をまとめて店舗平均を作る。計測は相関集約に渡す行数。 |
| Q2 | DuckDB | 62.739246 | 36.00952 | 26.729726 | [DayOfWeekNullablePartialSums.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L28)　Q2 は販売2経路の曜日別 nullable SUM を部分集約して結合する。計測は UNION 後に渡す行数。 |
| Q2 | ClickHouse | 62.739246 | 58.184059000000005 | 4.555187 | [ShiftedWeekArrayJoinMultiplicity.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L35)　Q2 は53週ずれた自己結合に配列参照を使い、日付側の重複度は保持する。計測は比較した週ペア数。 |
| Q4 | DuckDB | 292.692385 | 245.678457 | 47.013928 | [YearChannelSlotsReplaceSelfJoin.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L43)　Q4 は1顧客に必要な販売経路×2年の6スロットを用意する。計測は year_total の自己プローブ数。 |
| Q10 | DuckDB | 25.885935 | 20.152741 | 5.733194 | [StoreAndRemoteCustomerMembership.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L53)　Q10 は店舗あり、かつ web／catalog のどちらかありという顧客集合を先に求める。計測は EXISTS プローブ数。 |
| Q11 | DuckDB | 196.094094 | 147.795473 | 48.298621 | [LateFunctionalCustomerAttributes.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L63)　Q11 は重い顧客属性を年別比較の後に付ける。一意な顧客 ID に従属する属性に限る。計測は集約キー幅。 |
| Q14 | DuckDB | 447.975802 | 224.467556 | 223.508246 | [ThreeChannelProductKeyIntersection.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L75)　Q14 はブランド・クラス・カテゴリの組の全経路共通部分を一度作る。計測は item 側の会員照会数。 |
| Q15 | DuckDB | 17.970043 | 15.104213 | 2.865830 | [AddressMatchBeforePricePredicate.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L84)　Q15 は住所側の ZIP／州通過を先に印づけ、未通過行だけ売価を比較する。計測は売価のしきい値比較数。 |
| Q16 | DuckDB | 8.068372 | 6.571695 | 1.496677 | [OtherNonnullWarehouseFromExtrema.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L94)　Q16 は同一注文に別の非 NULL 倉庫があるかを両端で判定する。計測は catalog_sales の再走査行。 |
| Q17 | DuckDB | 42.978414 | 41.4628 | 1.515614 | [MultiplicityWeightedSecondMoments.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L103)　Q17 は結合で複製される数量の2次モーメントを多重度で更新する。計測は STDDEV 状態の更新回数。 |
| Q21 | DuckDB | 22.161395 | 8.601199 | 13.560196 | [SplitInventorySumsAtDateBoundary.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L112)　Q21 は在庫日の境界で分けた2本のストリームから before/after を加算する。計測は CASE 分岐数。 |
| Q25 | DuckDB | 40.710855 | 13.964939 | 26.745916 | [FanoutWeightedProfitAndLoss.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L122)　Q25 は完全な結合キー別に利益・損失を畳み、他側の fanout を一度掛ける。計測は三重結合の出力件数。 |
| Q29 | DuckDB | 46.113509 | 43.009894 | 3.103615 | [CustomerItemFilterBeforeQuantityRead.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L131)　Q29 は ticket 側の一致が存在する item/customer の catalog 数量だけを読む。計測は数量ペイロード読取件数。 |
| Q30 | DuckDB | 26.497252 | 15.579951 | 10.917301 | [CandidateStatesPreserveAverageInputs.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L140)　Q30 は候補の返品州を一度だけ抽出し、その州の全 CTE 行から平均を求める。計測は構築した州別状態数。 |
| Q35 | DuckDB | 81.787608 | 54.128318 | 27.659290 | [ConstantGroupKeyExtrema.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L150)　Q35 はグループキーに含まれる扶養者数の MIN/MAX を代表キーに置き換える。計測は集約状態バイト。 |
| Q38 | DuckDB | 202.973396 | 75.113849 | 127.859547 | [OrderedChannelIntersectionDedup.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L159)　Q38 は3チャネルを氏名・日付の同じキー空間で交差させる。計測は二重 DISTINCT 入力数。 |
| Q38 | ClickHouse | 202.973396 | 182.945677 | 20.027719 | [DisjointDatePartitionIntersectionCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L168)　Q38 は日付で互いに素な区画を作り、区画ごとの共通件数だけを集める。計測は大域ハッシュ表バイト。 |
| Q39 | DuckDB | 91.90918 | 58.880199 | 33.028981 | [RestrictMonthStatisticsBeforeSelfJoin.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L179)　Q39 は1月と2月だけで在庫の統計を作り自己結合する。計測は統計状態の個数。 |
| Q40 | DuckDB | 7.652188 | 5.641449 | 2.010739 | [ReturnCountAndRefundOuterJoinSubtotal.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L189)　Q40 は返品のない売上も1回残し、返品件数と返金合計から差引額を作る。計測は外部結合中間件数。 |
| Q49 | DuckDB | 51.566608 | 40.911298 | 10.655310 | [UnionOfRanksWithTies.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L198)　Q49 は2つの RANK<=10 の和集合だけを詳細化する。同順位を全件残す。計測は順位付け後の転送量。 |
| Q57 | DuckDB | 54.494798 | 46.054786 | 8.440012 | [AdjacentRankLookupByWindow.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L211)　Q57 は隣接 rank の探索を整列済み3行窓に置く。連番で重複のないパーティションが条件。計測は自己結合プローブ数。 |
| Q58 | DuckDB | 32.284941 | 24.283436 | 8.001505 | [SharedWeekDateFilterAcrossChannels.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L220)　Q58 は該当週の日付を一度解決して各チャネルの事実表に配る。計測は date_dim の検索回数。 |
| Q59 | DuckDB | 128.52101 | 92.576172 | 35.944838 | [MatchingPriorWeekPrefilter.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L229)　Q59 は52週差で相手がある店舗・週だけを比較に渡す。日付側の重複は保持する。計測は join への入力。 |
| Q62 | DuckDB | 21.539908 | 11.977043 | 9.562865 | [DisjointShippingDelayBuckets.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L238)　Q62 は発送遅延を1つのバケット番号に変換する。計測は5本の CASE の比較回数。 |
| Q63 | DuckDB | 20.57663 | 20.450539 | 0.126091 | [MonthlyDeviationFromTotalAndCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L250)　Q63 は manager 内の月別合計に対する平均との差を、合計と有効月数で評価する。計測は窓行の再読回数。 |
| Q66 | DuckDB | 47.029658 | 23.064789 | 23.964869 | [ChannelMonthlySlotSums.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L259)　Q66 はチャネル別月次売上を12スロットで統合する。計測は数値状態の merge 回数。 |
| Q72 | DuckDB | 143.576229 | 80.828578 | 62.747651 | [SortedStockPrefixCounts.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L269)　Q72 は商品・週の在庫数量を整列し、販売数量未満の件数を境界位置から得る。計測は在庫比較数。 |
| Q74 | DuckDB | 199.858649 | 126.930734 | 72.927915 | [CompleteYearChannelPairsBeforeRatios.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L277)　Q74 は両経路の2年間が揃った顧客だけで比率を計算する。計測は年次集約後の除算数。 |
| Q75 | DuckDB | 164.557912 | 106.207996 | 58.349916 | [ReturnMultiplicityWeightedQuantityDelta.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L287)　Q75 は各チャネルで返品による重複を反映して数量差を集める。計測は年月別統合への入力件数。 |
| Q79 | DuckDB | 54.838424 | 40.91799 | 13.920434 | [CityPrefixAfterLocalGrouping.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L295)　Q79 は city の30文字切出しを (city, ticket) 集約後に行う。計測は文字列切出し回数。 |
| Q81 | DuckDB | 38.68099 | 30.754218 | 7.926772 | [CustomerBoundaryTiesBeforeAddressSort.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L305)　Q81 は顧客IDで100位の境界を求め、同値を全件残してから住所順を解決する。計測は住所列復元バイト。 |
| Q83 | DuckDB | 9.850464 | 8.467404 | 1.383060 | [OverlappingWeekFilterUnion.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L317)　Q83 は3つの日付が属する週の和集合で返品をフィルタする。同じ週が重なっても重複させない。計測は日付プローブ。 |
| Q85 | DuckDB | 22.432493 | 19.295183 | 3.137310 | [IndependentNullableAverageStates.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L325)　Q85 は3つの nullable AVG が分母を混同しない部分状態を使う。計測は結合行の再走査数。 |
| Q86 | DuckDB | 11.575137 | 10.890539 | 0.684598 | [RollupTagSeparatesNullRankPartitions.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L338)　Q86 は subtotal NULL と実データ NULL を別の grouping-tag で順位付けする。計測は順位区画のソート入力。 |
| Q87 | DuckDB | 201.276095 | 88.942737 | 112.333358 | [ExclusionByUnionOfChannels.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L348)　Q87 は2経路の氏名・日付の和集合を除外表にする。計測は EXCEPT 中間表の要素数。 |
| Q90 | DuckDB | 7.184505 | 5.219163 | 1.965342 | [SinglePassTimeBandCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L356)　Q90 は午前と午後のフィルタ済み入力を同一走査で数える。計測は web_sales の読取バイト。 |
| Q99 | DuckDB | 49.123805 | 14.6726 | 34.451205 | [MergeShippingDelayHistograms.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/SalesAndReturns.lean#L366)　Q99 は遅延区分の局所ヒストグラムを warehouse・配送・call center ごとに足す。計測は大域状態の更新数。 |

## ClickBench

| 種目 | 相手 | tinylamb (ms) | 相手 (ms) | 同点までの短縮 (ms) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| Q12 | DuckDB | 58.039003 | 47.045908 | 10.993095 | [LocalDedupPreservesGlobalDistinct.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L18)　Q12 は端末・機種の組ごとに UserID を辞書化し、ブロック内で重複を落とす。計測は大域 DISTINCT への挿入数。 |
| Q14 | ClickHouse | 316.759758 | 310.30555799999996 | 6.454200 | [ModuloPartitionDistinctCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L26)　Q14 は UserID で分割して同じ検索語に対する局所 DISTINCT 件数を加算する。計測は統合時の ID 転送量。 |
| Q18 | ClickHouse | 27.885142 | 8.659509 | 19.225633 | [FixedCandidateGroupCounts.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L34)　Q18 は最初の10グループを固定し、残りの走査ではその組だけを加算する。計測はグループ表の最大要素数。 |
| Q20 | DuckDB | 14.036155 | 6.560445 | 7.475710 | [BlockExtremaPreservePointCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L43)　Q20 はブロックの UserID 最小・最大で点検索対象を絞る。計測はデコードしたブロック数。 |
| Q20 | ClickHouse | 14.036155 | 8.318037 | 5.718118 | [PointFilterFromDictionaryFrequency.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L51)　Q20 は圧縮辞書における対象 ID の出現数から定数行を生成する。計測は元行ペイロードの読取数。 |
| Q21 | DuckDB | 662.362965 | 343.792455 | 318.570510 | [SubstringCountFromDictionaryFrequency.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L59)　Q21 は URL 辞書の語ごとに部分一致し、頻度で COUNT を得る。計測は文字列照合回数。 |
| Q21 | ClickHouse | 662.362965 | 480.338403 | 182.024562 | [RequiredSubstringBlockPruning.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L68)　Q21 は必須部分文字列 goo のある区画だけを検査する。計測は再照合した URL バイト数。 |
| Q22 | DuckDB | 743.699382 | 345.750543 | 397.948839 | [GlobalMinimumFromLocalMinima.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L77)　Q22 は確定した局所群の MIN(URL) を統合する。計測は検索語ごとの文字列コピー数。 |
| Q22 | ClickHouse | 743.699382 | 579.906182 | 163.793200 | [PhraseSubstringCountFromPairFrequencies.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L86)　Q22 は同じ URL の gram 適合を一度だけ判定し、検索語別の頻度を残す。計測は LIKE の評価数。 |
| Q23 | DuckDB | 541.222585 | 329.805751 | 211.416834 | [SplitTitleAndUrlFilter.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L96)　Q23 は Title 条件と非空検索語を先に通し、残りの URL にだけ否定 LIKE を適用する。計測は URL 照合バイト。 |
| Q24 | DuckDB | 42.771759 | 26.683103 | 16.088656 | [TopRowsBeforePayloadGather.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L105)　Q24 は完全な順位キーで10件の RID を決めてから全列を gather する。計測は幅広い行の復元件数。 |
| Q28 | ClickHouse | 192.340097 | 96.780356 | 95.559741 | [NonZeroLengthSumAndCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L116)　Q28 は同じ CounterID の URL 長を終点差と非空件数に集約する。計測は本文のデコード量。 |
| Q31 | ClickHouse | 179.471802 | 142.070447 | 37.401355 | [PairCountAfterLeadingKeyPartition.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L125)　Q31 は代表 SearchEngineID を先に分配して ClientIP だけを内部キーにする。計測はハッシュキー比較バイト。 |
| Q32 | DuckDB | 255.075536 | 235.976146 | 19.099390 | [UniqueLeadingKeyImpliesSingletonGroups.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L133)　Q32 は WatchID が一意な区画の単一行集約を直接作る。計測は一意性を確認した区画のハッシュ挿入数。 |
| Q32 | ClickHouse | 255.075536 | 166.24250500000002 | 88.833031 | [SelectionBeforePairGather.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L141)　Q32 は非空検索語の通過 RID だけから WatchID と IP を gather する。計測は2列の gather 行数。 |
| Q37 | DuckDB | 87.762905 | 18.375582 | 69.387323 | [PageFrequencyUpperBoundExcludesTopK.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L149)　Q37 は URL ごとの部分件数に未読件数を足しても10位に届かない群を捨てる。計測は最後まで保持した群数。 |
| Q37 | ClickHouse | 87.762905 | 18.775190000000002 | 68.987715 | [FilteredLocalDictionaryHistogramMerge.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L162)　Q37 はフラグと日付の通過 RID だけで局所 URL コードを数え、同名の辞書項目を統合する。計測は解決する文字列の個数。 |
| Q38 | DuckDB | 13.800956 | 4.061535 | 9.739421 | [MergeTitleFrequencyBeforeRanking.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L174)　Q38 は異なる部分辞書に同名 Title があれば加算してから順位を決める。計測は大域集約への入力行。 |
| Q38 | ClickHouse | 13.800956 | 10.228506 | 3.572450 | [NonemptyTitleAndFlagCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L182)　Q38 は非空 Title と固定フラグの通過ビットを同時に数える。計測は圧縮ブロックの走査回数。 |
| Q39 | DuckDB | 7.552648 | 5.248962 | 2.303686 | [OffsetTopKFromPartitionPrefixes.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L192)　Q39 はグループ件数確定後に各区画で1010位までを残す。計測は OFFSET 部分を含む merge 入力。 |
| Q40 | DuckDB | 38.747607 | 31.651879 | 7.095728 | [ConditionalReferrerGather.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L200)　Q40 は検索・広告 ID がともにゼロの枝だけで参照元を取得する。計測は Referer の gather 件数。 |
| Q42 | DuckDB | 1.74969 | 1.748718 | 0.000972 | [DistinctCardinalityMakesOffsetEmpty.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L209)　Q42 は URLHash 通過後の distinct 解像度数で OFFSET の空結果を確定する。計測は回避した順位ソート件数。 |
| Q43 | DuckDB | 1.972486 | 1.886501 | 0.085985 | [MinuteTopKBeforeFrequencyCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/DictionaryFiltersAndRanking.lean#L217)　Q43 は実際に存在する分だけを整列し、返却する10分の頻度だけを求める。計測は集約カウンタ数。 |

## JOB

| 種目 | 相手 | tinylamb (ms) | 相手 (ms) | 同点までの短縮 (ms) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| J3 | DuckDB | 23.069893941283226 | 8.88522295281291 | 14.184671 | [CastRoleBoundedPersonFilter.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L17)　J3 は role を通った cast の person_id 範囲だけに女性名フィルタを作る。計測は名前フィルタの占有バイト。 |
| J3 | ClickHouse | 23.069893941283226 | 21.60785300657153 | 1.462041 | [CastFrequencyWeightedYearTotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L27)　J3 は通過した出演を映画ごとに数えてから制作年を掛ける。計測は title へのプローブ回数。 |

## STATS-CEB

| 種目 | 相手 | tinylamb (ms) | 相手 (ms) | 同点までの短縮 (ms) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| C1 | DuckDB | 28.18385697901249 | 23.378083016723394 | 4.805774 | [VoteWeightedCommentAggregates.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L38)　C1 は post ごとの票数でコメントの COUNT と nullable SUM を重み付けする。計測は生成したコメント×票のペア数。 |
| C1 | ClickHouse | 28.18385697901249 | 25.06233798339963 | 3.121519 | [SkipVoteReadsWithoutComments.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L48)　C1 は適格 post にコメントが存在する場合だけ投票リストを読む。計測は読んだ投票ペイロード行数。 |
| C2 | DuckDB | 26.50205697864294 | 12.640772853046656 | 13.861284 | [CommenterFrequencyWithUserMultiplicity.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L58)　C2 は各投稿の commenter 別頻度に reputation 条件を通った users 行の多重度を掛ける。計測は users への反復プローブ数。 |

## H2O

| 種目 | 相手 | tinylamb (ms) | 相手 (ms) | 同点までの短縮 (ms) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| H2 | DuckDB | 17.703027930110693 | 11.847498826682568 | 5.855529 | [DenseCompositeCountIndex.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L67)　H2 は範囲内の2列キーを衝突なく1次元化し、密な COUNT 配列を使う。計測は複合キーのハッシュ回数。 |
| H4 | DuckDB | 23.118075914680958 | 9.684008080512285 | 13.434068 | [ZoneMinimumPrunesNullableAverageInput.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L76)　H4 は id3 のページ下限で対象外ページを飛ばし、AVG の非 NULL 値を元の順序で渡す。計測は v1 のデコードページ数。 |
| H5 | DuckDB | 32.632009126245975 | 12.932559940963984 | 19.699449 | [FactorizedNullableJoinSums.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L86)　H5 は id1・id4 ごとに両側を部分集約し、2本の SUM を相手側の件数で復元する。計測は結合ペア生成数。 |

## LDBC SNB

| 種目 | 相手 | tinylamb (ms) | 相手 (ms) | 同点までの短縮 (ms) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| L2 | DuckDB | 231.3564671203494 | 198.81617091596127 | 32.540296 | [ForumMembershipWeightedMessageTotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L97)　L2 は適格な会員・人物の結合件数を forum 単位で求め、年別メッセージに掛ける。計測は forum 展開後の行数。 |
| L4 | DuckDB | 9.556916076689959 | 3.754798322916031 | 5.802118 | [ForumReplyFrequencyJoinCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L107)　L4 は2012年の返信メッセージを forum ごとに畳んでから forum と結合する。計測は forum 結合の入力数。 |
| L5 | DuckDB | 9.066789876669645 | 4.218424204736948 | 4.848366 | [OutdegreeWeightedNullableAgeTotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L117)　L5 は country 内の各人の出次数で COUNT と年齢の AVG 状態を更新する。計測は person_knows から復元した行数。 |
| L5 | ClickHouse | 9.066789876669645 | 8.42925300821662 | 0.637537 | [CountryYearSourceSemijoin.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L129)　L5 は人物側から対象の country と年に合う始点集合を作り、辺の走査で棄却する。計測は国別集約に届く辺数。 |

## BigBench

| 種目 | 相手 | tinylamb (ms) | 相手 (ms) | 同点までの短縮 (ms) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| B3 | DuckDB | 16.966682393103838 | 4.785547032952309 | 12.181135 | [BoundedRegionCategoryYearKeyEncoding.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L139)　B3 は region・category・対象6年の複合キーを整数化する。計測はグループキー比較バイト。 |
| B3 | ClickHouse | 16.966682393103838 | 13.920385856181383 | 3.046297 | [CustomerFilterBeforeDimensionLookups.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L148)　B3 は適格 customer の印を先に読み、不一致の販売では店舗・商品を引かない。計測は2つの次元表へのプローブ数。 |
| B4 | DuckDB | 5.162345711141825 | 2.9920018278062344 | 2.170344 | [ShortCircuitCountryOrSubstringCount.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L157)　B4 は米国行を先に加算し、残りだけ部分文字列検索する。計測は LIKE に渡した文字列バイト。 |
| B5 | DuckDB | 7.730227895081043 | 2.602898981422186 | 5.127329 | [PerBlockDistinctPriceLookup.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L170)　B5 は売上ブロックごとの相異なる商品だけで価格条件を引く。一意な商品表で選択マスクを再利用する。計測は価格照会数。 |
| B5 | ClickHouse | 7.730227895081043 | 7.116216234862804 | 0.614012 | [HighPriceMultiplicityWeightedNullableSales.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/AnalyticalJoins.lean#L180)　B5 は年内の商品別売上を部分集約し、高価格の商品行数を掛けて nullable SUM を合成する。計測は価格結合を渡る行数。 |

## Public BI

| 種目 | 相手 | tinylamb (ms) | 相手 (ms) | 同点までの短縮 (ms) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| MLB Q108 | DuckDB | 3.451038 | 0.840084 | 2.610954 | [IntersectedRowMasksDetermineGroupExistence.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/FileRangesAndExtrema.lean#L18)　MLB Q108 は league・pos・yr の RID 集合の共通部分だけから単一の GROUP BY 状態を開く。計測は集約に渡す RID 数。 |
| MLB Q108 | ClickHouse | 3.451038 | 2.115589 | 1.335449 | [QualifiedPageExtremaReuse.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/FileRangesAndExtrema.lean#L29)　MLB Q108 は全行が条件を満たすページで4列の MIN/MAX 要約を使い、混在ページだけを走査する。計測は実デコードした指標値数。 |

## Iceberg SQL

| 種目 | 相手 | tinylamb (ms) | 相手 (ms) | 同点までの短縮 (ms) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| base | DuckDB | 0.681091 | 0.565289 | 0.115802 | [ManifestWeightedFileTotals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/FileRangesAndExtrema.lean#L44)　Iceberg SQL base はファイルごとの行数と rid 合計を manifest の一致数で合成する。計測は実体化したファイル結合行数。 |

## 保存サイズ

| 種目 | 相手 | tinylamb (B) | 相手 (B) | 勝利に必要な削減 (B) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| TPC-H SF10 | DuckDB | 6,697,242,348 | 2,875,469,824 | 3,821,772,525 | [SignedDateDeltaRoundtrip.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/ColumnCodecs.lean#L21)　TPC-H は lineitem の shipdate を基準に commitdate・receiptdate の符号付き差を連結する。計測は例外と日付ヘッダ込みの総バイト。 |
| TPC-H SF10 | ClickHouse | 6,697,242,348 | 2,656,602,703 | 4,040,639,646 | [RunEndOrderKeyLookup.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/ColumnCodecs.lean#L33)　TPC-H は lineitem の同一注文の run を終了位置で索引し、orderkey の反復を除く。計測は run 長と探索索引を含む列バイト。 |
| TPC-DS SF10 | DuckDB | 10,319,679,752 | 4,934,610,944 | 5,385,068,809 | [GridKeysWithSparseExceptions.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/ColumnCodecs.lean#L44)　TPC-DS は inventory の規則的な商品×倉庫の並びを暗黙化し、飛びと順序の乱れを例外表にする。計測は例外 RID とブロック起点を含むキー列バイト。 |
| TPC-DS SF10 | ClickHouse | 10,319,679,752 | 3,658,807,249 | 6,660,872,504 | [NullableRankIndexedResiduals.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/ColumnCodecs.lean#L56)　TPC-DS は売上・返品の nullable キーを有効ビットと密な残差に分離し rank で点参照する。計測は rank 索引・基準値込みのキー列バイト。 |
| ClickBench | DuckDB | 59,421,543,500 | 19,384,250,368 | 40,037,293,133 | [SparseOffsetsWithLocalLengthLookup.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/ColumnCodecs.lean#L69)　ClickBench は URL の終点を間引き、区画起点と格納長から本文だけを点参照する。計測は長さ列・起点列と圧縮本文の総バイト。 |
| ClickBench | ClickHouse | 59,421,543,500 | 9,410,847,370 | 50,010,696,131 | [MedianClockResidualRoundtrip.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/ColumnCodecs.lean#L81)　ClickBench は3時計を時点ごとの中央値と符号付き残差で表し、正規化した時刻をランダム復号する。計測は共通時計・残差幅・書式例外込みのバイト。 |
| 派生分析5種合計 | DuckDB | 1,082,859,325 | 190,328,832 | 892,530,494 | [ByteLaneTransposeRoundtrip.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/ColumnCodecs.lean#L98)　派生分析5種はフレーム内のバイトを位置の剰余で転置してから圧縮する。計測は幅タグと WAL 込みの全バイト。 |
| 派生分析5種合計 | ClickHouse | 1,082,859,325 | 324,908,462 | 757,950,864 | [IndexedFrameRangeRead.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/ColumnCodecs.lean#L109)　派生分析5種は blocks を独立圧縮し、論理オフセットを索引で引いて必要な範囲だけ再構築する。計測は索引・カタログ・WAL 込みの全バイト。 |

## オフライン試作

| 種目 | 相手 | tinylamb (B) | 相手 (B) | 勝利に必要な削減 (B) | 未証明命題と観測量 |
|---|---|---:|---:|---:|---|
| ClickBench 1 MiB | ClickHouse | 10,114,664,031 | 9,410,847,370 | 703,816,662 | [SharedUrlPrefixRoundtrip.Goal](../../TinylambOpt/ResearchGoals/QueryRewrites/ColumnCodecs.lean#L125)　ClickBench の1 MiB 試作は URL と OriginalURL の共通接頭辞を残差化する。計測は置換フレーム・索引をすべて再圧縮した純減バイト。 |
