# WASM融合実行: 最初の実測と採否の保留

Status: experimental / not accepted (2026-11-13)

## 決定

`TODO.md` S0の最初の候補としてWasmtimeを実測した。現時点では本番Executorを
WASMへ移行しない。代表的な2段probe＋前集約がネイティブの1.3倍以内という
受け入れ条件に達していない。WAMRとWASM→LLVMの比較、実ストレージの配置、
他OSでの測定が残っており、WASM一般の不採用を意味するものではない。

独立に、TopNの入力全行の復元を避ける改修を進める。これはWASMの採否に依存しない
仕事量の削減であり、S2〜S10の完了とは扱わない。

## 実験

`benchmark/wasm_spike/run.py`。Ryzen 9 9950X3D、Linux x86-64、CPU 0–7、
Clang 21 -O2、Wasmtime 50.0.1、1000万行、warmup 1＋7回の中央値。
ネイティブとWASMは同じC++ループからコンパイルし、WASMも最適化済みの入力を渡す。
probeは独立した2本のキーと保存済みINT64計測値を持つ24-byte factを読む。
価格のDOUBLE・NULL・SQL桁あふれを含む完全なP3ではなく、形状のスパイクである。
先行の `spike-scalar` / `spike-simd` は単一キーを両probeで共有し、計測値もキーから
剰余で計算する簡略形だった。以下はその合成演算を除いた補正版の測定であり、
先行結果と混ぜない。
3関数をまとめたModuleのコンパイル中央値はscalar 0.503 ms、SIMD 0.480 ms。
これはSQLパイプライン生成時間を含まず、全パイプラインの2 ms達成を証明しない。
以下の実行時間はModuleのコンパイルとinstance生成を含まない。生成済みのmodule・
instanceを再利用しwarmup後に測っており、コンパイルキャッシュ命中時に相当する。
従ってコンパイル結果のキャッシュ導入だけで以下の実行時間は短縮しない。
比較対象は融合済みのネイティブカーネルであり、現行SQL Executorの間接呼出しや
中間データを融合で削減する効果は別途SQL全体で測る必要がある。

キャッシュは生成コードと計算結果を区別する。値を埋め込まない生成コードなら、
データ更新だけでは失効させず、IR・型・NULL可否・符号化/ABI版等に依存して再利用できる。
辞書や統計などデータ依存の仮定を埋め込む場合は、その世代の検証と失効が必要になる。
クエリ結果や集計済みの状態の再利用は別の最適化であり、本実験には含めない。

| カーネル | workers | native ms | WASM ms | WASM/native |
|---|---:|---:|---:|---:|
| filter + SUM | 1 | 2.546 | 3.828 | 1.50 |
| filter + SUM | 8 | 0.372 | 1.010 | 2.72 |
| 17-bit FOR + SUM | 1 | 4.760 | 6.844 | 1.44 |
| 17-bit FOR + SUM | 8 | 0.600 | 1.340 | 2.23 |
| bloom + 2 probe + groups | 1 | 50.871 | 71.413 | 1.40 |
| bloom + 2 probe + groups | 8 | 6.979 | 9.985 | 1.43 |
| AVX2 / SIMD128 filter + SUM | 8 | 0.199 | 0.770 | 3.88 |
| AVX2 / SIMD128 17-bit FOR + SUM | 8 | 0.380 | 1.219 | 3.21 |
| AVX2 / SIMD128 bloom + 2 probe + groups | 8 | 6.860 | 9.972 | 1.45 |

probeのWASM 2→8 workersは3.70倍。ネイティブ比1.3倍のゲートが不成立。
Pythonのdispatch/FFIを両者の計時に含めるため、サブmsの比率は純粋な機械語の
比較ではない。microbenchmarkの結果をSQL高速化率へ外挿しない。

## メモリについて確認したこと

Wasmtime所有の8 GiB仮想予約をshared memory64として8個のinstanceにimportし、
4 GiBより上の同じ入力をネイティブとWASMで読む。行ごとのホスト呼出しはなく、
クエリごとの入力コピーは0 bytes。全SUMと各workerの256集約値が一致。
範囲外読み取りのtrap後も正常な関数が実行できる。

既定の4 GiB予約では、`memory.size`が65540 pagesを返しても4 GiB超への読み出しが
trapした。明示的に予約を上限の8 GiBへ揃えると同じ最小再現が成功した。
Python bindingのshared-memory externハンドルに対する余分なpointer indirectionは
実験用subclassで補正し、インストール済みパッケージは変更していない。

**未証明:** M1のhost予約領域提供、PaxChunkの無コピー配置、永続mmapの取り込み、
アリーナ解放・断片化、同時クエリの所有権、grow、epoch/fuelによる取消。
実エンジンのSQL NULL・桁あふれ・例外意味論はこのスパイクの対象外。

公式API: [SharedMemory](https://docs.wasmtime.dev/c-api/sharedmemory_8h.html)、
[Config / memory creator](https://docs.wasmtime.dev/c-api/config_8h.html)。

## 次のゲート

- C/C++埋め込みAPIによるdispatch費用の分離、モーセル長ごとの損益測定。
- WAMR / WASM→LLVM候補と、手書きWAT 3形状の測定。
- 本物のLSTable/PaxChunk・64-bit offset ABIでの所有権、可視性、予算、取消の試験。
- macOS arm64 / Windows、ASan/TSan対応。
- S1の二重実行ハーネス、S2のPipeline IR、S3以降の生成器は未実装。

生データ（git管理外）:
`build/benchmarks/clickbench-tenth-20261113/spike-p3-scalar/results.json`、
`build/benchmarks/clickbench-tenth-20261113/spike-p3-simd/results.json`。
コマンド・生成物・SHA256・全サンプルを各ディレクトリに保存した。
