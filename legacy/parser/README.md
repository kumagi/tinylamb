# Legacy SQL parser (アーカイブ)

Tokenizer, recursive-descent parser, and Pratt expression parser used by
historical unit tests only. The `tinylamb` SQL executable uses the pinned
GoogleSQL `execute_query` binary instead.

Sources: `legacy/parser/*.cpp`  
Headers (shared with tests): `parser/*.hpp`  
Statement AST: `query/statement.hpp` (`parser/ast.hpp` は互換シム。executor 側の
参照がなくなっており、残存参照 (`parser/parser.hpp`, `parser/pratt_parser.hpp`,
`parser/ast_extra_test.cpp`, `query/googlesql_ast_fuzzer.hpp`) の置き替え後に削除予定)

## 運用方針 (A2-3 決着: 保持 + アーカイブ明示)

- **現行 (canonical) の SQL frontend は `query/`** (GoogleSQL frontend +
  `sql_engine.cpp`)。レガシーパーサーは SQL 実行経路から完全に外れている
  (「not parsed again」)。実行系がこのコードに依存することはない
- **本ディレクトリは互換目的のアーカイブ**。歴史的単体テスト
  (`parser/*_test.cpp`, `parser/ast_extra_test.cpp`,
  `legacy/parser/parser_fuzzer.cpp`) だけがビルド対象として参照する
- **新機能は追加しない**。バグ修正も、実行経路に影響がない限り原則行わない。
  挙動を変える変更は削除か置き替えで対応する
- **tidy / CI のレイヤー検査対象外**。clang-tidy の新規警告の解消、
  `scripts/check_layering.py` による include 検査 (legacy/ は除外済み)、
  依存整理タスクのいずれにも本ディレクトリを含めない。ビルド時間と
  警告ノイズの節約のため、この状態を維持する
