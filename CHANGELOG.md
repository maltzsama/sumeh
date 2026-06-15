# CHANGELOG

<!-- version list -->

## v3.0.0 (2026-06-15)

### Bug Fixes

- Pyspark replace coalesce with OR for multi-field completeness
  ([`5695591`](https://github.com/maltzsama/sumeh/commit/5695591b6a857c8e8bcc2eeb839fc0e3ee2ed64a))

### Chores

- Expand mypy ignore list and fix polars type annotation
  ([`1e16ded`](https://github.com/maltzsama/sumeh/commit/1e16ded5578b1442a554046b7b71a7f84183e068))

### Code Style

- Format code with ruff and fix blank lines
  ([`ad495f1`](https://github.com/maltzsama/sumeh/commit/ad495f18d3de3a2cc11c3bfdad1033e076274cac))

- Reorganize imports and fix module order
  ([`e29fc37`](https://github.com/maltzsama/sumeh/commit/e29fc376a703514b703677cb6ddf116579bd55b0))

### Continuous Integration

- Add CI workflow with Codecov integration
  ([`e6beaaa`](https://github.com/maltzsama/sumeh/commit/e6beaaa58bf13ecb39566338d76c2b156e2c4ec6))

- Add test gate before PyPI publish
  ([`beca5eb`](https://github.com/maltzsama/sumeh/commit/beca5ebcd33a3687fa11ab1a35229eaf222a2431))

- Add workflow_call to ci.yml and rename yaml → yml
  ([`4479834`](https://github.com/maltzsama/sumeh/commit/44798343a37af02bc81c7dd42115c5fdac0a1e2c))

- Remove pyflink from CI to fix build error
  ([`a19553a`](https://github.com/maltzsama/sumeh/commit/a19553ae8f7ae8b44a2936ae81c894fc574b0d8e))

- Replace uv with pip to fix CI cache errors
  ([`747a0aa`](https://github.com/maltzsama/sumeh/commit/747a0aa7091f39fbecc3e48ea6252ff289bc3130))

### Features

- Add PyFlink stream validator protocol
  ([`580e8da`](https://github.com/maltzsama/sumeh/commit/580e8da6aa0776ff37b2c855a84d4641b4dff091))

- Expand SQL engine coverage and fix manifest engine support
  ([`71b2ded`](https://github.com/maltzsama/sumeh/commit/71b2dedd31bf95ca26684f5bcb58192e076d8aa9))

### Refactoring

- Rename ray_data module to ray for consistency
  ([`7aecf16`](https://github.com/maltzsama/sumeh/commit/7aecf163c66d14f2ef19d1ba892532bf558dee2d))

### Testing

- Add comprehensive sql_core unit tests
  ([`7090acb`](https://github.com/maltzsama/sumeh/commit/7090acbbcde07dd77029d1bb2ec512f338da1b32))

### Breaking Changes

- Ray engine now imported as 'from sumeh import ray' instead of 'ray_data'


## v2.1.0 (2026-06-14)

### Documentation

- Expand README with comprehensive rule documentation and Why Sumeh section
  ([`cb91074`](https://github.com/maltzsama/sumeh/commit/cb9107464ef4f0719f27ec66d320fe4858e0a280))

### Features

- Add PEP 561 typing marker
  ([`3ea5333`](https://github.com/maltzsama/sumeh/commit/3ea5333ce8fd0b6b694d9ade1cf8890c54b5fa58))

- Improve type safety across DDL, rules, and SQL engines
  ([`981cfb8`](https://github.com/maltzsama/sumeh/commit/981cfb8923283ad0ce83edbb5af2378a238a25ff))

- Type safety improvements and SQL execution tracking
  ([`a85a7e0`](https://github.com/maltzsama/sumeh/commit/a85a7e008cf54dfe9d6ad20207bd98ebbbab7bbf))


## v2.0.1 (2026-03-10)

### Bug Fixes

- **polars**: Update deprecated Polars API and add missing unit tests
  ([`45adf0b`](https://github.com/maltzsama/sumeh/commit/45adf0b6c9bf2da6176138c6ff29b945f24efd61))

- **schema**: Correct type mapping logic and add comprehensive test suite
  ([`051df56`](https://github.com/maltzsama/sumeh/commit/051df563b34778806dd132d14f7d0a3f51b47c9e))

### Code Style

- Add missing blank lines and fix line break formatting
  ([`54c2a12`](https://github.com/maltzsama/sumeh/commit/54c2a124dd325334ed7ecf564399d1aaffb1894a))

### Continuous Integration

- Migrate build system from Poetry to Hatch
  ([`f4fff7b`](https://github.com/maltzsama/sumeh/commit/f4fff7bac5eed5895f55b4f1b4de5e62ab437cc3))

### Documentation

- Add Mermaid architecture diagram and API docstrings
  ([`4c54b20`](https://github.com/maltzsama/sumeh/commit/4c54b2094da10b3daaff6f23cfa39b17daf90a4a))

- Complete v2.0 documentation overhaul
  ([`1583ee6`](https://github.com/maltzsama/sumeh/commit/1583ee6163b9dbbd5feec42d28d29c15f989cf19))

### Testing

- Add comparator logic unit tests
  ([`5572ef4`](https://github.com/maltzsama/sumeh/commit/5572ef4a7495d25985c3b32a0ac3048948e9249a))

- Add core rule registry and rule model tests
  ([`b1d149d`](https://github.com/maltzsama/sumeh/commit/b1d149d6b7e29b6c1acee476bd3d6195db581181))

- Add global fixtures and CSV config tests for all rule categories
  ([`7161882`](https://github.com/maltzsama/sumeh/commit/7161882abc7caa3f057f3ab66b992f354e912495))

- Add OpenMetadata exporter unit tests
  ([`de5e791`](https://github.com/maltzsama/sumeh/commit/de5e791b53fda6ec263e156c652e44e481b2a548))

- Add pandas engine unit tests
  ([`e5477d9`](https://github.com/maltzsama/sumeh/commit/e5477d94280a690c4ff3ccbe955933017ff8f438))

- Add validation models unit tests
  ([`ff2840a`](https://github.com/maltzsama/sumeh/commit/ff2840a4af55e410b48df3eee6075a83713e45e0))

- **cli**: Add full test coverage for CLI commands (ddl, sql, validate)
  ([`e77d2e2`](https://github.com/maltzsama/sumeh/commit/e77d2e247505753d59d2e794d188e66c135e814d))

- **config**: Add validation tests for rules schema and rule dicts
  ([`7c1402e`](https://github.com/maltzsama/sumeh/commit/7c1402ea92d8d4b7de7d44641a33026e93287161))

- **duckdb**: Add full test coverage for SQL core and DuckDB engine
  ([`7f8c341`](https://github.com/maltzsama/sumeh/commit/7f8c34178c41d8b89a98188230bc4de181aebb6f))

- **generators**: Add comprehensive test suite for SQL DDL generation
  ([`ce5c302`](https://github.com/maltzsama/sumeh/commit/ce5c3023184ed9617696ddc07085f17f9b1d339d))

- **generators**: Add comprehensive tests for SQL transpiler
  ([`dfad1cc`](https://github.com/maltzsama/sumeh/commit/dfad1cc7f9f6c50cd948a866269e067814b105eb))

- **profiler**: Add comprehensive unit tests for DataProfiler
  ([`99b95b9`](https://github.com/maltzsama/sumeh/commit/99b95b9955ca9a3a13de000d9de439fbc8f6deb4))


## v2.0.0 (2026-03-09)

### Bug Fixes

- **pandas**: Resolve indexing mismatch when filling empty error lists
  ([`ea89997`](https://github.com/maltzsama/sumeh/commit/ea89997b96722db748caa8b963d82739b52a313f))

- **sql**: Ensure proper table identifier formatting and import structure
  ([`cb2de80`](https://github.com/maltzsama/sumeh/commit/cb2de8024a90b2c5cb8c45c78f3864eb0e138ba5))

### Build System

- **deps**: Update Python version to 3.10 and bump dependencies
  ([`9f9d32c`](https://github.com/maltzsama/sumeh/commit/9f9d32c0ba19160770b6e8a5e32487f94fc6193c))

### Chores

- Clean up pyproject.toml dependencies and extras
  ([`571aa3b`](https://github.com/maltzsama/sumeh/commit/571aa3b8ca7ca1f8c1f203fd98f270bb74a38242))

- Remove legacy monolithic engine
  ([`5c96bcc`](https://github.com/maltzsama/sumeh/commit/5c96bcca8f92b33b69099768fe85aefe6aac9204))

- Remove legacy monolithic engine files
  ([`a21b217`](https://github.com/maltzsama/sumeh/commit/a21b2172292a6f07c799fb80ea744e3e36f9cc39))

- **cleanup**: Remove empty baseline and sinks modules
  ([`53fffbf`](https://github.com/maltzsama/sumeh/commit/53fffbf61b86ae2859fb2e33e77d8969427e58c2))

- **tests**: Remove outdated v1.3.0 test suite
  ([`d7b2f21`](https://github.com/maltzsama/sumeh/commit/d7b2f21eca2576283f58a1cb7a6d8f9bc8e3bebb))

### Code Style

- Apply consistent code formatting across all modules
  ([`7db9778`](https://github.com/maltzsama/sumeh/commit/7db9778b8d3404d11bb4a46498432e9a7a1fe8e9))

- Fix formatting and imports across sink modules
  ([`aac1e9f`](https://github.com/maltzsama/sumeh/commit/aac1e9f76fbc21543e93fa4df4cc6c7d271fc412))

- Fix import ordering and remove unused imports across codebase
  ([`01d6c59`](https://github.com/maltzsama/sumeh/commit/01d6c59f5f6f9dd04f0a99c1f8dd1ed5825c1f24))

- Format code with consistent imports and line breaks
  ([`cbff8e0`](https://github.com/maltzsama/sumeh/commit/cbff8e0dfa3946e2a58b6091154262e51cb4c461))

- Normalize quotes and formatting across codebase
  ([`bbd15a3`](https://github.com/maltzsama/sumeh/commit/bbd15a3bc67d46d64558d4725cc81c0633954236))

### Documentation

- Update module docstrings and implement lazy engine loading
  ([`85aafaa`](https://github.com/maltzsama/sumeh/commit/85aafaa12a25428f9e501fb6537556ce6e793312))

- **init**: Shorten top-level engine exports comment
  ([`30d5173`](https://github.com/maltzsama/sumeh/commit/30d5173f8d223ead60273e5a2b458f3dac289452))

### Features

- Add auto-profiler, schema validation, and engine capabilities
  ([`6a47750`](https://github.com/maltzsama/sumeh/commit/6a477509457cab5ccdcf54d329e6c4ea05a7d211))

- Add AWS Athena SQL-based validation engine
  ([`7a2b018`](https://github.com/maltzsama/sumeh/commit/7a2b018ad1d78fd526d54e1e049085a7be6a6b48))

- Add CSV rule loader and SQL generators
  ([`32bee27`](https://github.com/maltzsama/sumeh/commit/32bee274f9c49802025cb03649694712c9d52e0d))

- Add DataFrame accessors to ValidationReport
  ([`5c426c5`](https://github.com/maltzsama/sumeh/commit/5c426c5010e2e9f152c77cffe3925eddf610f075))

- Add distributed Dask engine with lazy evaluation
  ([`976f2f3`](https://github.com/maltzsama/sumeh/commit/976f2f3a1671bace1eab7addb47c0b561054c84f))

- Add get_validation_sql helper to BigQuery and DuckDB engines
  ([`f304dc3`](https://github.com/maltzsama/sumeh/commit/f304dc320eea4fbaf5df2079b6da370d168fda0f))

- Add OpenMetadata sink with SinkProtocol
  ([`fe4a940`](https://github.com/maltzsama/sumeh/commit/fe4a9401750d5289c3c2beca177963da68d5f19a))

- Add PyFlink streaming validation engine
  ([`e62d2d5`](https://github.com/maltzsama/sumeh/commit/e62d2d55d85ed3bda8945ecb69d71ad86c736f2f))

- Add PySpark engine with pure Column API (zero UDFs)
  ([`f488d47`](https://github.com/maltzsama/sumeh/commit/f488d475b2526154df63d39941ca0bba45599309))

- Add rule validation and metadata preservation
  ([`0722918`](https://github.com/maltzsama/sumeh/commit/072291883a24ab1d73150c5e924984a78f2ec9ae))

- Add Snowflake engine with pure SQL validation
  ([`5c5d416`](https://github.com/maltzsama/sumeh/commit/5c5d4163c292d39f69afb4a20a699866434fa019))

- Add SQL generator for Flink validation queries
  ([`15df523`](https://github.com/maltzsama/sumeh/commit/15df523a331356547589ad65406824522aed9024))

- Add SQLCore-based BigQuery engine
  ([`00b6b23`](https://github.com/maltzsama/sumeh/commit/00b6b23499dd3b82db3f4fece34f58835b2a9e3f))

- Add Trino, Redshift, and Doris SQL engines
  ([`13a3d3e`](https://github.com/maltzsama/sumeh/commit/13a3d3e73c0ef8305da686adde9d1834888c6eb3))

- Complete analyzers with all date, comparison, and aggregation rules
  ([`02616fe`](https://github.com/maltzsama/sumeh/commit/02616fefb36a26d19e3d8a1deaf1f5efeaf687cf))

- Complete README rewrite and project restructure for v2.0 release
  ([`6ff4d39`](https://github.com/maltzsama/sumeh/commit/6ff4d39ac3e31c538ce69de9744012e851bca9ea))

- Expand PyFlink validation engine to full feature parity
  ([`c5840d4`](https://github.com/maltzsama/sumeh/commit/c5840d4348423d9d0d076c9d24ece04661486097))

- Implement DuckDB engine with SQLCore validation
  ([`8aef6a7`](https://github.com/maltzsama/sumeh/commit/8aef6a7c01551d1dd5328744498a0b54bdf288f9))

- **duckdb**: Add data bifurcation and standardize code formatting
  ([`b64bbc1`](https://github.com/maltzsama/sumeh/commit/b64bbc1bfd8aeca4846cc090d8854c6e34d3339a))

- **exporters**: Add IExporter protocol for pure metadata formatting
  ([`4866921`](https://github.com/maltzsama/sumeh/commit/4866921cd6d643889c8d821167093160e8a8a241))

- **init**: Replace dynamic engine loading with explicit imports for better IDE support
  ([`cfc3859`](https://github.com/maltzsama/sumeh/commit/cfc385938f8599c6005abf39f199e2c691b4f3f0))

- **polars**: Add Polars engine with optimized bulk error aggregation
  ([`6b337ef`](https://github.com/maltzsama/sumeh/commit/6b337ef748abcc1393a549a0aa8233ddde5352cd))

- **sumeh**: V2.0 rewrite with analyzer/constraint architecture
  ([`cc05b17`](https://github.com/maltzsama/sumeh/commit/cc05b17dc1b0ee3fdf4d3cddec103afcf5e10d06))

### Performance Improvements

- Optimize pandas and polars engines with bulk error aggregation
  ([`bba5d81`](https://github.com/maltzsama/sumeh/commit/bba5d818edba342170a0c0176b37ee7ed8cab333))

### Refactoring

- Remove OOM-prone ID collection from analyzers
  ([`d5b6401`](https://github.com/maltzsama/sumeh/commit/d5b640143ea7a68abf874ca5ad686f7463bac952))

- Simplify Ray Data engine implementation
  ([`4e753d9`](https://github.com/maltzsama/sumeh/commit/4e753d914af0afd167fefc6988b3ebb9fe9c9b30))

- Split CLI into modular command files
  ([`29253e3`](https://github.com/maltzsama/sumeh/commit/29253e32a447dc1ddbf6da31d7a1aa8fd7a5c40b))

- **core**: Consolidate protocols and remove dead code
  ([`ef6b43e`](https://github.com/maltzsama/sumeh/commit/ef6b43e5cd3b1384b1a1d2b23cce8b6dcdd45262))

- **core**: Rename RuleDef to RuleDefinition for consistency
  ([`c54432c`](https://github.com/maltzsama/sumeh/commit/c54432c194383fa4759e6779483058ddb5c29f1d))

- **core**: Reorganize core module structure for better maintainability
  ([`79a3298`](https://github.com/maltzsama/sumeh/commit/79a32983a025fd09c3315e03dfe1b5670a11f747))

- **engines**: Simplify engine package exports
  ([`deb9c45`](https://github.com/maltzsama/sumeh/commit/deb9c45dc33f7b96851521d2052d7564c5449700))

- **models**: Remove duplicate SinkResult class
  ([`a8579bb`](https://github.com/maltzsama/sumeh/commit/a8579bb358cc49c1874a2f3cc94d3e679de47158))

- **polars**: Use consistent timestamp and simplify error aggregation
  ([`c57fd5e`](https://github.com/maltzsama/sumeh/commit/c57fd5e98ac11b9ba4c2c0e421db44e7ca92cbae))

- **profiler**: Clean up DataProfiler with better docs and safety checks
  ([`3ba2565`](https://github.com/maltzsama/sumeh/commit/3ba25657471378ae72765bab6627ec747ba7b900))


## v1.3.0 (2025-10-16)

### Bug Fixes

- Dask dependancie error
  ([`f8c1b62`](https://github.com/maltzsama/sumeh/commit/f8c1b62a09c2e503c8d342a27092fc82ea15e59b))

### Code Style

- Improve readability with standardized line breaks and spacing
  ([`a220071`](https://github.com/maltzsama/sumeh/commit/a220071979be0b9444da74e598519de3b0caae86))

- Standardize and clean up import ordering
  ([`99cc1ae`](https://github.com/maltzsama/sumeh/commit/99cc1ae5b36c4013e7762855455f60beb71e77b0))

### Continuous Integration

- Simplify Poetry install by using `--all-extras` in publish workflow
  ([`a8d59b3`](https://github.com/maltzsama/sumeh/commit/a8d59b32559d51e6d279d174278d677de383a7cf))

### Features

- Add full BigQuery table-level validation support and unify rule model across engines
  ([`af58f5c`](https://github.com/maltzsama/sumeh/commit/af58f5cc9c877b14e1544537455f8fc27801126f))

- Enhance DuckDB detection and refactor Pandas date checks
  ([`39a64ca`](https://github.com/maltzsama/sumeh/commit/39a64ca9547ffcdca772f2add84a4dfbacb9d81f))

- Enhance rule parsing and standardize rule usage in engines
  ([`945ae8b`](https://github.com/maltzsama/sumeh/commit/945ae8bc98fe25fb0c0406acca9450d228c36ec6))

- Refactor validation engine to use RuleDef model and fix ambiguity issues
  ([`808ff22`](https://github.com/maltzsama/sumeh/commit/808ff222254484a1387a901966f25761f1a9edb0))

- Standardize aggregation checks and implement multi-level validation
  ([`bc56830`](https://github.com/maltzsama/sumeh/commit/bc5683029991f84bf25fb3f788a40fffcd37d181))

- Unify table-level validation engine interface across all backends
  ([`e1234fa`](https://github.com/maltzsama/sumeh/commit/e1234fa615227af4e722866d84e243edabcace16))

- **core**: Implement Dispatcher pattern for core modules
  ([`13a4349`](https://github.com/maltzsama/sumeh/commit/13a43495cb5c5902bf53724b9c58e23345d7c4c5))

- **duckdb**: Enhance validation dispatchers, add robust error handling & input checks**
  ([`82c4274`](https://github.com/maltzsama/sumeh/commit/82c427413d76c97db722fde9673790c1f9c37e61))

### Refactoring

- Clean up and organize imports across core modules and engines
  ([`7953b20`](https://github.com/maltzsama/sumeh/commit/7953b20899bfb9b37b68325b56ddd4304fa6a681))

- Introduce RuleDef model and registry for configuration
  ([`90522e5`](https://github.com/maltzsama/sumeh/commit/90522e52aa8dc146f4b1eef805bb2c60b6f05720))

- Remove obsolete extract_params test and align test suite with current codebase_
  ([`5bfc07b`](https://github.com/maltzsama/sumeh/commit/5bfc07b7c45098db01c7eaf0df4037176705e051))

- Standardize code formatting and improve error handling in BigQuery engine
  ([`20c994a`](https://github.com/maltzsama/sumeh/commit/20c994ad953d09b6877e543e75adeb41afd3944c))

- Unify and modernize configuration dispatchers with clear, consistent API
  ([`8915815`](https://github.com/maltzsama/sumeh/commit/8915815e87407cc369368cbfeff3b7ae0653fde2))

- Unify date validation aliases across all engines for consistency
  ([`7becdd5`](https://github.com/maltzsama/sumeh/commit/7becdd5285f803f8fbf5dc59721670b48f6f5e3b))

- **cli**: Migrate CLI implementation from argparse to Typer
  ([`e161748`](https://github.com/maltzsama/sumeh/commit/e161748a4493dfe52216c14313c5bde731412f3e))

- **pyspark**: Standardize validation functions and remove legacy logic
  ([`fc03b78`](https://github.com/maltzsama/sumeh/commit/fc03b78e2713b9432c33a31659c283a6f520dcde))


## v1.2.0 (2025-10-09)

### Chores

- **deps**: Update AWS, caching, and core dependencies
  ([`e6821e0`](https://github.com/maltzsama/sumeh/commit/e6821e03c02aa435a901a09239c0bc9a1e2250d2))

### Documentation

- Show private members in MkDocs API documentation
  ([`3f447e5`](https://github.com/maltzsama/sumeh/commit/3f447e54308c001a628daa494822949a21e29da0))

### Features

- **bigquery**: Implement native Data Quality validation and summarization
  ([`eeaf615`](https://github.com/maltzsama/sumeh/commit/eeaf615fb5040d093a70e58cec8730dab271c8eb))

- **bigquery**: Rewrite validation to use 100% SQLGlot and improve docs
  ([`5ad0d7f`](https://github.com/maltzsama/sumeh/commit/5ad0d7f760ae07a86c482144c58fa2438b068971))


## v1.1.0 (2025-10-08)

### Features

- **schema**: Decouple schema extraction and improve validation output
  ([`852c36b`](https://github.com/maltzsama/sumeh/commit/852c36b2205385dbe0a6946341ba7428b6254ae2))

### Refactoring

- **core, duckdb**: Minor cleanup and improved schema error formatting
  ([`cfbb695`](https://github.com/maltzsama/sumeh/commit/cfbb695f08f969c25682eec86f4de6b7e4f671d7))


## v1.0.1 (2025-10-08)

### Bug Fixes

- Correctly parse field lists and handle complex string inputs
  ([`66a5a39`](https://github.com/maltzsama/sumeh/commit/66a5a393705c149c9b8c7cbb1fd91545295813f7))


## v1.0.0 (2025-10-08)

### Bug Fixes

- Sync version numbers with latest release tag
  ([`b65b420`](https://github.com/maltzsama/sumeh/commit/b65b42082290d3d1f462292c2f1b6ac99b868c05))


## v1.0.0-rc.1 (2025-10-07)

### Bug Fixes

- **engines**: Correct inverse logic for comparison validation functions
  ([`64dd3da`](https://github.com/maltzsama/sumeh/commit/64dd3da6a99b2cdb73384bc23a84f50b41a89f54))

### Build System

- Update pyproject.toml with complete metadata
  ([`03f4fd2`](https://github.com/maltzsama/sumeh/commit/03f4fd23ad610aff264447ee75b3b351ec778059))

### Continuous Integration

- Adopt Trusted Publishers for PyPI deployment and refactor release flow
  ([`e507717`](https://github.com/maltzsama/sumeh/commit/e5077175e51797b12dafe6d08e4f0fb0c7778830))

- Fix on ci/cd deployment
  ([`420454f`](https://github.com/maltzsama/sumeh/commit/420454f7ed6854a90268cece69aeda10d5ae7958))

- **config**: Add python-semantic-release configuration
  ([`95b3113`](https://github.com/maltzsama/sumeh/commit/95b311390e95f73db7749ecca253e9d522ebcfb2))

- **workflow**: Configure conditional PyPI publishing for releases
  ([`72f3bb6`](https://github.com/maltzsama/sumeh/commit/72f3bb6db37d9132f5b5906f873684b569ea3f01))

### Documentation

- Improve configuration examples and workflow clarity
  ([`bf09a5f`](https://github.com/maltzsama/sumeh/commit/bf09a5f2e27401b9fa2049867448de557ae48a61))

- Update documentation structure following module refactoring
  ([`f938382`](https://github.com/maltzsama/sumeh/commit/f9383828c1aa9aa0877e8d0503c4bed95030629f))

### Features

- Add Schema Validation feature and various data source support
  ([`4415c92`](https://github.com/maltzsama/sumeh/commit/4415c920224e7dfe7c91013b059b1400ad0129c8))

- Centralized schema definition using Schema Registry
  ([`53ee185`](https://github.com/maltzsama/sumeh/commit/53ee185602f121f46f8399582b9716c83dfe71ed))

- Implement interactive Streamlit dashboard for validation results
  ([`7c9804a`](https://github.com/maltzsama/sumeh/commit/7c9804adaf243b7c04a48989592506adba233c5a))

- Introduce Databricks rule source and refine configuration methods
  ([`03ef55c`](https://github.com/maltzsama/sumeh/commit/03ef55c0d6ebb1a81a5bebc32ca2d76b3f92329c))

- **ci**: Major package refactoring, automate PyPI publishing, and enhance SQL connections
  ([`69bd9c7`](https://github.com/maltzsama/sumeh/commit/69bd9c7d65a0b01ca17f7776f4bf30559da54913))

- **cli**: Add SQL DDL generation for 8 database dialects
  ([`82ca12c`](https://github.com/maltzsama/sumeh/commit/82ca12cdaca563b527199eecbbdfc2f081b6611b))

- **dashboard**: Rework Streamlit dashboard with advanced visuals and filters
  ([`64dd3da`](https://github.com/maltzsama/sumeh/commit/64dd3da6a99b2cdb73384bc23a84f50b41a89f54))

### Refactoring

- General code cleanup and API simplification
  ([`99368aa`](https://github.com/maltzsama/sumeh/commit/99368aa8b001cebe9f6ab47326ac821a94d51943))

- Make schema lookup flexible and enhance security checks
  ([`ee2b41d`](https://github.com/maltzsama/sumeh/commit/ee2b41de96b1ab28b5cd51a8ab2562654cc5956f))

- **core, cli**: Introduce core utility modules and prepare for 'validate' command
  ([`7911465`](https://github.com/maltzsama/sumeh/commit/791146565bb0c6d78ea25479e653e4f97493b944))

- **core, config**: Standardize config/schema API and enforce required parameters
  ([`75614bc`](https://github.com/maltzsama/sumeh/commit/75614bc74c1a2697d3045eaea02aa0c9b036ac0b))


## v0.3.0 (2025-05-16)

### Bug Fixes

- **dask_engine**: Invert validation logic to flag non-compliant records
  ([`2a76fe7`](https://github.com/maltzsama/sumeh/commit/2a76fe7152270647fcdbdf9e2cbc4ebb09fcd810))

### Code Style

- Apply code formatting and cleanup across core and engine files
  ([`dd0a0ad`](https://github.com/maltzsama/sumeh/commit/dd0a0ad16761d76a5c6abddc45ef1d2bcf131353))

- Clean up whitespace and formatting in test files
  ([`aa47a97`](https://github.com/maltzsama/sumeh/commit/aa47a973a101fd3befaa66419e94d86bbf9d798a))

### Documentation

- Complete Pandas engine docstrings and enhance core module documentation
  ([`65d5110`](https://github.com/maltzsama/sumeh/commit/65d511065f37b22ddd6d61a50422152d57935af4))

- Enhance documentation and reorganize validation rules
  ([`0873ca3`](https://github.com/maltzsama/sumeh/commit/0873ca3412dd0d0a2bb0d13c11edbddcceee5fc8))

- **polars_engine**: Add comprehensive docstrings for data quality functions
  ([`dcc93c5`](https://github.com/maltzsama/sumeh/commit/dcc93c532ba1e418fb8b534646138792b0bb3bc5))

### Features

- Add 'is_in' and 'not_in' rule aliases to engines
  ([`b522cb0`](https://github.com/maltzsama/sumeh/commit/b522cb08fb6259fb1af45a086818e651c80a3dcc))

- Add comprehensive date and numeric validation functions to pandas engine
  ([`c23d513`](https://github.com/maltzsama/sumeh/commit/c23d5133c80992c300a610ebb77083957bb805a4))

- Add date and numeric validation functions to Polars engine
  ([`2675e84`](https://github.com/maltzsama/sumeh/commit/2675e840a86c5dafc3b78b133c7471cbd312c001))

- Improve date and numeric validation rules in DuckDB engine
  ([`6de088c`](https://github.com/maltzsama/sumeh/commit/6de088c728cf5eba92cf66f664e5002628fccb0c))

- **dask**: Implement numeric threshold and detailed date/weekday validation rules
  ([`522d332`](https://github.com/maltzsama/sumeh/commit/522d33210f40e769c6fd59a1985f8e8e0e0dfd21))

- **duckdb**: Implement numeric threshold and detailed date/weekday validation rules
  ([`8112e96`](https://github.com/maltzsama/sumeh/commit/8112e96e37c603566cd470e05c1b1888178eaced))

- **pandas**: Add new engine for Pandas DataFrames with comprehensive rule support
  ([`d31234b`](https://github.com/maltzsama/sumeh/commit/d31234b6df94f7240fd25bb4aed000d9ff5ff47a))

- **pyspark**: Implement numeric threshold and detailed date validation rules
  ([`073c0ad`](https://github.com/maltzsama/sumeh/commit/073c0ad73e65494a7cf189d9759b94afca6ff24e))


## v0.2.6 (2025-05-16)

### Documentation

- Add docstrings for date validation rules in Dask and DuckDB engines
  ([`42cb80a`](https://github.com/maltzsama/sumeh/commit/42cb80a2ce0e183b5bbb9db5225c562f2d954f23))

### Features

- **dask**: Implement date validation rules and add dedicated tests
  ([`0a719d4`](https://github.com/maltzsama/sumeh/commit/0a719d440c9ffb63f1de42344690f1f2a74b9d2a))

- **duckdb**: Implement date and additional validation rules
  ([`d45afc8`](https://github.com/maltzsama/sumeh/commit/d45afc874303f982d8e819365685265ea7935382))

- **polars**: Implement multiple validation rules and enhance documentation
  ([`fc83ae6`](https://github.com/maltzsama/sumeh/commit/fc83ae6c927f8a7fb2573d6f1c39aa9042b7f1b9))


## v0.2.5 (2025-05-16)

### Documentation

- Update README with logo path and completed tasks
  ([`85fcc94`](https://github.com/maltzsama/sumeh/commit/85fcc940e918f190df99da0bd085a937455714e7))


## v0.2.4 (2025-05-16)

### Chores

- Version
  ([`92fa3c5`](https://github.com/maltzsama/sumeh/commit/92fa3c5ee63d70233cec60fee72e7458c96952e4))

### Features

- Add quickstart guide and list supported validation rules
  ([`d307c85`](https://github.com/maltzsama/sumeh/commit/d307c85be7310dabc69b1ca2e723df36885f6810))


## v0.2.0 (2025-04-29)

- Initial Release
