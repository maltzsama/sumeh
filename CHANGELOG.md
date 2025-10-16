# CHANGELOG

<!-- version list -->

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
