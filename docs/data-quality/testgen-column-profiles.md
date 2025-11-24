# TestGen Column Profile UX Breakdown

The upstream TestGen UI renders different layouts depending on the detected data type of the profiled column. This reference captures the required sections and metrics so we can reproduce (or intentionally evolve) the experience inside ConversionCentral.

## Shared Sections

Both numeric and text/character columns surface the following groupings:

- **Column Characteristics** – includes
  - Data Type (physical type collected from the warehouse)
  - Semantic Data Type (TestGen classification, e.g., "ID-Unique" or "Person Given Name")
  - First Detected timestamp
  - Suggested Data Type (when available)
- **Column Tags** – catalog metadata fields (description, stakeholder group, transform level, aggregation level, critical data element flag, data source/system/process, business domain, data product)
- **Potential PII** – flag + detail rows indicating whether PII heuristics were triggered
- **Hygiene Issues** – highlights profiling alerts (null spikes, duplicate ratios, etc.)
- **Test Issues** – summarizes failing validation tests tied to the column
- **Related Test Suites** – list of suites containing rules scoped to the column

## Numeric Columns

When the column is numeric (integer, decimal, float), the Value Distribution and statistics pane mirrors the following fields:

### Value Distribution

| Metric | Notes |
| --- | --- |
| Record Count | Total rows scanned for the column |
| Value Count | Number of non-null observations |
| Numeric Distribution | Broken bar highlighting Non-Zero / Zero / Null counts |
| Distinct Values | Cardinality |
| Average Value | Mean |
| Standard Deviation | Population std dev |
| Minimum Value | Full-range minimum |
| Minimum Value > 0 | First positive minimum |
| Maximum Value | Max observed |
| 25th Percentile | Lower quartile |
| Median Value | 50th percentile |
| 75th Percentile | Upper quartile |

The UI also renders:

- **Box-and-whisker chart** plotting min/25th/median/75th/max and highlighting the average ± std dev band.
- **Pill badges** for semantic classifications (e.g., "Not a critical data element").

### Additional Expectations

- Column tags module doubles as a quick view of catalog attributes (data definition, stakeholder group, data product, etc.).
- Potential PII / Hygiene / Test Issues collapse when empty but should still show the count (e.g., "No potential PII detected").

## Text / Character Columns

Textual columns include the same shared sections plus a much richer profiling card focused on patterns:

### Value Distribution

| Metric | Notes |
| --- | --- |
| Record Count | Total rows scanned |
| Value Count | Number of non-null values |
| Missing Values | Count + percentage; stacked bar visualization |
| Duplicate Values | Count + percentage; stacked bar |
| Case Distribution | Mixed, lower, upper, non-alpha breakdown |

### Pattern & Shape Metrics

- **Frequent Patterns** – histogram of the most common alpha/numeric shape patterns (e.g., `AaAaa`).
- **Includes Digits** – boolean indicator (Yes/No) plus counts.
- **Quoted Values** – count of values surrounded by quotes.
- **Numeric Values** – count of values consisting solely of digits.
- **Leading Spaces** – count of values prefixed with whitespace.
- **Zero Values** – count of literal "0" entries.
- **Embedded Spaces** – count of values containing internal spaces.
- **Date Values** – detected date-formatted entries.
- **Average Embedded Spaces** – average number of interior spaces per value.
- **Minimum Length** – shortest string length.
- **Maximum Length** – longest string length.
- **Average Length** – mean string length.
- **Minimum Text** – lexicographically smallest string (example shown: "Abigail").
- **Maximum Text** – lexicographically largest string (example shown: "Zachary").
- **Standard Pattern Match** – count of values matching the chosen canonical regex (with total matches).
- **Distinct Patterns** – number of unique shape patterns observed.

### Visualizations

- **Stacked bars** for Missing/Duplicate/Case distributions.
- **Inline spark bars** for frequent patterns list.

## Implementation Notes

- The backend must expose the raw metrics above so the React components can pick the correct layout based on column type. Our new `/data-quality/datasets/profiling-stats` endpoint surfaces table-level rollups; column-level endpoints should mirror these structures.
- Numeric vs. text detection should be driven by the profiling payload (`data_type`, `semantic_data_type`, `dq_profile_results` flags) to avoid front-end heuristics.
- When a metric is unavailable (e.g., `minimum_value_gt_zero` for text columns), hide the row instead of rendering `null`.

Use this checklist when designing schemas, serializers, and UI components to ensure the reconstructed column profile view aligns with the screenshots provided above.
