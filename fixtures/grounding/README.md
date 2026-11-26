# Grounding HTML fixture provenance

`sec_8k_ex99_1_excerpt.html` is a byte-preserving excerpt from the real SEC
Exhibit 99.1 HTML filing for Benchmark 2026-V25 Mortgage Trust, CIK `2155221`,
accession `0001539497-26-003041`, filed 2026-11-18. The source document is:

https://www.sec.gov/Archives/edgar/data/0002155221/000153949726003041/exh99_1-crefimlpa.htm

The excerpt retains source tags, comments, entity references, a numeric loan
amount, and the source script element so tests can prove that non-visible HTML
content is not searchable. Metadata was located through the CMD+RVL EDGAR data
service query over `DBT_STAGING_EDGAR.STG_EDGAR_8K_EXHIBIT_INDEX`.
