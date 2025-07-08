# 🔄 MongoDB-Based Invoice Matching Engine

This script performs **batch-wise invoice matching** between two MongoDB collections:

- `parsed_data`: Your internally processed or parsed invoices
- `two_b`: Official reference data (e.g., GSTR-2B)

It applies **exact**, **fuzzy**, and **GSTIN-only** logic to determine the best match and then updates the documents in both collections with match results and diagnostics (date diff, amount diff, etc.).

---

##  Features

-  Cleans and normalizes invoice numbers and GSTINs
-  Exact and fuzzy invoice number matching (using `difflib`)
-  Fallback matching using GSTIN when invoice number fails
-  Calculates date difference (in days)
-  Calculates invoice amount difference
-  Updates both `parsed_data` and `two_b` collections with results
- Diagnostic print logs for each match

