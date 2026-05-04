# Reflection
**Student Name:** Nguyen Minh Hieu
**Student ID:** 2A202600180

Based on Slide §5, the anti-pattern my data architecture would be most at risk of is **"The Small-File Problem"**. 

**Reasoning:**
My system regularly ingests application logs via streaming or continuous micro-batches throughout the day. If data is written directly to the Data Lake upon arrival without periodic maintenance, it will generate thousands of tiny `.parquet` files (often just a few KBs each). Consequently, when querying, engines like Spark or DuckDB will spend the majority of execution time just opening files and reading metadata (I/O overhead) rather than scanning the actual data, degrading query performance by orders of magnitude.

**Mitigation:**
To resolve this, I need to establish a maintenance job that periodically runs the `OPTIMIZE` and `Z-ORDER` commands (as practiced in this Lab). This will compact the small files into larger, more optimal ones (e.g., ~256MB) while sorting the data by frequently queried columns, fully leveraging Delta Lake's file-skipping capabilities.
