# Description
Used to store local long sql style storage for CME OI daily report.<br>
CME report link: [here](https://www.cmegroup.com/markets/metals/precious/gold.volume.html)<br>

# Usage
1. Copy Volume & OI data into sample excel file and save
2. Run python script `gen_cme_oi.py` beside the excel file via cmd like `python ./gen_cme_oi.py --file-path "./cme_gold_20260218.xlsx" --ticker "GC" --trade-date "2026-02-18"`
3. parquet file saved with hive style under folder `cme_oi_parquet` under same path
