import argparse
import duckdb
import pandas

def clean_oi(file_path:str="./cme_gold_oi.xlsx", ticker:str="GC") -> pandas.DataFrame:
    df = pandas.read_excel(file_path, sheet_name="data", header=[0, 1, 2, 3], dtype="str")
    df.columns = df.columns.get_level_values(2)
    df.columns = [it.lower().replace(" ", "_") for it in df.columns.to_list()]
    df = df.rename(columns={
        "deliveries": "deliveries_value",
        "at_close": "at_close_value",
        "change": "change_value"
    })

    volume_cols:list[str] = ["globex", "open_outcry", "pnt_clearport", "total_volume", "block_trades", "efp", "efr", "tas"]
    deliveries_cols:list[str] = ["deliveries_value"]
    open_interest_cols:list[str] = ["at_close_value", "change_value"]

    long_df = pandas.DataFrame()

    #Volume
    volume_df = pandas.melt(
        df,
        id_vars=["month"],
        value_vars=volume_cols,
        var_name="subcategory",
        value_name="value"
    )
    long_df = pandas.concat([long_df, volume_df], ignore_index=True)

    #Delivery
    deliveries_df = pandas.melt(
        df,
        id_vars=["month"],
        value_vars=deliveries_cols,
        var_name="subcategory",
        value_name="value"
    )
    deliveries_df["category"] = "deliveries"
    long_df = pandas.concat([long_df, deliveries_df], ignore_index=True)

    #Open Interest
    open_interest_df = pandas.melt(
        df,
        id_vars=["month"],
        value_vars=open_interest_cols,
        var_name="subcategory",
        value_name="value"
    )
    open_interest_df["category"] = "open_interest"
    long_df = pandas.concat([long_df, open_interest_df], ignore_index=True)

    long_df["value"] = pandas.to_numeric(long_df["value"], errors="coerce").astype("Int64")
    long_df["ticker"] = ticker

    return long_df[["ticker", "month", "category", "subcategory", "value"]]


def export_parquet(df:pandas.DataFrame, trade_date:str) -> None:
    conn = duckdb.connect()
    conn.register("raw", df)

    conn.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW clean_src AS

        SELECT
            UPPER(TRIM(ticker)) AS ticker,
            UPPER(TRIM(month)) AS month,
            LOWER(TRIM(category)) AS category,
            LOWER(TRIM(subcategory)) AS subcategory,
            CAST (value AS LONG) AS value,
            DATE '{trade_date}' AS trade_date,
            TODAY() AS last_update_date,
            '{trade_date}' AS ds
        FROM raw
        """
    )

    conn.sql(
        """
        COPY clean_src TO 'cme_oi_parquet'
        (FORMAT parquet, PARTITION_BY (ds), OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'file_{i}');
        """
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-path", type=str)
    parser.add_argument("--ticker", type=str)
    parser.add_argument("--trade-date", type=str)
    args = parser.parse_args()

    df = clean_oi(args.file_path, args.ticker)
    export_parquet(df, args.trade_date)

