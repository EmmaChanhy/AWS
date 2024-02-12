# Import
import awswrangler as wr
import pandas as pd

# Parameters
s3_raw_output_path = "s3://dev-raw-container-port/ports.csv"
s3_processed_output_path = "s3://dev-processed-container-port/ports.parquet"

# Read csv
df = pd.read_csv(s3_raw_output_path)
df = df.drop(df.columns[0], axis=1)
df = df.melt(
    id_vars=["Port","Country/ Region","Region","Location"], 
    var_name="Year", 
    value_name="thousand TEUs").iloc[10:].reset_index(drop=True)

# Export csv to S3 processed
wr.s3.to_parquet(
    df = df,
    path= s3_processed_output_path)
