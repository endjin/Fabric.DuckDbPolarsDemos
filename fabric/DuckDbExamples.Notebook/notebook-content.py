# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3881b34b-dd06-4203-a06c-0bedb6858b09",
# META       "default_lakehouse_name": "PricePaidData",
# META       "default_lakehouse_workspace_id": "f324daa0-83dc-421d-a61e-637be44b4ff5"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # DuckDB - Land Registry Price Paid Data Demo
# 
# In this notebook we aim to provide an end to end demo of DuckDB.
# 
# To provide some data to work with, we are importing open data from the UK Governement's Land Registry department.
# 
# The Land Registry has published all house sales since the year 1995 in a data set called Price Paid Data.
# 
# The documentation for the data can be found here: [How to access HM Land Registry Price Paid Data](https://www.gov.uk/guidance/about-the-price-paid-data)
# 
# There is on average ~1 million houses sold per year.
# 
# At time of writing, the full dataset (~30 years) is ~30 million rows and about 5 Gigabytes in CSV form.
# 
# The data is available in both TXT and CSV format.  You can download all of the data in one file.  But for the purposes of this demo to allow us to control the volume of data we work with, we are going to load individual files for each year in CSV format.
# See separate `Copy Files From Land Registry` pipeline which is used to ingest the raw CSV files into the Lakehouse.


# CELL ********************

import duckdb
import polars as pl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# A single year of data
price_paid_data_single_year = "/lakehouse/default/Files/land_registry/pp-2023.csv"
# All ~20 years of data using globbing
price_paid_data_complete = "/lakehouse/default/Files/land_registry/pp-*.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Here we can see how Fabric automatically configures the secret required to connect to onelake.  This **should** enable DuckDB to interact directly with files in the Lakehouse - both reading and writing.
# 
# Unfortunately at time of writing this notebook the (DuckDB delta extension)[https://duckdb.org/docs/stable/extensions/delta] only supports read, not writing Delta format.  But it does natively support writing to Parquet format.
# 
# During this notebook we try:
# 
# - ✅Reading single CSV file from Files area of Lakehouse.
# - ✅Reading multiple CSV files using globbing from Files area of Lakehouse.
# - ❌Writing table to Tables area of Lakehouse in Parquet format.
# - ✅Reading Delta data from Tables area of the Lakehouse.

# CELL ********************

duckdb.sql("SELECT * FROM duckdb_secrets();")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Load single year of data
# 
# Just to get going, let's look at daa for a single year, in this case 2023.

# CELL ********************

duckdb.sql(f"FROM read_csv('{price_paid_data_single_year}')")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

duckdb.sql(f"DESCRIBE SELECT * FROM read_csv('{price_paid_data_single_year}')")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Add column names and features

# CELL ********************

duckdb.sql(
    f"""
    SELECT
    column00 AS 'id',
    column01 AS 'price',
    column02 AS 'date',
    column03 AS 'postcode',
    column04 AS 'property_type',
    column05 AS 'old_new',
    column06 AS 'duration',
    column07 AS 'paon',
    column08 AS 'saon',
    column09 AS 'street',
    column10 AS 'locale',
    column11 AS 'town_city',
    column12 AS 'district',
    column13 AS 'county',
    column14 AS 'ppd_category',
    column15 AS 'record_type',
    year(date) AS 'year_of_sale',
    month(date) AS 'month_of_sale'
    FROM read_csv(
        '{price_paid_data_single_year}',
        header=False
    );
    """
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Work with a DuckDB database
# 
# We now want to show how you can work with DuckDB database.
# 
# There are two options:
# - Work with memory
# - Persist the database to file system (in this case we are using the local file system for the Notebook which is presented as in the `Resources` part of the Explorer pane)
# 
# Comment out the option below.

# CELL ********************

# Connect to an in-memory DuckDB database
# db = duckdb.connect(':memory:')
# Alternatively we can persist the DuckDB database in the built-in area which is available
db = duckdb.connect('./builtin/my_local_database.duckdb')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Load data into database table
# 
# We now use the new `db` object to create a new table and load it with data.

# CELL ********************

db.sql("DROP TABLE IF EXISTS price_paid;")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

db.sql(
    f"""
    CREATE TABLE 'price_paid' AS
    SELECT
    column00 AS 'id',
    column01 AS 'price',
    column02 AS 'date',
    column03 AS 'postcode',
    column04 AS 'property_type',
    column05 AS 'old_new',
    column06 AS 'duration',
    column07 AS 'paon',
    column08 AS 'saon',
    column09 AS 'street',
    column10 AS 'locale',
    column11 AS 'town_city',
    column12 AS 'district',
    column13 AS 'county',
    column14 AS 'ppd_category',
    column15 AS 'record_type',
    year(date) AS 'year_of_sale',
    month(date) As 'month_of_sale'
    FROM read_csv(
        '{price_paid_data_single_year}',
        header=False
    );
    """
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

db.sql("SELECT COUNT (*) FROM 'price_paid'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Query table in database
# 
# Having loaded the data into the database, we can now query it.
# 
# TODO: show how we can build on BASE TABLE to create views over the data to filter, summarise, aggregate and transform it before writing back to Lakehouse.

# CELL ********************

db.sql(
    f"""
    SELECT month_of_sale, COUNT(*) AS 'count_of_sales'
    FROM price_paid
    GROUP BY ALL
    ORDER BY month_of_sale ASC;
    """
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Load full dataset from file and summarise it
# 
# Let's return to working with the core DuckDB engine and querying all of the raw CSV files.

# CELL ********************

duckdb.sql(
    f"""
    SELECT year(column02) AS 'year_of_sale', COUNT(*)
    FROM read_csv(
        '{price_paid_data_complete}',
        header=False
    )
    GROUP BY ALL
    ORDER BY year_of_sale;
    """
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Setting up to write to OneLake
# 
# Given there is no support currently to write directly to Delta from DuckDB, we wanted to try writing to the "Tables" area of the lake in Parquet.  Then allow OneLake to discover this data and register it as a table.
# 
# We run 4 experiments:
# 
# 1. Proving the concept -  use the DuckDB `COPY` statement to load the data, transform it and then write the result to local file system.  Result: ✅ **Success**
# 2. **Preferred Option** - use the DuckDB `COPY` statement to load the data, transform it and then write the result  directly to Onelake in Parquet format.  Result: ❌ **Not implemented Error**.
# 3. Workaround - load **single year of data** into DuckDB table, convert to Polars dataframe and then write this Onelake Tables area in Delta format.  Result: ✅ **Success**
# 4. Workaround - load **full dataset** into DuckDB table, convert to Polars dataframe and then write this Onelake Tables area in Delta format.  Result (despite reported memory efficient copy between DuckDB and Polars due to both supporting Apache Arrow): ❌ **Out of memory exception**. 
# 
# See below for more details.


# MARKDOWN ********************

# ## Writing to Delta via Polars Dataframe
# 


# MARKDOWN ********************

# ### ✅Experiment 1 - load full dataset into DuckDB table, use COPY to write to local file system

# CELL ********************

%mkdir ./builtin/price_paid_parquet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

duckdb.sql(
    f"""
    COPY
    (
        SELECT
        column00 AS 'id',
        column01 AS 'price',
        column02 AS 'date',
        column03 AS 'postcode',
        column04 AS 'property_type',
        column05 AS 'old_new',
        column06 AS 'duration',
        column07 AS 'paon',
        column08 AS 'saon',
        column09 AS 'street',
        column10 AS 'locale',
        column11 AS 'town_city',
        column12 AS 'district',
        column13 AS 'county',
        column14 AS 'ppd_category',
        column15 AS 'record_type',
        year(date) AS 'year_of_sale'
        FROM read_csv(
            '{price_paid_data_single_year}',
            header=False
        )
        WHERE property_type <> 'O'
    )
    TO './builtin/price_paid_parquet/data.parquet' (FORMAT parquet);
    """
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

%ls -l ./builtin/price_paid_parquet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### ❌Experiment 2 - load full dataset into DuckDB table, use COPY to write directly to Lakehouse in Parquet format
# 
# Give the operation above has failed for the full dataset, we next tried to write in parquet format using the DuckDB `COPY` command.
# 
# Unfortunately, the following cells fails with the following error:
# 
# ```
# NotImplementedException: Not implemented Error: Writing to Azure containers is currently not supported
# ```


# CELL ********************

try:
    duckdb.sql(
        f"""
        COPY
        (
            SELECT
            column00 AS 'id',
            column01 AS 'price',
            column02 AS 'date',
            column03 AS 'postcode',
            column04 AS 'property_type',
            column05 AS 'old_new',
            column06 AS 'duration',
            column07 AS 'paon',
            column08 AS 'saon',
            column09 AS 'street',
            column10 AS 'locale',
            column11 AS 'town_city',
            column12 AS 'district',
            column13 AS 'county',
            column14 AS 'ppd_category',
            column15 AS 'record_type',
            year(date) AS 'year_of_sale'
            FROM read_csv(
                '{price_paid_data_single_year}',
                header=False
            )
            WHERE property_type <> 'O'
        )
        TO 'abfss://DuckDbPolarsDemos@onelake.dfs.fabric.microsoft.com/PricePaidData.Lakehouse/Tables/land_registry/house_sales_duckdb_parquet' (FORMAT parquet);
        """
    )
except Exception as e:
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### ✅Experiment 3 - load single year (smaller dataset) in DuckDB table, convert to Polars dataframe, write dataframe to Lakehouse in Delta format
# 
# Let's try first writing a single year of data (about ~700K rows) to Delta.

# CELL ********************

table_path_polars_delta = "abfss://DuckDbPolarsDemos@onelake.dfs.fabric.microsoft.com/PricePaidData.Lakehouse/Tables/land_registry/house_sales_polars_delta"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

storage_options = {"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

duckdb.sql(
    f"""
    SELECT
    column00 AS 'id',
    column01 AS 'price',
    column02 AS 'date',
    column03 AS 'postcode',
    column04 AS 'property_type',
    column05 AS 'old_new',
    column06 AS 'duration',
    column07 AS 'paon',
    column08 AS 'saon',
    column09 AS 'street',
    column10 AS 'locale',
    column11 AS 'town_city',
    column12 AS 'district',
    column13 AS 'county',
    column14 AS 'ppd_category',
    column15 AS 'record_type',
    year(date) AS 'year_of_sale'
    FROM read_csv(
        '{price_paid_data_single_year}',
        header=False
    )
    WHERE property_type <> 'O'
    ;
    """
).pl().write_delta(
    table_path_polars_delta,
    mode='overwrite',
    storage_options=storage_options)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### ❌Experiment 4 - load full dataset (25 years) in DuckDB table, convert to Polars dataframe, write dataframe to Lakehouse in Delta format
# 
# When we try to write the full dataset, DuckDB can load the data, but the operation below fails during the conversion to Polars and writing to the lake with the following error.
# 
# We've commented out the code because it kills the kernel.
# 
# ```
# Kernel died: Kernel python3.11 has died, please restart the kernel.
# 
# Diagnostic Info:
# session id: eac9fb6c-83d2-480a-ae54-e89f12a7d6bc
# pid: 39
# exit code: -9 (Forced-process termination. This is often caused by insufficient memory causing the process to be killed. Please check memory usage)
# cluster name: cfc72989-b358-418b-a2e1-6c6188e0000d
# Maximum memory in 2 minutes: 14.94GB
# Maximum cpu usage in 2 minutes: 100.0%
# ```

# CELL ********************

# try:
#     complete_dataset = duckdb.sql(
#         f"""
#         SELECT
#         column00 AS 'id',
#         column01 AS 'price',
#         column02 AS 'date',
#         column03 AS 'postcode',
#         column04 AS 'property_type',
#         column05 AS 'old_new',
#         column06 AS 'duration',
#         column07 AS 'paon',
#         column08 AS 'saon',
#         column09 AS 'street',
#         column10 AS 'locale',
#         column11 AS 'town_city',
#         column12 AS 'district',
#         column13 AS 'county',
#         column14 AS 'ppd_category',
#         column15 AS 'record_type',
#         year(date) AS 'year_of_sale'
#         FROM read_csv(
#             '{price_paid_data_complete}',
#             header=False
#         )
#         WHERE property_type <> 'O'
#         ;
#         """
#     ).pl().write_delta(
#         table_path,
#         mode='overwrite',
#         storage_options=storage_options)
# except Exception as e:
#     print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Reading from a Table in the lakehouse
# 
# We can then use DuckDB to read the data that was written by Experiment 3 above.

# CELL ********************

duckdb.sql(
    f"""
    SELECT year_of_sale, COUNT(*) FROM delta_scan('{table_path_polars_delta}') GROUP BY ALL
    """
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
