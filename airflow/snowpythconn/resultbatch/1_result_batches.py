from pickle import TRUE
import sys
from tkinter import N
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector

cur = snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()

#Resultset

# Execute a query.
cur.execute('select seq4() as n from table(generator(rowcount => 1000000));')
# cur.execute('SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER;')

# Get the list of result batches
result_batch_list = cur.get_result_batches()

# Get the number of result batches in the list.
num_result_batches = len(result_batch_list)
print(result_batch_list)
print(num_result_batches)

type(result_batch_list)

print(result_batch_list[0])

# Arrow result chunk?

# Split the list of result batches into two
# to distribute the work of fetching results
# between two workers.
result_batch_list_1 = result_batch_list[:: 2]
result_batch_list_2 = result_batch_list[1 :: 2]

print(result_batch_list_2)

# Iterate over the list of result batches.
for batch in result_batch_list_1:
    # Iterate over the subset of rows in a result batch.
    for row in batch:
        print(row[0])


print(result_batch_list_1[0])

# Materialize the subset of results for the first result batch
# in the list.
first_result_batch = result_batch_list_1[5]

print(first_result_batch)

first_result_batch_data = list(first_result_batch)
print(first_result_batch_data)

# Get the number of rows in a result batch.
num_rows = first_result_batch.rowcount

print(num_rows)

# Get the size of the data in a result batch.
compressed_size = first_result_batch.compressed_size
uncompressed_size = first_result_batch.uncompressed_size

first_result_batch.uncompressed_size

print(result_batch_list_2[1]._chunk_headers)

print(result_batch_list_2[1].compressed_size)

print(result_batch_list_2[1]._remote_chunk_info)

print(result_batch_list_2[1])

print(compressed_size)
print(uncompressed_size)





#Pyarrow
# Execute a query.
cur.execute('select seq4() as n from table(generator(rowcount => 100000));')

# Return a PyArrow table containing all of the results.
table = cur.fetch_arrow_all()

type(table)
table.num_rows
table.column_names
table.chunks
table.select('N')
batches= table.to_batches()
print(batches)


batches = cur.fetch_arrow_batches()
type(batches)
repr(batches)

print(batches)

# Iterate over a list of PyArrow tables for result batches.
for table_for_batch in cur.fetch_arrow_batches():
    print(table_for_batch)





#Pandas
# Execute a query.
cur.execute('select seq4() as n from table(generator(rowcount => 100000));')

# Return a Pandas DataFrame containing all of the results.
table = cur.fetch_pandas_all()
type(table)

# Iterate over a list of Pandas DataFrames for result batches.
for dataframe_for_batch in cur.fetch_pandas_batches():
    print(dataframe_for_batch)





cur.execute("select col1 from demo_db.public.test_table")
batches = cur.get_result_batches()
print(batches)

# Get the row from the ResultBatch as a Pandas DataFrame.
dataframe = batches[0].to_pandas()
print(dataframe)

# Get the row from the ResultBatch as a PyArrow table.
table = batches[0].to_arrow()
print(table)
table.num_rows