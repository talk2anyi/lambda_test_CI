import pyarrow as pa
from pandas import Timestamp as pd_Timestamp
from pyarrow.parquet import read_table, write_table

PARQUET_INDICES_KEY = "__index_level_0__"


def writer(list_of_dicts, flavor="spark"):
    """
    Returns a byte stream that can written to disk or s3
    """

    cols = {}
    for index, cur_dict in enumerate(list_of_dicts):
        for key, value in cur_dict.items():
            if key not in cols:
                cols[key] = [None] * len(list_of_dicts)
            cols[key][index] = value

    labels = []
    vectors = []

    for col, vector in cols.items():
        labels.append(col)
        arr = pa.array(vector)
        # Convert dates to ns precision to ensure it's written as INT96 in pq files
        if isinstance(arr, pa.TimestampArray):
            arr = arr.cast(pa.timestamp("ns"))
        vectors.append(arr)

    pq_table = pa.Table.from_arrays(vectors, labels)

    out_stream = pa.BufferOutputStream()
    write_table(pq_table, out_stream, flavor=flavor)

    return out_stream.getvalue()


def reader(in_stream, drop_indices=True):
    """
    Reads a stream and returns a list of dictionaries
    """

    key_filters = []
    if drop_indices:
        key_filters.append(PARQUET_INDICES_KEY)

    table = read_table(pa.BufferReader(in_stream.read()))

    cols_dict = table.to_pydict()
    num_rows = table.num_rows
    keys = cols_dict.keys()
    list_of_dicts = []

    keys = [key for key in keys if key not in key_filters]

    for i in range(num_rows):
        cur_dict = {}
        for key in keys:
            value = cols_dict[key][i]
            if isinstance(value, pd_Timestamp):
                value = value.to_pydatetime()
            cur_dict[key] = value
        list_of_dicts.append(cur_dict)

    return list_of_dicts
