from typing import Optional, List, Tuple

import polars as pl
from deltalake import DeltaTable


def get_uc_uri(object_name: str) -> str:
    UC_URI_SCHEME="uc"
    return f"{UC_URI_SCHEME}://{object_name}"


def polars_read_uc(
    table_uc_name: str,
    partitions: Optional[List[Tuple[str, str, str]]] = None,
    columns: Optional[List[str]] = None
) -> pl.DataFrame:
    """
    See https://delta-io.github.io/delta-rs/python/usage.html on how to pass partitions and columns
    """
    dt = DeltaTable(get_uc_uri(table_uc_name))
    pyarrow_table = dt.to_pyarrow_table(
        partitions=partitions,
        columns=columns
    )
    return pl.from_arrow(pyarrow_table)