import polars as pl
from deltalake import DeltaTable


def get_uc_uri(object_name: str) -> str:
    UC_URI_SCHEME="uc"
    return f"{UC_URI_SCHEME}://{object_name}"


def polars_read_uc(table_uc_name) -> pl.DataFrame:
    dt = DeltaTable(f"{table_uc_name}")
    return pl.from_arrow(dt.to_pyarrow_table())