# %%
import duckdb
import os
from dagster._utils import file_relative_path

# %%
duckdb_path = file_relative_path(
    __file__, os.path.join(file_relative_path(__file__, "../dbt_project"), "localdb.db"))
con = duckdb.connect(duckdb_path)

# %%
con.sql('SELECT * FROM system.information_schema.tables').show()
# %%
con.sql('SELECT * FROM dbt_schema.dim_leagues').show()

# %%
con.close()

# %%
