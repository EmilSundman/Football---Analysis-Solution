# %%
import duckdb
import os
from dagster._utils import file_relative_path

# %%
duckdb_path = file_relative_path(
    __file__, os.path.join(os.path.expanduser(
        '~'), "localdb.db"))
con = duckdb.connect(duckdb_path)

# %%
con.sql('SELECT * FROM system.information_schema.tables').show()
# %%
con.sql('SELECT * FROM public.fixture_events_df').show()

# %%
con.close()

# %%
