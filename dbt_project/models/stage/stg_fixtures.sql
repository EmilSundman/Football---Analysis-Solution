select
        *       
from {{ source('raw_data', 'raw_fixtures') }}