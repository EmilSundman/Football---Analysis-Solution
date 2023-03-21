select
        VenueId, 
        VenueName, 
        VenueCity
from {{ source('raw_data', 'raw_fixtures') }}