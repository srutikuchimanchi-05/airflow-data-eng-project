select
    facility_id,
    facility_name,
    state,
    ownership_type,
    overall_rating
from {{ ref('stg_hospitals') }}