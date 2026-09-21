select
    r.facility_id,
    r.measure_name,
    r.excess_readmission_ratio,
    r.predicted_readmission_rate,
    r.expected_readmission_rate,
    h.state,
    h.ownership_type,
    h.overall_rating
from {{ ref('stg_readmissions') }} r
inner join {{ ref('stg_hospitals') }} h
    on r.facility_id = h.facility_id