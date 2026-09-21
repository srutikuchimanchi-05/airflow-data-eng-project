select
    facility_id,
    facility_name,
    state,
    ownership_type,
    cast(overall_rating as int64) as overall_rating
from `healthcare-pipeline-509120.hospital_data.hospital_ratings`
where overall_rating is not null