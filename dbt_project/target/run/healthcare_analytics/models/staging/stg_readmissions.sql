

  create or replace view `healthcare-pipeline-509120`.`hospital_data`.`stg_readmissions`
  OPTIONS()
  as select
    facility_id,
    measure_name,
    excess_readmission_ratio,
    predicted_readmission_rate,
    expected_readmission_rate,
    start_date,
    end_date
from `healthcare-pipeline-509120.hospital_data.hospital_readmissions`
where excess_readmission_ratio is not null;

