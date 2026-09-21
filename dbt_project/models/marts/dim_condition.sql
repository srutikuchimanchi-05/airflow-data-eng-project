select distinct
    measure_name
from {{ ref('stg_readmissions') }}