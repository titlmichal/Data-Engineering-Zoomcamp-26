with
    trips as (select * from {{ ref("int_trips_unioned") }}),

    vendors as (
        select distinct
            vendor_id,
            {{ get_vendor_names("vendor_id") }} as vendor_name
        from trips
    )

select *
from vendors
