with versions as (
    select
        'customer' as entity_type,
        to_varchar(customer_id) as entity_id,
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to
    from {{ ref('snap_customer') }}

    union all

    select
        'product' as entity_type,
        to_varchar(cd_product) as entity_id,
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to
    from {{ ref('snap_product') }}
),

checked as (
    select
        *,
        count(*) over (
            partition by entity_type, entity_id, valid_from
        ) as same_start_count,

        count_if(valid_to is null) over (
            partition by entity_type, entity_id
        ) as current_version_count,

        max(valid_to) over (
            partition by entity_type, entity_id
            order by valid_from
            rows between unbounded preceding and 1 preceding
        ) as previous_max_end,

        sum(
            case when valid_to is null then 1 else 0 end
        ) over (
            partition by entity_type, entity_id
            order by valid_from
            rows between unbounded preceding and 1 preceding
        ) as previous_open_versions

    from versions
)

select *
from checked
where entity_id is null
    or valid_from is null
    or same_start_count > 1
    or current_version_count > 1
    or valid_to <= valid_from
    or previous_max_end > valid_from
    or previous_open_versions > 0