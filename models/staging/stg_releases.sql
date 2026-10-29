-- One row per published release. Drafts are unpublished, so they are dropped.
with latest as (
    select *
    from {{ source('raw', 'raw_releases') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    (payload->>'id')::bigint                as release_id,
    payload->>'tag_name'                    as tag_name,
    (payload->>'published_at')::timestamptz as published_at,
    (payload->>'prerelease')::boolean       as is_prerelease
from latest
where not (payload->>'draft')::boolean
