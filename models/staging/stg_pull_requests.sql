-- One row per pull request (all PRs Task 1 landed; the window filter happens in fct_pull_requests).
with latest as (
    select *
    from {{ source('raw', 'raw_pull_requests') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    repo || '#' || (payload->>'number')         as pr_key,  -- PR numbers repeat across repos
    (payload->>'number')::integer               as number,
    payload->'user'->>'login'                   as author_login,
    {{ is_bot('user') }}                        as author_is_bot,
    payload->>'state'                           as state,
    (payload->>'created_at')::timestamptz       as created_at,
    (payload->>'updated_at')::timestamptz       as updated_at,
    (payload->>'closed_at')::timestamptz        as closed_at,
    (payload->>'merged_at')::timestamptz        as merged_at
from latest
