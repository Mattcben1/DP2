-- One row per commit. author can be null when the commit email isn't linked to a GitHub account.
with latest as (
    select *
    from {{ source('raw', 'raw_commits') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
)

select
    repo,
    payload->>'sha'                                    as sha,
    payload->'author'->>'login'                        as author_login,
    {{ is_bot('author') }}                             as author_is_bot,
    (payload->'commit'->'author'->>'date')::timestamptz as committed_at
from latest
