-- One row per submitted review. PENDING reviews are drafts with no submitted_at, so they are dropped.
with latest as (
    select *
    from {{ source('raw', 'raw_pr_reviews') }}
    qualify row_number() over (partition by repo, id order by fetched_at desc) = 1
),

parsed as (
    select
        repo,
        (payload->>'id')::bigint                                            as review_id,
        -- pull_request_url ends in the PR number: .../pulls/28040
        regexp_extract(payload->>'pull_request_url', '(\d+)$', 1)::integer  as pr_number,
        payload->'user'->>'login'                                           as reviewer_login,
        {{ is_bot('user') }}                                                as reviewer_is_bot,
        payload->>'state'                                                   as state,
        (payload->>'submitted_at')::timestamptz                             as submitted_at
    from latest
)

select
    repo,
    review_id,
    repo || '#' || pr_number as pr_key,
    pr_number,
    reviewer_login,
    reviewer_is_bot,
    state,
    submitted_at
from parsed
where state <> 'PENDING'
