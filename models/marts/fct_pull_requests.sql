-- One row per PR created in the window, with every precalculated flag and duration.
-- PRs still open at run time are kept: their merge/close fields stay NULL, never a fake close date.
with prs as (
    select *
    from {{ ref('stg_pull_requests') }}
    where created_at >= {{ window_start() }}
),

-- Only reviews that count as real feedback: not by the PR's own author, not by a bot.
counted_reviews as (
    select
        r.pr_key,
        min(r.submitted_at)                as first_review_at,
        bool_or(r.state = 'APPROVED')      as has_approving_review
    from {{ ref('stg_pr_reviews') }} r
    join prs p on p.pr_key = r.pr_key
    where not r.reviewer_is_bot
      and r.reviewer_login is distinct from p.author_login
    group by r.pr_key
),

durations as (
    select
        p.*,
        p.merged_at is not null                                          as is_merged,
        p.state = 'closed' and p.merged_at is null                       as is_closed_unmerged,
        p.state = 'open'                                                 as is_open,
        cr.first_review_at,
        date_diff('second', p.created_at, cr.first_review_at) / 3600.0   as hours_to_first_review,
        date_diff('second', p.created_at, p.merged_at) / 3600.0          as hours_to_merge,
        date_diff('second', p.created_at, p.closed_at) / 3600.0          as hours_to_close,
        coalesce(cr.has_approving_review, false)                         as has_approving_review
    from prs p
    left join counted_reviews cr on cr.pr_key = p.pr_key
)

select
    repo,
    pr_key,
    number,
    author_login,
    author_is_bot,
    state,
    created_at,
    updated_at,
    closed_at,
    merged_at,
    is_merged,
    is_closed_unmerged,
    is_open,
    first_review_at,
    hours_to_first_review,
    coalesce(hours_to_first_review <= 168, false)                         as reviewed_within_7d,
    hours_to_merge,
    hours_to_close,
    is_open and updated_at < {{ run_time() }} - interval 180 day          as is_stale,
    has_approving_review
from durations
