-- Repo x month from the window start through the run month. The spine guarantees a row
-- (with 0s) for months with no activity instead of the month disappearing.
with repos as (
    select distinct repo from {{ ref('stg_pull_requests') }}
),

months as (
    select
        month::timestamptz as month,
        row_number() over (order by month) - 1 as month_index
    from generate_series(
        date_trunc('month', {{ window_start() }}),
        date_trunc('month', {{ run_time() }}),
        interval 1 month
    ) as t(month)
),

spine as (
    select r.repo, m.month, m.month_index
    from repos r
    cross join months m
),

-- Each PR event is counted in the month it happened, so these are three separate groupings.
opened as (
    select
        repo,
        date_trunc('month', created_at)  as month,
        count(*)                         as prs_opened,
        median(hours_to_first_review)    as median_first_review_hours,
        count(distinct author_login) filter (where not author_is_bot) as active_contributors
    from {{ ref('fct_pull_requests') }}
    group by 1, 2
),

merged as (
    select
        repo,
        date_trunc('month', merged_at)   as month,
        count(*)                         as prs_merged,
        median(hours_to_merge)           as median_merge_hours
    from {{ ref('fct_pull_requests') }}
    where is_merged
    group by 1, 2
),

closed_unmerged as (
    select
        repo,
        date_trunc('month', closed_at)   as month,
        count(*)                         as prs_closed_unmerged
    from {{ ref('fct_pull_requests') }}
    where is_closed_unmerged
    group by 1, 2
),

commits as (
    select
        repo,
        date_trunc('month', committed_at) as month,
        count(*)                          as commits
    from {{ ref('stg_commits') }}
    where committed_at >= {{ window_start() }}
    group by 1, 2
),

joined as (
    select
        s.repo,
        s.month,
        s.month_index,
        coalesce(o.prs_opened, 0)            as prs_opened,
        coalesce(m.prs_merged, 0)            as prs_merged,
        coalesce(c.prs_closed_unmerged, 0)   as prs_closed_unmerged,
        o.median_first_review_hours,
        m.median_merge_hours,
        coalesce(k.commits, 0)               as commits,
        coalesce(o.active_contributors, 0)   as active_contributors
    from spine s
    left join opened o          on o.repo = s.repo and o.month = s.month
    left join merged m          on m.repo = s.repo and m.month = s.month
    left join closed_unmerged c on c.repo = s.repo and c.month = s.month
    left join commits k         on k.repo = s.repo and k.month = s.month
)

select
    repo || '|' || strftime(month, '%Y-%m')                         as repo_month_key,
    repo,
    month,
    month_index,
    prs_opened,
    prs_merged,
    prs_closed_unmerged,
    prs_merged / nullif(prs_merged + prs_closed_unmerged, 0)        as merge_ratio,
    median_first_review_hours,
    median_merge_hours,
    commits,
    active_contributors,
    month = date_trunc('month', {{ run_time() }})                   as is_partial_month
from joined
