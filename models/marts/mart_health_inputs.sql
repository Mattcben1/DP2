-- One row per repo, one column per candidate Task 5 metric.
-- Every column is a rate, share, median, CV, or slope (never a raw count), so big repos
-- don't win on volume alone.
with window_len as (
    select date_diff('second', {{ window_start() }}, {{ run_time() }}) / 86400.0 as days
),

pr_metrics as (
    select
        repo,
        median(hours_to_first_review)                                   as median_hours_to_first_review,
        -- only PRs at least 7 days old have had a full 7 days to get a review
        avg(reviewed_within_7d::int) filter (
            where created_at <= {{ run_time() }} - interval 7 day
        )                                                               as pct_reviewed_within_7d,
        count(*) filter (where is_merged)
            / nullif(count(*) filter (where is_merged or is_closed_unmerged), 0) as merge_rate,
        median(hours_to_merge)                                          as median_hours_to_merge,
        avg(has_approving_review::int) filter (where is_merged)         as pct_merged_with_approval,
        avg(is_stale::int) filter (where is_open)                       as pct_open_stale
    from {{ ref('fct_pull_requests') }}
    group by repo
),

-- Complete months only: the partial run month would drag the slope and averages down.
monthly_metrics as (
    select
        repo,
        sum(commits)                                    as commits_in_window,
        regr_slope(commits, month_index) filter (where not is_partial_month)
            / nullif(avg(commits) filter (where not is_partial_month), 0) as commit_trend_pct,
        avg(active_contributors) filter (where not is_partial_month)      as active_authors_per_month
    from {{ ref('mart_repo_monthly') }}
    group by repo
),

release_metrics as (
    select
        repo,
        count(*)                                                    as releases_in_window,
        stddev_samp(days_since_previous) / nullif(avg(days_since_previous), 0) as release_interval_cv
    from {{ ref('fct_releases') }}
    group by repo
)

select
    m.repo,
    m.commits_in_window / (w.days / 7)                          as commits_per_week,
    m.commits_in_window / (w.days / 7)
        / nullif(m.active_authors_per_month, 0)                 as commits_per_active_author_per_week,
    m.commit_trend_pct,
    p.median_hours_to_first_review,
    p.pct_reviewed_within_7d,
    p.merge_rate,
    p.median_hours_to_merge,
    p.pct_merged_with_approval,
    p.pct_open_stale,
    coalesce(r.releases_in_window, 0) / (w.days / 365.25)       as releases_per_year,
    r.release_interval_cv,
    m.active_authors_per_month
from monthly_metrics m
cross join window_len w
left join pr_metrics p      on p.repo = m.repo
left join release_metrics r on r.repo = m.repo
