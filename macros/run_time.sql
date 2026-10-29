{#
  "Run time" = the newest fetched_at across all raw tables (README, Task 3).
  Using the data's own timestamp instead of now() means a rebuild without new data
  produces identical results.
#}
{% macro run_time() %}
(
    select max(fetched_at)::timestamptz
    from (
        select fetched_at from {{ source('raw', 'raw_commits') }}
        union all select fetched_at from {{ source('raw', 'raw_pull_requests') }}
        union all select fetched_at from {{ source('raw', 'raw_pr_reviews') }}
        union all select fetched_at from {{ source('raw', 'raw_releases') }}
    )
)
{% endmacro %}

{% macro window_start() %}
'{{ var("window_start") }}'::timestamptz
{% endmacro %}
