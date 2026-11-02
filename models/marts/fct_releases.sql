-- One row per published release in the window.
-- Both lag()s run over ALL releases before the window filter, so the first in-window release
-- still measures its gap to the release before it.
with gaps as (
    select
        repo,
        release_id,
        tag_name,
        published_at,
        is_prerelease,
        date_diff(
            'day',
            lag(published_at) over (partition by repo order by published_at),
            published_at
        ) as days_since_previous
    from {{ ref('stg_releases') }}
),

-- Gap between stable releases only: pre-releases (RCs, betas) are skipped, so a stable release
-- measures back to the previous stable one. Release cadence metrics use this.
stable_gaps as (
    select
        repo,
        release_id,
        date_diff(
            'day',
            lag(published_at) over (partition by repo order by published_at),
            published_at
        ) as days_since_previous_stable
    from {{ ref('stg_releases') }}
    where not is_prerelease
)

select
    g.*,
    s.days_since_previous_stable  -- NULL for pre-releases
from gaps g
left join stable_gaps s
    on s.repo = g.repo and s.release_id = g.release_id
where g.published_at >= {{ window_start() }}
