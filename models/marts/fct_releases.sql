-- One row per published release in the window.
-- lag() runs over ALL releases before the window filter, so the first in-window release
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
)

select *
from gaps
where published_at >= {{ window_start() }}
