{#
  Bot rule, defined once and used for commit authors, PR authors, and reviewers:
    is_bot = user.type = 'Bot' OR user.login LIKE '%[bot]'
  coalesced to false, so a missing user (unlinked commit email) is not a bot.
  user_key is the JSON key holding the user object: 'author' for commits, 'user' for PRs and reviews.
#}
{% macro is_bot(user_key) %}
coalesce(
    json_extract_string(payload, '$.{{ user_key }}.type') = 'Bot'
    or json_extract_string(payload, '$.{{ user_key }}.login') like '%[bot]',
    false
)
{% endmacro %}
