USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS fhr_api_search_errors_monthly
(
year_month STRING,
api_search_error_count BIGINT,
average_search_response_time_in_ms DOUBLE
)
