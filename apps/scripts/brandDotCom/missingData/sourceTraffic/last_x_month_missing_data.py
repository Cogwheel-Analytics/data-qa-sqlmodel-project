from collections import defaultdict
from sqlmodel import text
from apps.utils.database import get_session
from apps.utils.csv_export import export_hotel_months_to_csv

# SQL Query to find missing source_traffic data for the last 6 months
query = text(
    """
   WITH
    months AS (
        SELECT
            DATE_TRUNC ('month', CURRENT_DATE) - INTERVAL '1 month' * i AS month_start
        FROM
            generate_series (1, 6) AS i
    ),
    active_hotels AS (
        SELECT
            id AS hotel_id,
            code AS hotel_code
        FROM
            public.hotel
        WHERE
            is_active = true
    ),
    hotel_month_combinations AS (
        SELECT
            h.hotel_id,
            h.hotel_code,
            m.month_start
        FROM
            active_hotels h
            CROSS JOIN months m
    ),
    source_traffic_aggregated AS (
        SELECT
            hotel_id,
            DATE_TRUNC ('month', date) AS month_start,
            COALESCE(SUM(visits), 0) AS total_visits,
            COALESCE(SUM(revenue), 0) AS total_revenue
        FROM
            public.source_traffic
        WHERE
            date >= DATE_TRUNC ('month', CURRENT_DATE) - INTERVAL '6 month'
            AND date < DATE_TRUNC ('month', CURRENT_DATE)
        GROUP BY
            hotel_id,
            DATE_TRUNC ('month', date)
    ),
    missing_data AS (
        SELECT
            hmc.hotel_code,
            TO_CHAR (hmc.month_start, 'YYYY-MM') AS month
        FROM
            hotel_month_combinations hmc
            LEFT JOIN source_traffic_aggregated sta ON hmc.hotel_id = sta.hotel_id
            AND hmc.month_start = sta.month_start
        WHERE
            sta.hotel_id IS NULL
            OR (
                sta.total_visits = 0
                AND sta.total_revenue = 0
            )
    )
SELECT
    *
FROM
    missing_data
ORDER BY
    hotel_code,
    month;
    """
)


def get_hotels_with_missing_source_traffic():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


missing_rows = get_hotels_with_missing_source_traffic()

hotel_months = defaultdict(list)
for row in missing_rows:
    hotel_months[row.hotel_code].append(row.month)


if hotel_months:
    print("Hotels with Missing SourceTraffic Data (Last 6 Months):\n")
    for hotel_code, months in hotel_months.items():
        print(f"Hotel Code: {hotel_code}, Missing Months: {months}")
else:
    print("All active hotels have source traffic data for the past 6 months.")

export_hotel_months_to_csv(
    hotel_months,
    "missing_source_taffic_summary.csv",
    folder="csv_exports/brandDotCom/missingData/sourceTraffic/lastXmonths",
)
