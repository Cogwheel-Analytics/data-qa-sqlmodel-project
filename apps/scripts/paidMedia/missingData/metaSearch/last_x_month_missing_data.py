from collections import defaultdict
from sqlmodel import text
from apps.database import get_session

# List your MetaSearch normalized_source names here for easy access
DEFAULT_META_SOURCES = [
    "Google Hotel Ads (MetaSearch)",
    "Kayak MetaSearch",
    "SABRE",
    "TripAdvisor MetaSearch",
    "Sponsored Ads",
    "Trivago MetaSearch",
]


def get_query(source_filter: list[str] | None = None):
    source_filter_sql = ""
    if source_filter:
        source_placeholders = ", ".join([f"'{source}'" for source in source_filter])
        source_filter_sql = f"AND pms.normalized_source IN ({source_placeholders})"

    return text(
        f"""
        WITH
        months AS (
            SELECT DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' * i AS month_start
            FROM generate_series(1, 6) AS i
        ),
        active_hotels AS (
            SELECT id AS hotel_id, code AS hotel_code
            FROM public.hotel
            WHERE is_active = true
        ),
        hotel_month_combinations AS (
            SELECT h.hotel_id, h.hotel_code, m.month_start
            FROM active_hotels h
            CROSS JOIN months m
        ),
        meta_search_paid_media AS (
            SELECT
                pm.hotel_id,
                DATE_TRUNC('month', pm.date) AS month_start,
                COUNT(*) AS meta_entries
            FROM public.paid_media pm
            JOIN public.media_channel mc ON pm.media_id = mc.id
            LEFT JOIN public.paid_media_source pms ON pm.paid_media_source_id = pms.id
            WHERE
                mc.name = 'METASEARCH'
                AND pm.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '6 month'
                AND pm.date < DATE_TRUNC('month', CURRENT_DATE)
                {source_filter_sql}
            GROUP BY pm.hotel_id, DATE_TRUNC('month', pm.date)
        ),
        missing_meta_data AS (
            SELECT
                hmc.hotel_code,
                TO_CHAR(hmc.month_start, 'YYYY-MM') AS month
            FROM hotel_month_combinations hmc
            LEFT JOIN meta_search_paid_media ms ON hmc.hotel_id = ms.hotel_id
            AND hmc.month_start = ms.month_start
            WHERE ms.meta_entries IS NULL
        )
        SELECT * FROM missing_meta_data
        ORDER BY hotel_code, month;
        """
    )


def get_hotels_with_missing_paid_media(source_filter: list[str] | None = None):
    query = get_query(source_filter)
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


# For specific sources: e.g., ["TripAdvisor MetaSearch", "Google Hotel Ads (MetaSearch)"]
SOURCE_FILTER = ["Google Hotel Ads (MetaSearch)"]

missing_rows = get_hotels_with_missing_paid_media(SOURCE_FILTER)


hotel_months = defaultdict(list)
for row in missing_rows:
    hotel_months[row.hotel_code].append(row.month)

if hotel_months:
    print("Hotels with Missing Meta Search (Paid Media) Data (Last 6 Months):\n")
    for hotel_code, months in hotel_months.items():
        print(f"Hotel Code: {hotel_code}, Missing Months: {months}")
else:
    print("All active hotels have meta search data for the past 6 months.")
