from collections import defaultdict
from sqlmodel import text
from apps.database import get_session
from apps.utils.csv_export import export_hotel_months_to_csv

DEFAULT_META_SOURCES = [
    "Google Hotel Ads (MetaSearch)",
    "Kayak MetaSearch",
    "SABRE",
    "TripAdvisor MetaSearch",
    "Sponsored Ads",
    "Trivago MetaSearch",
]


def get_missing_meta_search_for_custom_months(month_list, source_filter=None):
    formatted_months = [f"{m}-01" for m in month_list]

    source_filter_sql = ""
    if source_filter:
        source_placeholders = ", ".join([f"'{src}'" for src in source_filter])
        source_filter_sql = f"AND pms.normalized_source IN ({source_placeholders})"

    query = text(
        f"""
        WITH months AS (
            SELECT TO_DATE(m, 'YYYY-MM-DD') AS month_start
            FROM UNNEST(:month_list) AS m
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
            WHERE mc.name = 'METASEARCH'
              {source_filter_sql}
              AND DATE_TRUNC('month', pm.date) IN (
                  SELECT month_start FROM months
              )
            GROUP BY pm.hotel_id, DATE_TRUNC('month', pm.date)
        ),
        missing_data AS (
            SELECT hmc.hotel_code, TO_CHAR(hmc.month_start, 'YYYY-MM') AS month
            FROM hotel_month_combinations hmc
            LEFT JOIN meta_search_paid_media ms
              ON hmc.hotel_id = ms.hotel_id AND hmc.month_start = ms.month_start
            WHERE ms.meta_entries IS NULL
        )
        SELECT * FROM missing_data
        ORDER BY hotel_code, month;
        """
    )

    with get_session() as session:
        result = session.execute(query, {"month_list": formatted_months}).fetchall()
        return result


custom_months = ["2023-12", "2025-01", "2025-03"]
source_filter = ["Google Hotel Ads (MetaSearch)"]

missing_rows = get_missing_meta_search_for_custom_months(custom_months, source_filter)

hotel_months = defaultdict(list)
for row in missing_rows:
    hotel_months[row.hotel_code].append(row.month)

print("=== Filter Used ===")
print(f"Custom Months: {custom_months}")
if source_filter:
    print(f"Filtered Sources: {source_filter}")
else:
    print("Filtered Sources: None (All Meta Search sources included)")

print("\n=== Result ===")
if hotel_months:
    print("Hotels with Missing Meta Search (Paid Media) Data:\n")
    for hotel_code, months in hotel_months.items():
        print(f"Hotel Code: {hotel_code}, Missing Months: {months}")
else:
    print("All active hotels have meta search data for the selected months.")

export_hotel_months_to_csv(
    hotel_months,
    "missing_meta_search_custom_summary.csv",
    folder="csv_exports/paidMedia/missingData/MetaSearch/customMonths",
)
