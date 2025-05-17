from collections import defaultdict
from sqlmodel import text
from apps.utils.database import get_session
from apps.utils.csv_export import export_hotel_months_to_csv

# List your OTA normalized_source names here for easy access
DEFAULT_OTA_SOURCES = [
    "Booking.com",
    "Booking Network Sponsored Ads",
    "Expedia TravelAds",
]


def get_query(source_filter: list[str] | None = None):
    source_filter_sql = ""
    if source_filter:
        source_placeholders = ", ".join([f"'{source}'" for source in source_filter])
        source_filter_sql = f"AND pms.normalized_source IN ({source_placeholders})"

    # SQL to find missing or empty PaidMedia (OTA) data for the last 6 months
    return text(
        f"""
    WITH
    months AS (
        SELECT DATE_TRUNC ('month', CURRENT_DATE) - INTERVAL '1 month' * i AS month_start
        FROM generate_series (1, 6) AS i
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
    ota_paid_media AS (
        SELECT
            pm.hotel_id,
            DATE_TRUNC ('month', pm.date) AS month_start,
            COUNT(*) AS ota_entries
        FROM public.paid_media pm
        JOIN public.media_channel mc ON pm.media_id = mc.id
        LEFT JOIN public.paid_media_source pms ON pm.paid_media_source_id = pms.id
        WHERE
            mc.name = 'OTA'
            AND pm.date >= DATE_TRUNC ('month', CURRENT_DATE) - INTERVAL '6 month'
            AND pm.date < DATE_TRUNC ('month', CURRENT_DATE)
            {source_filter_sql}
        GROUP BY
            pm.hotel_id, DATE_TRUNC ('month', pm.date)
    ),
    missing_ota_data AS (
        SELECT
            hmc.hotel_code,
            TO_CHAR (hmc.month_start, 'YYYY-MM') AS month
        FROM hotel_month_combinations hmc
        LEFT JOIN ota_paid_media ms ON hmc.hotel_id = ms.hotel_id
        AND hmc.month_start = ms.month_start
        WHERE ms.ota_entries IS NULL
    )
    SELECT * FROM missing_ota_data
    ORDER BY hotel_code, month;

    """
    )


def get_hotels_with_missing_paid_media(source_filter: list[str] | None = None):
    query = get_query(source_filter)
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


# For specific sources: e.g., ["Expedia TravelAds", "Booking.com"]
source_filter = ["Expedia TravelAds"]


missing_rows = get_hotels_with_missing_paid_media(source_filter)

hotel_months = defaultdict(list)
for row in missing_rows:
    hotel_months[row.hotel_code].append(row.month)

print("=== Filter Used ===")
if source_filter:
    print(f"Filtered Sources: {source_filter}")
else:
    print("Filtered Sources: None (All OTA sources included)")

print("\n=== Result ===")
if hotel_months:
    print("Hotels with Missing OTA (Paid Media) Data (Last 6 Months):\n")
    for hotel_code, months in hotel_months.items():
        print(f"Hotel Code: {hotel_code}, Missing Months: {months}")
else:
    print("All active hotels have OTA data for the past 6 months.")


export_hotel_months_to_csv(
    hotel_months,
    "missing_OTA_summary.csv",
    folder="csv_exports/paidMedia/missingData/OTA/lastXmonths",
)
