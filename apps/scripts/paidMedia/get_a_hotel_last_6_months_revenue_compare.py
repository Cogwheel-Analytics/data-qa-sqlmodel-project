from sqlmodel import text
from datetime import date
from dateutil.relativedelta import relativedelta
from apps.database import get_session

# Set your hotel_id here
HOTEL_ID = "2ef93e34-626b-4a55-8e7a-81c37584d232"

# Calculate the first day of 6 months ago
six_months_ago = date.today().replace(day=1) - relativedelta(months=6)

# Parameterized SQL query
query = text(
    """
    SELECT 
        DATE_TRUNC('month', date) AS month,
        SUM(revenue) AS monthly_total_revenue
    FROM public.paid_media
    WHERE date >= :start_date AND hotel_id = :hotel_id
    GROUP BY month
    ORDER BY month;
    """
)


def get_hotel_revenue_last_6_months(hotel_id: str):
    with get_session() as session:
        result = session.execute(
            query, {"start_date": six_months_ago, "hotel_id": hotel_id}
        ).fetchall()
        return result


# Example usage
monthly_data = get_hotel_revenue_last_6_months(HOTEL_ID)

# Print the result
if monthly_data:
    print(f"Monthly revenue for hotel {HOTEL_ID} (last 6 months):\n")
    for row in monthly_data:
        print(
            f"Month: {row.month.strftime('%Y-%m')}, Revenue: {row.monthly_total_revenue}"
        )
else:
    print("No revenue data found for this hotel in the last 6 months.")
