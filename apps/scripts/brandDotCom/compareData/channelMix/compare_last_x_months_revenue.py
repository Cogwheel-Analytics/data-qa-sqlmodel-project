from sqlmodel import text
from apps.database import get_session
from datetime import date
from dateutil.relativedelta import relativedelta

# Calculate 6 months ago from the start of the current month
six_months_ago = (date.today().replace(day=1) - relativedelta(months=6)).isoformat()

# SQL query to fetch total revenue grouped by month for the last 6 months for active hotels
query = text(
    f"""
    SELECT 
        DATE_TRUNC('month', cm.date) AS month,
        SUM(cm.revenue) AS monthly_total_revenue
    FROM public.channel_mix cm
    JOIN public.hotel h ON cm.hotel_id = h.id
    WHERE h.is_active = true
      AND cm.date >= '{six_months_ago}'
    GROUP BY month
    ORDER BY month;
    """
)


def get_last_6_months_revenue():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


monthly_revenues = get_last_6_months_revenue()

if monthly_revenues:
    print("Last 6 Months Channel Mix Revenue Comparison (Active Hotels):\n")
    for row in monthly_revenues:
        print(
            f"Month: {row.month.strftime('%Y-%m')}, Revenue: {row.monthly_total_revenue}"
        )
else:
    print("No revenue data found for the last 6 months.")
