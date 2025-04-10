from sqlmodel import text
from datetime import date
from dateutil.relativedelta import relativedelta
from apps.database import get_session

# Calculate date 6 months ago from today
six_months_ago = (date.today().replace(day=1) - relativedelta(months=6)).isoformat()

# SQL query to get monthly total revenue for last 6 months
query = text(
    f"""
    SELECT 
        DATE_TRUNC('month', date) AS month,
        SUM(revenue) AS monthly_total_revenue
    FROM public.channel_mix
    WHERE date >= '{six_months_ago}'
    GROUP BY month
    ORDER BY month;
"""
)


def get_monthly_revenue_last_6_months():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


# Example usage
monthly_data = get_monthly_revenue_last_6_months()

# Print result
if monthly_data:
    print("Monthly total revenue for the last 6 months:\n")
    for row in monthly_data:
        print(
            f"Month: {row.month.strftime('%Y-%m')}, Revenue: {row.monthly_total_revenue}"
        )
else:
    print("No data found for the last 6 months.")
