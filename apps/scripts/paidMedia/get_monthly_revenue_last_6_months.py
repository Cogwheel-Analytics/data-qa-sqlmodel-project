from sqlmodel import text
from datetime import date
from dateutil.relativedelta import relativedelta
from apps.database import get_session

# Calculate date 6 months ago from today
six_months_ago = (date.today().replace(day=1) - relativedelta(months=6)).isoformat()

# SQL query to get monthly total revenue for the last 6 months from paid_media
query = text(
    f"""
    SELECT 
        DATE_TRUNC('month', date) AS month,
        SUM(revenue) AS monthly_total_revenue
    FROM public.paid_media
    WHERE date >= '{six_months_ago}'
    GROUP BY month
    ORDER BY month;
    """
)


def get_monthly_revenue(query):
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


# Get monthly revenue data for paid_media
paid_media_data = get_monthly_revenue(query)

# Print results for paid_media
print("Monthly Total Revenue for Paid Media (Last 6 Months):\n")

if paid_media_data:
    # Convert the result to a dict for easier access
    paid_media_dict = {
        row.month.strftime("%Y-%m"): row.monthly_total_revenue
        for row in paid_media_data
    }

    months = sorted(paid_media_dict.keys())

    for month in months:
        print(f"Month: {month}, Paid Media Revenue: {paid_media_dict[month]}")
else:
    print("No data found for paid media in the last 6 months.")
