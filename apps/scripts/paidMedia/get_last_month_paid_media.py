from sqlmodel import text
from apps.database import get_session

# Raw SQL query to fetch paid media data for last month (for all hotels)
query = text(
    """
    SELECT *
    FROM public.paid_media
    WHERE date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
      AND date < DATE_TRUNC('month', CURRENT_DATE)
    ORDER BY date;
"""
)


def get_last_month_paid_media():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


# Example usage
paid_media_data = get_last_month_paid_media()

# Print results
if paid_media_data:
    print("Paid Media data for last month:\n")
    for row in paid_media_data:
        print(row)
    print(f"\nTotal records: {len(paid_media_data)}")
else:
    print("No data found for last month.")
