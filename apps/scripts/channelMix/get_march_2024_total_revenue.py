from sqlmodel import text
from apps.database import get_session

# Raw SQL query to calculate the monthly total revenue for March 2024
query = text(
    """
    SELECT SUM(revenue) AS monthly_total_revenue
    FROM public.channel_mix
    WHERE date BETWEEN '2024-03-01' AND '2024-03-31'
"""
)


# Function to retrieve the monthly total revenue for March 2024
def get_march_2024_total_revenue():
    with get_session() as session:
        # Execute the raw SQL query
        result = session.execute(query).fetchone()
        return result


# Example usage
total_revenue_march_2024 = get_march_2024_total_revenue()

# Printing the results
if total_revenue_march_2024 and total_revenue_march_2024[0]:
    print(f"Total Revenue for March 2024: {total_revenue_march_2024[0]}")
else:
    print("No revenue data found for March 2024.")
