from sqlmodel import text
from apps.database import get_session
import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
import os
import re
import matplotlib.dates as mdates

TARGET_HOTEL_CODE = "BOSFRUP"


def fetch_channel_type_revenue(hotel_code: str):
    query = text(
        """
        SELECT
            h.code AS hotel_code,
            ct.name AS channel_type,
            DATE_TRUNC('month', cm.date) AS month,
            SUM(cm.revenue) AS revenue
        FROM public.channel_mix cm
        JOIN public.hotel h ON cm.hotel_id = h.id
        JOIN public.channel_type ct ON cm.channel_type_id = ct.id
        WHERE cm.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '24 month'
        AND cm.date < DATE_TRUNC('month', CURRENT_DATE)
        AND h.code = :hotel_code
        AND h.is_active = TRUE
        GROUP BY h.code, ct.name, DATE_TRUNC('month', cm.date)
        ORDER BY ct.name, month
        """
    )
    with get_session() as session:
        result = session.execute(query, {"hotel_code": hotel_code})
        df = pd.DataFrame(
            result.fetchall(),
            columns=["hotel_code", "channel_type", "month", "revenue"],
        )
    return df


def analyze_revenue_trends(df, hotel_code: str):
    grouped = df.groupby(["channel_type"])
    output_dir = f"forecast_plots/{hotel_code}"
    os.makedirs(output_dir, exist_ok=True)

    for channel, group in grouped:
        prophet_df = (
            group.rename(columns={"month": "ds", "revenue": "y"})
            .sort_values("ds")
            .assign(ds=lambda df: df["ds"].dt.tz_localize(None))
        )

        if len(prophet_df) < 6:
            print(f"Skipping {hotel_code} - {channel} (not enough data)")
            continue

        model = Prophet(
            yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False
        )
        model.fit(prophet_df)

        future = model.make_future_dataframe(periods=3, freq="MS")
        forecast = model.predict(future)

        fig = model.plot(forecast)
        ax = fig.gca()

        # Set dynamic y-axis limit based on both actual and forecast
        full_revenue = pd.concat(
            [
                prophet_df[["ds", "y"]],
                forecast[["ds", "yhat"]].rename(columns={"yhat": "y"}),
            ]
        )
        global_max_revenue = full_revenue["y"].max() * 1.2
        ax.set_ylim(0, global_max_revenue)

        # Plot actual revenue points
        ax.scatter(
            prophet_df["ds"], prophet_df["y"], color="red", label="Actual Revenue"
        )

        # Annotate actual revenue values
        for x, y in zip(prophet_df["ds"], prophet_df["y"]):
            ax.annotate(
                f"{y:,.0f}",
                (x, y),
                textcoords="offset points",
                xytext=(0, 5),
                ha="center",
                fontsize=8,
                color="black",
            )

        # Plot forecasted future revenue points
        future_forecast = forecast[forecast["ds"] > prophet_df["ds"].max()]
        ax.scatter(
            future_forecast["ds"],
            future_forecast["yhat"],
            color="blue",
            marker="x",
            label="Forecasted Revenue",
        )

        # Annotate forecasted revenue values
        for x, y in zip(future_forecast["ds"], future_forecast["yhat"]):
            ax.annotate(
                f"{y:,.0f}",
                (x, y),
                textcoords="offset points",
                xytext=(0, 5),
                ha="center",
                fontsize=8,
                color="blue",
            )

        # Setup x-axis to show month names
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
        fig.autofmt_xdate()

        plt.title(f"{hotel_code} - {channel}")
        plt.legend()
        fig.tight_layout()

        safe_channel_name = re.sub(r"[^\w\-_.]", "_", str(channel))
        fig.savefig(f"{output_dir}/{safe_channel_name}.png")
        plt.close()

        print(f"Saved forecast for: {channel}")


def main():
    df = fetch_channel_type_revenue(TARGET_HOTEL_CODE)
    if not df.empty:
        analyze_revenue_trends(df, TARGET_HOTEL_CODE)
    else:
        print(f"No revenue data found for hotel: {TARGET_HOTEL_CODE}")


if __name__ == "__main__":
    main()
