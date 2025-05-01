from sqlmodel import text
from apps.database import get_session
import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
import os
import re

TARGET_HOTEL_CODE = "BOSFRUP"


def fetch_channel_type_room_nights(hotel_code: str):
    query = text(
        """
        SELECT
            h.code AS hotel_code,
            ct.name AS channel_type,
            DATE_TRUNC('month', cm.date) AS month,
            SUM(cm.room_nights) AS room_nights
        FROM public.channel_mix cm
        JOIN public.hotel h ON cm.hotel_id = h.id
        JOIN public.channel_type ct ON cm.channel_type_id = ct.id
        WHERE cm.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '36 month'
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
            columns=["hotel_code", "channel_type", "month", "room_nights"],
        )
    return df


def evaluate_forecast(prophet_df, model, horizon_months=3):
    train_df = prophet_df[:-horizon_months]
    test_df = prophet_df[-horizon_months:]

    model.fit(train_df)
    future = model.make_future_dataframe(periods=horizon_months, freq="MS")
    forecast = model.predict(future)

    forecast_df = forecast[["ds", "yhat"]].merge(test_df, on="ds", how="inner")

    mae = (forecast_df["yhat"] - forecast_df["y"]).abs().mean()
    rmse = ((forecast_df["yhat"] - forecast_df["y"]) ** 2).mean() ** 0.5
    mape = (
        abs((forecast_df["yhat"] - forecast_df["y"]) / forecast_df["y"])
    ).mean() * 100

    return mae, rmse, mape


def analyze_room_nights_trends(df, hotel_code: str):
    grouped = df.groupby(["channel_type"])
    output_dir = f"forecast_plots/room_nights/{hotel_code}"
    os.makedirs(output_dir, exist_ok=True)

    for channel, group in grouped:
        prophet_df = (
            group.rename(columns={"month": "ds", "room_nights": "y"})
            .sort_values("ds")
            .assign(ds=lambda df: df["ds"].dt.tz_localize(None))
        )

        if len(prophet_df) < 6:
            print(f"Skipping {hotel_code} - {channel} (not enough data)")
            continue

        # Evaluation using a temporary model instance
        eval_model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
        )
        mae, rmse, mape = evaluate_forecast(prophet_df, eval_model)

        # New model for actual forecasting
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
        )
        print(
            f"Forecast Evaluation for {hotel_code} - {channel}:\n"
            f"MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {mape:.2f}%\n"
        )

        # Refit model on full data
        model.fit(prophet_df)

        future = model.make_future_dataframe(periods=3, freq="MS")
        forecast = model.predict(future)

        fig = model.plot(forecast)
        ax = fig.gca()

        # Set dynamic y-axis limit
        full_room_nights = pd.concat(
            [
                prophet_df[["ds", "y"]],
                forecast[["ds", "yhat"]].rename(columns={"yhat": "y"}),
            ]
        )
        global_max_room_nights = full_room_nights["y"].max() * 1.2
        ax.set_ylim(0, global_max_room_nights)

        # Plot actual room_nights
        ax.scatter(
            prophet_df["ds"], prophet_df["y"], color="red", label="Actual Room Nights"
        )

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

        # Forecasted future room_nights
        future_forecast = forecast[forecast["ds"] > prophet_df["ds"].max()]
        ax.scatter(
            future_forecast["ds"],
            future_forecast["yhat"],
            color="blue",
            marker="x",
            label="Forecasted Room Nights",
        )

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

        fig.autofmt_xdate()
        plt.title(f"{hotel_code} - {channel}")
        plt.legend()
        fig.tight_layout()

        safe_channel_name = re.sub(r"[^\w\-_.]", "_", str(channel))
        fig.savefig(f"{output_dir}/{safe_channel_name}.png")
        plt.close()

        print(f"Saved forecast for: {channel}")


def main():
    df = fetch_channel_type_room_nights(TARGET_HOTEL_CODE)
    if not df.empty:
        analyze_room_nights_trends(df, TARGET_HOTEL_CODE)
    else:
        print(f"No room nights data found for hotel: {TARGET_HOTEL_CODE}")


if __name__ == "__main__":
    main()
