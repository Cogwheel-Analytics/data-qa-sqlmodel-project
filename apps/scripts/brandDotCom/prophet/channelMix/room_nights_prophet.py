import pandas as pd
from apps.database import get_session
from sqlalchemy import text
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import matplotlib.pyplot as plt
import os
import re

from apps.utils.csv_export import export_evaluation_metrics_to_csv


def fetch_all_hotel_data(hotel_code: str) -> pd.DataFrame:
    query = """
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
    with get_session() as session:
        result = session.execute(text(query), {"hotel_code": hotel_code})
        df = pd.DataFrame(
            result.fetchall(), columns=["hotel_code", "channel_type", "ds", "y"]
        )
    return df


def generate_forecast(df_channel: pd.DataFrame) -> tuple:
    df_channel["ds"] = pd.to_datetime(df_channel["ds"]).dt.tz_localize(None)
    df_channel = df_channel.sort_values("ds")

    if len(df_channel) < 6:
        return None, None, None, None, None

    # Train-test split
    split_index = int(len(df_channel) * 0.75)
    train = df_channel.iloc[:split_index]
    test = df_channel.iloc[split_index:]

    # Fit on full data
    model = Prophet(yearly_seasonality=True)
    model.fit(df_channel)

    # Predict on same data points
    future = df_channel[["ds"]].copy()
    forecast = model.predict(future)

    # Clip yhat to observed bounds
    y_min, y_max = df_channel["y"].min(), df_channel["y"].max()
    forecast["yhat"] = forecast["yhat"].clip(lower=y_min, upper=y_max)

    return model, forecast, train, test, df_channel


def evaluate_forecast(
    df_channel: pd.DataFrame,
    model: Prophet,
    forecast: pd.DataFrame,
    test: pd.DataFrame,
):
    forecast = forecast[forecast["ds"] <= df_channel["ds"].max()]
    forecast_filtered = (
        forecast[["ds", "yhat"]].set_index("ds").join(test.set_index("ds"))
    )
    forecast_filtered.dropna(inplace=True)

    if forecast_filtered.empty:
        return None

    mae = mean_absolute_error(forecast_filtered["y"], forecast_filtered["yhat"])
    rmse = np.sqrt(
        mean_squared_error(forecast_filtered["y"], forecast_filtered["yhat"])
    )
    mape = (
        np.mean(
            np.abs(
                (forecast_filtered["y"] - forecast_filtered["yhat"])
                / forecast_filtered["y"]
            )
        )
        * 100
    )
    return mae, rmse, mape


def plot_forecast(
    model: Prophet,
    forecast: pd.DataFrame,
    df_channel: pd.DataFrame,
    test: pd.DataFrame,
    hotel_code: str,
    channel: str,
    output_dir: str,
):
    fig = model.plot(forecast)
    plt.scatter(
        df_channel["ds"], df_channel["y"], color="black", label="Actual", zorder=5
    )

    # Mark train/test split
    split_index = int(len(df_channel) * 0.75)
    plt.axvline(
        df_channel["ds"].iloc[split_index],
        color="red",
        linestyle="--",
        label="Train/Test Split",
    )

    # X-axis formatting
    months = pd.date_range(df_channel["ds"].min(), df_channel["ds"].max(), freq="MS")
    ax = fig.gca()
    ax.set_xticks(months)
    ax.set_xticklabels(
        [d.strftime("%b %Y") for d in months], rotation=45, ha="right", fontsize=8
    )
    plt.title(f"Forecast vs Actual - {hotel_code} - {channel}")
    plt.legend()

    # Save plot
    os.makedirs(output_dir, exist_ok=True)
    safe_channel = re.sub(r"[^\w\-_.]", "_", channel)
    filename = f"{output_dir}/{safe_channel}.png"
    fig.savefig(filename, bbox_inches="tight")
    plt.close()
    print(f"Saved: {filename}")


def forecast_and_plot(
    df_channel: pd.DataFrame, hotel_code: str, channel: str, output_dir: str
):
    model, forecast, train, test, df_channel = generate_forecast(df_channel)
    if model is None:
        print(f"Skipping {hotel_code} - {channel} (not enough data)")
        return

    metrics = evaluate_forecast(df_channel, model, forecast, test)
    if not metrics:
        print(f"Skipping {hotel_code} - {channel} (no overlapping forecast/test data)")
        return

    mae, rmse, mape = metrics
    print(f"\n{hotel_code} - {channel}")
    print(f"MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {mape:.2f}%")

    plot_forecast(model, forecast, df_channel, test, hotel_code, channel, output_dir)


def main():
    hotel_code = "BOSFRUP"
    df = fetch_all_hotel_data(hotel_code)
    if df.empty:
        print("No data found.")
        return

    grouped = df.groupby("hotel_code")
    all_metrics = []

    for hotel_code, hotel_df in grouped:
        print(f"\n=== Processing {hotel_code} ===")
        output_dir = f"forecast_plots/channelMix/RoomNights/{hotel_code}"

        for channel in hotel_df["channel_type"].unique():
            df_channel = hotel_df[hotel_df["channel_type"] == channel].copy()
            model, forecast, train, test, df_channel = generate_forecast(df_channel)

            if model is None:
                print(f"Skipping {hotel_code} - {channel} (not enough data)")
                continue

            metrics = evaluate_forecast(df_channel, model, forecast, test)
            if not metrics:
                print(
                    f"Skipping {hotel_code} - {channel} (no overlapping forecast/test data)"
                )
                continue

            mae, rmse, mape = metrics
            print(f"\n{hotel_code} - {channel}")
            print(f"MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {mape:.2f}%")

            # Collect metrics for export
            all_metrics.append(
                {
                    "Hotel Code": hotel_code,
                    "Channel Type": channel,
                    "MAE": round(mae, 2),
                    "RMSE": round(rmse, 2),
                    "MAPE (%)": round(mape, 2),
                }
            )

            plot_forecast(
                model, forecast, df_channel, test, hotel_code, channel, output_dir
            )

    metrics_filename = f"csv_exports/brandDotCom/prophet/channelMix/room_nights/{hotel_code}/evaluation_metrics.csv"
    os.makedirs(os.path.dirname(metrics_filename), exist_ok=True)
    export_evaluation_metrics_to_csv(all_metrics, metrics_filename)


if __name__ == "__main__":
    main()
