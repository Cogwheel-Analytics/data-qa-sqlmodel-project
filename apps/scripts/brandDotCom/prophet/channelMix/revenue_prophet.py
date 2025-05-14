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
            SUM(cm.revenue) AS revenue
        FROM public.channel_mix cm
        JOIN public.hotel h ON cm.hotel_id = h.id
        JOIN public.channel_type ct ON cm.channel_type_id = ct.id
        WHERE cm.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '36 month'
          AND cm.date < DATE_TRUNC('month', CURRENT_DATE)
          AND h.code = :hotel_code
          AND h.is_active = TRUE
        GROUP BY h.code, ct.name, DATE_TRUNC('month', cm.date)
        ORDER BY h.code, ct.name, month
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

    # Fill missing months with NaN
    all_months = pd.date_range(
        start=df_channel["ds"].min(), end=df_channel["ds"].max(), freq="MS"
    )
    df_channel = df_channel.set_index("ds").reindex(all_months).reset_index()
    df_channel.rename(columns={"index": "ds"}, inplace=True)

    # Keep other columns intact
    df_channel["hotel_code"] = df_channel["hotel_code"].ffill()
    df_channel["channel_type"] = df_channel["channel_type"].ffill()

    if df_channel["y"].notna().sum() < 6:
        return None, None, None, None, None

    # Train-test split on non-null y values
    df_non_null = df_channel[df_channel["y"].notna()]
    split_index = int(len(df_non_null) * 0.75)
    train = df_non_null.iloc[:split_index]
    test = df_non_null.iloc[split_index:]

    # Fit model
    model = Prophet(yearly_seasonality=True)
    model.fit(df_channel[["ds", "y"]])  # Prophet can handle NaN in y

    # Forecast into the future (1 month ahead)
    future = model.make_future_dataframe(periods=1, freq="MS")
    forecast = model.predict(future)

    # Clip predictions within range of observed y
    y_min, y_max = df_non_null["y"].min(), df_non_null["y"].max()
    forecast["yhat"] = forecast["yhat"].clip(lower=y_min, upper=y_max)

    return model, forecast, train, test, df_channel


def evaluate_forecast(
    df_channel: pd.DataFrame,
    model: Prophet,
    forecast: pd.DataFrame,
    test: pd.DataFrame,
):
    # Restrict forecast to actual known data range
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

    # Train/Test split marker
    split_index = int(len(df_channel) * 0.75)
    plt.axvline(
        df_channel["ds"].iloc[split_index],
        color="red",
        linestyle="--",
        label="Train/Test Split",
    )

    # Forecast start marker
    plt.axvline(
        df_channel["ds"].max(),
        color="blue",
        linestyle=":",
        label="Forecast Start",
    )

    # X-axis formatting
    months = pd.date_range(forecast["ds"].min(), forecast["ds"].max(), freq="MS")
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
        output_dir = f"forecast_plots/channelMix/Revenue/{hotel_code}"

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

    metrics_filename = f"csv_exports/brandDotCom/prophet/channelMix/revenue/{hotel_code}/evaluation_metrics.csv"
    os.makedirs(os.path.dirname(metrics_filename), exist_ok=True)
    export_evaluation_metrics_to_csv(all_metrics, metrics_filename)


if __name__ == "__main__":
    main()
