import pandas as pd
import argparse
from metric_aggregates import from_timeline


def aggregate_timeline_data(metric_data):
    """
    Perform aggregations on raw metric data w/ Pandas

    Returns:
        pd.Dataframe: Dataframe containing aggregated results

    """
    df = pd.DataFrame.from_dict(metric_data)
    if len(metric_data):
        df['send_date'] = pd.to_datetime(df['send_date'], errors='coerce')
        return df.groupby(['message_id', 'campaign_name', 'event_name', df['send_date'].dt.date])\
                .agg({'customer': 'count'}).rename(columns={'customer': 'count'})
    else:
        return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Aggregate data for a one Klaviyo metric (for all messages) in an account '
                    'using the metric timeline endpoint.')
    parser.add_argument('api_key', type=str,
                        help='API key (or private key) used to access the Klaviyo API for a given account.')
    parser.add_argument('--metric_name',
                        choices=[
                            "Received Email",
                            "Opened Email",
                            "Bounced Email",
                            "Clicked Email"
                        ],
                        help='Name of the message metric you want to aggregate.')
    parser.add_argument('--since', default=None, help='')
    args = parser.parse_args()
    aggregator = from_timeline.AggregatesFromTimeline(args.api_key)

    # get raw event data by hitting the timeline API & paginating through all results
    # this step could take a while
    metric_data = aggregator.get_metric_data(args.metric_name)

    # Aggregate all timeline data & store in CSV
    df = aggregate_timeline_data(metric_data)
    filename = "../data/{api_key}_{metric_name}_aggregate_data.csv".format(api_key=args.api_key,
                                                                           metric_name=args.metric_name)
    df.to_csv(filename, mode='a', header=False)
