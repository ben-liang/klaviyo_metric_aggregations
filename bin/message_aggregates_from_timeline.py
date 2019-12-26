import json
import argparse
from metric_aggregates import from_timeline

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

    aggregator = from_timeline.AggregatesFromTimeline(args['api_key'])

    metric_data = aggregator.get_metric_data(args['metric_name'])



