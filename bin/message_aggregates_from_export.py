import json
import argparse
from metric_aggregates import from_individual_message_export


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
    parser.add_argument('--start_date', type=str,
                        help='Start date of metric export range, format `YYYY-mm-dd`')
    parser.add_argument('--end_date', type=str,
                        help='End date of metric export range, format `YYYY-mm-dd`')

    args = parser.parse_args()
    aggregator = from_individual_message_export.AggregatesFromIndividualMessageExport(args.api_key)

    # get raw event data by hitting the timeline API & paginating through all results
    # this step could take a while
    metric_data = aggregator.main(args.metric_name, args.start_date, args.end_date)

    # Store all data in CSV
    filename = "../data/{api_key}_{metric_name}_aggregate_data.json".format(api_key=args.api_key,
                                                                            metric_name=args.metric_name)
    with open(filename, "w") as file:
        json.dump(metric_data, file, indent=4, sort_keys=True)
