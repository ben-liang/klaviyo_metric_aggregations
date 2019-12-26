import time
import pandas as pd
import klaviyo
from . import utils
from tqdm import tqdm


class AggregatesFromTimeline(object):

    def __init__(self, api_key):
        self.api_key = api_key
        self.client = klaviyo.Klaviyo(private_token=api_key)

    def metric_timeline_request_handler(self, metric_id, since=None, count=None, max_retries=5, sleep=5):
        """
        Handler around request to metric API endpoint to deal w/ retrying & error notification
        """
        response = self.client.metric_timeline(metric_id=metric_id, since=since, count=count, sort='desc')
        retries = 0
        while (response.status_code != 200) & (retries <= max_retries):
            print("ERROR: {}".format(response.status_code))
            print("Sleeping for 5 seconds then retrying...")
            time.sleep(sleep)
            response = self.client.metric_timeline(metric_id=metric_id, since=since, count=count, sort='desc')
            retries += 1
        return response.json()

    def _get_metric_data_batch(self, metric_name, batch_size, sleep=None, since=None):
        """
        Gets a "batch" of metric timeline data, i.e. executed a fixed number of sequential calls to the timelines API
        and returns results.
        """
        metric_id = utils.get_metric_id(metric_name, self.client)
        metric_data = []
        print("Beginning metric timeline API request batch of {}, metric ID {}".format(batch_size, metric_id))
        for i in tqdm(range(batch_size)):
            response = self.metric_timeline_request_handler(metric_id, since=since)
            if response is None:
                break
            for e in response["data"]:
                customer_email = str(e["person"]["$email"]).strip()
                message_id = str(e["event_properties"]["$message"]).strip()
                campaign_name = str(e["event_properties"]["Campaign Name"]).strip()
                event_name = str(e["event_name"]).strip()
                send_date = str(e["datetime"]).strip()
                metric_data.append({"message_id": message_id, "campaign_name": campaign_name,
                                    "event_name": event_name, "customer": customer_email, "send_date": send_date})
            if "next" in response and response["next"] is not None:
                if sleep is not None:
                    time.sleep(sleep)
                since = response["next"]
            else:
                since = False
                break

        return metric_data, since

    def get_metric_data(self, metric_name, batch_size=10000, sleep_between_batches=3,
                        sleep_in_batches=None, since=None):
        """
        Call metric timeline API in batches of n in order to give it a break after n calls in order to avoid
        rate-limiting or any server-side performance issues.

        Args:
            metric_name:
            batch_size:
            sleep_between_batches: time in seconds to sleep in between batches of calls.
            sleep_in_batches: time to sleep in between individual calls in each batch
            since: UUID or Unix tstamp of next batch

        Returns:

        """

        metric_data, since = self._get_metric_data_batch(metric_name,
                                                         batch_size=batch_size,
                                                         sleep=sleep_in_batches,
                                                         since=since)

        while since:
            md, since = self._get_metric_data_batch(metric_name, batch_size=batch_size,
                                                    sleep=sleep_in_batches, since=since)
            metric_data.extend(md)
            if sleep_between_batches is not None:
                time.sleep(sleep_between_batches)
        return metric_data

    @staticmethod
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
