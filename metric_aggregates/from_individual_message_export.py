import requests
import klaviyo
from . import utils


class AggregatesFromIndividualMessageExport(object):

    URL_BASE = 'https://a.klaviyo.com/api/v1'

    def __init__(self, api_key):
        self.api_key = api_key
        self.client = klaviyo.Klaviyo(private_token=api_key)

    def campaign_url(self, page=0, count=100):
        """
        TODO: Campaigns endpoint isn't included in Klaviyo python client yet, have to manually format URL &
            use requests
        """
        return "{base_url}/campaigns?api_key={api_key}&page={page}&count={count}".format(base_url=self.URL_BASE,
                                                                                         api_key=self.api_key,
                                                                                         count=count, page=page)

    def get_campaigns(self):
        """
        Gets all campaigns in account, looping over pages if necessary.
        """
        campaigns = []
        page = 0
        more = True
        while more:
            response = requests.get(self.campaign_url(page=page)).json()
            camps = response["data"]
            campaigns.extend(camps)
            more = response["end"] < (response["total"] - 1)
            if more:
                page += 1
        return campaigns

    def get_metric_data_from_export_for_campaigns(self, metric_name, campaigns, start_date, end_date):
        """
        Gets metric data from metric export endpoint for metric_name, campaigns list. Performs a \
        single request per campaign in campaigns list.
        """
        metric_id = utils.get_metric_id(metric_name, self.client)
        metric_data = []
        for campaign in campaigns:
            message = '[["$message","=","{}"]]'.format(campaign["id"])
            response = self.client.metric_export(metric_id,
                                                 start_date=start_date,
                                                 end_date=end_date,
                                                 where=message,
                                                 unit="day")
            metric_data.append(response)
        return metric_data

    def get_metric_export_for_all_messages(self, metric_id, start_date, end_date):
        """
        Reproduces query used to produce original issue.
        """
        return self.client.metric_export(metric_id,
                                         start_date=start_date,
                                         end_date=end_date,
                                         unit="day",
                                         by="$message",
                                         count=10000)

    def main(self, metric_name, start_date, end_date):
        """
        Gets metric export data for specific metric name by first getting all campaigns,
        then looping over campaigns list and passing each campaign ID as message ID to
        single metric export API requests.
        """
        campaigns = self.get_campaigns()
        return self.get_metric_data_from_export_for_campaigns(metric_name, campaigns, start_date, end_date)
