import requests
import klaviyo
from tqdm import tqdm
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
            camps = map(lambda campaign: campaign["id"], response["data"])
            campaigns.extend(camps)
            more = response["end"] < (response["total"] - 1)
            if more:
                page += 1
        return campaigns

    @classmethod
    def flows_url(cls):
        return "{base_url}/flows".format(base_url=cls.URL_BASE)

    @classmethod
    def flow_actions_url(cls, flow_id):
        return '{base_url}/flow/{flow_id}/actions'.format(base_url=cls.URL_BASE,
                                                          flow_id=flow_id)

    @classmethod
    def flow_action_email_url(cls, flow_id, email_id):
        return '{base_url}/flow/{flow_id}/action/{email_id}/email'.format(base_url=cls.URL_BASE,
                                                                          flow_id=flow_id,
                                                                          email_id=email_id)

    def get_flow_message_ids(self):
        """
        Gets all flow message IDs from flow API endpoints.

        Executes three calls. First gets all flows, then gets all actions within each flow and looks for
        all "SEND_MESSAGE" actions. Finally, gets all emails for "SEND_MESSAGE" actions in flows and returns
        list of message IDs.

        :return:
        """
        payload = {'api_key': self.api_key}

        # first get all flows
        r = requests.get(self.flows_url(), params=payload)
        data = r.json()
        all_flows = []
        for flow in data['data']:
            all_flows.append(flow['id'])

        # Get all SEND_MESSAGE actions for each flow
        flow_to_action_mapping = {}
        ACTION_CONSTANT = "SEND_MESSAGE"
        for each_flow in all_flows:
            unique_flow_url = self.flow_actions_url(each_flow)
            fr = requests.get(unique_flow_url, params=payload)
            flow_actions = fr.json()
            flow_to_action_mapping[each_flow] = []
            for action in flow_actions:
                if action['type'] == ACTION_CONSTANT:
                    flow_to_action_mapping[each_flow].append(action['id'])

        # Get all message ids for each flow SEND_MESSAGE action
        all_message_ids = []
        for flows in flow_to_action_mapping:
            for email in flow_to_action_mapping[flows]:
                unique_email_url = self.flow_action_email_url(flows, email)
                far = requests.get(unique_email_url, params=payload)
                all_emails_returned = far.json()
                all_message_ids.append(all_emails_returned['id'])

        return all_message_ids

    def get_metric_data_from_export_for_message_ids(self, metric_name, message_ids, start_date, end_date):
        """
        Gets metric data from metric export endpoint for metric_name, campaigns list. Performs a \
        single request per campaign in campaigns list.
        """
        metric_id = utils.get_metric_id(metric_name, self.client)
        metric_data = []
        for message_id in tqdm(message_ids):
            message = '[["$message","=","{}"]]'.format(message_id)
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
        Gets metric export data for specific metric name by first getting all message IDs
        for campaigns & flows, then looping over message_id list and passing each message ID
        single metric export API requests.
        """

        message_ids = self.get_campaigns()
        print("{} Campaign message IDs found for account.".format(len(message_ids)))
        flow_ids = self.get_flow_message_ids()
        print("{} Flow message IDs found for account.".format(len(flow_ids)))
        message_ids.extend(flow_ids)
        print("Getting metric export data for {metric_name} from {start_date} to {end_date} "
              "for {message_count} messages (this may take a little while)...".format(metric_name=metric_name,
                                                                                      message_count=len(message_ids),
                                                                                      start_date=start_date,
                                                                                      end_date=end_date))
        return self.get_metric_data_from_export_for_message_ids(metric_name, message_ids, start_date, end_date)
