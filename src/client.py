import json
import logging
import requests
import dateparser
import urllib.parse as url_parse
from requests.auth import HTTPBasicAuth
import os

from keboola.component.base import ComponentBase  # noqa

# Get Authorization
# oauth = ComponentBase.configuration.oauth_credentials
'''
credentials = oauth["oauth_api"]["credentials"]["#data"]
credentials_json = json.loads(credentials)
oauth_token = credentials_json["access_token"]
app_key = oauth["oauth_api"]["credentials"]["appKey"]
app_secret = oauth["oauth_api"]["credentials"]["#appSecret"]
'''
'''
credentials = oauth.data
credentials_json = json.loads(credentials)
oauth_token = credentials_json['access_token']
app_key = oauth.appKey
app_secret = oauth.appSecret


# Handling Refresh Token
# If state file exist, look for new refresh token
refresh_token = credentials_json["refresh_token"]
logging.info("KBC refresh token: {0}XXXX{1}".format(
    refresh_token[0:4], refresh_token[-4:]))
'''

statefile_in_path = os.path.join(os.path.dirname(os.getcwd()), "data/in/state.json")
statefile_out_path = os.path.join(os.path.dirname(os.getcwd()), "data/out/state.json")

requesting = requests.Session()


class QuickBooksClientException(Exception):
    pass


class QuickbooksClient:
    """
    QuickBooks Requests Handler
    """

    def __init__(self, company_id, access_token, refresh_token, oauth, sandbox):
        self.app_key = oauth.appKey
        self.app_secret = oauth.appSecret

        if not sandbox:
            self.base_url = "https://quickbooks.api.intuit.com/v3/company"
        else:
            self.base_url = "https://sandbox-quickbooks.api.intuit.com/v3/company"

        # Parameters for request
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.access_token_refreshed = False
        self.new_refresh_token = False
        self.company_id = company_id
        self.reports_required_accounting_type = [
            "ProfitAndLoss",
            "ProfitAndLossDetail",
            "GeneralLedger",
            "BalanceSheet",
            "TrialBalance"
        ]

    def fetch(self, endpoint, report_api_bool, start_date, end_date, class_object=None, query=""):
        """
        Fetching results for the specified endpoint
        """
        # Initializing Parameters
        self.endpoint = endpoint
        self.report_api_bool = report_api_bool

        # Pagination Parameters
        self.startposition = 1
        self.maxresults = 1000
        # Start_date will be used as the custom query input field
        # if custom query is selected
        self.start_date = start_date
        self.end_date = end_date

        # Return
        # if report is returning accounting_type
        # data = Accrual Type
        # data2 = Cash Type
        self.data = []  # stores all the returns from request
        self.data_2 = []

        logging.info("Accessing QuickBooks API...")
        if report_api_bool:
            logging.info("Processing Report: {0}".format(endpoint))
            if self.endpoint == "CustomQuery":
                if query == '':
                    raise QuickBooksClientException("Please enter query for CustomQuery. Exit...")
                logging.info("Input Custom Query: {0}".format(self.start_date))
                self.custom_request(input_query=query)
            else:
                if not (self.start_date and self.end_date):
                    raise QuickBooksClientException("Start date and End date are required for {endpoint}.")
                self.report_request(endpoint, start_date, end_date)
        else:
            self.count = self.get_count()  # total count of records for pagination
            if self.count == 0:
                logging.info(
                    "There are no returns for {0}".format(self.endpoint))
                self.data = []
            else:
                self.data_request()

    def refresh_access_token(self):
        """
        Get a new access token with refresh token
        """

        # Basic authorization header for refresh token
        url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

        results = None
        request_success = False
        while not request_success:
            # Request Parameters
            param = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token
            }

            r = requests.post(url, auth=HTTPBasicAuth(
                self.app_key, self.app_secret), data=param)
            results = r.json()

            # If access token was not fetched
            if "error" in results:
                if not self.new_refresh_token:
                    if os.path.isfile(statefile_in_path):
                        with open(statefile_in_path, 'r') as f:
                            statefile = json.load(f)
                        if "refresh_token" in statefile:
                            logging.info("Loading Refresh Token from State file.")
                            self.refresh_token = statefile["refresh_token"]
                            logging.info("State refresh token: {0}XXXX{1}".format(
                                self.refresh_token[0:4], self.refresh_token[-4:]))
                    self.new_refresh_token = True

                else:
                    raise QuickBooksClientException("Failed to refresh access token, please re-authorize credentials.")
            else:
                request_success = True

        self.access_token = results["access_token"]
        self.refresh_token = results["refresh_token"]
        logging.info("Access Token Granted.")
        self.write_tokens_to_manifest()

        # Monitor if app has requested refresh token yet
        self.access_token_refreshed = True

    def get_count(self):
        """
        Fetch the number of records for the specified endpoint
        """

        # Request Parameters
        endpoint = self.endpoint
        url = "select count(*) from {0}".format(endpoint)
        encoded_url = self.url_encode(url)
        count_url = "{0}/{1}/query?query={2}".format(
            self.base_url, self.company_id, encoded_url)

        # Request the number of counts
        data = self._request(count_url)
        logging.info(data)

        total_counts = data["QueryResponse"]["totalCount"]
        logging.info("Total Number of Records for {0}: {1}".format(
            endpoint, total_counts))

        return total_counts

    @staticmethod
    def url_encode(query):
        """
        URL encoded the query parameter
        """
        out = url_parse.quote_plus(query)
        return out

    def _request(self, url):
        """
        Handles Request
        """
        results = None
        request_success = False
        while not request_success:
            headers = {
                "Authorization": "Bearer " + self.access_token,
                "Accept": "application/json"
            }
            logging.info('Requesting: {}'.format(url))
            data = requesting.get(url, headers=headers)

            # Outputting IntuitID
            logging.info(data.headers)
            try:
                results = json.loads(data.text)

            except json.decoder.JSONDecodeError as e:
                raise QuickBooksClientException(f"Cannot decode response: {data.text}") from e

            if "fault" in results or "Fault" in results:
                if not self.access_token_refreshed:
                    logging.info("Refreshing Access Token")
                    self.refresh_access_token()
                else:
                    logging.error('Response Headers: {}'.format(data.headers))
                    raise QuickBooksClientException(data.text)
            else:
                request_success = True

        if not results:
            raise QuickBooksClientException("Unable to fetch results.")
        return results

    def data_request(self):
        """
        Handles Request Parameters and Pagination
        """

        num_of_run = 0

        while self.startposition <= self.count:
            # Query Parameters
            # Custom query for Class endpoint
            if self.endpoint == 'Class':

                query = "SELECT * FROM {0} WHERE Active IN (true, false) STARTPOSITION {1} MAXRESULTS {2}".format(
                    self.endpoint, self.startposition, self.maxresults)

            else:

                query = "SELECT * FROM {0} STARTPOSITION {1} MAXRESULTS {2}".format(
                    self.endpoint, self.startposition, self.maxresults)

            logging.info("Request Query: {0}".format(query))
            encoded_query = self.url_encode(query)
            url = "{0}/{1}/query?query={2}".format(
                self.base_url, self.company_id, encoded_query)

            # Requests and concatenating results into class's data variable
            results = self._request(url)

            # If API returns error, raise exception and terminate application
            if "fault" in results or "Fault" in results:
                raise Exception(results)

            data = results["QueryResponse"][self.endpoint]

            # Concatenate with exist extracted data
            self.data = self.data + data

            # Handling pagination paramters
            self.startposition += self.maxresults
            num_of_run += 1

        logging.info("Number of Requests: {0}".format(num_of_run))

    def custom_request(self, input_query):
        """
        Handles Request Parameters and Pagination
        """

        # Query Parameters
        query = "{0}".format(input_query)

        logging.info("Request Query: {0}".format(query))
        encoded_query = self.url_encode(query)
        url = "{0}/{1}/query?query={2}".format(
            self.base_url, self.company_id, encoded_query)

        # Requests and concatenating results into class's data variable
        results = self._request(url)

        # If API returns error, raise exception and terminate application
        if "fault" in results or "Fault" in results:
            raise Exception(results)

        data = results["QueryResponse"]

        # Concatenate with exist extracted data
        self.data = data

    def report_request(self, endpoint, start_date, end_date, class_object=None):
        """
        API request for Report Endpoint
        """

        if start_date == "":
            date_param = ""

            # For GeneralLedger ONLY
            if endpoint == "GeneralLedger":
                date_param = "?columns=klass_name,account_name,account_num,chk_print_state,create_by,create_date," \
                             "cust_name,doc_num,emp_name,inv_date,is_adj,is_ap_paid,is_ar_paid,is_cleared,item_name," \
                             "last_mod_by,last_mod_date,memo,name,quantity,rate,split_acc,tx_date,txn_type,vend_name," \
                             "net_amount,tax_amount,tax_code,dept_name,subt_nat_amount,rbal_nat_amount,debt_amt," \
                             "credit_amt "
        else:

            startdate = (dateparser.parse(start_date)).strftime("%Y-%m-%d")
            enddate = (dateparser.parse(end_date)).strftime("%Y-%m-%d")

            if startdate > enddate:
                raise Exception(
                    "Please validate your date parameter for {0}".format(endpoint))

            date_param = "?start_date={0}&end_date={1}".format(
                startdate, enddate)

            # For GeneralLedger ONLY
            if endpoint == "GeneralLedger":
                date_param = date_param + "&columns=dklass_name,account_name,account_num,chk_print_state," \
                                          "create_by,create_date,cust_name,doc_num,emp_name,inv_date,is_adj," \
                                          "is_ap_paid,is_ar_paid," \
                                          "is_cleared,item_name,last_mod_by,last_mod_date,memo,name,quantity,rate," \
                                          "split_acc,tx_date," \
                                          "txn_type,vend_name,net_amount,tax_amount,tax_code,dept_name," \
                                          "subt_nat_amount,rbal_nat_amount,debt_amt,credit_amt" \
 \
        url = "{0}/{1}/reports/{2}{3}".format(self.base_url,
                                              self.company_id, endpoint, date_param)
        if endpoint in self.reports_required_accounting_type:

            accrual_url = url + "&accounting_method=Accrual"
            cash_url = url + "&accounting_method=Cash"

            results = self._request(accrual_url)
            self.data = results

            results_2 = self._request(cash_url)
            self.data_2 = results_2

        else:

            results = self._request(url)
            self.data = results

    @staticmethod
    def flatten_json(y):
        """
        # Credits: https://gist.github.com/amirziai/2808d06f59a38138fa2d
        # flat out the json objects
        """
        out = {}

        def flatten(x, name=''):

            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '/')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '/')
                    i += 1
            else:
                out[name[:-1]] = x

        flatten(y)

        return out

    def write_tokens_to_manifest(self):
        temp = {"refresh_token": self.refresh_token, "access_token": self.access_token}
        logging.info("Saving tokens to statefile.")
        with open(statefile_out_path, "w") as f:
            json.dump(temp, f)
