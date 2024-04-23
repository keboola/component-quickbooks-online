import os
import logging
import datetime
import requests
import backoff
import json


from mapping import Mapping
from client import QuickbooksClient, QuickBooksClientException
from report_mapping import ReportMapping
from datetime import date
from dateutil.relativedelta import relativedelta

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException  # noqa

URL_SUFFIXES = {"CURRENT_STACK": os.environ.get('KBC_STACKID', 'connection.keboola.com').replace('connection', '')}


# configuration variables
KEY_COMPANY_ID = 'companyid'
KEY_ENDPOINTS = 'endpoints'
KEY_REPORTS = 'reports'
GROUP_DATE_SETTINGS = 'date_settings'
KEY_START_DATE = 'start_date'
KEY_END_DATE = 'end_date'
KEY_GROUP_DESTINATION = 'destination'
KEY_LOAD_TYPE = 'load_type'
KEY_SUMMARIZE_COLUMN_BY = 'summarize_column_by'
KEY_SANDBOX = 'sandbox'


# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_COMPANY_ID, KEY_ENDPOINTS, KEY_REPORTS, KEY_GROUP_DESTINATION]

# QuickBooks Parameters
BASE_URL = "https://quickbooks.api.intuit.com"


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self.summarize_column_by = None
        self.incremental = None
        self.end_date = None
        self.start_date = None
        self.refresh_token = None
        self.access_token = None

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        # Input parameters
        endpoints = params.get(KEY_ENDPOINTS)
        reports = params.get(KEY_REPORTS)
        company_id = params.get(KEY_COMPANY_ID, []).replace(" ", "")
        endpoints.extend(reports)

        if params.get(GROUP_DATE_SETTINGS):
            date_settings = params.get(GROUP_DATE_SETTINGS)
            start_date = date_settings.get(KEY_START_DATE)
            end_date = date_settings.get(KEY_END_DATE)
        else:
            start_date = self.start_date
            end_date = self.end_date

        self.start_date = self.process_date(start_date)
        self.end_date = self.process_date(end_date)

        logging.info(f'Company ID: {company_id}')

        oauth = self.configuration.oauth_credentials
        self.refresh_token, self.access_token = self.get_tokens(oauth)

        sandbox = self.configuration.parameters.get(KEY_SANDBOX, False)
        if sandbox:
            logging.info("Sandbox environment enabled.")

        destination_params = params.get(KEY_GROUP_DESTINATION)
        if destination_params.get(KEY_LOAD_TYPE, False) == "incremental_load":
            self.incremental = True
        else:
            self.incremental = False
        logging.info(f"Load type incremental set to: {self.incremental}")

        self.summarize_column_by = params.get(KEY_SUMMARIZE_COLUMN_BY) if params.get(
            KEY_SUMMARIZE_COLUMN_BY) else self.summarize_column_by

        self.write_state_file({
            "tokens":
                {"ts": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                 "#refresh_token": self.refresh_token,
                 "#access_token": self.access_token}
        })

        quickbooks_param = QuickbooksClient(company_id=company_id, refresh_token=self.refresh_token,
                                            access_token=self.access_token, oauth=oauth, sandbox=sandbox)

        if not sandbox:
            self.process_oauth_tokens(quickbooks_param)

        # Fetching reports for each configured endpoint
        for endpoint in endpoints:

            if "**" in endpoint:
                endpoint = endpoint.split("**")[0]
                report_api_bool = True
            else:
                endpoint = endpoint
                report_api_bool = False

            # Phase 1: Request
            # Handling Quickbooks Requests
            self.fetch(quickbooks_param=quickbooks_param, endpoint=endpoint, report_api_bool=report_api_bool)

            # Phase 2: Mapping
            # Translate Input JSON file into CSV with configured mapping
            # For different accounting_type,
            # input_data will be outputting Accrual Type
            # input_data_2 will be outputting Cash Type
            logging.info("Parsing API results...")
            input_data = quickbooks_param.data

            # if there are no data
            # output blank
            if len(input_data) == 0:
                pass
            else:
                logging.info(
                    "Report API Template Enable: {0}".format(report_api_bool))
                if report_api_bool:
                    if endpoint == "CustomQuery":
                        # Not implemented
                        ReportMapping(endpoint=endpoint, data=input_data,
                                      query=self.start_date)
                    else:
                        if endpoint in quickbooks_param.reports_required_accounting_type:
                            input_data_2 = quickbooks_param.data_2
                            ReportMapping(endpoint=endpoint, data=input_data, accounting_type="accrual")
                            ReportMapping(endpoint=endpoint, data=input_data_2, accounting_type="cash")
                        else:
                            ReportMapping(endpoint=endpoint, data=input_data)
                else:
                    Mapping(endpoint=endpoint, data=input_data)

    def get_tokens(self, oauth):

        try:
            refresh_token = oauth["data"]["refresh_token"]
            access_token = oauth["data"]["access_token"]
        except TypeError:
            raise UserException("OAuth data is not available.")

        statefile = self.get_state_file()
        if statefile.get("tokens", {}).get("ts"):
            ts_oauth = datetime.datetime.strptime(oauth["created"], "%Y-%m-%dT%H:%M:%S.%fZ")
            ts_statefile = datetime.datetime.strptime(statefile["tokens"]["ts"], "%Y-%m-%dT%H:%M:%S.%fZ")

            if ts_statefile > ts_oauth:
                refresh_token = statefile["tokens"].get("#refresh_token")
                access_token = statefile["tokens"].get("#access_token")
                logging.debug("Loaded tokens from statefile.")
            else:
                logging.debug("Using tokens from oAuth.")
        else:
            logging.warning("No timestamp found in statefile. Using oAuth tokens.")

        return refresh_token, access_token

    def process_oauth_tokens(self, client) -> None:
        """Uses Quickbooks client to get new tokens and saves them using API if they have changed since the last run."""
        new_refresh_token, new_access_token = client.get_new_refresh_token()
        if self.refresh_token != new_refresh_token:
            self.save_new_oauth_tokens(new_refresh_token, new_access_token)

            # We also save new tokens to class vars, so we can save them unencrypted if case statefile update fails
            # in update_config_state() method.
            self.refresh_token = new_refresh_token
            self.access_token = new_access_token

    def save_new_oauth_tokens(self, refresh_token: str, access_token: str) -> None:
        logging.debug("Saving new tokens to state using Keboola API.")

        try:
            encrypted_refresh_token = self.encrypt(refresh_token)
            encrypted_access_token = self.encrypt(access_token)
        except requests.exceptions.RequestException:
            logging.warning("Encrypt API is unavailable. Skipping token save at the beginning of the run.")
            return

        new_state = {
            "component": {
                "tokens":
                    {"ts": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                     "#refresh_token": encrypted_refresh_token,
                     "#access_token": encrypted_access_token}
            }}
        try:
            self.update_config_state(region="CURRENT_STACK",
                                     component_id=self.environment_variables.component_id,
                                     configurationId=self.environment_variables.config_id,
                                     state=new_state,
                                     branch_id=self.environment_variables.branch_id)
        except requests.exceptions.RequestException:
            logging.warning("Storage API (update config state)"
                            "is unavailable. Skipping token save at the beginning of the run.")
            return

    def _get_storage_token(self) -> str:
        token = self.configuration.parameters.get('#storage_token') or self.environment_variables.token
        if not token:
            raise UserException("Cannot retrieve storage token from env variables and/or config.")
        return token

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def encrypt(self, token: str) -> str:
        url = "https://encryption.keboola.com/encrypt"
        params = {
            "componentId": self.environment_variables.component_id,
            "projectId": self.environment_variables.project_id,
            "configId": self.environment_variables.config_id
        }
        headers = {"Content-Type": "text/plain"}

        response = requests.post(url,
                                 data=token,
                                 params=params,
                                 headers=headers)
        response.raise_for_status()
        return response.text

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def update_config_state(self, region, component_id, configurationId, state, branch_id='default'):
        if not branch_id:
            branch_id = 'default'

        url = f'https://connection{URL_SUFFIXES[region]}/v2/storage/branch/{branch_id}' \
              f'/components/{component_id}/configs/' \
              f'{configurationId}/state'

        parameters = {'state': json.dumps(state)}
        headers = {'Content-Type': 'application/x-www-form-urlencoded', 'X-StorageApi-Token': self._get_storage_token()}
        response = requests.put(url,
                                data=parameters,
                                headers=headers)
        response.raise_for_status()

    def fetch(self, quickbooks_param, endpoint, report_api_bool, query="", params=None):
        logging.info(f"Fetching endpoint {endpoint} with date rage: {self.start_date} - {self.end_date}")
        try:
            quickbooks_param.fetch(
                endpoint=endpoint,
                report_api_bool=report_api_bool,
                start_date=self.start_date,
                end_date=self.end_date,
                query=query if query else "",
                params=params
            )
        except QuickBooksClientException as e:
            raise UserException(e) from e

    @staticmethod
    def process_date(dt):
        """Checks if date is in valid format. If not, raises UserException. If None, returns None"""
        if not dt:
            return None

        dt_format = '%Y-%m-%d'
        today = date.today()
        if dt == "PrevMonthStart":
            result = today.replace(day=1) - relativedelta(months=1)
        elif dt == "PrevMonthEnd":
            result = today.replace(day=1) - relativedelta(days=1)
        else:
            try:
                date.fromisoformat(dt)
            except ValueError:
                raise UserException(f"Date {dt} is invalid. Valid types are: "
                                    f"PrevMonthStart, PrevMonthEnd or YYYY-MM-DD")
            return dt
        return result.strftime(dt_format)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
