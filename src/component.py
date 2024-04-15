import logging

from mapping import Mapping
from client import QuickbooksClient, QuickBooksClientException
from report_mapping import ReportMapping
from datetime import date
from dateutil.relativedelta import relativedelta

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException  # noqa
from keboola.csvwriter import ElasticDictWriter

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

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        # Input parameters
        endpoints = params.get(KEY_ENDPOINTS)
        reports = params.get(KEY_REPORTS)
        company_id = params.get(KEY_COMPANY_ID, [])
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
        statefile = self.get_state_file()
        if statefile.get("#refresh_token", {}):
            refresh_token = statefile.get("#refresh_token")
            access_token = statefile.get("#access_token")
            logging.info("Loaded tokens from statefile.")
        else:
            refresh_token = oauth["data"]["refresh_token"]
            access_token = oauth["data"]["access_token"]
            logging.info("No oauth data found in statefile. Using data from Authorization.")
        if params.get("sandbox"):
            sandbox = True
            logging.info("Sandbox environment enabled.")
        else:
            sandbox = False

        destination_params = params.get(KEY_GROUP_DESTINATION)
        if destination_params.get(KEY_LOAD_TYPE, False) == "incremental_load":
            self.incremental = True
        else:
            self.incremental = False
        logging.info(f"Load type incremental set to: {self.incremental}")

        self.summarize_column_by = params.get(KEY_SUMMARIZE_COLUMN_BY) if params.get(
            KEY_SUMMARIZE_COLUMN_BY) else self.summarize_column_by

        self.write_state_file({
            "#refresh_token": refresh_token,
            "#access_token": access_token
        })

        quickbooks_param = QuickbooksClient(company_id=company_id, refresh_token=refresh_token,
                                            access_token=access_token, oauth=oauth, sandbox=sandbox)

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
