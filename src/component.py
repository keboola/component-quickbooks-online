import logging

from keboola.component.base import ComponentBase

from mapping import Mapping
from client import QuickbooksClient
from report_mapping import ReportMapping

# configuration variables
KEY_COMPANY_ID = 'companyid'
KEY_ENDPOINT = 'endpoints'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = []

# QuickBooks Parameters
BASE_URL = "https://quickbooks.api.intuit.com"

# destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        # Input parameters
        endpoints = params.get(KEY_ENDPOINT)
        company_id = params.get(KEY_COMPANY_ID)
        logging.info(f'Company ID: {company_id}')

        # INITIALIZING QUICKBOOKS INSTANCES
        oauth = self.configuration.oauth_credentials
        quickbooks_param = QuickbooksClient(company_id=company_id, oauth=oauth)

        # Fetching reports for each configured endpoint
        for endpt in endpoints:
            # Endpoint parameters
            if "**" in endpt["endpoint"]:
                endpoint = endpt["endpoint"].split("**")[0]
                report_api_bool = True
            else:
                endpoint = endpt["endpoint"]
                report_api_bool = False

            # Phase 1: Request
            # Handling Quickbooks Requests
            quickbooks_param.fetch(
                endpoint=endpoint,
                report_api_bool=report_api_bool,
                start_date=endpt["start_date"],
                end_date=endpt["end_date"]
            )

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
                        ReportMapping(endpoint=endpoint, data=input_data,
                                      query=endpt["start_date"])

                    else:
                        if endpoint in quickbooks_param.reports_required_accounting_type:
                            input_data_2 = quickbooks_param.data_2

                            ReportMapping(
                                endpoint=endpoint, data=input_data, accounting_type="accrual")
                            ReportMapping(
                                endpoint=endpoint, data=input_data_2, accounting_type="cash")

                        else:
                            ReportMapping(endpoint=endpoint, data=input_data)
                else:
                    Mapping(endpoint=endpoint, data=input_data)


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


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(2)
