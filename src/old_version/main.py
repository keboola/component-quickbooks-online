from old_version.report_mapping import report_mapping
from old_version.mapping import mapping
from old_version.quickbooks import quickbooks
from keboola import docker
import logging_gelf.handlers
import logging_gelf.formatters  # noqa
import urllib.parse as url_parse  # noqa
import copy  # noqa
import hashlib  # noqa
import requests  # noqa
import pandas as pd  # noqa
import json  # noqa
import csv  # noqa
import logging
import os
import sys  # noqa
"__author__ = 'Leo Chan'"
"__credits__ = 'Keboola 2017'"
"__project__ = 'kbc_quickbooks'"

"""
Python 3 environment
"""


# Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

logger = logging.getLogger()
logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
    host=os.getenv('KBC_LOGGER_ADDR'),
    port=int(os.getenv('KBC_LOGGER_PORT'))
)
logging_gelf_handler.setFormatter(
    logging_gelf.formatters.GELFFormatter(null_character=True))
logger.addHandler(logging_gelf_handler)

# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])

# Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
logging.info(f'CHECKING PARAMS: {params}')
COMPANY_ID = cfg.get_parameters()["companyid"]
endpoints = cfg.get_parameters()["endpoints"]

# Get proper list of tables
cfg = docker.Config('/data/')
in_tables = cfg.get_input_tables()
out_tables = cfg.get_expected_output_tables()
logging.info("IN tables mapped: "+str(in_tables))
logging.info("OUT tables mapped: "+str(out_tables))

# QuickBooks Parameters
BASE_URL = "https://quickbooks.api.intuit.com"

# destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"


def get_tables(in_tables):
    """
    Evaluate input and output table names.
    Only taking the first one into consideration!
    """

    # input file
    table = in_tables[0]
    in_name = table["full_path"]
    in_destination = table["destination"]
    logging.info("Data table: " + str(in_name))
    logging.info("Input table source: " + str(in_destination))

    return in_name


def get_output_tables(out_tables):
    """
    Evaluate output table names.
    Only taking the first one into consideration!
    """

    # input file
    table = out_tables[0]
    in_name = table["full_path"]
    in_destination = table["source"]
    logging.info("Data table: " + str(in_name))
    logging.info("Input table source: " + str(in_destination))

    return in_name


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


def main():
    """
    Main execution script.
    """

    # INITIALIZING QUICKBOOKS INSTANCES
    quickbooks_param = quickbooks(company_id=COMPANY_ID)

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
            # df = pd.DataFrame(input_data)
            # df.to_csv(DEFAULT_FILE_DESTINATION+endpoint+".csv", index=False)
            pass
        else:
            logging.info(
                "Report API Template Enable: {0}".format(report_api_bool))
            if report_api_bool:
                if endpoint == "CustomQuery":
                    report_mapping(endpoint=endpoint, data=input_data,
                                   query=endpt["start_date"])
                else:
                    if endpoint in quickbooks_param.reports_required_accounting_type:
                        input_data_2 = quickbooks_param.data_2

                        report_mapping(
                            endpoint=endpoint, data=input_data, accounting_type="accrual")
                        report_mapping(
                            endpoint=endpoint, data=input_data_2, accounting_type="cash")

                    else:
                        report_mapping(endpoint=endpoint, data=input_data)
            else:
                mapping(endpoint=endpoint, data=input_data)

    return


if __name__ == "__main__":

    main()
    """
    # TEST SCRIPTS
    with open("trialbalance.json", 'r') as f:
        data = json.load(f)
    print(data)

    # data_in = data["QueryResponse"]["BillPayment"]
    # data_in = data["QueryResponse"]["Budget"]
    data_in = data

    report_mapping(data=data, endpoint="TrialBalance")
    # mapping(data=data_in, endpoint="Budget")
    # mapping(data=data_in, endpoint="BillPayment")
    """
    logging.info("Done.")
