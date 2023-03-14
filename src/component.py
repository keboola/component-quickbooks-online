import logging

from mapping import Mapping
from client import QuickbooksClient
from report_mapping import ReportMapping

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException  # noqa
from keboola.csvwriter import ElasticDictWriter

# configuration variables
KEY_COMPANY_ID = 'companyid'
KEY_ENDPOINT = 'endpoints'
KEY_START_DATE = 'start_date'
KEY_END_DATE = 'end_date'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_COMPANY_ID, KEY_ENDPOINT]

# QuickBooks Parameters
BASE_URL = "https://quickbooks.api.intuit.com"


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
        self.end_date = None
        self.start_date = None

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        # Input parameters
        endpoints = params.get(KEY_ENDPOINT)
        company_id = params.get(KEY_COMPANY_ID)
        self.start_date = params.get(KEY_START_DATE)
        self.end_date = params.get(KEY_END_DATE)
        logging.info(f'Company ID: {company_id}')

        # INITIALIZING QUICKBOOKS INSTANCES
        oauth = self.configuration.oauth_credentials
        statefile = self.get_state_file()
        if statefile.get("refresh_token", {}) and statefile.get("access_token", {}):
            refresh_token = statefile.get("refresh_token")
            access_token = statefile.get("access_token")
            logging.info("Loaded tokens from statefile.")
        else:
            refresh_token = oauth["data"]["refresh_token"]
            access_token = oauth["data"]["access_token"]
            logging.info("No oauth data found in statefile. Using data from Authorization.")
            self.write_state_file({
                "refresh_token": refresh_token,
                "access_token": access_token
            })
        if params.get("sandbox"):
            sandbox = True
            logging.info("Sandbox environment enabled.")
        else:
            sandbox = False

        quickbooks_param = QuickbooksClient(company_id=company_id, refresh_token=refresh_token,
                                            access_token=access_token, oauth=oauth, sandbox=sandbox)

        # Fetching reports for each configured endpoint
        for endpoint in endpoints:
            # Endpoint parameters

            if endpoint == "ProfitAndLossQuery":
                self.process_pnl_report(quickbooks_param=quickbooks_param)
                continue

            if "**" in endpoint:
                endpoint = endpoint.split("**")[0]
                report_api_bool = True
            else:
                endpoint = endpoint
                report_api_bool = False

            # Phase 1: Request
            # Handling Quickbooks Requests
            quickbooks_param.fetch(
                endpoint=endpoint,
                report_api_bool=report_api_bool,
                start_date=self.start_date,
                end_date=self.end_date
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

    def process_pnl_report(self, quickbooks_param):

        def process_coldata(obj, obj_type, obj_group):
            col_data = obj["ColData"]
            name = col_data[0]["value"]
            value = col_data[1]["value"]
            results.append({
                "class": class_name,
                "name": name,
                "value": value,
                "obj_type": obj_type,
                "obj_group": obj_group
            })

        def process_object(obj, class_name):
            obj_type = obj.get("type", "")
            obj_group = obj.get("group", "")

            if "ColData" in obj:
                process_coldata(obj, obj_type, obj_group)

            if "Header" in obj:
                header_name = obj["Header"]["ColData"][0]["value"]
                header_value = obj["Header"]["ColData"][1]["value"]
                results.append({
                    "class": class_name,
                    "name": header_name,
                    "value": header_value,
                    "obj_type": obj_type,
                    "obj_group": obj_group
                })

            if "Summary" in obj:
                summary_name = obj["Summary"]["ColData"][0]["value"]
                summary_value = obj["Summary"]["ColData"][1]["value"]
                results.append({
                    "class": class_name,
                    "name": summary_name,
                    "value": summary_value,
                    "obj_type": obj_type,
                    "obj_group": obj_group
                })

            if "Rows" in obj:
                inner_objects = obj["Rows"]["Row"]
                for inner_object in inner_objects:
                    process_object(inner_object, class_name)

        quickbooks_param.fetch(
            endpoint="CustomQuery",
            report_api_bool=True,
            start_date=self.start_date,
            end_date=self.end_date,
            query="select * from Class"
        )

        query_result = quickbooks_param.data
        classes = [item["Name"] for item in query_result.get("Class", []) if item.get("Name")]
        logging.info(f"Found Classes: {classes}")

        if not len(classes) == query_result['totalCount']:
            raise NotImplementedError("Classes paging not implemented yet.")

        for class_name in classes:
            logging.info(f"Processing class: {class_name}")

            quickbooks_param.fetch(
                endpoint="ProfitAndLoss",
                report_api_bool=True,
                start_date=self.start_date,
                end_date=self.end_date
            )

            class_pnl = self.create_out_table_definition("ProfitAndLossQuery.csv")
            report = quickbooks_param.data['Rows']['Row']
            results = []

            for obj in report:
                process_object(obj, class_name)

            with ElasticDictWriter(class_pnl.full_path, ["class", "name", "value", "obj_type", "obj_group"]) as wr:
                wr.writeheader()
                wr.writerows(results)

            self.write_manifest(class_pnl)


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
