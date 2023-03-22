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
        start_date = params.get(KEY_START_DATE)
        end_date = params.get(KEY_END_DATE)
        self.start_date = self.process_date(start_date)
        self.end_date = self.process_date(end_date)

        logging.info(f'Company ID: {company_id}')

        # INITIALIZING QUICKBOOKS INSTANCES
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

        self.write_state_file({
            "#refresh_token": refresh_token,
            "#access_token": access_token
        })

        quickbooks_param = QuickbooksClient(company_id=company_id, refresh_token=refresh_token,
                                            access_token=access_token, oauth=oauth, sandbox=sandbox)

        # Fetching reports for each configured endpoint
        for endpoint in endpoints:

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
        results_cash = []
        result_accrual = []

        def save_result(class_name, name, value, obj_type, obj_group, method):
            res_dict = {
                "class": class_name,
                "name": name,
                "value": value,
                "obj_type": obj_type,
                "obj_group": obj_group
            }
            if method == "cash":
                results_cash.append(res_dict)
            elif method == "accrual":
                result_accrual.append(res_dict)
            else:
                raise UserException(f"Unknown accounting method: {method}")

        def process_coldata(obj, obj_type, obj_group, method):
            col_data = obj["ColData"]
            name = col_data[0]["value"]
            value = col_data[1]["value"]
            save_result(class_name, name, value, obj_type, obj_group, method)

        def process_object(obj, class_name, method):
            obj_type = obj.get("type", "")
            obj_group = obj.get("group", "")

            if "ColData" in obj:
                process_coldata(obj, obj_type, obj_group, method)

            if "Header" in obj:
                header_name = obj["Header"]["ColData"][0]["value"]
                header_value = obj["Header"]["ColData"][1]["value"]
                save_result(class_name, header_name, header_value, obj_type, obj_group, method)

            if "Summary" in obj:
                summary_name = obj["Summary"]["ColData"][0]["value"]
                summary_value = obj["Summary"]["ColData"][1]["value"]
                save_result(class_name, summary_name, summary_value, obj_type, obj_group, method)

            if "Rows" in obj:
                inner_objects = obj["Rows"]["Row"]
                for inner_object in inner_objects:
                    process_object(inner_object, class_name, method)

        self.fetch(quickbooks_param=quickbooks_param, endpoint="CustomQuery", report_api_bool=True,
                   query="select * from Class")

        query_result = quickbooks_param.data
        classes = [item["Name"] for item in query_result.get("Class", []) if item.get("Name")]
        logging.info(f"Found Classes: {classes}")

        if not len(classes) == query_result['totalCount']:
            raise NotImplementedError("Classes paging is not implemented.")

        for class_name in classes:
            logging.info(f"Processing class: {class_name}")

            self.fetch(quickbooks_param=quickbooks_param, endpoint="ProfitAndLoss", report_api_bool=True, query="")

            class_pnl_cash = self.create_out_table_definition("ProfitAndLossQuery_cash.csv")
            class_pnl_accrual = self.create_out_table_definition("ProfitAndLossQuery_accrual.csv")
            report_accrual = quickbooks_param.data['Rows']['Row']
            report_cash = quickbooks_param.data['Rows']['Row']

            for obj in report_cash:
                process_object(obj, class_name, method="cash")
            for obj in report_accrual:
                process_object(obj, class_name, method="accrual")

        with ElasticDictWriter(class_pnl_cash.full_path, ["class", "name", "value", "obj_type", "obj_group"]) as wr:
            wr.writeheader()
            wr.writerows(results_cash)

        with ElasticDictWriter(class_pnl_accrual.full_path, ["class", "name", "value", "obj_type", "obj_group"]) as wr:
            wr.writeheader()
            wr.writerows(result_accrual)

        self.write_manifest(class_pnl_cash)
        self.write_manifest(class_pnl_accrual)

    def fetch(self, quickbooks_param, endpoint, report_api_bool, query=""):
        logging.info(f"Fetching endpoint {endpoint} with date rage: {self.start_date} - {self.end_date}")
        try:
            quickbooks_param.fetch(
                endpoint=endpoint,
                report_api_bool=report_api_bool,
                start_date=self.start_date,
                end_date=self.end_date,
                query=query if query else ""
            )
        except QuickBooksClientException as e:
            raise UserException(e) from e

    def process_date(self, dt):
        """Checks if date is in valid format. If not, raises UserException."""
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
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(2)
