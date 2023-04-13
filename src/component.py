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
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

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

            if endpoint == "ProfitAndLossQuery**":
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
        results_accrual = []

        def save_result(class_name, name, value, obj_type, obj_group, method):
            res_dict = {
                "class": class_name,
                "name": name,
                "value": value,
                "obj_type": obj_type,
                "obj_group": obj_group,
                "start_date": self.start_date,
                "end_date": self.end_date
            }
            if method == "cash":
                results_cash.append(res_dict)
            elif method == "accrual":
                results_accrual.append(res_dict)
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

        params = {}
        summarize = False
        if self.summarize_column_by:
            summarize = True
            params["summarize_column_by"] = self.summarize_column_by

        for class_name in classes:
            logging.info(f"Processing class: {class_name}")

            self.fetch(quickbooks_param=quickbooks_param, endpoint="ProfitAndLoss", report_api_bool=True, query="",
                       params=params)

            summarize_by = quickbooks_param.data['Header'].get("SummarizeColumnsBy", False)

            if not summarize_by:

                report_accrual = quickbooks_param.data['Rows']['Row']
                report_cash = quickbooks_param.data_2['Rows']['Row']

                for obj in report_cash:
                    process_object(obj, class_name, method="cash")
                for obj in report_accrual:
                    process_object(obj, class_name, method="accrual")

            else:

                report_cash_data = quickbooks_param.data_2
                report_accrual_data = quickbooks_param.data

                header = quickbooks_param.data['Header']
                summarize_by = header['SummarizeColumnsBy']
                currency = header['Currency']

                results_cash = self.preprocess_dict(report_cash_data,
                                                    class_name,
                                                    summarize_by=summarize_by,
                                                    currency=currency)

                results_accrual = self.preprocess_dict(report_accrual_data,
                                                       class_name,
                                                       summarize_by=summarize_by,
                                                       currency=currency)

        if summarize:
            suffix = "_"+str(summarize_by)
        else:
            suffix = ""

        self.save_pnl_report_to_csv(table_name=f"ProfitAndLossQuery_cash{suffix}.csv", results=results_cash,
                                    summarize=summarize)
        self.save_pnl_report_to_csv(table_name=f"ProfitAndLossQuery_accrual{suffix}.csv", results=results_accrual,
                                    summarize=summarize)

    def preprocess_dict(self, obj, class_name, summarize_by, currency):
        results = []

        rows = obj['Rows']['Row']
        cols = obj['Columns']['Column']
        group_by = []
        for col in cols:
            group_by.append(col['ColTitle'])

        def save_result(_class_name, name, value, obj_type, obj_group, category_name="", category_id=""):
            res_dict = {
                "class": _class_name,
                "name": name,
                "value": value,
                "obj_type": obj_type,
                "obj_group": obj_group,
                "category_name": category_name,
                "category_id": category_id,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "summarize_by": summarize_by,
                "currency": currency
            }
            results.append(res_dict)

        def process_coldata(obj, obj_type, obj_group):
            col_data = obj["ColData"]
            category_name = col_data[0]["value"]
            category_id = col_data[0]["id"]
            for name, val in zip(group_by, col_data):
                if name:
                    save_result(class_name, name, val['value'], obj_type, obj_group, category_name, category_id)

        def process_object(obj, class_name):
            obj_type = obj.get("type", "")
            obj_group = obj.get("group", "")

            if "ColData" in obj:
                process_coldata(obj, obj_type, obj_group)

            if "Header" in obj:
                header_name = obj["Header"]["ColData"][0]["value"]
                header_value = obj["Header"]["ColData"][1]["value"]
                save_result(class_name, header_name, header_value, obj_type, obj_group)

            if "Summary" in obj:
                summary_name = obj["Summary"]["ColData"][0]["value"]
                summary_value = obj["Summary"]["ColData"][1]["value"]
                save_result(class_name, summary_name, summary_value, obj_type, obj_group)

            if "Rows" in obj:
                inner_objects = obj["Rows"]["Row"]
                for inner_object in inner_objects:
                    process_object(inner_object, class_name)

        for row in rows:
            process_object(row, class_name)

        return results

    def save_pnl_report_to_csv(self, table_name: str, results: list, summarize: bool):

        if not summarize:
            pk = ["class", "name", "obj_type", "start_date", "end_date"]
            columns = ["class", "name", "value", "obj_type", "obj_group", "start_date", "end_date"]
        else:
            pk = ["class", "name", "obj_type", "category_id", "start_date", "end_date"]
            columns = ["class", "name", "value", "obj_type", "obj_group", "category_name", "category_id",
                       "start_date", "end_date", "summarize_by", "currency"]

        table_def = self.create_out_table_definition(table_name, primary_key=pk, incremental=self.incremental)

        with ElasticDictWriter(table_def.full_path, columns) as wr:
            wr.writeheader()
            wr.writerows(results)

        self.write_manifest(table_def)

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
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(2)
