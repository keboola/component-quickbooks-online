import os
import logging
import csv
import json
import pandas as pd
import copy

# destination to fetch and output files
cwd_parent = os.path.dirname(os.getcwd())
DEFAULT_FILE_INPUT = os.path.join(cwd_parent, "data/in/tables/")
DEFAULT_FILE_DESTINATION = os.path.join(cwd_parent, "data/out/tables/")


class ReportMapping:
    """
    Parser dedicated for Report endpoint
    """

    def __init__(self, endpoint, data, query='', accounting_type=''):
        # Parameters
        self.endpoint = endpoint
        self.data = data
        self.header = self.construct_header(data)
        self.query = query
        self.accounting_type = accounting_type
        # Output
        self.data_out = []

        self.check_no_report_data(data)

        # Run
        report_cant_parse = [
            "CashFlow",
            "ProfitAndLossDetail",
            "TransactionList",
            "GeneralLedger",
            "TrialBalance"
        ]

        if endpoint not in report_cant_parse:

            try:
                self.itr = 1
                if "SummarizeColumnsBy" in data["Header"]:
                    if endpoint == "ProfitAndLoss":
                        self.data_out = self.parse_summarized(data)
                        self.columns = self.arrange_header(self.columns)
                        self.output(self.endpoint, self.data_out, self.primary_key)
                else:
                    self.columns = ["ReportName", "StartPeriod", "EndPeriod"]
                    self.primary_key = ["ReportName", "StartPeriod", "EndPeriod"]
                    self.data_out = self.parse(data["Rows"]["Row"], self.header, self.itr)
                    self.columns = self.arrange_header(self.columns)
                    self.output(self.endpoint, self.data_out, self.primary_key)
            except (KeyError, ValueError) as ex:
                logging.error(f"It caused an error while parsing! Message: {ex}")

        elif endpoint == "CustomQuery":

            self.columns = ["query", "value"]
            self.data_out.append(self.columns)
            self.data_out.append("{0}".format(json.dumps(data)))
            self.pk = []
            self.output_1cell(self.endpoint, self.columns,
                              self.data_out, self.pk)

        else:  # Outputting tables which cannot parse
            self.columns = ["ReportName", "StartPeriod", "EndPeriod"]
            self.primary_key = ["ReportName", "StartPeriod", "EndPeriod"]

            for item in self.columns:
                self.data_out.append(self.header[item])

            self.data_out.append("{0}".format(json.dumps(data)))
            self.columns.append("value")
            self.output_1cell(self.endpoint, self.columns,
                              self.data_out, self.primary_key)

    @staticmethod
    def check_no_report_data(data):
        """
        Check if the report contains no data
        """

        if 'Header' in data:
            if 'Option' in data['Header']:
                for option in data['Header']['Option']:
                    if option['Name'] == 'NoReportData':
                        no_report_data = json.loads(option['Value'])
                        if no_report_data:
                            raise Exception("No data found in the report. Please check the selected period.")

    @staticmethod
    def construct_header(data):
        """
        Constructing the base columns(Headers) for output
        *** Endpoint Report specific ***
        """

        if "Header" not in data:

            raise Exception("Header is missing. Unable to parse request.")

        else:

            temp = data["Header"]
            json_out = {
                "Time": temp["Time"],
                "ReportName": temp["ReportName"],
                "StartPeriod": temp["StartPeriod"],
                "EndPeriod": temp["EndPeriod"]
            }

        return json_out

    @staticmethod
    def arrange_header(columns):
        """
        Arrange the column headers in order
        """

        if columns.index("value") != (len(columns) - 1):
            # If "value" is not at the end of the row index
            columns.remove('value')

        if 'value' not in columns:
            # append the value back into the column if it does not exist
            columns.append("value")

        return columns

    # TODO: Tohle je vubec peklo. Ten Quickbooks muze byt ruzne nastaveny, a podle nastaveni se to chova ruzne.
    # Tady by to chtelo trochu peci, protoze to uz neni na miru jednomu klientovi.
    def parse(self, data_in, row, itr):
        """
        Main parser for rows
        Params:
        data_in     - input data for parser
        row         - output json formatted row for one subsection within the table
        itr         - record of the number of recursion
        """
        try:
            data_out = []
            temp_out = []
            for i in data_in:
                temp_row = copy.deepcopy(row)
                row_name = "Col_{0}".format(itr)

                if ("type" not in i) and ("group" in i):

                    if row_name not in self.columns:
                        self.columns.append(row_name)
                        self.primary_key.append(row_name)

                    row[row_name] = i["group"]
                    row["Col_{0}".format(itr + 1)] = i["ColData"][0]["value"]
                    row["value"] = i["ColData"][1]["value"]
                    temp_out = [row]
                    data_out = data_out + temp_out

                elif i["type"] == "Section":

                    if row_name not in self.columns:
                        self.columns.append(row_name)
                        self.primary_key.append(row_name)

                    # Use Group if Header is not found as column values
                    if "Header" in i:
                        row[row_name] = i["Header"]["ColData"][0]["value"]
                        # Recursion when type data is not found
                        temp_out = self.parse(i["Rows"]["Row"], row, itr + 1)

                    elif "group" in i:

                        # Column name
                        row[row_name] = i["group"]

                        # Row value , assuming no more recursion
                        row["Col_{0}".format(
                            itr + 1)] = i["Summary"]["ColData"][0]["value"]
                        row["value"] = i["Summary"]["ColData"][1]["value"]
                        temp_out = [row]

                        if "Col_{0}".format(itr + 1) not in self.columns:
                            self.columns.append("Col_{0}".format(itr + 1))
                            self.primary_key.append("Col_{0}".format(itr + 1))

                    data_out = data_out + temp_out  # Append data back to section

                elif (i["type"] == "Data") or ("ColData" in i):

                    if row_name not in self.columns:
                        self.columns.append(row_name)
                        self.primary_key.append(row_name)
                    temp_row[row_name] = i["ColData"][0]["value"]

                    row_value = "value"
                    if row_value not in self.columns:
                        self.columns.append(row_value)
                    temp_row[row_value] = i["ColData"][1]["value"]

                    data_out.append(temp_row)

                else:
                    raise Exception("No type found within the row. Please validate the data.")

            return data_out

        except (KeyError, ValueError) as e:
            logging.warning(f"Parsing error - {type(e).__name__} occurred. Details: {e}")

    def parse_summarized(self, data):
        """
        Parser for summarized data with expanded structure
        Params:
        data - input data for parser in the new structure
        """
        try:
            header = data['Header']
            columns = data['Columns']['Column']
            rows = data['Rows']['Row']
            currency = header['Currency']
            summarize_by = header['SummarizeColumnsBy']

            # Initialize columns and primary keys
            self.columns = ['ReportName', 'StartPeriod', 'EndPeriod', 'Currency', 'Summarize_columns_by',
                            'Summarize_columns_value', 'value']
            self.primary_key = ['ReportName', 'StartPeriod', 'EndPeriod', 'Currency', 'Summarize_columns_by',
                                'Summarize_columns_value']

            def parse_row(row_data, current_row, itr):
                local_data_out = []  # Use local list to gather data within this function
                for item in row_data:
                    temp_row = copy.deepcopy(current_row)
                    row_name = "Col_{0}".format(itr)

                    if 'Header' in item and 'ColData' in item['Header']:
                        if row_name not in self.columns:
                            self.columns.append(row_name)
                            self.primary_key.append(row_name)

                        temp_row[row_name] = item['Header']['ColData'][0]['value']
                        temp_out = parse_row(item['Rows']['Row'], temp_row, itr + 1)
                        local_data_out.extend(temp_out)

                    elif 'ColData' in item:
                        account_col = "Col_{0}".format(itr)
                        for idx, col in enumerate(columns[1:], start=1):  # Skip the first column (Account)
                            if account_col not in self.columns:
                                self.columns.append(account_col)
                                self.primary_key.append(account_col)

                            summarize_column_value = col['ColTitle']
                            temp_row = copy.deepcopy(current_row)
                            temp_row[account_col] = item['ColData'][0]['value']
                            temp_row['Summarize_columns_value'] = summarize_column_value
                            temp_row['value'] = item['ColData'][idx]['value'] if idx < len(item['ColData']) else ""
                            temp_row['Summarize_columns_by'] = summarize_by
                            local_data_out.append(temp_row)

                    elif 'Summary' in item:
                        account_col = "Col_{0}".format(itr)
                        for idx, col in enumerate(columns[1:], start=1):  # Skip the first column (Account)
                            if account_col not in self.columns:
                                self.columns.append(account_col)
                                self.primary_key.append(account_col)

                            summarize_column_value = col['ColTitle']
                            temp_row = copy.deepcopy(current_row)
                            temp_row[account_col] = item['Summary']['ColData'][0]['value']
                            temp_row['Summarize_columns_value'] = summarize_column_value
                            temp_row['value'] = item['Summary']['ColData'][idx]['value'] if idx < len(
                                item['Summary']['ColData']) else ""
                            temp_row['Summarize_columns_by'] = summarize_by
                            local_data_out.append(temp_row)
                    else:
                        raise Exception("Unexpected data structure found within the row.")

                return local_data_out  # Ensure the function always returns a list

            initial_row = {
                'ReportName': header['ReportName'],
                'StartPeriod': header['StartPeriod'],
                'EndPeriod': header['EndPeriod'],
                'Currency': currency
            }

            data_out = parse_row(rows, initial_row, 1)

            return data_out

        except (KeyError, ValueError) as e:
            logging.warning(f"Parsing error - {type(e).__name__} occurred. Details: {e}")

    @staticmethod
    def produce_manifest(file_name, primary_key):
        """
        Dummy function to return header per file type.
        """

        file = DEFAULT_FILE_DESTINATION + str(file_name) + ".manifest"

        manifest_template = {
            "incremental": bool(True)
        }

        manifest = manifest_template
        manifest["primary_key"] = primary_key

        try:
            with open(file, 'w') as file_out:
                json.dump(manifest, file_out)
                logging.info(
                    "Output manifest file ({0}) produced.".format(file_name))
        except Exception as e:
            logging.error("Could not produce output file manifest.")
            logging.error(e)

    def output(self, endpoint, data, pk):
        """
        Outputting JSON
        """

        temp_df = pd.DataFrame(data)
        if self.accounting_type == '':
            filename = endpoint + ".csv"
        else:
            filename = "{0}_{1}.csv".format(endpoint, self.accounting_type)

        logging.info("Outputting {0}...".format(filename))
        file_out_path = DEFAULT_FILE_DESTINATION + filename
        print(f"Saving file to: {file_out_path}")
        temp_df.to_csv(file_out_path,
                       index=False, columns=self.columns)
        self.produce_manifest(filename, pk)

    def output_1cell(self, endpoint, columns, data, pk):
        """
        Output everything into one cell
        """

        # Construct output filename
        if self.accounting_type == '':
            filename = endpoint + ".csv"
        else:
            filename = "{0}_{1}.csv".format(endpoint, self.accounting_type)

        # if file exist, not outputing column header
        if os.path.isfile(DEFAULT_FILE_DESTINATION + filename):
            data_out = [data]
        else:
            data_out = [columns, data]

        with open(DEFAULT_FILE_DESTINATION + filename, "a") as f:
            writer = csv.writer(f)
            writer.writerows(data_out)
        f.close()
        logging.info("Outputting {0}... ".format(filename))
        self.produce_manifest(filename, pk)
