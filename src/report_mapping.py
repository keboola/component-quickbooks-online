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
        self.columns = [
            "ReportName",
            "StartPeriod",
            "EndPeriod"
        ]
        self.primary_key = ["ReportName", "StartPeriod", "EndPeriod"]
        self.query = query
        self.accounting_type = accounting_type
        # Output
        self.data_out = []

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
                self.data_out = self.parse(
                    data["Rows"]["Row"], self.header, self.itr)
                self.columns = self.arrange_header(self.columns)
                self.output(self.endpoint, self.data_out, self.primary_key)
            except (KeyError, ValueError):
                logging.warning(
                    "Report contains no data. Please check if the selected period is correct.")

        elif endpoint == "CustomQuery":

            self.columns = ["query", "value"]
            self.data_out.append(self.columns)
            self.data_out.append("{0}".format(json.dumps(data)))
            self.pk = []
            self.output_1cell(self.endpoint, self.columns,
                              self.data_out, self.pk)

        else:  # Outputting tables which cannot parse

            for item in self.columns:
                self.data_out.append(self.header[item])

            self.data_out.append("{0}".format(json.dumps(data)))
            self.columns.append("value")
            self.output_1cell(self.endpoint, self.columns,
                              self.data_out, self.primary_key)

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
        row         - output json formatted row for one sub section within the table
        itr         - record of the number of recursion
        """
        try:
            data_out = []
            for i in data_in:
                temp_row = copy.deepcopy(row)
                row_name = "Col_{0}".format(itr)

                if ("type" not in i) and ("group" in i):

                    if row_name not in self.columns:
                        self.columns.append(row_name)
                        self.primary_key.append(row_name)

                    temp_out = []
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
                        if "rows" in i:
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
                    raise Exception(
                        "No type found within the row. Please validate the data.")

            return data_out

        except (KeyError, ValueError):
            logging.warning("Report contains no data., Please check if the selected period is correct.")

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
