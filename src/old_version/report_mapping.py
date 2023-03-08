from keboola import docker # noqa
import logging_gelf.handlers # noqa
import logging_gelf.formatters # noqa
import copy
import hashlib # noqa
import uuid # noqa
import urllib.parse as url_parse # noqa
import requests # noqa
import pandas as pd
import json
import csv
import logging
import os # noqa
import sys # noqa
"__author__ = 'Leo Chan'"
"__credits__ = 'Keboola 20`17'"
"__project__ = 'kbc_quickbooks'"

"""
Python 3 environment 
"""


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
logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
logger.addHandler(logging_gelf_handler)

# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])


# destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"


class report_mapping():
    """
    Parser dedicated for Report endpoint
    """

    def __init__(self, endpoint, data, query='', accounting_type=''):
        # Parameters
        self.endpoint = endpoint
        self.data = data
        self.header = self.construct_header(data)
        self.columns = [
            # "Time",
            "ReportName",
            # "DateMacro",
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
            self.itr = 1
            self.data_out = self.parse(
                data["Rows"]["Row"], self.header, self.itr)
            self.columns = self.arrange_header(self.columns)
            self.output(self.endpoint, self.data_out, self.primary_key)
        elif endpoint == "CustomQuery":
            self.columns = ["query", "value"]
            self.data_out.append(self.columns)
            self.data_out.append("{0}".format(json.dumps(data)))
            self.pk = []
            self.output_1cell(self.endpoint, self.columns,
                              self.data_out, self.pk)
        else:  # Outputting tables which cannot parse
            # sself.data_out = copy.deepcopy(self.header)
            for item in self.columns:
                self.data_out.append(self.header[item])
            (self.data_out).append("{0}".format(json.dumps(data)))
            self.columns.append("value")
            self.output_1cell(self.endpoint, self.columns,
                              self.data_out, self.primary_key)

    def construct_header(self, data):
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
                # "DateMacro": temp["DateMacro"],
                "StartPeriod": temp["StartPeriod"],
                "EndPeriod": temp["EndPeriod"]
            }

        return json_out

    def arrange_header(self, columns):
        """
        Arrange the column headers in order
        """

        if columns.index("value") != (len(columns)-1):
            # If "value" is not at the end of the row index
            columns.remove('value')

        if 'value' not in columns:
            # append the value back into the column if it does not exist
            columns.append("value")

        return columns

    def parse(self, data_in, row, itr):  # , data_out):
        """
        Main parser for rows
        Params:
        data_in     - input data for parser
        row         - output json formatted row for one sub section within the table
        itr         - record of the number of recursion
        """

        data_out = []
        for i in data_in:
            temp_row = copy.deepcopy(row)
            row_name = "Col_{0}".format(itr)

            if ("type" not in i) and ("group" in i):
                # print(i)
                # print("NO TYPE")
                if row_name not in self.columns:
                    self.columns.append(row_name)
                    self.primary_key.append(row_name)
                temp_out = []
                row[row_name] = i["group"]
                row["Col_{0}".format(itr+1)] = i["ColData"][0]["value"]
                row["value"] = i["ColData"][1]["value"]
                # print("ROW: {0}".format(row))
                temp_out = [row]
                # print("TEMP_OUT: {0}".format(temp_out))
                """row[row_name] = i["group"]
                temp_in = {}
                temp_in["ColData"] = i["ColData"]
                print("TEMP_IN: {0}".format(temp_in))

                temp_out = self.parse(temp_in, row, itr+1)"""

                # data_out.append(row)
                # data_out = data_out+row
                data_out = data_out + temp_out
                # data_out = [row]

            elif i["type"] == "Section":
                # row_name = "Col_{0}".format(itr)
                # print(row_name)
                # print(i["Header"]["ColData"][0]["value"])
                # print(i)
                # print("")

                if row_name not in self.columns:
                    self.columns.append(row_name)
                    self.primary_key.append(row_name)

                # Use Group if Header is not found as column values
                if "Header" in i:
                    # print("hEAdER")
                    row[row_name] = i["Header"]["ColData"][0]["value"]
                    # Recursion when type data is not found
                    temp_out = self.parse(i["Rows"]["Row"], row, itr+1)

                elif "group" in i:
                    # print("GROUP")
                    # print(i["Summary"])
                    # Column name
                    row[row_name] = i["group"]
                    # print(row)
                    # Row value , assuming no more recursion
                    # stemp_out = self.parse(i["Summary"], row, itr+1)
                    row["Col_{0}".format(
                        itr+1)] = i["Summary"]["ColData"][0]["value"]
                    row["value"] = i["Summary"]["ColData"][1]["value"]
                    temp_out = [row]

                    if "Col_{0}".format(itr+1) not in self.columns:
                        self.columns.append("Col_{0}".format(itr+1))
                        self.primary_key.append("Col_{0}".format(itr+1))

                data_out = data_out+temp_out  # Append data back to section

            elif (i["type"] == "Data") or ("ColData" in i):
                # srow_name = "Col_{0}".format(itr)

                if row_name not in self.columns:
                    self.columns.append(row_name)
                    self.primary_key.append(row_name)
                temp_row[row_name] = i["ColData"][0]["value"]

                row_value = "value"
                if row_value not in self.columns:
                    self.columns.append(row_value)
                temp_row[row_value] = i["ColData"][1]["value"]

                data_out.append(temp_row)

                """elif "type" not in i and "group" in i:
                    print("Phase 3")
                    if row_name not in self.columns:
                        self.columns.append(row_name)
                        self.primary_key.append(row_name)
                    row[row_name] = i["group"]
                    temp_in = {}
                    temp_in["ColData"] = i["ColData"]

                    temp_out = self.parse(temp_in, row, itr+1)

                    data_out = data_out+temp_out"""

            else:
                raise Exception(
                    "No type found within the row. Please validate the data.")
        # print("finish loop")
        return data_out

    def produce_manifest(self, file_name, primary_key):
        """
        Dummy function to return header per file type.
        """

        file = "/data/out/tables/"+str(file_name)+".manifest"
        # destination_part = file_name.split(".csv")[0]

        manifest_template = {
            # "source": "myfile.csv"
            # ,"destination": "in.c-mybucket.table"
            "incremental": bool(True)
            # ,"primary_key": ["VisitID","Value","MenuItem","Section"]
            # ,"columns": [""]
            # ,"delimiter": "|"
            # ,"enclosure": ""
        }

        column_header = []  # noqa

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

        return

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
        temp_df.to_csv(DEFAULT_FILE_DESTINATION+filename,
                       index=False, columns=self.columns)
        self.produce_manifest(filename, pk)

        return

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
        if os.path.isfile(DEFAULT_FILE_DESTINATION+filename):
            data_out = [data]
        else:
            data_out = [columns, data]

        with open(DEFAULT_FILE_DESTINATION+filename, "a") as f:
            writer = csv.writer(f)
            # writer.writerow(["range", "start_date", "end_date", "content"])
            # writer.writerow([date_concat, start_date, end_date, "{0}".format(self.content)])
            writer.writerows(data_out)
            # f.write(["content"])
            # f.write(["{0}"].format(self.content))
        f.close()
        logging.info("Outputting {0}... ".format(filename))
        # if not os.path.isfile(DEFAULT_FILE_DESTINATION+filename):
        self.produce_manifest(filename, pk)

        return
