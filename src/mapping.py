import csv
import uuid
import pandas as pd
import json
import logging
import sys  # noqa
import os


# destination to fetch and output files
cwd_parent = os.path.dirname(os.getcwd())
DEFAULT_FILE_INPUT = os.path.join(cwd_parent, "data/in/tables/")
DEFAULT_FILE_DESTINATION = os.path.join(cwd_parent, "data/out/tables/")


class TableStreamWriter:
    """
    Holds one open CSV writer per output table for the duration of a single endpoint's
    pagination, so each table's header and column order are established once - on the
    first page that produces rows for that table - and reused for every later page.

    This is what lets the data endpoints stream: each page is parsed, appended through
    this writer, and discarded, so peak memory stays at O(page_size) instead of growing
    with the total record count.

    The column set is taken from the first row of each table. The mapping (mappings.json)
    drives parsing, so every row of a given table carries the exact same keys in the same
    order - the first row is therefore the complete, correctly ordered header. We still
    pass ``extrasaction="ignore"`` and ``restval=""`` as a safety net so an unexpected
    key never shifts columns or raises.
    """

    def __init__(self, destination=DEFAULT_FILE_DESTINATION):
        self.destination = destination
        self._files = {}
        self._writers = {}

    def write_rows(self, table_name, rows):
        if not rows:
            return
        for row in rows:
            writer = self._get_writer(table_name, row)
            writer.writerow(row)

    def _get_writer(self, table_name, sample_row):
        if table_name not in self._writers:
            file_dest = self.destination + table_name + ".csv"
            file_handle = open(file_dest, "w", newline="")
            writer = csv.DictWriter(
                file_handle,
                fieldnames=list(sample_row.keys()),
                extrasaction="ignore",
                restval="",
                lineterminator="\n",
            )
            writer.writeheader()
            self._files[table_name] = file_handle
            self._writers[table_name] = writer
            logging.info("Streaming table output: {0}...".format(file_dest))
        return self._writers[table_name]

    def close(self):
        for file_handle in self._files.values():
            file_handle.close()
        self._files.clear()
        self._writers.clear()


class Mapping:
    """
    Handling Generic Ex Mapping
    """

    def __init__(self, endpoint, data, writer=None):
        self.endpoint = endpoint
        self.writer = writer
        self.mapping = self.mapping_check(self.endpoint)
        self.out_file = {self.endpoint: []}
        self.out_file_pk = {self.endpoint: []}  # destination name from mapping
        self.out_file_pk_raw = {}  # raw destination name from API output
        self.get_primary_key(endpoint, self.mapping)

        # Runs
        self.root_parse(data)
        self.output()

    @staticmethod
    def mapping_check(endpoint):
        """
        Selecting the Right Mapping for the specified endpoint
        """
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "mappings.json"), "r") as f:
            out = json.load(f)
        f.close()
        return out[endpoint]

    def root_parse(self, data):
        """
        Parsing the Root property of the return data
        """

        # data = self.data
        mapping = self.mapping

        for row in data:
            # Looping row by row
            self.parsing(self.endpoint, mapping, row)

    def parsing(self, table_name, mapping, data):
        """
        Outputting data results based on configured mapping
        """

        # If new table property is found,
        # create a new array to store values
        if table_name not in self.out_file:
            self.out_file[table_name] = []

        row_out = {}  # Storing row output

        # Looping through the keys of the mapping
        for column in mapping:
            if mapping[column]["type"] == "column":
                # Delimit mapping variables
                if "." in column:
                    temp_value = column.split(".")

                    try:
                        # Looping through the array
                        # value = data[temp_value[0]][temp_value[1]]
                        value = data
                        for word in temp_value:
                            value = value[word]
                    except Exception:
                        value = ""
                else:
                    try:
                        value = data[column]
                    except Exception:
                        value = ""
                header = mapping[column]["mapping"]["destination"]

            elif mapping[column]["type"] == "table":
                # Setting up table parameters,
                # mappings and values for parsing the nested table
                mapping_name = column
                # Mapping for the table
                mapping_in = mapping[column]["tableMapping"]
                # New table output name
                sub_table_name = mapping[column]["destination"]
                sub_table_exist = True  # Determine if the table column exist as a property in source file
                sub_table_row_exist = True  # Determine if there are any rows within the sub table

                # Passing the function if the JSON property is not found
                try:
                    if "." in mapping_name:
                        temp_value = mapping_name.split(".")
                        data_in = data
                        for word in temp_value:
                            data_in = data_in[word]
                    else:
                        data_in = data[mapping_name]

                    if len(data_in) == 0:
                        sub_table_row_exist = False
                except KeyError:
                    sub_table_exist = False

                # Verify if the sub-table exist in the root table
                if sub_table_exist and sub_table_row_exist:
                    # Setting up nested table primary key
                    # Using current table id to create unique pk with md5
                    string_of_pk = ""  # Concat all the PK as a string # noqa
                    # Iterate through all the pk
                    sub_table_pk = mapping[column]["destination"] + "-" + str(uuid.uuid4().hex)

                    mapping_in["parent_table"] = {"type": "pk", "value": sub_table_pk}

                    # Loop nested table
                    self._parse_table(sub_table_name, mapping_in, data_in)

                    # Returning sub table PK
                    value = sub_table_pk

                else:
                    value = ""

                # Primary key return to the root table
                header = column

            # Sub table's Primary Key
            # Source: injected new property when new table is found in the mapping
            elif mapping[column]["type"] == "pk":
                header = column
                value = mapping[column]["value"]

            # Injecting new table elements for the row
            row_out[header] = value

        # Storing JSON tables
        out_file = self.out_file
        out_file[table_name].append(row_out)
        self.out_file = out_file

    def _parse_table(self, table_name, mapping, data):
        """
        Parsing table data
        Determining the type of the sub-table
        *** Sub-function of parse() ***
        """

        if isinstance(data, dict):
            self.parsing(table_name, mapping, data)

        elif isinstance(data, list):
            for row in data:
                self.parsing(table_name, mapping, row)

    def get_primary_key(self, table_name, mapping):
        """
        Filtering out all the primary keys within the mapping table
        """

        # If table_name does not exist in the PK list
        if table_name not in self.out_file_pk_raw:
            self.out_file_pk_raw[table_name] = []
            self.out_file_pk[table_name] = []

        for column in mapping:
            # Column type is "column"
            if mapping[column]["type"] == "column":
                # Search if the primaryKey property is within the mapping configuration
                if "primaryKey" in mapping[column]["mapping"]:
                    # Confirm if the primary key tab is true
                    if mapping[column]["mapping"]["primaryKey"]:
                        self.out_file_pk_raw[table_name].append(column)
                        self.out_file_pk[table_name].append(mapping[column]["mapping"]["destination"])

            # Column type is "table"
            if mapping[column]["type"] == "table":
                # Recursively run the tableMapping
                self.get_primary_key(table_name=mapping[column]["destination"], mapping=mapping[column]["tableMapping"])

    @staticmethod
    def produce_manifest(file_name, primary_key):
        """
        Dummy function to return header per file type.
        """

        file = "/data/out/tables/" + str(file_name) + ".manifest"
        logging.info("Manifest output: {0}".format(file))

        manifest_template = {
            # "source": "myfile.csv"
            # ,"destination": "in.c-mybucket.table"
            # "incremental": bool(incremental)
            # ,"primary_key": ["VisitID","Value","MenuItem","Section"]
            # ,"columns": [""]
            # ,"delimiter": "|"
            # ,"enclosure": ""
        }

        column_header = []  # noqa

        manifest = manifest_template
        # manifest["primary_key"] = primary_key

        try:
            with open(file, "w") as file_out:
                json.dump(manifest, file_out)
                # logging.info("Output manifest file ({0}) produced.".format(file_name))
        except Exception as e:
            logging.error("Could not produce output file manifest.")
            logging.error(e)
            sys.exit(1)

        return

    def output(self):
        """
        Output Data with its desired file name.

        When a streaming ``writer`` is supplied (the default for paginated data
        endpoints) each table's rows are appended through it, so the full dataset is
        never materialised in memory. Without a writer the legacy single-shot behaviour
        is kept for backward compatibility.
        """

        out_file = self.out_file

        if self.writer is not None:
            for table_name in out_file:
                self.writer.write_rows(table_name, out_file[table_name])
        else:
            for file in out_file:
                out_df = pd.DataFrame(out_file[file])
                file_dest = DEFAULT_FILE_DESTINATION + file + ".csv"
                out_df.to_csv(file_dest, index=False)
                logging.info("Table output: {0}...".format(file_dest))

        # Outputting manifest file if incremental
        out_file_pk = self.out_file_pk  # noqa
