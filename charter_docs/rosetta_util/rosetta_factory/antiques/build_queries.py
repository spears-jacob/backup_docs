#from file_handling import *
import os, pprint


"""
    file_handling.py
        read_csv
        - Read and output headers from CSV file
        - Read and output records from CSV file
        write_file
        - lines to an output_fileName
        read_hql_file
        - lines from a file

"""
import csv


def read_csv(input_file, delimiter):
    """ read_csv
        input_file - a file with headers
        returns a list of headers from the csv and a 2d list of rows and cols
    """
    # setup file reader
    file = open(input_file, "r")
    reader = csv.reader(file, delimiter=delimiter)
    # separate header from rows
    headers, rows = [], []
    get_headers = True
    for row in reader:
        if get_headers:
            headers = row
            get_headers = False
        else:
            rows.append(row)
    return headers, rows


# returns a list of dictionaries using the field headers as keys for each row
def csv_to_dicts(input_file, delimiter):
    file = open(input_file, 'r')
    reader = csv.DictReader(file, delimiter=delimiter)
    return reader


# -------------------------------------------------
# File Handling
def write_file(output_fileName, lines):
    output_file = open(output_fileName, "w")
    output_file.writelines(lines)
#


def read_hql_file(fileName):
    file = open(fileName, "r")
    return file.readlines()


# ----------------------------------------------------------------------------------------------------------------------
# ---- Query Building
# Build a metric script in properly formed HQL
#   Adds a prefix,
#   Iterates over a list of blocks and adds logic for each into block statements#
#   Adds a suffix
# ----------------------------------------------------------------------------------------------------------------------


def build_query_script(query_type, dest_name, unit_type, block_type, run):
    # read in HQL parts
    if query_type == 'definitions':
        met_agg_01 = read_hql_file('bin/' + query_type + "_met_agg_01.hql")
        met_agg_02 = read_hql_file('bin/' + query_type + "_met_agg_02.hql")
        building_query_met_agg = []
        for line in met_agg_01:  # PREFIX
            building_query_met_agg.append(replace_params(line, run))
        all_blocks = build_all_blocks(query_type, run['block_type'], dest_name, run['unit_type'])
        building_query_met_agg.append(all_blocks)
        for line in met_agg_02:  # LOGIC - midsection
            building_query_met_agg.append(replace_params(line, run))
        return building_query_met_agg
    elif query_type == 'sets' and dest_name == 'set_agg_01':
        set_agg_01 = read_hql_file('bin/' + query_type + '_set_agg_01.hql')
        building_query_set_agg_01 = []
        for line in set_agg_01:  # PREFIX
            building_query_set_agg_01.append(replace_params(line, run))
        all_blocks = build_all_blocks(query_type, run['block_type'], dest_name, run['unit_type'])
        building_query_set_agg_01.append(all_blocks)
        return building_query_set_agg_01
    elif query_type == 'sets' and dest_name == 'set_agg_02':
        set_agg_02 = read_hql_file('bin/' + query_type + '_set_agg_02.hql')
        building_query_set_agg_02 = []
        for line in set_agg_02:  # PREFIX
            building_query_set_agg_02.append(replace_params(line, run))
        all_blocks = build_all_blocks(query_type, run['block_type'], run['dest_name'], run['unit_type'])
        building_query_set_agg_02.append(all_blocks)
        return building_query_set_agg_02
    elif query_type == 'sets' and dest_name == 'set_agg_03':
        set_agg_03 = read_hql_file('bin/' + query_type + '_set_agg_03.hql')
        building_query_set_agg_03 = []
        for line in set_agg_03:  # PREFIX
            building_query_set_agg_03.append(replace_params(line, run))
        all_blocks = build_all_blocks(query_type, run['block_type'], run['dest_name'], run['unit_type'])
        building_query_set_agg_03.append(all_blocks)
        return building_query_set_agg_03
    elif query_type == 'sets' and dest_name == 'set_agg_04':
        set_agg_04 = read_hql_file('bin/' + query_type + '_set_agg_04.hql')
        building_query_set_agg_04 = []
        for line in set_agg_04:  # PREFIX
            building_query_set_agg_04.append(replace_params(line, run))
        return building_query_set_agg_04
    else:
        building_query_end = []
    return building_query_end


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# REPLACE KEYS
#   #    - for each key, run through the queries and swap out the keys
# ----------------------------------------------------------------------------------------------------------------------


def replace_params(line, run):
    for key in run.keys():
        line = line.replace(str('{' + key + '}'), str(run[key]))
    return line


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# COMPILE METRICS
#    - get_metrics use components to build block statements
#    - query_type specifies the spreadsheet to read in
#    - returns a list of tuples [(metricLabel, comment, list of conditions)...]
# ----------------------------------------------------------------------------------------------------------------------


def get_metrics(query_type, dest_name):
    tsv_name = query_type
    if query_type in ('sets'):
        tsv_name = 'definitions'
    file_name = 'bin/' + tsv_name + '.tsv'
    headers, rows = read_csv(file_name, '\t')  # tsv
    metrics = []
    for row in rows:
        run_flag = row[0]  # turns metrics on or off for a query build run
        if run_flag == 'TRUE':
            hive_name = [_f for _f in row[1:] if _f]
            conditions = [_f for _f in row[7:] if _f]
            metrics.append((hive_name, conditions))
    print(metrics)
    return metrics


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# BUILDING BLOCKS
#    - blocks runs block generation for all blocks using the metrics file
# ----------------------------------------------------------------------------------------------------------------------


def build_all_blocks(query_type, block_type, dest_name, unit_type):
    all_blocks = ''
    metrics = get_metrics(query_type, dest_name)
    for metric_fields in metrics:
        hive_name, conditions = metric_fields[0], metric_fields[1]
        line_end = ''
        if metric_fields != metrics[-1] and query_type == 'pivot':
            line_end = ','
        all_blocks += compose_block(query_type, hive_name, block_type, conditions, unit_type) + line_end
#    print(all_blocks)
    return all_blocks


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# DEFINE BLOCK STRING#    -
#    - returns a bunch of strings depending on the block_type
# ----------------------------------------------------------------------------------------------------------------------


def block_strings(block_type, query_type, hive_name, dest_name, unit_type):
    if block_type == 'sum_if_block':
        block_start = '        SUM(IF('
        block_end = ', 1, 0)) AS ' + hive_name + ',\n'
        block_pivot = '\n                      \'' + hive_name + '\', ' + hive_name + '\n'
    elif block_type == 'sum_as_block':
        block_start = '        SUM('
        block_end = ') AS ' + hive_name + '\n'
        block_pivot = '\n                      \'' + hive_name + '\', ' + hive_name + '\n'
    elif block_type == 'if_sum_block':
        block_start = '        IF(SUM(' + hive_name
        block_end = ') > 0, 1, 0) AS ' + hive_name + ',\n'
        block_pivot = '\n                      \'' + hive_name + '\', ' + hive_name + '\n'
    elif block_type == 'dummy_block':
        block_start = '     ' + hive_name + ' AS ' + hive_name
        block_end = '\n'
        block_pivot = '\n'
    elif block_type == 'sum_block':
        block_start = "     '" + hive_name + "', SUM(" + hive_name
        block_end = '),\n'
        block_pivot = '\n'
    elif block_type == 'null_block':
        block_start = ''
        block_end = '\n'
        block_pivot = '\n'
    else:
        block_start = block_end = block_pivot = ''
    return block_start, block_end, block_pivot

# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# COMPOSE BLOCK STYLE DEPENDENT ON QUERY TYPE
#    - compose_block generates one block in the block statement, run for each block to check
#    - label - string - for THEN clause
#    - comment - string (single line) - for final comment
#    - conditions - list strings - for WHEN clause
# ----------------------------------------------------------------------------------------------------------------------


def compose_block(query_type, block_type, conditions, hive_name, unit_type):
    block_start, block_end, block_pivot = block_strings(block_type, query_type, hive_name, 'dest_name', unit_type)
    if query_type == 'definitions':
        # loop over all conditions and add with AND
        for i in range(len(conditions)):
            if i == 0:
                block_start += conditions[i]
            else:
                block_start += ' AND ' + conditions[i] + ' '
        block = block_start + block_end
        return block
    elif query_type in ('sets', 'init'):
        # loop over all conditions
        for i in range(len(hive_name)):
            if i == 0:
                block_start += hive_name[i]
            else:
                '___LOGIC_ERROR___'
        block = block_start + block_end
        return block
    elif query_type == 'pivot':
        return block_pivot
    else:
        return 'Unrecognized query_type: ' + query_type


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# RUN DETAILS
#    - Get the run parameters from the input file, use the mapping file for additional info for each dest_name (a join)
# ----------------------------------------------------------------------------------------------------------------------


def get_runs(input_parameters_file='bin/qb_input_parameters.csv'):
    headers, rows = read_csv(input_parameters_file, ',')
    mappings = {}
    for row in rows:
        mappings[row[1]] = {  # key is dest_name in row[1]
            'unit_identifier': row[2].strip(),
            'unit_type': row[3].strip(),
            'is_run': row[0].strip()
        }
    input_dicts = csv_to_dicts(input_parameters_file, ',')  # print input_dicts.fieldnames
    runs = []
    for row in input_dicts:
        run = {}
        print(run)
        for key in input_dicts.fieldnames:
            run[key] = row[key].strip()
        dest_name = run['dest_name']  # Join on dest_name, pull in mapping fields
        unit_type = run['unit_type']  # Join on unit_type, pull in mapping fields
        print('dest_name' + ' ' + dest_name)
        print('unit_type' + ' ' + unit_type)
        runs.append(run)
    return runs


# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
# MAIN
#    - Add exception parameters
#    - Define arguments
# ----------------------------------------------------------------------------------------------------------------------


def add_table_params(input_params):
    table_params = input_params.copy()
    return table_params


def main(argv):
    if len(argv) != 3:
        print("""Must be called with 1 input parameter:
                               1: input_parameters ('bin/qb_input_parameters.csv')""")
        pass
    else:
        output_file_list = []
        print(output_file_list)
        runs = get_runs(argv[1])
        print(runs)
        for input_params in runs:
            run = add_table_params(input_params)
            if run['is_run'] == 'TRUE':
                print("\nBuilding queries for the following parameters:")
                pp = pprint.PrettyPrinter(indent=8)
                pp.pprint(run)
                print('Output to: ', run['relative_output_path_file_name'], '\n')
                write_file(run['relative_output_path_file_name'], build_query_script(str(run['query_type']), str(run['dest_name']),str(run['unit_type']),str(run['block_type']), run))
                output_file_list.append(run['relative_output_path_file_name'])
        print("\nSuggested commands for job files:")
        i = 1


if __name__ == "__main__":
    import sys
    main(sys.argv)

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
# END
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
