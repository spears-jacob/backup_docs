from fileHandling import *
import os, pprint


# ---- Query Building

#---------------------------------------------------
# Build a metric script in properly formed HQL
#   Adds a prefix,
#   Iterates over a list of cases and adds logic for each into a case statement or sumifs,
#   Adds the from clause,
#   Adds a suffix
def buildQueryScript(queryType, run):
    # read in HQL parts
    prefix = readHqlFile('bin/'+queryType+"_prefix.hql")
    logic = readHqlFile('bin/'+queryType+"_logic.hql")

    buildingQuery = []
    for line in prefix:             # PREFIX
        buildingQuery.append(replaceParams(line, run))
    allCases = buildAllCases(queryType, run['unit'], run['sourceName'])
    buildingQuery.append(allCases)
    for line in logic:              # LOGIC - midsection
        buildingQuery.append(replaceParams(line, run))
    if queryType in ('counts'):     # SUFFIX
        buildingQuery.extend(buildQueryScript('pivot', run))
    return buildingQuery

# for each key, run through the queries and swap out the keys
def replaceParams(line, run):
    for key in run.keys():
        line = line.replace(str('{'+key+'}'), str(run[key]))
    return line

#-------------------------------------------------
# getMetrics use components to build case statements
#   queryType specifies the spreadsheet to read in
#   returns a list of tuples [(metricLabel, comment, list of conditions)...]
def getMetrics(queryType, sourceName):
    tsvName = queryType
    if queryType in ('pivot'): # Share tsv... should probably move this up
        tsvName = 'counts'
    fileName = 'bin/'+tsvName+'.tsv'
    headers, rows = readCsv(fileName, '\t') # tsv
    # current metrics.tsv is configured like this: comment	label1	label2	label3	label4	label5	condition1	condition2	condition3	condition4	condition5
    metrics = []
    for row in rows:
        metric_store_id = row[4] #identifies block of metrics, often different sources
        runFlag = row[0] # turns metrics on or off for a query build run
        if (runFlag == 'TRUE' and sourceName == metric_store_id ): # spreadsheet string, not a boolean
            comment = row[1]
            label = "|".join([row[2], row[3], row[4], row[5], row[6]]) # more than 5 is probably excessive for labeling
            hiveName = row[2]+row[5]
            conditions = filter(None, row[7:]) # extensible
            metrics.append((label, comment, conditions, hiveName))
    return metrics

# cases runs case generation for all cases using the metrics file
def buildAllCases(queryType, unit, sourceName):
    allCases = ''
    metrics = getMetrics(queryType,sourceName)
    for metricFields in metrics:
        label, comment, conditions, hiveName = metricFields[0], metricFields[1], metricFields[2], metricFields[3]
        lineEnd = ''
        if metricFields != metrics[-1] and queryType == 'pivot':
            lineEnd = ','
        allCases += composeCase(queryType, unit, label, comment, conditions, hiveName) + lineEnd
    return allCases

# returns a bunch of strings depending on the unit
# For instances, use sumifs, for visits/households, use size collect set
def caseStrings(unit, label, hiveName):
    if unit == 'instances':
        caseStart = '        SUM(IF( '
        caseEnd = ', 1, 0)) AS ' + hiveName + ',\n' # hive name goes here
        casePivot = '\n                      \''+ label + '\', ' + hiveName
        vectorized = 'SET hive.vectorized.execution.enabled = false;'
    elif unit == 'visits':
        caseStart = '        SIZE(COLLECT_SET(IF( '
        caseEnd = ', visit__visit_id, Null))) AS ' + hiveName + '_visits,\n'
        casePivot = '\n                      \''+ label + '\', ' + hiveName + '_visits'
    elif unit == 'households':
        caseStart = '        SIZE(COLLECT_SET(IF( '
        caseEnd = ', visit__account__enc_account_number, Null))) AS ' + hiveName + '_households,\n'
        casePivot = '\n                      \''+ label + '\', ' + hiveName + '_households'
    elif unit == 'devices':
        caseStart = '        SIZE(COLLECT_SET(IF( '
        caseEnd = ', visit__device__enc_uuid, Null))) AS ' + hiveName + '_devices,\n'
        casePivot = '\n                      \''+ label + '\', ' + hiveName + '_devices'
    else:
        caseStart = caseEnd = casePivot = 'Unknown Case'
    return caseStart, caseEnd, casePivot

# composeCase generates one case in the case statement, run for each case to check
#   label - string - for THEN clause
#   comment - string (single line) - for final comment
#   conditions - list strings - for WHEN clause
def composeCase(queryType, unit, label, comment, conditions, hiveName):
    caseStart, caseEnd, casePivot = caseStrings(unit, label, hiveName)
    if queryType == 'counts':
        # loop over all conditions and add with AND
        for i in range(len(conditions)):
            if i == 0:
                caseStart += conditions[i]
            else:
                caseStart += '  AND ' + conditions[i] + ' '
        case = caseStart + caseEnd
        return case
    elif queryType =='pivot':
        return casePivot
    else:
        return 'Unrecognized queryType: '+queryType



# ------ Run Details

# Get the run parameters from the input file, use the mapping file for additional info for each sourceName (a join)
def getRuns(inputParametersFile = 'bin/qb_input_parameters.csv', domainMappingFile = 'bin/domain_mapping.csv'):
    headers, rows = readCsv(domainMappingFile, ',')
    mappings = {}
    for row in rows:
        mappings[row[0]] = {   # key is sourceName in row[0]
                'platform': row[1].strip(),
                'domain': row[2].strip(),
                'company': row[3].strip(),
                'timestampMultiplier': row[4].strip()
            }
    inputDicts = csvToDicts(inputParametersFile, ',')    # print inputDicts.fieldnames
    runs = []
    for row in inputDicts:
        run = {}
        for key in inputDicts.fieldnames:
            run[key] = row[key].strip()
        sourceName = run['sourceName']         # Join on sourceName, pull in mapping fields
        run['platform'] = mappings[sourceName]['platform']
        run['domain'] = mappings[sourceName]['domain']
        run['company'] = mappings[sourceName]['company']
        run['timestampMultiplier'] = mappings[sourceName]['timestampMultiplier']
        runs.append(run)
    return runs

# Add a bunch of exception parameters
def addTableParams(inputParams):
    tableParams = inputParams.copy()
    tableParams['partition']  = 'partition_date_hour_utc'
    tableParams['start_partition'] ='${env:START_DATE_TZ}'
    tableParams['end_partition'] ='${env:END_DATE_TZ}'
    if tableParams['sourceTable'] == 'asp_v_net_events':  # needed to handle differences in tables -_-
        tableParams['partition'] = 'partition_date'
        tableParams['start_partition'] ='${env:START_DATE}'
        tableParams['end_partition'] ='${env:END_DATE}'
    if (tableParams['sourceName'] == 'MySpectrum_adobe'):
        tableParams['company'] = '''CASE
            WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
            WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
            WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
            WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
            ELSE 'UNDEFINED'
        END'''
    elif ('adobe' not in tableParams['sourceName']):
          tableParams['company'] = '''COALESCE (visit__account__details__mso,'Unknown')'''
    else:
        tableParams['company'] = "'" + tableParams['company'] + "'"
    return tableParams

# add some parameters that are dependent on the cadence unit being run
def addUnitBasedParams(params):
    unitParams = params.copy()
    unitParams['columnsTable'] = '${env:TMP_db}.'+unitParams['outputTable']+ '_'+unitParams['queryType']+'_'+unitParams['unit']+'_columns'
    if unitParams['tGrain'] == 'hourly':
        unitParams['hours'] = "prod.epoch_datehour(cast("+params['timestampMultiplier']+" as bigint),'America/Denver') as date_hour_denver,"
        unitParams['hoursGroup'] = "prod.epoch_datehour(cast("+params['timestampMultiplier']+" as bigint),'America/Denver'),"
        unitParams['tableHours'] = "date_hour_denver,"
    else:
        unitParams['hours'] = unitParams['hoursGroup'] = unitParams['tableHours'] = ''
    # time grain based parameters for different cadences
    if (unitParams['tGrain'] == 'hourly' or unitParams['tGrain'] == 'daily'):
        unitParams['partition_field'] = 'date_denver'
        unitParams['time_grain_calc'] = "epoch_converter(cast("+params['timestampMultiplier']+" as bigint),'America/Denver')"
        unitParams['cadence_join'] = ''
    elif (unitParams['tGrain'] == 'monthly'):
        unitParams['partition_field'] = 'year_month_denver'
        unitParams['time_grain_calc'] = "date_yearmonth(epoch_converter(cast("+params['timestampMultiplier']+" as bigint),'America/Denver'))"
        unitParams['cadence_join'] = ''
    elif (unitParams['tGrain'] == 'fiscal_monthly'):
        unitParams['partition_field'] = 'year_fiscal_month_denver'
        unitParams['time_grain_calc'] = 'fiscal_month'
        unitParams['cadence_join'] = "LEFT JOIN prod_lkp.chtr_fiscal_month ON epoch_converter(cast(" + params['timestampMultiplier'] + " as bigint),'America/Denver') = partition_date"
    elif (unitParams['tGrain'] == 'weekly'):
        unitParams['partition_field'] = 'week_ending_date_denver'
        unitParams['time_grain_calc'] = 'week_ending_date'
        unitParams['cadence_join'] = "LEFT JOIN prod_lkp.week_ending ON epoch_converter(cast(" + params['timestampMultiplier'] + " as bigint),'America/Denver') = partition_date"
        unitParams['partition']  = 'week_ending_date'
        unitParams['start_partition'] ='${env:START_DATE}'
        unitParams['end_partition'] ='${env:END_DATE}'
    outputFileName = replaceParams(unitParams['relativeOutputPathFileName'], unitParams)
    unitParams['relativeOutputPathFileName'] = outputFileName
    return unitParams

###################### __main__
def main(argv):
    if (len(argv) <> 3):
        print """Must be called with 2 input parameters:
                        1: inputParameters ('bin/qb_input_parameters.csv')
                        2: domainMapping ('bin/domain_mapping.csv')"""
        pass
    else:
        outputFileList = []
        runs = getRuns(argv[1], argv[2])
        for inputParams in runs:
            params2 = addTableParams(inputParams)
            run = addUnitBasedParams(params2)
            if run['isRun'] == 'TRUE':
                print "\nBuilding queries for the following parameters:"
                pp = pprint.PrettyPrinter(indent=8)
                pp.pprint(run)
                print 'Output to: ', run['relativeOutputPathFileName'], '\n'
                writeFile(run['relativeOutputPathFileName'], buildQueryScript(run['queryType'], run))
                outputFileList.append(run['relativeOutputPathFileName'])
        print "\nSuggested commands for job files:"
        i = 1
        for file in outputFileList:
            print 'command.' + str(i) + """=/bin/bash -c 'export START_DATE_TZ="${START_DATE_TZ}"; echo $START_DATE_TZ; export END_DATE_TZ="${END_DATE_TZ}"; echo $END_DATE_TZ;  hive -f """ + file.replace('net_adobe/extracts/pre_post/','') + "'"
            i += 1
if __name__ == "__main__":
    import sys
    main(sys.argv)
