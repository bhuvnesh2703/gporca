#!/usr/bin/env python

"""
The purpose of this utility is to identify tables with outdated statistics involved in a query. It accepts an input query file of the form: EXPLAIN VERBOSE query. It will execute the input file and scan explain verbose plan of the query to identify the scanned tables. It will execute a query to check count of records on the identified table and compare it with the reltuples value stored in pg_class. If the difference between record count and reltuples is within a defined percentage, stats are considered to be up-to-date else are marked as outdated. It will also capture explain plan for the query in the log file in current working directory

Please report bugs to bhuvnesh2703@gmail.com
"""

try:
    from pygresql import pgdb
    import os
    import subprocess
    import logging
    import optparse
    import argparse
    import sys
    import re
    import time
    import getpass
except ImportError:
    print "Cannot import modules, Please source greenplum_path.sh"

"""
execute_query_on_db_sql takes a query as input, set's the client_min_messages as LOG so that we could identify
if the query will fallback to legacy planner. stderr is used to capture the messages from the database server.
The current approach is not good, since even ERROR messages will be sent to stderr and may cause issues.
However, in order to avoid database query failures while executing the query provided by user, before running
the query by this module, to verify we execute it against execute_query_on_db module using pgdb
"""


def execute_query_on_db_psql(common_query_from_file):
    try:
        query_with_guc = 'SET client_min_messages = \'LOG\' ;' + common_query_from_file
        query_process = subprocess.Popen(
            ['psql', '-p', '%d' % port, '-c', '%s' % query_with_guc, '-d', '%s' % database], stderr=subprocess.PIPE,
            stdout=subprocess.PIPE)
        query_process_output = query_process.communicate()
        explain_verbose_plan = query_process_output[0]
        client_min_messages = query_process_output[1]
        return explain_verbose_plan.split('\n'), client_min_messages.split('\n')
    except Exception, e:
        logger.fatal("Execution failed with error: %s" % str(e))
        sys.exit(1)


"""
execute_query_on_db takes a query as input and is a common module to execute queries on database using pgdb.
It fetches all the rows and returns the rows to the calling function.
"""


def execute_query_on_db(common_query_from_file):
    try:
        db_connection_object = initialize_db_connection(db_connection_params)
        cursor = db_connection_object.cursor()
        cursor.execute(common_query_from_file)
        output_from_database = cursor.fetchall()
        cursor.close()
        return output_from_database
    except Exception, e:
        logger.fatal("Execution failed with below error. Exiting !! \n%s" % str(e))
        sys.exit(1)


"""
convert_list_to_string takes a list as an input, converts it to a string and strips off the first and last char,
i.e '['and']'
"""


def convert_list_to_string(list):
    list_to_string = str(list).strip('[]')
    return list_to_string


'''
list_to_tabular_formatting takes a list, and an optional header as an input. 
Calculates maximum length for each column across all the rows in a list and creates a list to hold the maximum length of
each column. Based on the maximum length generates a format string for consistency across each row
Converts the output to a tabular formatted string for better look
'''


def list_to_tabular_formatting(matrix, header=None):
    lengths = []
    if header:
        for column in header:
            lengths.append(len(column))
    '''
Calculate maximum length of each column
'''
    for row in matrix:
        for column in row:
            i = row.index(column)
            column = str(column)
            cl = len(column)
            try:
                ml = lengths[i]
                if cl > ml:
                    lengths[i] = cl
            except IndexError:
                lengths.append(cl)
    lengths = tuple(lengths)
    format_string = ""
    for length in lengths:
        format_string += "| "
        format_string += "%-" + str(length + 4) + "s "
    format_string += "|" + "\n"
    matrix_str = ""
    if header:
        matrix_str += format_string % header
        matrix_str += "-" * (len(matrix_str.split('\n')[0])) + "\n"
    for row in matrix:
        matrix_str += format_string % tuple(row)
        # matrix_str += "-" * (len(matrix_str.split('\n')[0]) ) + "\n"
    matrix_str += "-" * (len(matrix_str.split('\n')[0])) + "\n"
    top_border_length = (len(matrix_str.split('\n')[0]))
    return matrix_str, top_border_length


'''
Perform calculation based on the results retrieved.
Difference between reltuples and actuall record count from table
Difference %, and if the variation is higher than 5%, it's most likely that the stats are not updated.
'''


def prepare_output_summary(final_output, header):
    summary_output = []
    for record in final_output:
        reltuple_relcount_diff = record[1] - record[2]
        retuple_relcount_variation = format(reltuple_relcount_diff / float(1 if record[1] == 0 else record[1]) * 100,
                                            '.2f')
        record.append(abs(reltuple_relcount_diff))
        record.append('Stats are outdated, please ANALYZE' if abs(
            float(retuple_relcount_variation)) > variation_permitted else 'Stats seems ok')
        summary_output.append(record)

    result, top_border_length = list_to_tabular_formatting(summary_output, header)
    #print("Stat check summary".center(top_border_length))
    #print "-" * top_border_length
    #print result.strip('\n')
    logger.info("Stat check summary\n%s\n%s" % (("-" * top_border_length), result))
    logger.info(
        "Differences between record count and estimated count higher than %d%% variation suggests outdated stats"
        % variation_permitted)

    '''
Create a sql file with analyze statements
'''
    try:
        with open(analyze_sql_filename, 'w') as f:
            for row in summary_output:
                if re.findall("ANALYZE", row[-1]):
                    # if re.findall("ANALYZE",row[len(row)-1]):
                    f.write("ANALYZE %s ; \n" % row[0])
    except Exception, e:
        logger.error("Failed to write %s file. Error message: %s" % (analyze_sql_filename, str(e)))
        sys.exit(1)

    try:
        analyze_table_count_p = subprocess.Popen(['awk', 'END{print NR}', analyze_sql_filename], stdout=subprocess.PIPE)
        analyze_table_count = analyze_table_count_p.communicate()[0]
        analyze_table_count = int(analyze_table_count)
    except:
        logger.error("awk 'END{print NR}' %s command failed. Exiting !!" % analyze_sql_filename)
        sys.exit(1)

    if analyze_table_count > 0:
        logger.info(
            "To analyze tables with outdated stats, please run: psql -d %s -f %s" % (database, analyze_sql_filename))
    else:
        subprocess.call(['rm -f %s' % analyze_sql_filename], shell=True, stdout=subprocess.PIPE)

    #logger.info("Final result is : \n%s" % result)


'''
Define the query to collect statistics for table
'''


def get_stats_query(fully_qualified_table_name):
    stats_collect_query = "select pn.nspname||'.'||pg.relname,(select count(*) from %s),reltuples::bigint from \
                          pg_catalog.pg_class pg, pg_catalog.pg_namespace pn where pg.relnamespace = pn.oid and \
                          pg.oid='%s'::regclass::oid" % (fully_qualified_table_name, fully_qualified_table_name)
    return stats_collect_query


'''
Define the query to collect the fully qualified table name
'''


def get_relname_query(relation_oid_comma_separated):
    relname_query = "select pn.nspname||'.'||pg.relname, pg.relstorage, pg.relhassubclass from pg_class pg,\
                    pg_namespace pn where pg.relnamespace = pn.oid and pg.oid in (%s)" % relation_oid_comma_separated
    return relname_query


'''
Print number of tables already scanned to evaluate record count vs reltuples
'''


def progress_status(i, no_of_elements):
    if i <= no_of_elements:
        sys.stdout.write('\r')
        sys.stdout.write("Tables scanned: %d" % (i))
        sys.stdout.flush()


"""
start_execution is the main module. It checks if the input query files exists, & verifies if the first word is explain.
Executes the user query to evaluate the explain plan, retrieves the table names identified by the plan.
Pulls out all the tables which have the same table name, irrespective of the schema. Since explain plan does not provide
the schema name, thus its challenging to collect
the respective schema name, so we collect all the entries from pg_class with the same table name. We believe, that often
a same table name may exists in 4-5 schema's, since users may have development, staging, production schema etc, in the
same database having same table name.
"""


def start_execution():
    check_file_exists_and_permission(queryfile, os.R_OK, 'f')

    with open(queryfile, 'r') as file_handle:
        query_from_file = 'EXPLAIN VERBOSE ' + file_handle.read().lstrip(' \t\n\r')
    logger.debug("Query provided in file %s is : \n%s" % (queryfile, query_from_file))

    if not re.match("explain\s+verbose\s+", query_from_file, re.IGNORECASE):
        logger.error("Input file %s does not have EXPLAIN VERBOSE clause, please add and rerun. Exiting !!" % queryfile)
        sys.exit(1)
    """
    Below statement is just to verify the query provided in the input file. At this point, currently to redirect the
    client_min_messages we use psql to execute the query and we capture stderr to capture if the query fall back to
    legacy planner since stats verification is different for orca and legacy planner"
    """
    execute_query_on_db(query_from_file)

    query_from_file = "set client_min_messages ='log';" + query_from_file
    logger.debug("EXPLAIN VERBOSE keyword is present in file %s." % queryfile)

    explain_verbose_plan, client_min_messages_log = execute_query_on_db_psql(query_from_file)
    logger.debug("Message from the server:\n%s" % client_min_messages_log)
    planner = 'orca'
    for element in client_min_messages_log:
        if re.findall('\s+Planner\s+produced\s+plan\s+:', element) or len(element) == 0:
            planner = 'legacy'
            logger.info("Input query will be executed using the legacy planner.")
            if len(element) != 0 and int(element.split(':')[2]) != 0:
                logger.info("Unexpected event, should not non-zero")
    if planner != 'legacy':
        logger.debug("ORCA generated plan")

    relid_list = []
    explain_plan_entry = 0
    explain_plan_only = []
    for element in explain_verbose_plan:
        if re.findall('\s+:relid\s+', element):
            relid_list.append(int(element.split()[1]))
        if re.findall('Gather\s+Motion', element):
            explain_plan_entry = 1
        if explain_plan_entry == 1:
            explain_plan_only.append(element)
    relation_oid_comma_separated = convert_list_to_string(relid_list)
    explain_plan_str = '\n'.join(explain_plan_only)
    logger.debug("Plan:\n" + explain_plan_str)

    fully_qualified_table_name_query = get_relname_query(relation_oid_comma_separated)
    logger.debug("Query to collect fully qualified table names is: \n%s" % fully_qualified_table_name_query)
    full_qualified_table_name = execute_query_on_db(fully_qualified_table_name_query)

    final_query_list = []
    views_parent_table_list = []

    logger.info("No of tables scanned during user query execution: %d." % len(full_qualified_table_name))
    logger.info("Stats check in progress, please wait for completion, as it may take time.")
    i = 1
    for collect_schema_query_op in full_qualified_table_name:
        no_of_elements = len(full_qualified_table_name)
        relstorage = collect_schema_query_op[1]
        relhassubclass = collect_schema_query_op[2]
        if (collect_schema_query_op[1] in ('x', 'v') or collect_schema_query_op[2] is True) and planner == 'legacy':
            if relstorage == 'x':
                table_type = 'External Table'
            elif relstorage == 'v':
                table_type = 'View'
            elif relhassubclass is True:
                table_type = 'Top level partition'
            collect_schema_query_op[1] = table_type
            views_parent_table_entry = collect_schema_query_op[:2]
            views_parent_table_list.append(views_parent_table_entry)
            progress_status(i, no_of_elements)
            i = i + 1
            continue
        query = get_stats_query(collect_schema_query_op[0])
        logger.debug("%s" % query)
        final_query_list.append(execute_query_on_db(query)[0])
        progress_status(i, no_of_elements)
        i = i + 1
    header = ("Table Name", "Record Count", "Estimated Count", "Variation", "Comments - based on variation %")
    print('\n')
    prepare_output_summary(final_query_list, header)

    if planner == 'legacy' and len(views_parent_table_list) > 0:
        logger.info("Stats check for below tables is not required, as they are either view or top level partition")
        header = ("Table Name", "Table Type")
        result, top_border_length = list_to_tabular_formatting(views_parent_table_list, header)
        print "-" * top_border_length
        print result.strip('\n')


"""
Check if the input query file has permission to read
"""


def check_file_exists_and_permission(file_dir_name, permission_mode, file_dir_flag):
    if 'f' == file_dir_flag:
        if os.path.isfile(file_dir_name):
            if not (os.access(file_dir_name, permission_mode)):
                logger.error("Input file %s does not have read permission. Exiting !!" % file_dir_name)
                sys.exit(1)
        else:
            logger.error("Input file %s does not exist. Exiting !!" % file_dir_name)
            sys.exit(1)


"""
initialize_db_connection is used to verify if a connection can be established with the database
"""


def initialize_db_connection(db_connection_params):
    try:
        db_connection_object = pgdb.connect(**db_connection_params)
        return db_connection_object
    except Exception, e:
        logger.error("%s. Connection to database %s at port %d failed. Exiting !! " % (
        str(e).strip('\n'), options.database, options.port))
        sys.exit(1)


'''
Validate arguments, prepare database connection arguments, and create a logger
'''

if __name__ == "__main__":
    parser = optparse.OptionParser("Usage: %prog [options] ./gp_stats_check -f query1.sql -p database port")

    group = optparse.OptionGroup(parser, "Database specific optional arguments")
    group.add_option("-p", "--port", action="store", dest="port", type='int', default=5432,
                     help="Port of database to connect [default: 5432]")
    group.add_option("-d", "--database", action="store", default='template1',
                     help="Name of the database to connect [default: template1]")
    group.add_option("-u", "--user", action="store", default=getpass.getuser(),
                     help="Name of the user to connect to database [default: User who executes the script")
    group.add_option("-s", "--server", action="store", default='localhost',
                     help="Hostname used for database connection")
    parser.add_option_group(group)
    parser.add_option("-f", "--queryfile", dest="queryfile", action="store", help="Filename holding SQL")
    parser.add_option("-?", "--usage", dest="usage", action="store_false")
    parser.add_option("-v", "--version", dest="version", action="store_false")
    parser.add_option("-V", "--verbose", dest="verbose", action="store_false")
    (options, args) = parser.parse_args()

    '''
Capture values of options
'''
    version = options.version
    if version is not None:
        sys.exit("%s v1.0" % os.path.basename(__file__))
    usage = options.usage
    queryfile = options.queryfile
    port = options.port
    os.environ['PGPORT'] = str(port)
    database = options.database
    user = options.user
    host = options.server
    verbose = options.verbose

    '''
Initialize some variables used during execution
'''
    timeStamp = time.strftime("%Y%m%d%H%M%S")
    db_connection_params = {'host': host, 'database': database, 'user': user}
    variation_permitted = 5.0
    analyze_sql_filename = os.path.basename(__file__).strip('.py') + '_' + timeStamp + '.sql'

    '''
Create loggers, logger.info to write on console. logger.debug and logger.info messages go to the logs as well
'''
    logger = logging.getLogger('%s ' % os.path.basename(__file__))
    prefix_for_files = os.path.basename(__file__).strip('.py')
    if verbose is None:
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG

    try:
        logging.basicConfig(level=logging_level, \
                            format='%(asctime)s [%(levelname)s] %(message)s', \
                            datefmt='%Y-%m-%d %H:%M', \
                            filename=prefix_for_files + '_' + timeStamp + '.log', \
                            filemode='w')
    except:
        print "ERROR: Output log file cannot be created in the current working directory %s. Check directory \
              permissions. Exiting!! " % os.getcwd()
        sys.exit(1)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s]:- %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

    logger.debug("Command line: %s" % str(sys.argv))

    if usage is False:
        parser.print_help()
        sys.exit(1)

    if queryfile is None:
        logger.error("Please provide the query in a file using -f option. Exiting !!")
        sys.exit(1)

    logger.info("Execution started. Please refer to log file name: %s for details in current working directory." % (
    prefix_for_files + '_' + timeStamp + '.log'))
    try:
        start_execution()
    except KeyboardInterrupt:
        logger.error("\nExecution terminated by user using CTRL+C")
        sys.exit(1)
    logger.info("Execution finished successfully !!")
