"""Run a single query from the big data benchmark on a remote EC2 cluster.

   This will execute a single query from the benchmark multiple times
   and output percentile results. Note that to run an entire suite of
   queries, you'll need to wrap this script and call run multiple times.
"""

import subprocess
import sys
from sys import stderr
from optparse import OptionParser
import os
import time
from pg8000 import DBAPI

# A scratch directory on your filesystem
LOCAL_TMP_DIR = "/tmp"

### Benchmark Queries ###
TMP_TABLE = "result"
TMP_TABLE_CACHED = "result_cached"
CLEAN_QUERY = "DROP TABLE %s;" % TMP_TABLE

# TODO: Factor this out into a separate file
QUERY_1a_HQL = "SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1000"
QUERY_1b_HQL = QUERY_1a_HQL.replace("1000", "100")
QUERY_1c_HQL = QUERY_1a_HQL.replace("1000", "10")

QUERY_2a_HQL = "SELECT SUBSTR(sourceIP, 1, 8), SUM(adRevenue) FROM " \
                 "uservisits GROUP BY SUBSTR(sourceIP, 1, 8)"
QUERY_2b_HQL = QUERY_2a_HQL.replace("8", "10")
QUERY_2c_HQL = QUERY_2a_HQL.replace("8", "12")

QUERY_3a_HQL = """SELECT sourceIP, 
                          sum(adRevenue) as totalRevenue, 
                          avg(pageRank) as pageRank 
                   FROM
                     rankings R JOIN
                     (SELECT sourceIP, destURL, adRevenue 
                      FROM uservisits UV 
                      WHERE UV.visitDate > "1980-01-01"
                      AND UV.visitDate < "1980-04-01") 
                      NUV ON (R.pageURL = NUV.destURL) 
                   GROUP BY sourceIP
                   ORDER BY totalRevenue DESC
                   LIMIT 1"""
QUERY_3a_HQL = " ".join(QUERY_3a_HQL.replace("\n", "").split())
QUERY_3b_HQL = QUERY_3a_HQL.replace("1980-04-01", "1983-01-01")
QUERY_3c_HQL = QUERY_3a_HQL.replace("1980-04-01", "2010-01-01")

QUERY_4_HQL = """DROP TABLE IF EXISTS url_counts_partial;
                 CREATE TABLE url_counts_partial AS 
                   SELECT TRANSFORM (line) 
                   USING "python /root/url_count.py" as (sourcePage, 
                     destPage, count) from documents;
                 DROP TABLE IF EXISTS url_counts_total;
                 CREATE TABLE url_counts_total AS 
                   SELECT SUM(count) AS totalCount, destpage 
                   FROM url_counts_partial GROUP BY destpage;"""
QUERY_4_HQL = " ".join(QUERY_4_HQL.replace("\n", "").split())

QUERY_1_PRE = "CREATE TABLE %s (pageURL STRING, pageRank INT);" % TMP_TABLE
QUERY_2_PRE = "CREATE TABLE %s (sourceIP STRING, adRevenue DOUBLE);" % TMP_TABLE
QUERY_3_PRE = "CREATE TABLE %s (sourceIP STRING, " \
    "adRevenue DOUBLE, pageRank DOUBLE);" % TMP_TABLE

QUERY_1a_SQL = QUERY_1a_HQL
QUERY_1b_SQL = QUERY_1b_HQL
QUERY_1c_SQL = QUERY_1c_HQL

QUERY_2a_SQL = QUERY_2a_HQL.replace("SUBSTR", "SUBSTRING")
QUERY_2b_SQL = QUERY_2b_HQL.replace("SUBSTR", "SUBSTRING")
QUERY_2c_SQL = QUERY_2c_HQL.replace("SUBSTR", "SUBSTRING")
QUERY_3a_SQL = """SELECT sourceIP, totalRevenue, avgPageRank
                    FROM
                      (SELECT sourceIP,
                              AVG(pageRank) as avgPageRank,
                              SUM(adRevenue) as totalRevenue
                      FROM Rankings AS R, UserVisits AS UV
                      WHERE R.pageURL = UV.destinationURL
                      AND UV.visitDate 
                        BETWEEN Date('1980-01-01') AND Date('1980-04-01')
                      GROUP BY UV.sourceIP)
                   ORDER BY totalRevenue DESC LIMIT 1""".replace("\n", "")
QUERY_3a_SQL = " ".join(QUERY_3a_SQL.replace("\n", "").split())
QUERY_3b_SQL = QUERY_3a_SQL.replace("1980-04-01", "1983-01-01")
QUERY_3c_SQL = QUERY_3a_SQL.replace("1980-04-01", "2010-01-01")

def create_as(query):
  return "CREATE TABLE %s AS %s;" % (TMP_TABLE, query)
def insert_into(query):
  return "INSERT INTO TABLE %s %s;" % (TMP_TABLE, query)

IMPALA_MAP = {'1a': QUERY_1_PRE, '1b': QUERY_1_PRE, '1c': QUERY_1_PRE,
              '2a': QUERY_2_PRE, '2b': QUERY_2_PRE, '2c': QUERY_2_PRE,
              '3a': QUERY_3_PRE, '3b': QUERY_3_PRE, '3c': QUERY_3_PRE}

QUERY_MAP = {
             '1a':  (create_as(QUERY_1a_HQL), insert_into(QUERY_1a_HQL), 
                     create_as(QUERY_1a_SQL)),
             '1b':  (create_as(QUERY_1b_HQL), insert_into(QUERY_1b_HQL), 
                     create_as(QUERY_1b_SQL)),
             '1c':  (create_as(QUERY_1c_HQL), insert_into(QUERY_1c_HQL), 
                     create_as(QUERY_1c_SQL)),
             '2a': (create_as(QUERY_2a_HQL), insert_into(QUERY_2a_HQL), 
                    create_as(QUERY_2a_SQL)),
             '2b': (create_as(QUERY_2b_HQL), insert_into(QUERY_2b_HQL), 
                    create_as(QUERY_2b_SQL)),
             '2c': (create_as(QUERY_2c_HQL), insert_into(QUERY_2c_HQL), 
                    create_as(QUERY_2c_SQL)),
             '3a': (create_as(QUERY_3a_HQL), insert_into(QUERY_3a_HQL), 
                    create_as(QUERY_3a_SQL)),
             '3b': (create_as(QUERY_3b_HQL), insert_into(QUERY_3b_HQL), 
                    create_as(QUERY_3b_SQL)),
             '3c': (create_as(QUERY_3c_HQL), insert_into(QUERY_3c_HQL), 
                    create_as(QUERY_3c_SQL)),
             '4':  (QUERY_4_HQL, None, None)}

# Turn a given query into a version using cached tables
def make_input_cached(query):
  return query.replace("uservisits", "uservisits_cached") \
              .replace("rankings", "rankings_cached") \
              .replace("url_counts_partial", "url_counts_partial_cached") \
              .replace("url_counts_total", "url_counts_total_cached") \
              .replace("documents", "documents_cached")

# Turn a given query into one that creats cached tables
def make_output_cached(query):
  return query.replace(TMP_TABLE, TMP_TABLE_CACHED)

### Runner ###
def parse_args():
  parser = OptionParser(usage="run_query.py [options]")

  parser.add_option("-m", "--impala", action="store_true", default=False,
      help="Whether to include Impala")
  parser.add_option("-s", "--shark", action="store_true", default=False,
      help="Whether to include Shark")
  parser.add_option("-r", "--redshift", action="store_true", default=False,
      help="Whether to include Redshift")

  parser.add_option("-g", "--shark-no-cache", action="store_true", 
      default=False, help="Disable caching in Shark")
  parser.add_option("-v", "--shark-use-hive", action="store_true",
      default=False, help="Causes Shark to use Hive's execution mode") 
  parser.add_option("--impala-use-hive", action="store_true",
      default=False, help="Use Hive for query executio on Impala nodes") 
  parser.add_option("-t", "--reduce-tasks", type="int", default=150, 
      help="Number of reduce tasks in Shark")
  parser.add_option("-z", "--clear-buffer-cache", action="store_true",
      default=False, help="Clear disk buffer cache between query runs")

  parser.add_option("-a", "--impala-hosts",
      help="Hostnames of Impala nodes (comma seperated)")
  parser.add_option("-b", "--shark-host",
      help="Hostname of Shark master node")
  parser.add_option("-c", "--redshift-host",
      help="Hostname of Redshift ODBC endpoint")

  parser.add_option("-x", "--impala-identity-file",
      help="SSH private key file to use for logging into Impala node")
  parser.add_option("-y", "--shark-identity-file",
      help="SSH private key file to use for logging into Shark node")
  parser.add_option("-u", "--redshift-username",
      help="Username for Redshift ODBC connection")
  parser.add_option("-p", "--redshift-password",
      help="Password for Redshift ODBC connection")
  parser.add_option("-e", "--redshift-database",
      help="Database to use in Redshift")
  parser.add_option("--num-trials", type="int", default=10,
      help="Number of trials to run for this query")


  parser.add_option("-q", "--query-num", default="1",
      help="Which query to run in benchmark")

  (opts, args) = parser.parse_args()

  # Don't bother trying to cache if we are using Hive
  if opts.shark_use_hive:
    opts.shark_no_cache = True

  if not (opts.impala or opts.shark or opts.redshift):
    parser.print_help()
    sys.exit(1)

  if opts.impala and (opts.impala_identity_file is None or 
                      opts.impala_hosts is None):
    print >> stderr, "Impala requires identity file and hostname"
    sys.exit(1)
  
  if opts.shark and (opts.shark_identity_file is None or 
                     opts.shark_host is None):
    print >> stderr, \
        "Shark requires identity file and hostname"
    sys.exit(1)
  
  if opts.redshift and (opts.redshift_username is None or
                        opts.redshift_password is None or
                        opts.redshift_host is None or
                        opts.redshift_database is None):
    print >> stderr, \
        "Redshift requires host, username, password and db"
    sys.exit(1)

  if opts.impala:
    hosts = opts.impala_hosts.split(",")
    print >> stderr, "Impala hosts:\n%s" % "\n".join(hosts)
    opts.impala_hosts = hosts

  if opts.query_num not in QUERY_MAP:
    print >> stderr, "Unknown query number: %s" % opts.query_num
    sys.exit(1)
    
  return opts

# Run a command on a host through ssh, throwing an exception if ssh fails
def ssh(host, username, identity_file, command):
  subprocess.check_call(
      "ssh -t -o StrictHostKeyChecking=no -i %s %s@%s '%s'" %
      (identity_file, username, host, command), shell=True)

# Copy a file to a given host through scp, throwing an exception if scp fails
def scp_to(host, identity_file, username, local_file, remote_file):
  subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s' '%s@%s:%s'" %
      (identity_file, local_file, username, host, remote_file), shell=True)

# Copy a file to a given host through scp, throwing an exception if scp fails
def scp_from(host, identity_file, username, remote_file, local_file):
  subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s@%s:%s' '%s'" %
      (identity_file, username, host, remote_file, local_file), shell=True)

def run_shark_benchmark(opts):
  def ssh_shark(command):
    ssh(opts.shark_host, "root", opts.shark_identity_file, command)

  print "Restarting standalone scheduler..."
  ssh_shark("/root/spark/bin/stop-all.sh")
  time.sleep(30)
  ssh_shark("/root/spark/bin/stop-all.sh")
  ssh_shark("/root/spark/bin/start-all.sh")
  time.sleep(10)

  prefix = str(time.time()).split(".")[0]
  query_file_name = "%s_workload.sh" % prefix
  local_query_file = os.path.join(LOCAL_TMP_DIR, query_file_name)
  query_file = open(local_query_file, 'w')
  remote_result_file = "/mnt/%s_results" % prefix
  remote_tmp_file = "/mnt/%s_out" % prefix
  remote_query_file = "/mnt/%s" % query_file_name

  runner = "/root/shark-0.2/bin/shark-withinfo"
  
  # Two modes here: If using Shark disk or Hive, clear buffer cache in-between
  # each query. If using Shark in memory, send one large query manifest. This saves
  # the time of re-building the cached table each time. 
  if opts.shark_use_hive or opts.shark_no_cache:
    query_list = "set mapred.reduce.tasks = %s;" % opts.reduce_tasks
    if opts.shark_use_hive:
      query_list = query_list + "set shark.exec.mode='hive';" 

   # Throw away query for JVM warmup
    query_list = query_list + "SELECT COUNT(*) FROM scratch;"
    if '4' not in opts.query_num:
      query_list = query_list + CLEAN_QUERY 
    query_list = query_list + QUERY_MAP[opts.query_num][0]

    query_file.write(
      "%s -e '%s' > %s 2>&1\n" % (runner, query_list, remote_tmp_file))

    query_file.write(
        "cat %s | grep Time | grep -v INFO |grep -v MapReduce >> %s" % (
          remote_tmp_file, remote_result_file))
    query_file.close()

    print "Copying files to Shark"
    scp_to(opts.shark_host, opts.shark_identity_file, "root", local_query_file, 
        remote_query_file)
    ssh_shark("chmod 775 %s" % remote_query_file)
    
    # Run benchmark
    print "Running remote benchmark..."
    for i in range(opts.num_trials):
      if opts.clear_buffer_cache:
        ssh_shark("python /root/shark-0.2/bin/dev/clear-buffer-cache.py")
      ssh_shark("%s" % remote_query_file)
  else:
    query_list = "DROP TABLE IF EXISTS uservisits_cached;" \
                 "DROP TABLE IF EXISTS rankings_cached;" \
                 "CREATE TABLE uservisits_cached AS SELECT * FROM uservisits;" \
                 "CREATE TABLE rankings_cached AS SELECT * FROM rankings;" \
                 "set mapred.reduce.tasks = %s;" % opts.reduce_tasks

    if '4' in opts.query_num:
      # Query 4 uses entirely different tables
      query_list = "DROP TABLE IF EXISTS documents_cached;" \
                   "CREATE TABLE documents_cached AS SELECT * FROM documents;" \
                   "set mapred.reduce.tasks = %s;" % opts.reduce_tasks

    for i in range(opts.num_trials):
      if '4' not in opts.query_num:
        query_list = query_list + make_output_cached(CLEAN_QUERY)
      query_list = query_list + make_output_cached(
          make_input_cached(QUERY_MAP[opts.query_num][0]))

    query_file.write(
        "%s -e '%s' > %s 2>&1\n" % (runner, query_list, remote_tmp_file))
    query_file.write(
      "cat %s | grep Time |grep -v INFO | grep -v MapReduce > %s" % (
        remote_tmp_file, remote_result_file))
    query_file.close()

    print "Copying files to Shark"
    scp_to(opts.shark_host, opts.shark_identity_file, "root", local_query_file, 
        remote_query_file)
    ssh_shark("chmod 775 %s" % remote_query_file)

    # Run benchmark
    print "Running remote benchmark..."
    ssh_shark("%s" % remote_query_file)

  # Collect results
  local_results_file = os.path.join(LOCAL_TMP_DIR, "%s_results" % prefix)
  scp_from(opts.shark_host, opts.shark_identity_file, "root", 
      "/mnt/%s_results" % prefix, local_results_file) 
  contents = open(local_results_file).readlines()
  results = map(lambda x: float(x.split(": ")[1].split(" ")[0]), contents)

  def every_n_entries(lst, n, offset): # assumes offset < n
    return [k[1] for k in enumerate(lst) if k[0] % n == offset]

  print opts.query_num
  if '4' in opts.query_num:
    if not opts.shark_no_cache and not opts.shark_use_hive:  
      results = results[2:] # Remove results from cached table creation
      results_a = every_n_entries(results, 4, 1)
      results_b = every_n_entries(results, 4, 3)
    else:
      results_a = every_n_entries(results, 5, 2)
      results_b = every_n_entries(results, 5, 4)
    print "Parts: %s, %s" % (results_a, results_b)
    zipped = zip(results_a, results_b)
    results = map(lambda k: float(k[0]) + float(k[1]), zipped)
  else:
    if not opts.shark_no_cache and not opts.shark_use_hive:
      results = results[4:] # Remove results from table creation
      results = every_n_entries(results, 2, 1) # Remove cleanup query
    else:
      results = every_n_entries(results, 3, 2) # Remove cleanup and warmup

  # Clean-up
  #ssh_shark("rm /mnt/%s*" % prefix)
  os.unlink(local_query_file)
  os.unlink(local_results_file)

  return results

def run_impala_benchmark(opts):
  impala_host = opts.impala_hosts[0]
  def ssh_impala(command): 
    ssh(impala_host, "ubuntu", opts.impala_identity_file, command)

  runner = "impala-shell -r -q"
  if (opts.impala_use_hive):
    runner = "hive -e"

  prefix = str(time.time()).split(".")[0]
  query_file_name = "%s_workload.sh" % prefix
  local_query_file = os.path.join(LOCAL_TMP_DIR, query_file_name)
  query_file = open(local_query_file, 'w')
  remote_tmp_file = "/tmp/%s_tmp" % prefix
  remote_result_file = "/tmp/%s_results" % prefix

  query_file.write("hive -e '%s'\n" % IMPALA_MAP[opts.query_num])
  query = QUERY_MAP[opts.query_num][1]

  # Populate the full buffer cache if running Impala + cached
  if (not opts.impala_use_hive) and (not opts.clear_buffer_cache):
    query = "select count(*) from rankings;" + query
    query = "select count(*) from uservisits;" + query

  connect_stmt = "connect localhost;"
  if (opts.impala_use_hive):
    connect_stmt = ""

  query_file.write(
      "%s '%s%s' > %s 2>&1;\n" % (runner, connect_stmt, query, remote_tmp_file))
  query_file.write("cat %s |egrep 'Inserted|Time' |grep -v MapReduce >> %s;\n" % (
      remote_tmp_file, remote_result_file)) 
  query_file.write("hive -e '%s';\n" % CLEAN_QUERY)
  query_file.close()

  remote_query_file = "/tmp/%s" % query_file_name
  print >> stderr, "Copying files to Impala"
  scp_to(impala_host, opts.impala_identity_file, "ubuntu", 
      local_query_file, remote_query_file)
  ssh_impala("chmod 775 %s" % remote_query_file)

  # Run benchmark
  print >> stderr, "Running remote benchmark..."
  for i in range(opts.num_trials):
    if opts.clear_buffer_cache:
      for host in opts.impala_hosts:
        ssh(host, "ubuntu", opts.impala_identity_file,
            "sudo bash -c \"sync && echo 3 > /proc/sys/vm/drop_caches\"")
    ssh_impala("sudo -u hdfs %s" % remote_query_file)

  # Collect results
  local_result_file = os.path.join(LOCAL_TMP_DIR, "%s_results" % prefix)
  scp_from(impala_host, opts.impala_identity_file, "ubuntu", 
      remote_result_file, local_result_file) 
  contents = open(local_result_file).readlines()

  if opts.impala_use_hive:
    results = map(lambda x: float(x.split(": ")[1].split(" ")[0]), contents)
  else:
    results = map(lambda x: float(x.split("in ")[1].split("s")[0]), contents)

  # Clean-up
  #ssh_impala("rm -f /tmp/%s*" % prefix) # Temporarily disabled
  os.unlink(local_query_file)
  os.unlink(local_result_file)

  return results

def run_redshift_benchmark(opts):
  conn = DBAPI.connect(
    host = opts.redshift_host,
    database = opts.redshift_database,
    user = opts.redshift_username,
    password = opts.redshift_password,
    port = 5439,
    socket_timeout=6000)
  print >> stderr, "Connecting to Redshift..."
  cursor = conn.cursor()

  print >> stderr, "Connection succeeded..."
  times = []
  # Clean up old table if still exists
  try:
    cursor.execute(CLEAN_QUERY)
  except:
    pass
  for i in range(opts.num_trials):
    t0 = time.time()
    cursor.execute(QUERY_MAP[opts.query_num][2])
    times.append(time.time() - t0)
    cursor.execute(CLEAN_QUERY)
  return times

def get_percentiles(in_list):
  def get_pctl(lst, pctl):
    return lst[int(len(lst) * pctl)]
  in_list = sorted(in_list)
  return "%s\t%s\t%s" % (
    get_pctl(in_list, 0.05),
    get_pctl(in_list, .5),
    get_pctl(in_list, .95)
  )

def main():
  opts = parse_args()
  print "Query %s:" % opts.query_num
  if opts.impala:
    results = run_impala_benchmark(opts)
  if opts.shark:
    results = run_shark_benchmark(opts)
  if opts.redshift:
    results = run_redshift_benchmark(opts)

  def prettylist(lst):
    return ",".join([str(k) for k in lst]) 

  print "Resutlts: %s" % prettylist(results)
  print "Percentiles: %s" % get_percentiles(results)
  print "Best: %s"  % min(results)

if __name__ == "__main__":
  main()
