# Copyright 2013 The Regents of The University California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
import datetime
import re
import multiprocessing
from StringIO import StringIO
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

QUERY_4_HQL_HIVE_UDF = """DROP TABLE IF EXISTS url_counts_partial;
                 CREATE TABLE url_counts_partial AS 
                   SELECT TRANSFORM (line) 
                   USING "python /tmp/url_count.py" as (sourcePage, 
                     destPage, count) from documents;
                 DROP TABLE IF EXISTS url_counts_total;
                 CREATE TABLE url_counts_total AS 
                   SELECT SUM(count) AS totalCount, destpage 
                   FROM url_counts_partial GROUP BY destpage;"""
QUERY_4_HQL_HIVE_UDF = " ".join(QUERY_4_HQL_HIVE_UDF.replace("\n", "").split())

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
def count(query):
  return query
  return "SELECT COUNT(*) FROM (%s) q;" % query

IMPALA_MAP = {'1a': QUERY_1_PRE, '1b': QUERY_1_PRE, '1c': QUERY_1_PRE,
              '2a': QUERY_2_PRE, '2b': QUERY_2_PRE, '2c': QUERY_2_PRE,
              '3a': QUERY_3_PRE, '3b': QUERY_3_PRE, '3c': QUERY_3_PRE}

TEZ_MAP =    {'1a':(count(QUERY_1a_HQL),), '1b':(count(QUERY_1b_HQL),), '1c': (count(QUERY_1c_HQL),),
              '2a':(count(QUERY_2a_HQL),), '2b':(count(QUERY_2b_HQL),), '2c': (count(QUERY_2c_HQL),),
              '3a':(count(QUERY_3a_HQL),), '3b':(count(QUERY_3b_HQL),), '3c': (count(QUERY_3c_HQL),)}

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
             '4':  (QUERY_4_HQL, None, None),
             '4_HIVE':  (QUERY_4_HQL_HIVE_UDF, None, None)}

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
  parser.add_option("--hive", action="store_true", default=False,
      help="Whether to include Hive")
  parser.add_option("--tez", action="store_true", default=False,
      help="Use in conjunction with --hive")
  parser.add_option("--hive-cdh", action="store_true", default=False,
      help="Hive on CDH cluster")

  parser.add_option("-g", "--shark-no-cache", action="store_true", 
      default=False, help="Disable caching in Shark")
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
  parser.add_option("--hive-host",
      help="Hostname of Hive master node")
  parser.add_option("--hive-slaves",
      help="Hostnames of Hive slaves (comma seperated)")

  parser.add_option("-x", "--impala-identity-file",
      help="SSH private key file to use for logging into Impala node")
  parser.add_option("-y", "--shark-identity-file",
      help="SSH private key file to use for logging into Shark node")
  parser.add_option("--hive-identity-file",
      help="SSH private key file to use for logging into Hive node")
  parser.add_option("-u", "--redshift-username",
      help="Username for Redshift ODBC connection")
  parser.add_option("-p", "--redshift-password",
      help="Password for Redshift ODBC connection")
  parser.add_option("-e", "--redshift-database",
      help="Database to use in Redshift")
  parser.add_option("--num-trials", type="int", default=10,
      help="Number of trials to run for this query")
  parser.add_option("--prefix", type="string", default="",
      help="Prefix result files with this string")

  parser.add_option("-q", "--query-num", default="1a",
                    help="Which query to run in benchmark: " \
                    "%s" % ", ".join(QUERY_MAP.keys()))

  (opts, args) = parser.parse_args()

  if not (opts.impala or opts.shark or opts.redshift or opts.hive or opts.hive_cdh):
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
  if opts.hive or opts.hive_cdh:
    opts.hive_slaves = opts.hive_slaves.split(",")
    print >> stderr, "Hive slaves:\n%s" % "\n".join(opts.hive_slaves)

  if opts.query_num not in QUERY_MAP:
    print >> stderr, "Unknown query number: %s" % opts.query_num
    sys.exit(1)
    
  return opts

# Run a command on a host through ssh, throwing an exception if ssh fails
def ssh(host, username, identity_file, command):
  return subprocess.check_call(
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
    command = "source /root/.bash_profile; %s" % command
    ssh(opts.shark_host, "root", opts.shark_identity_file, command)

  local_clean_query = CLEAN_QUERY
  local_query_map = QUERY_MAP

  prefix = str(time.time()).split(".")[0]
  query_file_name = "%s_workload.sh" % prefix
  slaves_file_name = "%s_slaves" % prefix
  local_query_file = os.path.join(LOCAL_TMP_DIR, query_file_name)
  local_slaves_file = os.path.join(LOCAL_TMP_DIR, slaves_file_name)
  query_file = open(local_query_file, 'w')
  remote_result_file = "/mnt/%s_results" % prefix
  remote_tmp_file = "/mnt/%s_out" % prefix
  remote_query_file = "/mnt/%s" % query_file_name

  runner = "/root/shark/bin/shark-withinfo"

  print "Getting Slave List"
  scp_from(opts.shark_host, opts.shark_identity_file, "root",
           "/root/spark-ec2/slaves", local_slaves_file)
  slaves = map(str.strip, open(local_slaves_file).readlines())

  print "Restarting standalone scheduler..."
  ssh_shark("/root/spark/bin/stop-all.sh")
  ensure_spark_stopped_on_slaves(slaves)
  time.sleep(30)
  ssh_shark("/root/spark/bin/stop-all.sh")
  ssh_shark("/root/spark/bin/start-all.sh")
  time.sleep(10)

  # Two modes here: Shark Mem and Shark Disk. If using Shark disk clear buffer
  # cache in-between each query. If using Shark Mem, used cached tables.

  query_list = "set mapred.reduce.tasks = %s;" % opts.reduce_tasks

  # Throw away query for JVM warmup
  query_list += "SELECT COUNT(*) FROM scratch;"

  # Create cached queries for Shark Mem
  if not opts.shark_no_cache:
    local_clean_query = make_output_cached(CLEAN_QUERY)

    def convert_to_cached(query):
      return (make_output_cached(make_input_cached(query[0])), )

    local_query_map = {k: convert_to_cached(v) for k, v in QUERY_MAP.items()}

    # Set up cached tables
    if '4' in opts.query_num:
      # Query 4 uses entirely different tables
      query_list += """
                    DROP TABLE IF EXISTS documents_cached;
                    CREATE TABLE documents_cached AS SELECT * FROM documents;
                    """
    else:
      query_list += """
                    DROP TABLE IF EXISTS uservisits_cached;
                    DROP TABLE IF EXISTS rankings_cached;
                    CREATE TABLE uservisits_cached AS SELECT * FROM uservisits;
                    CREATE TABLE rankings_cached AS SELECT * FROM rankings;
                    """

  # Warm up for Query 1
  if '1' in opts.query_num:
    query_list += "DROP TABLE IF EXISTS warmup;"
    query_list += "CREATE TABLE warmup AS SELECT pageURL, pageRank FROM scratch WHERE pageRank > 1000;"

  if '4' not in opts.query_num:
    query_list += local_clean_query
  query_list += local_query_map[opts.query_num][0]

  query_list = re.sub("\s\s+", " ", query_list.replace('\n', ' '))

  print "\nQuery:"
  print query_list.replace(';', ";\n")

  if opts.clear_buffer_cache:
    query_file.write("python /root/shark/bin/dev/clear-buffer-cache.py\n")

  query_file.write(
    "%s -e '%s' > %s 2>&1\n" % (runner, query_list, remote_tmp_file))

  query_file.write(
      "cat %s | grep Time | grep -v INFO |grep -v MapReduce >> %s\n" % (
        remote_tmp_file, remote_result_file))

  query_file.close()

  print "Copying files to Shark"
  scp_to(opts.shark_host, opts.shark_identity_file, "root", local_query_file,
      remote_query_file)
  ssh_shark("chmod 775 %s" % remote_query_file)

  # Run benchmark
  print "Running remote benchmark..."

  # Collect results
  results = []
  contents = []

  for i in range(opts.num_trials):
    print "Stopping Executors on Slaves....."
    ensure_spark_stopped_on_slaves(slaves)
    print "Query %s : Trial %i" % (opts.query_num, i+1)
    ssh_shark("%s" % remote_query_file)
    local_results_file = os.path.join(LOCAL_TMP_DIR, "%s_results" % prefix)
    scp_from(opts.shark_host, opts.shark_identity_file, "root",
        "/mnt/%s_results" % prefix, local_results_file)
    content = open(local_results_file).readlines()
    all_times = map(lambda x: float(x.split(": ")[1].split(" ")[0]), content)

    if '4' in opts.query_num:
      query_times = all_times[-4:]
      part_a = query_times[1]
      part_b = query_times[3]
      print "Parts: %s, %s" % (part_a, part_b)
      result = float(part_a) + float(part_b)
    else:
      result = all_times[-1] # Only want time of last query

    print "Result: ", result
    print "Raw Times: ", content

    results.append(result)
    contents.append(content)

    # Clean-up
    #ssh_shark("rm /mnt/%s*" % prefix)
    print "Clean Up...."
    ssh_shark("rm /mnt/%s_results" % prefix)
    os.remove(local_results_file)

  os.remove(local_slaves_file)
  os.remove(local_query_file)

  return results, contents

def run_impala_benchmark(opts):
  impala_host = opts.impala_hosts[0]
  def ssh_impala(command): 
    ssh(impala_host, "ubuntu", opts.impala_identity_file, command)

  def clear_buffer_cache_impala(host):
    ssh(host, "ubuntu", opts.impala_identity_file,
        "sudo bash -c \"sync && echo 3 > /proc/sys/vm/drop_caches\"")

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

  if '3c' in opts.query_num:
    query = query.replace('JOIN', 'JOIN [SHUFFLE]')

  query = "INSERT INTO TABLE warmup  SELECT pageURL, pageRank FROM scratch WHERE pageRank > 1000;" + query
  query = "CREATE TABLE warmup (pageURL STRING, pageRank INT);" + query
  query = "DROP TABLE IF EXISTS warmup;" + query

  # Populate the full buffer cache if running Impala + cached
  if (not opts.impala_use_hive) and (not opts.clear_buffer_cache):
    query = "set mem_limit=68g;" + query
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

  print query

  # Run benchmark
  print >> stderr, "Running remote benchmark..."
  for i in range(opts.num_trials):
    if opts.clear_buffer_cache:
      print >> stderr, "Clearing Buffer Cache..."
      map(clear_buffer_cache_impala, opts.impala_hosts)
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
    results = results[1::2]

  # Clean-up
  #ssh_impala("rm -f /tmp/%s*" % prefix) # Temporarily disabled
  os.unlink(local_query_file)
  os.unlink(local_result_file)

  return results, contents

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

def run_hive_benchmark(opts):
  def ssh_hive(command, user="root"):
    command = 'sudo -u %s %s' % (user, command)
    print command
    ssh(opts.hive_host, "root", opts.hive_identity_file, command)

  def clear_buffer_cache_hive(host):
    print >> stderr, "Clearing", host
    ssh(host, "root", opts.hive_identity_file,
        "sudo bash -c \"sync && echo 3 > /proc/sys/vm/drop_caches\"")

  prefix = str(time.time()).split(".")[0]
  query_file_name = "%s_workload.sh" % prefix
  slaves_file_name = "%s_slaves" % prefix
  local_query_file = os.path.join(LOCAL_TMP_DIR, query_file_name)
  local_slaves_file = os.path.join(LOCAL_TMP_DIR, slaves_file_name)
  query_file = open(local_query_file, 'w')
  remote_result_file = "/mnt/%s_results" % prefix
  remote_tmp_file = "/mnt/%s_out" % prefix
  remote_query_file = "/mnt/%s" % query_file_name

  query_list = "set mapreduce.reduce.input.limit = -1; set mapred.reduce.tasks = %s; " % opts.reduce_tasks
  query_list += "DROP TABLE IF EXISTS scratch_rank;"
  query_list += "CREATE TABLE scratch_rank AS SELECT pageURL, pageRank FROM scratch WHERE pageRank > 1000;"

  if opts.tez:
    runner = "HIVE_HOME=/opt/apache-hive-0.13.0.2.1.0.0-92-bin HIVE_CONF_DIR=$HIVE_HOME/conf PATH=$HIVE_HOME/bin:$PATH HADOOP_CLASSPATH=/opt/tez-0.2.0.2.1.0.0-92/*:/opt/tez-0.2.0.2.1.0.0-92/lib/* HADOOP_USER_CLASSPATH_FIRST=true HADOOP_USER_NAME=hive /opt/apache-hive-0.13.0.2.1.0.0-92-bin/bin/hive -i /root/benchmark/runner/tez/Stinger-Preview-Quickstart/configs/stinger.settings -hiveconf hive.optimize.tez=true"
    #query_map = TEZ_MAP
    query_map = QUERY_MAP
  else:
    runner = "HADOOP_USER_NAME=hdfs hive"
    query_map = QUERY_MAP

  # Throw away query for JVM warmup
  # query_list += "SELECT COUNT(*) FROM scratch;"

  if '4' not in opts.query_num:
    query_list += CLEAN_QUERY
  else:
    opts.query_num = '4_HIVE'

  query_list += query_map[opts.query_num][0]

  query_list = re.sub("\s\s+", " ", query_list.replace('\n', ' '))

  print "\nQuery:"
  print query_list.replace(';', ";\n")

  query_file.write(
    "%s -e '%s' > %s 2>&1\n" % (runner, query_list, remote_tmp_file))

  query_file.write(
      "cat %s | grep Time | grep -v INFO |grep -v MapReduce >> %s\n" % (
        remote_tmp_file, remote_result_file))

  query_file.close()

  print "Copying files to Hive"
  scp_to(opts.hive_host, opts.hive_identity_file, "root", local_query_file,
      remote_query_file)
  ssh_hive("chmod 775 %s" % remote_query_file)

  # Run benchmark
  print "Running remote benchmark..."

  # Collect results
  results = []
  contents = []

  for i in range(opts.num_trials):
    print "Query %s : Trial %i" % (opts.query_num, i+1)
    if opts.clear_buffer_cache:
      print >> stderr, "Clearing Buffer Cache..."
      map(clear_buffer_cache_hive, opts.hive_slaves)
    ssh_hive("%s" % remote_query_file)
    local_results_file = os.path.join(LOCAL_TMP_DIR, "%s_results" % prefix)
    scp_from(opts.hive_host, opts.hive_identity_file, "root",
        "/mnt/%s_results" % prefix, local_results_file)
    content = open(local_results_file).readlines()
    all_times = map(lambda x: float(x.split(": ")[1].split(" ")[0]), content)

    if '4' in opts.query_num:
      query_times = all_times[-4:]
      part_a = query_times[1]
      part_b = query_times[3]
      print "Parts: %s, %s" % (part_a, part_b)
      result = float(part_a) + float(part_b)
    else:
      result = all_times[-1] # Only want time of last query

    print "Result: ", result
    print "Raw Times: ", content

    results.append(result)
    contents.append(content)

    # Clean-up
    #ssh_hive("rm /mnt/%s*" % prefix)
    print "Clean Up...."
    ssh_hive("rm /mnt/%s_results" % prefix)
    os.remove(local_results_file)

  os.remove(local_query_file)

  return results, contents


def run_hive_cdh_benchmark(opts):
  def ssh_hive(command):
    command = 'HADOOP_USER_NAME=%s %s' % ("hdfs", command)
    print command
    ssh(opts.hive_host, "ubuntu", opts.hive_identity_file, command)

  def clear_buffer_cache_hive(host):
    print >> stderr, "Clearing", host
    ssh(host, "ubuntu", opts.hive_identity_file,
        "sudo bash -c \"sync && echo 3 > /proc/sys/vm/drop_caches\"")

  prefix = str(time.time()).split(".")[0]
  query_file_name = "%s_workload.sh" % prefix
  slaves_file_name = "%s_slaves" % prefix
  local_query_file = os.path.join(LOCAL_TMP_DIR, query_file_name)
  local_slaves_file = os.path.join(LOCAL_TMP_DIR, slaves_file_name)
  query_file = open(local_query_file, 'w')
  remote_result_file = "/tmp/%s_results" % prefix
  remote_tmp_file = "/tmp/%s_out" % prefix
  remote_query_file = "/tmp/%s" % query_file_name

  runner = "hive"

  query_list = "set mapred.reduce.tasks = %s;" % opts.reduce_tasks
  query_list += "DROP TABLE IF EXISTS scratch_rank;"
  query_list += "CREATE TABLE scratch_rank AS SELECT pageURL, pageRank FROM scratch WHERE pageRank > 1000;"

  # Throw away query for JVM warmup
  # query_list += "SELECT COUNT(*) FROM scratch;"

  if '4' not in opts.query_num:
    query_list += CLEAN_QUERY
  else:
    opts.query_num = '4_HIVE'

  query_list += QUERY_MAP[opts.query_num][0]

  query_list = re.sub("\s\s+", " ", query_list.replace('\n', ' '))

  print "\nQuery:"
  print query_list.replace(';', ";\n")

  query_file.write(
    "%s -e '%s' > %s 2>&1\n" % (runner, query_list, remote_tmp_file))

  query_file.write(
      "cat %s | grep Time | grep -v INFO |grep -v MapReduce >> %s\n" % (
        remote_tmp_file, remote_result_file))

  query_file.close()

  print "Copying files to Hive"
  scp_to(opts.hive_host, opts.hive_identity_file, "ubuntu", local_query_file,
      remote_query_file)
  ssh_hive("chmod 775 %s" % remote_query_file)

  # Run benchmark
  print "Running remote benchmark..."

  # Collect results
  results = []
  contents = []

  for i in range(opts.num_trials):
    print "Query %s : Trial %i" % (opts.query_num, i+1)
    if opts.clear_buffer_cache:
      print >> stderr, "Clearing Buffer Cache..."
      map(clear_buffer_cache_hive, opts.hive_slaves)
    ssh_hive("%s" % remote_query_file)
    local_results_file = os.path.join(LOCAL_TMP_DIR, "%s_results" % prefix)
    scp_from(opts.hive_host, opts.hive_identity_file, "ubuntu",
        "/tmp/%s_results" % prefix, local_results_file)
    content = open(local_results_file).readlines()
    all_times = map(lambda x: float(x.split(": ")[1].split(" ")[0]), content)

    if '4' in opts.query_num:
      query_times = all_times[-4:]
      part_a = query_times[1]
      part_b = query_times[3]
      print "Parts: %s, %s" % (part_a, part_b)
      result = float(part_a) + float(part_b)
    else:
      result = all_times[-1] # Only want time of last query

    print "Result: ", result
    print "Raw Times: ", content

    results.append(result)
    contents.append(content)

    # Clean-up
    #ssh_hive("rm /mnt/%s*" % prefix)
    print "Clean Up...."
    ssh_hive("rm /tmp/%s_results" % prefix)
    os.remove(local_results_file)

  os.remove(local_query_file)

  return results, contents

def get_percentiles(in_list):
  def get_pctl(lst, pctl):
    return lst[int(len(lst) * pctl)]
  in_list = sorted(in_list)
  return "%s\t%s\t%s" % (
    get_pctl(in_list, 0.05),
    get_pctl(in_list, .5),
    get_pctl(in_list, .95)
  )

def ssh_ret_code(host, user, id_file, cmd):
  try:
    return ssh(host, user, id_file, cmd)
  except subprocess.CalledProcessError as e:
    return e.returncode

def ensure_spark_stopped_on_slaves(slaves):
  stop = False
  while not stop:
    cmd = "jps | grep ExecutorBackend"
    ret_vals = map(lambda s: ssh_ret_code(s, "root", opts.shark_identity_file, cmd), slaves)
    print ret_vals
    if 0 in ret_vals:
      print "Spark is still running on some slaves... sleeping"
      cmd = "jps | grep ExecutorBackend | cut -d \" \" -f 1 | xargs -rn1 kill -9"
      map(lambda s: ssh_ret_code(s, "root", opts.shark_identity_file, cmd), slaves)
      time.sleep(2)
    else:
      stop = True

def main():
  global opts
  opts = parse_args()
  print "Query %s:" % opts.query_num
  if opts.impala:
    results, contents = run_impala_benchmark(opts)
  if opts.shark:
    results, contents = run_shark_benchmark(opts)
  if opts.redshift:
    results = run_redshift_benchmark(opts)
  if opts.hive:
    results, contents = run_hive_benchmark(opts)
  if opts.hive_cdh:
    results, contents = run_hive_cdh_benchmark(opts)

  if opts.impala:
    if opts.clear_buffer_cache:
      fname = "impala_disk"
    else:
      fname = "impala_mem"
  elif opts.shark and opts.shark_no_cache:
    fname = "shark_disk"
  elif opts.shark:
    fname = "shark_mem"
  elif opts.redshift:
    fname = "redshift"
  elif opts.hive:
    if opts.clear_buffer_cache:
      fname = "hive_clear_cache"
    else:
      fname = "hive"
  elif opts.hive_cdh:
    if opts.clear_buffer_cache:
      fname = "cdh_hive_clear_cache"
    else:
      fname = "cdh_hive"

  fname = opts.prefix + fname

  def prettylist(lst):
    return ",".join([str(k) for k in lst]) 

  output = StringIO()
  outfile = open('results/%s_%s_%s' % (fname, opts.query_num, datetime.datetime.now()), 'w')

  try:
    if not opts.redshift:
      print >> output, "Contents: \n%s" % str(prettylist(contents))
    print >> output, "=================================="
    print >> output, "Results: %s" % prettylist(results)
    print >> output, "Percentiles: %s" % get_percentiles(results)
    print >> output, "Best: %s"  % min(results)
    if not opts.redshift:
      print >> output, "Contents: \n%s" % str(prettylist(contents))
    print output.getvalue()
    print >> outfile, output.getvalue()
  except:
    print output.getvalue()
    print >> outfile, output.getvalue()

  output.close()
  outfile.close()

if __name__ == "__main__":
  main()
