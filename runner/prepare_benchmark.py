"""Prepare the big data benchmark on one or more EC2 or Redshift clusters.

   This script will copy the appropriately sized input data set from s3
   into the provided cluster or clusters.
"""

import subprocess
import sys
from sys import stderr
from optparse import OptionParser
import os
import time
from pg8000 import DBAPI
import pg8000.errors

# A scratch directory on your filesystem
LOCAL_TMP_DIR = "/tmp"

# Maps cluster sizes to S3 path prefixes
SCALE_FACTOR_MAP = {
  0: "tiny",
  1: "1node",
  5: "5nodes",
  10: "10nodes"
}

### Runner ###
def parse_args():
  parser = OptionParser(usage="prepare_benchmark.py [options]")

  parser.add_option("-m", "--impala", action="store_true", default=False,
      help="Whether to include Impala")
  parser.add_option("-s", "--shark", action="store_true", default=False,
      help="Whether to include Shark")
  parser.add_option("-r", "--redshift", action="store_true", default=False,
      help="Whether to include Redshift")

  parser.add_option("-a", "--impala-host",
      help="Hostname of Impala state store node")
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

  parser.add_option("-n", "--scale-factor", type="int", default=5,
      help="Number of database nodes (dataset is scaled accordingly)")
  parser.add_option("-f", "--file-format", default="sequence-snappy",
      help="File format to copy (text, text-deflate, "\
           "sequence, or sequence-snappy)")

  parser.add_option("-d", "--aws-key-id",
      help="Access key ID for AWS")
  parser.add_option("-k", "--aws-key",
      help="Access key for AWS")

  parser.add_option("--skip-s3-import", action="store_true", default=False,
      help="Assumes s3 data is already loaded")

  (opts, args) = parser.parse_args()

  if not (opts.impala or opts.shark or opts.redshift):
    parser.print_help()
    sys.exit(1)

  if opts.scale_factor not in SCALE_FACTOR_MAP.keys():
    print >> stderr, "Unsupported cluster size: %s" % opts.scale_factor
    sys.exit(1)

  opts.data_prefix = SCALE_FACTOR_MAP[opts.scale_factor]

  if opts.impala and (opts.impala_identity_file is None or 
                      opts.impala_host is None or
                      opts.aws_key_id is None or
                      opts.aws_key is None):
    print >> stderr, "Impala requires identity file, hostname, and AWS creds"
    sys.exit(1)
  
  if opts.shark and (opts.shark_identity_file is None or 
                     opts.shark_host is None or
                     opts.aws_key_id is None or
                     opts.aws_key is None):
    print >> stderr, \
        "Shark requires identity file, shark hostname, and AWS credentials"
    sys.exit(1)

  if opts.redshift and (opts.redshift_username is None or
                        opts.redshift_password is None or
                        opts.redshift_host is None or
                        opts.redshift_database is None or
                        opts.aws_key_id is None or
                        opts.aws_key is None):
    print >> stderr, \
        "Redshift requires host, username, password, db, and AWS credentials"
    sys.exit(1)
  
  return opts

# Run a command on a host through ssh, throwing an exception if ssh fails
def ssh(host, username, identity_file, command):
  command = "source /root/.bash_profile; %s" % command
  cmd = "ssh -t -o StrictHostKeyChecking=no -i %s %s@%s '%s'" % (identity_file,
                                                                 username,
                                                                 host, command)
  print cmd
  subprocess.check_call(cmd, shell=True)

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

# Insert AWS credentials into a given XML file on the given remote host
def add_aws_credentials(remote_host, remote_user, identity_file, 
                       remote_xml_file, aws_key_id, aws_key):
  local_xml = os.path.join(LOCAL_TMP_DIR, "temp.xml")
  scp_from(remote_host, identity_file, remote_user, 
           remote_xml_file, local_xml)
  lines = open(local_xml).readlines()
  # Manual XML munging... this makes me cry a little bit
  lines = filter(lambda x: "configuration" not in x and "xml" not in x 
                           and "fs.s3" not in x, lines)
  lines = map(lambda x: x.strip(), lines)
  key_conf = "<property><name>fs.s3n.awsAccessKeyId</name>" \
    + ("<value>%s</value>" % aws_key_id) + "</property><property>" \
    + "<name>fs.s3n.awsSecretAccessKey</name>" \
    + ("<value>%s</value>" % aws_key) + "</property>" 
  lines.insert(0, "<configuration>")
  lines.append(key_conf)
  lines.append("</configuration>")
  out = open(local_xml, 'w')
  for l in lines:
    print >> out, l
  out.close()
  scp_to(remote_host, identity_file, remote_user, local_xml, remote_xml_file)

def prepare_shark_dataset(opts):
  def ssh_shark(command):
    ssh(opts.shark_host, "root", opts.shark_identity_file, command)

  if not opts.skip_s3_import:  
    print "=== IMPORTING BENCHMARK DATA FROM S3 ==="
    try:
      ssh_shark("/root/ephemeral-hdfs/bin/hdfs dfs -mkdir /user/shark/benchmark")
    except Exception:
      pass # Folder may already exist

    add_aws_credentials(opts.shark_host, "root", opts.shark_identity_file,
        "/root/mapreduce/conf/core-site.xml", opts.aws_key_id, opts.aws_key)

    ssh_shark("/root/mapreduce/bin/start-mapred.sh")
    
    ssh_shark( 
      "/root/mapreduce/bin/hadoop distcp " \
      "s3n://big-data-benchmark/pavlo/%s/%s/rankings/ " \
      "/user/shark/benchmark/rankings/" % (opts.file_format, opts.data_prefix))

    ssh_shark( 
      "/root/mapreduce/bin/hadoop distcp " \
      "s3n://big-data-benchmark/pavlo/%s/%s/uservisits/ " \
      "/user/shark/benchmark/uservisits/" % (
        opts.file_format, opts.data_prefix))
    
    ssh_shark( 
      "/root/mapreduce/bin/hadoop distcp " \
      "s3n://big-data-benchmark/pavlo/%s/%s/crawl/ " \
      "/user/shark/benchmark/crawl/" % (opts.file_format, opts.data_prefix))

    # Scratch table used for JVM warmup
    ssh_shark(
      "/root/mapreduce/bin/hadoop distcp /user/shark/benchmark/rankings " \
      "/user/shark/benchmark/scratch"
    )

  print "=== CREATING HIVE TABLES FOR BENCHMARK ==="
  scp_to(opts.shark_host, opts.shark_identity_file, "root", "udf/url_count.py",
      "/root/url_count.py")
  ssh_shark("/root/spark-ec2/copy-dir /root/url_count.py")
  
  ssh_shark(
    "/root/shark/bin/shark -e \"DROP TABLE IF EXISTS rankings; " \
    "CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, " \
    "avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\",\\\" " \
    "STORED AS TEXTFILE LOCATION \\\"/user/shark/benchmark/rankings\\\";\"")

  ssh_shark(
    "/root/shark/bin/shark -e \"DROP TABLE IF EXISTS scratch; " \
    "CREATE EXTERNAL TABLE scratch (pageURL STRING, pageRank INT, " \
    "avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\",\\\" " \
    "STORED AS TEXTFILE LOCATION \\\"/user/shark/benchmark/scratch\\\";\"")

  ssh_shark(
    "/root/shark/bin/shark -e \"DROP TABLE IF EXISTS uservisits; " \
    "CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING," \
    "visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING," \
    "languageCode STRING,searchWord STRING,duration INT ) " \
    "ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\",\\\" " \
    "STORED AS TEXTFILE LOCATION \\\"/user/shark/benchmark/uservisits\\\";\"")
  
  ssh_shark("/root/shark/bin/shark -e \"DROP TABLE IF EXISTS documents; " \
    "CREATE EXTERNAL TABLE documents (line STRING) STORED AS TEXTFILE " \
    "LOCATION \\\"/user/shark/benchmark/crawl\\\";\"")

  print "=== FINISHED CREATING BENCHMARK DATA ==="

def prepare_impala_dataset(opts):
  def ssh_impala(command): 
    ssh(opts.impala_host, "ubuntu", opts.impala_identity_file, command)

  if not opts.skip_s3_import:
    print "=== IMPORTING BENCHMARK FROM S3 ==="
    try:
      ssh_impala("hdfs dfs -mkdir /tmp/benchmark")
    except Exception:
      pass # Folder may already exist

    ssh_impala("sudo chmod 777 /etc/hadoop/conf/hdfs-site.xml") 
    ssh_impala("sudo chmod 777 /etc/hadoop/conf/core-site.xml") 

    add_aws_credentials(opts.impala_host, "ubuntu", opts.impala_identity_file,
        "/etc/hadoop/conf/hdfs-site.xml", opts.aws_key_id, opts.aws_key)
    add_aws_credentials(opts.impala_host, "ubuntu", opts.impala_identity_file,
        "/etc/hadoop/conf/core-site.xml", opts.aws_key_id, opts.aws_key)
  
    ssh_impala( 
      "sudo -u hdfs hadoop distcp s3n://big-data-benchmark/pavlo/%s/%s/rankings/ " \
      "/tmp/benchmark/rankings/" % (opts.file_format, opts.data_prefix))
    ssh_impala( 
      "sudo -u hdfs hadoop distcp s3n://big-data-benchmark/pavlo/%s/%s/uservisits/ " \
      "/tmp/benchmark/uservisits/" % (opts.file_format, opts.data_prefix))
  
  print "=== CREATING HIVE TABLES FOR BENCHMARK ==="
  ssh_impala(
    "hive -e \"DROP TABLE IF EXISTS rankings; " \
    "CREATE EXTERNAL TABLE rankings (pageURL STRING, " \
    "pageRank INT, avgDuration INT) ROW FORMAT DELIMITED FIELDS " \
    "TERMINATED BY \\\"\\001\\\" " \
    "STORED AS SEQUENCEFILE LOCATION \\\"/tmp/benchmark/rankings\\\";\"")
 
  ssh_impala(
    "hive -e \"DROP TABLE IF EXISTS uservisits; " \
    "CREATE EXTERNAL TABLE uservisits (sourceIP STRING, "\
    "destURL STRING," \
    "visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING," \
    "languageCode STRING,searchWord STRING,duration INT ) " \
    "ROW FORMAT DELIMITED FIELDS TERMINATED BY \\\"\\001\\\" " +\
    "STORED AS SEQUENCEFILE LOCATION \\\"/tmp/benchmark/uservisits\\\";\"")
  print "=== FINISHED CREATING BENCHMARK DATA ==="

def prepare_redshift_dataset(opts):
  def query_and_print(cursor, query):
    cursor.execute(query)
    for res in cursor:
      print res
  
  def query_with_catch(cursor, query):
    try: 
      cursor.execute(query)
    except pg8000.errors.InternalError as e:
      print >> stderr, "Received error from pg8000: %s" % e
      print >> stderr, "Attempting to continue..."
  
  conn = DBAPI.connect(
    host = opts.redshift_host,
    database = opts.redshift_database,

    user = opts.redshift_username,
    password = opts.redshift_password,
    port = 5439,
    socket_timeout=60 * 45)
  cursor = conn.cursor()
  cred = "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'" % (
      opts.aws_key_id,
      opts.aws_key)

  try:
    cursor.execute("DROP TABLE rankings;")
  except Exception:
    pass

  try:
    cursor.execute("DROP TABLE uservisits;")
  except Exception:
    pass

  create_rankings_q = "CREATE TABLE rankings (pageURL VARCHAR(300), "\
      "pageRank INT, avgDuration INT);"
  create_uservisits_q = "CREATE TABLE uservisits (sourceIP "\
      "VARCHAR(116), destinationURL VARCHAR(100), visitDate DATE, adRevenue "\
      "FLOAT, UserAgent VARCHAR(256), cCode CHAR(3), lCode CHAR(6), searchWord "\
      "VARCHAR(32), duration INT);"
  cursor.execute(create_rankings_q)
  cursor.execute(create_uservisits_q)

  print "Loading Rankings table into Redshift..."
  rankings_url = "s3://big-data-benchmark/pavlo/text/%s/rankings/" % \
      opts.data_prefix
  load_rankings_q= "copy rankings from '%s' %s delimiter ',';" % (
      rankings_url, cred)
  cursor.execute(load_rankings_q)

  print "Loading UserVisits table into Redshift..."
  uservisits_url = "s3://big-data-benchmark/pavlo/text/%s/uservisits/" % \
      opts.data_prefix
  load_uservisits_q= "copy uservisits from '%s' %s delimiter ',';" % (
      uservisits_url, cred)
  cursor.execute(load_uservisits_q)

  conn.commit()

  print "Size of Rankings table in Redshift:"
  query_and_print(cursor, "SELECT COUNT(*) from rankings;")

  print "Size of UserVisits table in Redshift:"
  query_and_print(cursor, "SELECT COUNT(*) from uservisits;")

def print_percentiles(in_list):
  print "Got list %s" % in_list
  def get_pctl(lst, pctl):
    return lst[int(len(lst) * pctl)]
  in_list = sorted(in_list)
  print "%s\t%s\t%s" % (
    get_pctl(in_list, 0.05),
    get_pctl(in_list, .5),
    get_pctl(in_list, .95)
  )

def main():
  opts = parse_args()
  
  if opts.impala:
    prepare_impala_dataset(opts)
  if opts.shark:
    prepare_shark_dataset(opts)
  if opts.redshift:
    prepare_redshift_dataset(opts)

if __name__ == "__main__":
  main()
