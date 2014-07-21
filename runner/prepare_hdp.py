#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# TODO: Ambari UI took a long time -> scriptable?
  # main things: (1) put all master services into master node (2) enter host
  # names (3) set admin id and passwd (and nagios requires admin email..)

# TODO: action 'login' doesn't work

# TODO(zongheng): ./prepare_hdp should save the hostnames to somewhere instead
# of only printing them out.

from __future__ import with_statement

import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import urllib2
import threading
from optparse import OptionParser
from sys import stderr
import boto
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from boto import ec2

# Constants
AMBARI_REPO_URL = """http://public-repo-1.hortonworks.com/ambari/centos6/1.x/updates/1.6.0/ambari.repo"""

# Configure and parse our command-line arguments
def parse_args():
  parser = OptionParser(usage="spark-ec2 [options] <action> <cluster_name>"
      + "\n\n<action> can be: launch, destroy, login, stop, start, get-master",
      add_help_option=False)
  parser.add_option("-h", "--help", action="help",
                    help="Show this help message and exit")
  parser.add_option("-s", "--slaves", type="int", default=1,
      help="Number of slaves to launch (default: 1)")
  parser.add_option("-w", "--wait", type="int", default=120,
      help="Seconds to wait for nodes to start (default: 120)")
  parser.add_option("-k", "--key-pair",
      help="Key pair to use on instances")
  parser.add_option("-i", "--identity-file",
      help="SSH private key file to use for logging into instances")
  parser.add_option("-t", "--instance-type", default="m1.large",
      help="Type of instance to launch (default: m1.large). " +
           "WARNING: must be 64-bit; small instances won't work")
  parser.add_option("-m", "--master-instance-type", default="",
      help="Master instance type (leave empty for same as instance-type)")
  parser.add_option("-r", "--region", default="us-east-1",
      help="EC2 region zone to launch instances in")
  parser.add_option("-z", "--zone", default="",
      help="Availability zone to launch instances in, or 'all' to spread " +
           "slaves across multiple (an additional $0.01/Gb for bandwidth" +
           "between zones applies)")
  parser.add_option("-a", "--ami", help="Amazon Machine Image ID to use",
                    default="ami-a25415cb")
  parser.add_option("-v", "--spark-version", default="0.8.0",
      help="Version of Spark to use: 'X.Y.Z' or a specific git hash")
  parser.add_option("--spark-git-repo",
      default="https://github.com/mesos/spark",
      help="Github repo from which to checkout supplied commit hash")
  parser.add_option("--hadoop-major-version", default="1",
      help="Major version of Hadoop (default: 1)")
  parser.add_option("-D", metavar="[ADDRESS:]PORT", dest="proxy_port",
      help="Use SSH dynamic port forwarding to create a SOCKS proxy at " +
            "the given local address (for use with login)")
  parser.add_option("--resume", action="store_true", default=False,
      help="Resume installation on a previously launched cluster " +
           "(for debugging)")
  parser.add_option("--ebs-vol-size", metavar="SIZE", type="int", default=0,
      help="Attach a new EBS volume of size SIZE (in GB) to each node as " +
           "/vol. The volumes will be deleted when the instances terminate. " +
           "Only possible on EBS-backed AMIs.")
  parser.add_option("--swap", metavar="SWAP", type="int", default=1024,
      help="Swap space to set up per node, in MB (default: 1024)")
  parser.add_option("--spot-price", metavar="PRICE", type="float",
      help="If specified, launch slaves as spot instances with the given " +
            "maximum price (in dollars)")
  parser.add_option("--ganglia", action="store_true", default=True,
      help="Setup Ganglia monitoring on cluster (default: on). NOTE: " +
           "the Ganglia page will be publicly accessible")
  parser.add_option("--no-ganglia", action="store_false", dest="ganglia",
      help="Disable Ganglia monitoring for the cluster")
  parser.add_option("-u", "--user", default="root",
      help="The SSH user you want to connect as (default: root)")
  parser.add_option("--delete-groups", action="store_true", default=False,
      help="When destroying a cluster, delete the security groups that were created")

  (opts, args) = parser.parse_args()
  if len(args) != 2:
    parser.print_help()
    sys.exit(1)
  (action, cluster_name) = args
  if opts.identity_file == None and action in ['launch', 'login', 'start']:
    print >> stderr, ("ERROR: The -i or --identity-file argument is " +
                      "required for " + action)
    sys.exit(1)

  # Boto config check
  # http://boto.cloudhackers.com/en/latest/boto_config_tut.html
  home_dir = os.getenv('HOME')
  if home_dir == None or not os.path.isfile(home_dir + '/.boto'):
    if not os.path.isfile('/etc/boto.cfg'):
      if os.getenv('AWS_ACCESS_KEY_ID') == None:
        print >> stderr, ("ERROR: The environment variable AWS_ACCESS_KEY_ID " +
                          "must be set")
        sys.exit(1)
      if os.getenv('AWS_SECRET_ACCESS_KEY') == None:
        print >> stderr, ("ERROR: The environment variable AWS_SECRET_ACCESS_KEY " +
                          "must be set")
        sys.exit(1)
  return (opts, action, cluster_name)


# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name):
  groups = conn.get_all_security_groups()
  group = [g for g in groups if g.name == name]
  if len(group) > 0:
    return group[0]
  else:
    print "Creating security group " + name
    return conn.create_security_group(name, "Spark EC2 group")


# Wait for a set of launched instances to exit the "pending" state
# (i.e. either to start running or to fail and be terminated)
def wait_for_instances(conn, instances):
  while True:
    for i in instances:
      i.update()
    if len([i for i in instances if i.state == 'pending']) > 0:
      time.sleep(5)
    else:
      return


# Check whether a given EC2 instance object is in a state we consider active,
# i.e. not terminating or terminated. We count both stopping and stopped as
# active since we can restart stopped clusters.
def is_active(instance):
  return (instance.state in ['pending', 'running', 'stopping', 'stopped'])

# Launch a cluster of the given name, by setting up its security groups,
# and then starting new instances in them.
# Returns a tuple of EC2 reservation objects for the master and slaves
# Fails if there already instances running in the cluster's groups.
def launch_cluster(conn, OPTS, cluster_name):
  print "Setting up security groups..."
  master_group = get_or_make_group(conn, cluster_name + "-master")
  slave_group = get_or_make_group(conn, cluster_name + "-slaves")
  ambari_group = get_or_make_group(conn, cluster_name + "-ambari")

  if master_group.rules == []: # Group was just now created
    master_group.authorize(src_group=master_group)
    master_group.authorize(src_group=slave_group)
    master_group.authorize(src_group=ambari_group)
    # TODO: Currently Group is completely open
    master_group.authorize('tcp', 0, 65535, '0.0.0.0/0')
  if slave_group.rules == []: # Group was just now created
    slave_group.authorize(src_group=master_group)
    slave_group.authorize(src_group=slave_group)
    slave_group.authorize(src_group=ambari_group)
    # TODO: Currently Group is completely open
    slave_group.authorize('tcp', 0, 65535, '0.0.0.0/0')
  if ambari_group.rules == []: # Group was just now created
    ambari_group.authorize(src_group=master_group)
    ambari_group.authorize(src_group=slave_group)
    ambari_group.authorize(src_group=ambari_group)
    # TODO: Currently Group is completely open
    ambari_group.authorize('tcp', 0, 65535, '0.0.0.0/0')

  # Check if instances are already running in our groups
  if OPTS.resume:
    return get_existing_cluster(conn, OPTS, cluster_name, die_on_error=False)
  else:
    active_nodes = get_existing_cluster(conn, OPTS, cluster_name, die_on_error=False)
    if any(active_nodes):
      print >> stderr, ("ERROR: There are already instances running in " +
          "group %s or %s" % (master_group.name, slave_group.name))
      sys.exit(1)

    print "Launching instances..."

    try:
      image = conn.get_all_images(image_ids=[OPTS.ami])[0]
    except:
      print >> stderr, "Could not find AMI " + OPTS.ami
      sys.exit(1)

    # Create block device mapping so that we can add an EBS volume if asked to
    block_map = BlockDeviceMapping()
    device = BlockDeviceType()
    device.ephemeral_name = 'ephemeral0'
    device.delete_on_termination = True
    block_map["/dev/sdv"] = device

    # Launch slaves
    if OPTS.spot_price != None:
      # Launch spot instances with the requested price
      print ("Requesting %d slaves as spot instances with price $%.3f" %
            (OPTS.slaves, OPTS.spot_price))
      zones = get_zones(conn, OPTS)
      num_zones = len(zones)
      i = 0
      my_req_ids = []
      ambari_req_ids = []
      master_req_ids = []
      for zone in zones:
        num_slaves_this_zone = get_partition(OPTS.slaves, num_zones, i)
        ambari_reqs = conn.request_spot_instances(
            price = OPTS.spot_price,
            image_id = OPTS.ami,
            launch_group = "launch-group-%s" % cluster_name,
            placement = zone,
            count = 1,
            key_name = OPTS.key_pair,
            security_groups = [ambari_group],
            instance_type = OPTS.master_instance_type,
            block_device_map = block_map)
        master_reqs = conn.request_spot_instances(
            price = OPTS.spot_price,
            image_id = OPTS.ami,
            launch_group = "launch-group-%s" % cluster_name,
            placement = zone,
            count = 1,
            key_name = OPTS.key_pair,
            security_groups = [master_group],
            instance_type = OPTS.master_instance_type,
            block_device_map = block_map)
        slave_reqs = conn.request_spot_instances(
            price = OPTS.spot_price,
            image_id = OPTS.ami,
            launch_group = "launch-group-%s" % cluster_name,
            placement = zone,
            count = num_slaves_this_zone,
            key_name = OPTS.key_pair,
            security_groups = [slave_group],
            instance_type = OPTS.instance_type,
            block_device_map = block_map)
        my_req_ids += [req.id for req in slave_reqs]
        ambari_req_ids += [req.id for req in ambari_reqs]
        master_req_ids += [req.id for req in master_reqs]
        i += 1

      print "Waiting for spot instances to be granted..."
      try:
        while True:
          time.sleep(10)
          reqs = conn.get_all_spot_instance_requests()
          id_to_req = {}
          for r in reqs:
            id_to_req[r.id] = r
          active_instance_ids = []
          ambari_instance_ids = []
          master_instance_ids = []
          for i in my_req_ids:
            if i in id_to_req and id_to_req[i].state == "active":
              active_instance_ids.append(id_to_req[i].instance_id)
          for i in master_req_ids:
            if i in id_to_req and id_to_req[i].state == "active":
              master_instance_ids.append(id_to_req[i].instance_id)
          for i in ambari_req_ids:
            if i in id_to_req and id_to_req[i].state == "active":
              ambari_instance_ids.append(id_to_req[i].instance_id)
          if len(active_instance_ids) == OPTS.slaves and len(master_instance_ids) == 1 and len(ambari_instance_ids) == 1:
            print "All %d slaves granted" % OPTS.slaves
            slave_nodes = []
            master_nodes = []
            ambari_nodes = []
            for r in conn.get_all_instances(active_instance_ids):
              slave_nodes += r.instances
            for r in conn.get_all_instances(master_instance_ids):
              master_nodes += r.instances
            for r in conn.get_all_instances(ambari_instance_ids):
              ambari_nodes += r.instances
            break
          else:
            print "%d of %d slaves granted, waiting longer" % (
              len(active_instance_ids), OPTS.slaves)
      except Exception as e:
        print e
        print "Canceling spot instance requests"
        conn.cancel_spot_instance_requests(my_req_ids)
        # Log a warning if any of these requests actually launched instances:
        (master_nodes, slave_nodes, ambari_nodes) = get_existing_cluster(
            conn, OPTS, cluster_name, die_on_error=False)
        running = len(master_nodes) + len(slave_nodes) + len(ambari_nodes)
        if running:
          print >> stderr, ("WARNING: %d instances are still running" % running)
        sys.exit(0)
    else:
      # Launch non-spot instances
      zones = get_zones(conn, OPTS)
      num_zones = len(zones)
      i = 0
      slave_nodes = []
      for zone in zones:
        num_slaves_this_zone = get_partition(OPTS.slaves, num_zones, i)
        if num_slaves_this_zone > 0:
          slave_res = image.run(key_name = OPTS.key_pair,
                                security_groups = [slave_group],
                                instance_type = OPTS.instance_type,
                                placement = zone,
                                min_count = num_slaves_this_zone,
                                max_count = num_slaves_this_zone,
                                block_device_map = block_map)
          slave_nodes += slave_res.instances
          print "Launched %d slaves in %s, regid = %s" % (num_slaves_this_zone,
                                                          zone, slave_res.id)
        i += 1

      # Launch masters
      master_type = OPTS.master_instance_type
      if master_type == "":
        master_type = OPTS.instance_type
      if OPTS.zone == 'all':
        OPTS.zone = random.choice(conn.get_all_zones()).name
      master_res = image.run(key_name = OPTS.key_pair,
                            security_groups = [master_group],
                            instance_type = master_type,
                            placement = OPTS.zone,
                            min_count = 1,
                            max_count = 1,
                            block_device_map = block_map)
      master_nodes = master_res.instances
      print "Launched master in %s, regid = %s" % (zone, master_res.id)

      ambari_type = OPTS.master_instance_type
      if ambari_type == "":
        ambari_type = OPTS.instance_type
      if OPTS.zone == 'all':
        OPTS.zone = random.choice(conn.get_all_zones()).name
      ambari_res = image.run(key_name = OPTS.key_pair,
                            security_groups = [ambari_group],
                            instance_type = ambari_type,
                            placement = OPTS.zone,
                            min_count = 1,
                            max_count = 1,
                            block_device_map = block_map)
      ambari_nodes = ambari_res.instances
      print "Launched ambari in %s, regid = %s" % (zone, ambari_res.id)

    # Return all the instances
    return (master_nodes, slave_nodes, ambari_nodes)


# Get the EC2 instances in an existing cluster if available.
# Returns a tuple of lists of EC2 instance objects for the masters and slaves
def get_existing_cluster(conn, OPTS, cluster_name, die_on_error=True):
  print "Searching for existing cluster " + cluster_name + "..."
  reservations = conn.get_all_instances()
  master_nodes = []
  slave_nodes = []
  ambari_nodes = []
  # print "reservations: ", str(reservations)
  for res in reservations:
    active = [i for i in res.instances if is_active(i)]
    if len(active) > 0:
      # print "found active instances", active
      group_names = [g.name for g in res.groups]
      # print group_names
      if group_names == [cluster_name + "-master"]:
        master_nodes += res.instances
      elif group_names == [cluster_name + "-slaves"]:
        slave_nodes += res.instances
      elif group_names == [cluster_name + "-ambari"]:
        ambari_nodes += res.instances
  if any((master_nodes, slave_nodes, ambari_nodes)):
    print ("Found %d master(s), %d slaves, %d ambari" %
           (len(master_nodes), len(slave_nodes), len(ambari_nodes)))
  if (master_nodes != [] and slave_nodes != [] and ambari_nodes != []) or not die_on_error:
    return (master_nodes, slave_nodes, ambari_nodes)
  else:
    print "ERROR: Could not find any existing cluster"
    sys.exit(1)


# Deploy configuration files and run setup scripts on a newly launched
# or started EC2 cluster.
def setup_cluster(conn, master_nodes, slave_nodes, ambari_nodes, OPTS, deploy_ssh_key, cluster_name):
  master = master_nodes[0]
  ambari = ambari_nodes[0]
  all_nodes = master_nodes + slave_nodes + ambari_nodes

  print "Enabling root on all nodes..."
  OPTS.user = "ec2-user"
  concurrent_map(enable_root, all_nodes)

  OPTS.user = "root"

  print "Copying SSH key %s to ambari & master..." % OPTS.identity_file
  concurrent_map(deploy_key, (ambari, master))

  print "Configuring Nodes..."
  concurrent_map(configure_node, all_nodes)

  wait_for_cluster(conn, 90, master_nodes, slave_nodes, ambari_nodes)

  print "Setting up ambari node..."
  setup_ambari_master(ambari, OPTS)

  print "Starting All Services..."
  concurrent_map(start_services, master_nodes + slave_nodes)

  # FIXME?: don't hardcode this?
  # args = {
    # 'runner' : '/Users/ahirreddy/Work/benchmark/spark-0.8.0-incubating/ec2/spark-ec2',
    # 'keyname' : OPTS.key_pair,
    # 'idfile' : OPTS.identity_file,
    # 'cluster' : cluster_name,
  # }

  ssh(ambari.public_dns_name, OPTS, "ambari-server start;")

  print "Ambari: %s" % ambari.public_dns_name
  print "Master: %s" % master.public_dns_name
  for slave in slave_nodes:
    print "Slave: %s" % slave.public_dns_name

  print "Master: %s" % master.private_dns_name
  print "Slaves:"
  for slave in slave_nodes:
    print "\t", slave.private_dns_name

def enable_root(node):
  cmd = """
  echo "PermitRootLogin yes" | sudo tee -a /etc/ssh/sshd_config;
  echo "JAVA_HOME=/usr/local" | sudo tee -a /root/.bash_profile;
  echo "SCALA_HOME=/usr/local" | sudo tee -a /root/.bash_profile;
  sudo cp /home/ec2-user/.ssh/authorized_keys /root/.ssh/authorized_keys;
  sudo /etc/init.d/sshd restart;
  """

  ssh(node.public_dns_name, OPTS, cmd)

def configure_node(node):
  cmd = """
        yum -y install screen;
        yum -y install git;
        sed -e 's/SELINUX=enforcing//g' /etc/selinux/config > /etc/selinux/config;
        echo "SELINUX=disabled" >> /etc/selinux/config;
        chkconfig iptables off;
        chkconfig ip6tables off;
        shutdown -r now;
        """

  ssh(node.public_dns_name, OPTS, cmd)

def start_services(node):
  cmd = """
  mkfs.ext4 /dev/xvdz;
  mkdir /hadoop;
  mount /dev/xvdz /hadoop;
  /etc/init.d/ntpd restart;
  """

  return ssh(node.public_dns_name, OPTS, cmd)

# Deploy private key to ambari and master nodes
def deploy_key(node):
    ssh(node.public_dns_name, OPTS, 'mkdir -p ~/.ssh')
    scp(node.public_dns_name, OPTS, OPTS.identity_file, '~/.ssh/id_rsa')

    ssh(node.public_dns_name, OPTS, 'chmod 600 ~/.ssh/id_rsa')

# Setup the Ambari Master and start the ambari server
def setup_ambari_master(ambari, OPTS):
  # Hack: attempt to skip user interaction in the setup process by using the default options.
  # Works the first time this setup command is called on a node.
  #     n: customize user account for ambari-server daemon [y/n] (n)?
  #     y: Do you accept the Oracle Binary Code License Agreement [y/n] (y)?
  #     n: Enter advanced database configuration [y/n] (n)?
  ambari_setup_cmd = """yes "" | ambari-server setup"""
  cmd = """
        wget %s;
        cp ambari.repo /etc/yum.repos.d;
        yum -y install epel-release;
        yum -y repolist;
        yum -y install ambari-server;
        %s;
        ambari-server start;
        ambari-server status;
        """ % (AMBARI_REPO_URL, ambari_setup_cmd)

  ssh(ambari.public_dns_name, OPTS, cmd, stdin=None)


# Wait for a whole cluster (masters, slaves and ZooKeeper) to start up
def wait_for_cluster(conn, wait_secs, master_nodes, slave_nodes, ambari_nodes):
  print "Waiting for instances to start up..."
  time.sleep(5)
  wait_for_instances(conn, master_nodes)
  wait_for_instances(conn, slave_nodes)
  wait_for_instances(conn, ambari_nodes)
  print "Waiting %d more seconds..." % wait_secs
  time.sleep(wait_secs)


# Copy a file to a given host through scp, throwing an exception if scp fails
def scp(host, OPTS, local_file, dest_file):
  subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s' '%s@%s:%s'" %
      (OPTS.identity_file, local_file, OPTS.user, host, dest_file), shell=True)


# Download a file from a given host through scp, throwing an exception if scp fails
def scp_download(host, OPTS, remote_file, local_file):
  subprocess.check_call(
      "scp -q -o StrictHostKeyChecking=no -i %s '%s@%s:%s' '%s'" %
      (OPTS.identity_file, OPTS.user, host, remote_file, local_file), shell=True)


# Run a command on a host through ssh, retrying up to two times
# and then throwing an exception if ssh continues to fail.
def ssh(host, OPTS, command, stdin=open(os.devnull, 'w')):
  command = command.replace('\n', ' ')
  cmd = "ssh -t -t -o StrictHostKeyChecking=no -i %s %s@%s '%s'" % (OPTS.identity_file, OPTS.user, host, command)
  print cmd
  tries = 0
  while True:
    try:
      return subprocess.check_call(
        cmd, shell=True, stdin=stdin)
    except subprocess.CalledProcessError as e:
      if (tries > 2):
        raise e
      print "Couldn't connect to host {0}, waiting 30 seconds".format(e)
      time.sleep(30)
      tries = tries + 1


# Gets a list of zones to launch instances in
def get_zones(conn, OPTS):
  if OPTS.zone == 'all':
    zones = [z.name for z in conn.get_all_zones()]
  else:
    zones = [OPTS.zone]
  return zones


# Gets the number of items in a partition
def get_partition(total, num_partitions, current_partitions):
  num_slaves_this_zone = total / num_partitions
  if (total % num_partitions) - current_partitions > 0:
    num_slaves_this_zone += 1
  return num_slaves_this_zone


def main():
  global OPTS
  (OPTS, action, cluster_name) = parse_args()
  try:
    conn = ec2.connect_to_region(OPTS.region)
  except Exception as e:
    print >> stderr, (e)
    sys.exit(1)

  # Select an AZ at random if it was not specified.
  if OPTS.zone == "":
    OPTS.zone = random.choice(conn.get_all_zones()).name

  if action == "launch":
    (master_nodes, slave_nodes, ambari_nodes) = launch_cluster(conn, OPTS, cluster_name)
    wait_for_cluster(conn, OPTS.wait, master_nodes, slave_nodes, ambari_nodes)
    setup_cluster(conn, master_nodes, slave_nodes, ambari_nodes, OPTS, True, cluster_name)
  elif action == "info":
    (master, slave_nodes, ambari) = get_existing_cluster(
      conn, OPTS, cluster_name, die_on_error=False)
    print "Ambari: %s" % ambari[0].public_dns_name
    print "Master: %s" % master[0].public_dns_name
    for slave in slave_nodes:
      print "Slave: %s" % slave.public_dns_name
    print "Master: %s" % master[0].private_dns_name
    print "Slaves:"
    for slave in slave_nodes:
      print slave.private_dns_name
  elif action == "ambari-start":
    (master, slave_nodes, ambari) = get_existing_cluster(
      conn, OPTS, cluster_name, die_on_error=False)
    print ambari[0].public_dns_name
    ssh(ambari[0].public_dns_name, OPTS, "ambari-server start; ambari-server status;")
  elif action == "destroy":
    response = raw_input("Are you sure you want to destroy the cluster " +
        cluster_name + "?\nALL DATA ON ALL NODES WILL BE LOST!!\n" +
        "Destroy cluster " + cluster_name + " (y/N): ")
    if response == "y":
      (ambari_nodes, master_nodes, slave_nodes) = get_existing_cluster(
          conn, OPTS, cluster_name, die_on_error=False)
      print "Terminating ambari..."
      for inst in ambari_nodes:
        inst.terminate()
      print "Terminating master..."
      for inst in master_nodes:
        inst.terminate()
      print "Terminating slaves..."
      for inst in slave_nodes:
        inst.terminate()
  elif action == "stop":
      response = raw_input(
          "Are you sure you want to stop the cluster " +
          cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
          "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" +
          "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
          "All data on spot-instance slaves will be lost.\n" +
          "Stop cluster " + cluster_name + " (y/N): ")
      if response == "y":
          (master_nodes, slave_nodes, ambari_nodes) = get_existing_cluster(
              conn, OPTS, cluster_name, die_on_error=False)
          # print "GOT NODES: " + str((master_nodes, slave_nodes, ambari_nodes))
          print "Stopping master..."
          for inst in master_nodes:
              if inst.state not in ["shutting-down", "terminated"]:
                  inst.stop()
          print "Stopping slaves..."
          for inst in slave_nodes:
              if inst.state not in ["shutting-down", "terminated"]:
                  if inst.spot_instance_request_id:
                      inst.terminate()
                  else:
                      inst.stop()
          print "Stopping ambari..."
          for inst in slave_nodes:
              if inst.state not in ["shutting-down", "terminated"]:
                  if inst.spot_instance_request_id:
                      inst.terminate()
                  else:
                      inst.stop()

def concurrent_map(func, data):
    """
    Similar to the bultin function map(). But spawn a thread for each argument
    and apply `func` concurrently.

    Note: unlike map(), we cannot take an iterable argument. `data` should be an
    indexable sequence.
    """

    N = len(data)
    result = [None] * N

    # wrapper to dispose the result in the right slot
    def task_wrapper(i):
        result[i] = func(data[i])

    threads = [threading.Thread(target=task_wrapper, args=(i,)) for i in xrange(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    return result

if __name__ == "__main__":
  logging.basicConfig()
  main()

