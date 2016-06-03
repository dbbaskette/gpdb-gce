__author__ = 'dbaskette'

import argparse
import os
import pprint
import shutil
import socket
import time
import paramiko
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider
from paramiko import AuthenticationException,BadHostKeyException,SSHException
from paramiko import WarningPolicy

SERVER_TYPE = 'n1-standard-16'
IMAGE = 'centos-6-v20160526'
ZONE = 'us-east1-d'
DISK_TYPE = 'pd-standard'
PROJECT = "pivotal-1211"
GPADMIN_PW = "p1v0tal"
SSH_USERNAME = "dbaskette"
SSH_KEY_PATH = "/Users/dbaskette/.ssh/google_compute_engine"
KEY = "/Users/dbaskette/Downloads/Pivotal-8b6ceb84c23f.json"
SVC_ACCOUNT = "libcloud@pivotal-1211.iam.gserviceaccount.com"
DISK_SIZE = 1200
MADLIB="madlib-ossv1.9_pv1.9.5_gpdb4.3orca-rhel5-x86_64.gppkg"
PLR="plr-ossv8.3.0.15_pv2.1_gpdb4.3orca-rhel5-x86_64.gppkg"


def cliParse():
    VALID_ACTION = ["create","destroy","query"]
    parser = argparse.ArgumentParser(description='Pivotal Education Lab Builder')
    subparsers = parser.add_subparsers(help='sub-command help', dest="subparser_name")
    parser_create = subparsers.add_parser("create", help="Create a Cluster")
    parser_destroy = subparsers.add_parser("destroy", help="Destroy a Cluster")
    parser_query = subparsers.add_parser("query", help="Destroy a Cluster")

    parser_create.add_argument("--clustername", dest='clustername', action="store",help="Name of Cluster to be Created",required=True)
    parser_create.add_argument("--nodes", dest='nodes', default=1, action="store", help="Number of Nodes to be Created",required=True)
    parser_create.add_argument("-v", dest='verbose', action='store_true',required=False)


    parser_destroy.add_argument("--clustername", dest='clustername', action="store",help="Name of Cluster to be Deleted",required=True)
    args = parser.parse_args()
    clusterDictionary = {}
    if (args.subparser_name == "create"):
        clusterDictionary["clusterName"] = args.clustername
        clusterDictionary["nodes"] = args.nodes
        if (args.verbose == True):
            createCluster(clusterDictionary,False)  #These are opposite because  the logging value is quiet_stdout so True is no logging
        else:
            createCluster(clusterDictionary,True)

    elif (args.subparser_name == "destroy"):
        clusterDictionary["clusterName"] = args.clustername
    elif (args.subparser_name == "query"):
        clusterDictionary["clusterName"] = args.clustername



def initGPDB(clusterDictionary):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(WarningPolicy())
    for node in clusterDictionary["clusterNodes"]:
        if ("master" in node["role"]):
            client.connect(node["externalIP"], 22, "gpadmin", GPADMIN_PW,timeout=120)
            print "Initializing Greenplum Database"
            (stdin, stdout, stderr) = client.exec_command("gpinitsystem -c /tmp/gpinitsystem_config -a")
            output = stdout.readlines()
            (stdin, stdout, stderr) = client.exec_command("echo git clonegpinitsystem -c /tmp/gpinitsystem_config -a")


def installAnalytics(clusterDictionary):

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(WarningPolicy())
    for node in clusterDictionary["clusterNodes"]:
        if ("master" in node["role"]):
            print node["nodeName"] + ": Installing MADlib"
            ssh.connect(node["externalIP"], 22, "gpadmin", password=GPADMIN_PW, timeout=120)
            (stdin, stdout, stderr) =ssh.exec_command("gppkg -i /tmp/"+MADLIB)
            stderr.readlines()
            stdout.readlines()
            ssh.exec_command("$GPHOME/madlib/bin/madpack install -s madlib -p greenplum -c gpadmin@"+node["nodeName"]+"/template1")
            ssh.exec_command("$GPHOME/madlib/bin/madpack install -s madlib -p greenplum -c gpadmin@"+node["nodeName"]+"/gpadmin")
            print "     - Installing R on all the Nodes.  This might take awhile."
            (stdin, stdout, stderr) =ssh.exec_command("gpssh -f /tmp/allhosts 'sudo yum -y install R'")
            stderr.readlines()
            stdout.readlines()
            ssh.exec_command("createlang plpythonu -d template1")
            ssh.exec_command("createlang plpythonu -d gpadmin")
            (stdin, stdout, stderr) =   ssh.exec_command("gppkg -i /tmp/"+PLR)
            stderr.readlines()
            stdout.readlines()
            ssh.exec_command("createlang plr -d template1")
            ssh.exec_command("createlang plr -d gpadmin")
            print "     - MADLib, PL/Python, PL/R Enabled"


def hostPrep(clusterDictionary):
    print "Running hostPrep"
    print "Creating Data Directories and Sharing gpadmin keys across Cluster for passwordless ssh"


    #client.connect(clusterNode["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH, timeout=120)
    for node in clusterDictionary["clusterNodes"]:

        connected = False
        attemptCount = 0
        while not connected:
            try:
                attemptCount += 1
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(WarningPolicy())
                # (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir /data")
                # print stderr.readlines()
                # print stdout.readlines()
                # (stdin, stdout, stderr) = ssh.exec_command("sudo  sh -c 'echo LABEL=gpdbdata /data xfs rw,noatime,inode64,allocsize=16m 0 0 >> /etc/fstab'")
                # print stderr.readlines()
                # print stdout.readlines()
                # (stdin, stdout, stderr) = ssh.exec_command("sudo mount -a")
                # print stderr.readlines()
                # print stdout.readlines()
                ssh.connect(node["externalIP"], 22, "gpadmin", password=GPADMIN_PW, timeout=120)
                ssh.exec_command("echo 'source /usr/local/greenplum-db/greenplum_path.sh\n' >> ~/.bashrc")
                ssh.exec_command("echo 'export MASTER_DATA_DIRECTORY=/data/master/gpseg-1\n' >> ~/.bashrc")
                print node["externalIP"] + ": Configuring Node"
                print " - Sharing gpadmin public key across cluster"
                (stdin, stdout, stderr) = ssh.exec_command("echo -e  'y\n'|ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''")
                stderr.readlines()
                stdout.readlines()
                print " - Install Software for Passwordless SSH"
                print " - Repair Repos and Install Software"
                (stdin, stdout, stderr) = ssh.exec_command("sudo rm -f /etc/yum.repos.d/CentOS-SCL*;sudo yum clean all;sudo yum install -y epel-release;sudo yum clean all;sudo yum install -y sshpass git")
                stderr.readlines()
                stdout.readlines()
                print " - Share gpadmin keys"
                ssh.exec_command("echo 'Host *\nStrictHostKeyChecking no' >> ~/.ssh/config;chmod 400 ~/.ssh/config")
                for node1 in clusterDictionary["clusterNodes"]:
                    (stdin, stdout, stderr) = ssh.exec_command(
                        "sshpass -p " + GPADMIN_PW + "  ssh-copy-id  gpadmin@" + node1["nodeName"])
                    stderr.readlines()
                    stdout.readlines()


                if ("master" in node["role"] or ("standby" in node["role"])):

                    print "     - Configuring Role as Master/Standby"
                    ssh.exec_command("sudo mkdir -p /data/master;sudo chown -R gpadmin: /data")
                    print "     - Share Keys among all hosts"
                    (stdin, stdout, stderr) = ssh.exec_command("gpssh-exkeys -f /tmp/allhosts")
                    stderr.readlines()
                    stdout.readlines()
                    print "     - Build GPDB Config File"

                    with open("./gpinitsystem_config", 'r+') as gpConfigFile:
                        content = gpConfigFile.read()
                        gpConfigFile.seek(0)
                        gpConfigFile.truncate()
                        gpConfigFile.write(content.replace("%MASTER%", node["nodeName"]))
                    sftp = ssh.open_sftp()
                    sftp.put("./gpinitsystem_config", "/tmp/gpinitsystem_config")
                elif ("worker" in node["role"]):
                    print " - Configuring Role as Worker"
                    ssh.exec_command("sudo mkdir -p /data/primary /data/mirror;sudo chown -R gpadmin: /data")
                else:
                    print " - Configuring Role as ETL Host"
                    ssh.exec_command("sudo mkdir -p /data;sudo chown -R gpadmin: /data")
                connected = True
            except Exception as e:
                # print e
                print node["nodeName"] + ": Attempting SSH Connection"
                time.sleep(3)
                if attemptCount > 40:
                    print "Failing Process"
                    exit()
            finally:
                ssh.close()








def hostsFiles(clusterDictionary):
    print "Running hostsFile to Build all needed Hosts files."

    clusterPath = "./" + clusterDictionary["clusterName"]

    with open ("hosts","w") as hostsFile:
        hostsFile.write("######  GPDB-GCE ENTRIES #######\n")
        for node in clusterDictionary["clusterNodes"]:
            hostsFile.write(node["internalIP"]+"  "+node["nodeName"]+"\n")

    with open ("workers","w") as workersFile:
        with open("allhosts", "w") as allhostsFile:
            for node in clusterDictionary["clusterNodes"]:
                if "master" in node["role"] :
                    allhostsFile.write(node["nodeName"] + "\n")
                elif "standby" in node["role"]:
                    allhostsFile.write(node["nodeName"] + "\n")
                elif "etl" in node["role"]:
                    allhostsFile.write(node["nodeName"] + "\n")
                else:
                    workersFile.write(node["nodeName"] + "\n")
                    allhostsFile.write(node["nodeName"] + "\n")






    for node in clusterDictionary["clusterNodes"]:

        connected = False
        attemptCount = 0
        while not connected:
            try:
                attemptCount += 1
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(WarningPolicy())

                ssh.connect(node["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH,timeout=120)
                sftp = ssh.open_sftp()
                sftp.put("hosts", "/tmp/hosts")
                sftp.put("allhosts", "/tmp/allhosts")
                sftp.put("workers", "/tmp/workers")
                ssh.exec_command("sudo sh -c 'cat /tmp/hosts >> /etc/hosts'")
                connected = True
            except Exception as e:
                #print e
                print node["nodeName"]+": Attempting SSH Connection"
                time.sleep(3)
                if attemptCount > 40:
                    print "Failing Process"
                    exit()
            finally:
                ssh.close()







def createCluster(clusterDictionary,verbose):

    print "Create Cluster via Apache libCloud"
    if not os.path.exists(clusterDictionary["clusterName"]):
        os.makedirs(clusterDictionary["clusterName"])
    clusterPath = "./" + clusterDictionary["clusterName"]
    os.chdir(clusterPath)
    shutil.copyfile("../configs/gpinitsystem_config", "./gpinitsystem_config")
    ComputeEngine = get_driver(Provider.GCE)
    driver = ComputeEngine('libcloud@pivotal-1211.iam.gserviceaccount.com', '~/Downloads/Pivotal-0556bc44fea8.json',
                           project='pivotal-1211', datacenter='us-east1-d')

    serverType = 'n1-standard-4'
    image = 'centos-6-v20160526'
    zone = 'us-east1-d'
    diskType = 'pd-standard'
    project = "pivotal-1211"
    SSH_USERNAME = "dbaskette"
    SSH_KEY_PATH = "/Users/dbaskette/.ssh/google_compute_engine"
    KEY = "/Users/dbaskette/Downloads/Pivotal-8b6ceb84c23f.json"

    sa_scopes = [{'scopes': ['compute', 'storage-full']}]

    clusterNodes=[]
    print "Creating Cluster Nodes"
    nodes = driver.ex_create_multiple_nodes(clusterDictionary["clusterName"], SERVER_TYPE, IMAGE,
                                    int(clusterDictionary["nodes"]), ZONE,
                                    ex_network='default', ex_tags=None, ex_metadata=None, ignore_errors=True,
                                    use_existing_disk=True, poll_interval=2, external_ip='ephemeral',
                                    ex_disk_type='pd-standard', ex_disk_auto_delete=True, ex_service_accounts=None,
                                    timeout=180, description=None, ex_can_ip_forward=None, ex_disks_gce_struct=None,
                                    ex_nic_gce_struct=None, ex_on_host_maintenance=None, ex_automatic_restart=None)
    for nodeCnt in range(int(clusterDictionary["nodes"])):
        clusterNode = {}
        nodeName = clusterDictionary["clusterName"] + "-" + str(nodeCnt).zfill(3)
        print nodeName + ": Enhancing Cluster Node"
        print nodeName + ": Creating Data Disk Volume"
        volume = driver.create_volume(DISK_SIZE, nodeName + "-data-disk", None, None, None, False, "pd-standard")
        clusterNode["nodeName"] = nodeName
        clusterNode["dataVolume"] = volume
        print nodeName + ": Attaching Disk Volume"
        node = driver.ex_get_node(nodeName)
        driver.attach_volume(node, volume, device=None, ex_mode=None, ex_boot=False, ex_type=None, ex_source=None,
                      ex_auto_delete=True, ex_initialize_params=None, ex_licenses=None, ex_interface=None)

        clusterNode["externalIP"] = str(node).split(",")[3].split("'")[1]
        clusterNode["internalIP"] = str(node).split(",")[4].split("'")[1]

        print nodeName + ": External IP: " + clusterNode["externalIP"]
        print nodeName + ": Internal IP: " + clusterNode["internalIP"]
        # Set Server Role

        if (nodeCnt) == 0:
            clusterNode["role"] = "etl"
        elif (nodeCnt) == 1:
            clusterNode["role"] = "master"
        elif (nodeCnt) == 2:
            clusterNode["role"] = "standby"
        else:
            clusterNode["role"] = "worker"

        clusterNodes.append(clusterNode)

        print nodeName+": Prepping Host"


        connected = False
        attemptCount=0
        while not connected:
            try:
                attemptCount+=1
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(WarningPolicy())

                ssh.connect(clusterNode["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH,timeout=120)
                sftp = ssh.open_sftp()
                sftp.put("../configs/sysctl.conf.gpdb", "/tmp/sysctl.conf.gpdb")
                sftp.put("../configs/limits.conf.gpdb", "/tmp/limits.conf.gpdb")
                sftp.put("../configs/fstab.gpdb", "/tmp/fstab.gpdb")

                sftp.put("../scripts/prepareHost.sh", "/tmp/prepareHost.sh")

                ssh.exec_command("sudo chmod +x /tmp/prepareHost.sh")
                print nodeName + ": Running /tmp/prepareHost.sh"
                (stdin, stdout, stderr) = ssh.exec_command("/tmp/prepareHost.sh &> /tmp/prepareHost.log")
                stdout.readlines()
                stderr.readlines()
                (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir /data")
                stdout.readlines()
                stderr.readlines()


                print  nodeName + ": Rebooting to make System Config Changes"
                ssh.close()
                driver.reboot_node(node)
                connected=True
            except Exception as e:
                #print e
                print nodeName+": Attempting SSH Connection"
                time.sleep(3)

                if attemptCount > 40:
                    print "Failing Process"
                    exit()
            finally:
                ssh.close()





    clusterDictionary["clusterNodes"]=clusterNodes

    pprint.pprint(clusterNodes)
    hostsFiles(clusterDictionary)
    #
    hostPrep(clusterDictionary)
    initGPDB(clusterDictionary)
    installAnalytics(clusterDictionary)



# def check_ssh(ip):
#     initial_wait=5
#     interval=3
#     retries=40
#     ssh = paramiko.SSHClient()
#     ssh.set_missing_host_key_policy(WarningPolicy())
#
#     time.sleep(initial_wait)
#
#     for x in range(retries):
#         try:
#
#             ssh.connect(ip, 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH,timeout=120)
#             ssh.close()
#             return True
#         except Exception as e:
#             print e
#             time.sleep(interval)
#             print "Testing SSH Connectivity"
#     return False




if __name__ == '__main__':
    cliParse()

# CLASS SETUP
#https://github.com/mgoddard-pivotal/dsip_ec2_setup