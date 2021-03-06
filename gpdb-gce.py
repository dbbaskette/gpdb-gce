__auth__author__ = 'dbaskette'

import argparse
import os
import pprint
import shutil
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

from paramiko import AuthenticationException,BadHostKeyException,SSHException,WarningPolicy
from paramiko import WarningPolicy

import paramiko
import socket
import time


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


def hostPrep(clusterDictionary):
    print "Running hostPrep"
    print "Creating Data Directories and Sharing gpadmin keys across Cluster for passwordless ssh"

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(WarningPolicy())
    #client.connect(clusterNode["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH, timeout=120)
    for node in clusterDictionary["clusterNodes"]:
        client.connect(node["externalIP"], 22, "gpadmin", password=GPADMIN_PW,timeout=120)
        client.exec_command("echo 'source /usr/local/greenplum-db/greenplum_path.sh\n' >> ~/.bashrc")
        client.exec_command("echo 'export MASTER_DATA_DIRECTORY=/data/master/gpseg-1\n' >> ~/.bashrc")
        print "Configuring Node"
        print "- Sharing gpadmin public key across cluster"
        (stdin, stdout, stderr)=client.exec_command("echo -e  'y\n'|ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''")
        print stderr.readlines()
        print stdout.readlines()
        print " - Install Software for Passwordless SSH"
        (stdin, stdout, stderr) = client.exec_command("sudo mv /etc/yum.repos.d/CentOS-SCL.repo /etc/yum.repos.d/CentOS-SCL.repo.old;sudo yum clean all;sudo yum install -y epel-release;sudo yum install -y sshpass git")
        print stderr.readlines()

        print stdout.readlines()

        client.exec_command("echo 'Host *\nStrictHostKeyChecking no' >> ~/.ssh/config;chmod 400 ~/.ssh/config")
        for node1 in clusterDictionary["clusterNodes"]:
            (stdin, stdout, stderr) = client.exec_command("sshpass -p "+GPADMIN_PW+ "  ssh-copy-id  gpadmin@" + node1["nodeName"])
            print stderr.readlines()

            print stdout.readlines()


        if ("master" in node["role"] or ("standby" in node["role"])):
            print "- Configuring Master/Standby"
            client.exec_command("sudo mkdir -p /data/master;sudo chown -R gpadmin: /data")
            (stdin, stdout, stderr) = client.exec_command("gpssh-exkeys -f /tmp/allhosts")
            print stderr.readlines()

            print stdout.readlines()
            with open("./gpinitsystem_config", 'r+') as gpConfigFile:
                content = gpConfigFile.read()
                gpConfigFile.seek(0)
                gpConfigFile.truncate()
                gpConfigFile.write(content.replace("%MASTER%", node["nodeName"]))
            sftp = client.open_sftp()
            sftp.put("./gpinitsystem_config", "/tmp/gpinitsystem_config")
        else:
            print "- Configuring Worker"
            client.exec_command("sudo mkdir -p /data/primary /data/mirror;sudo chown -R gpadmin: /data")
        client.close()

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
                else:
                    workersFile.write(node["nodeName"] + "\n")
                    allhostsFile.write(node["nodeName"] + "\n")






    for node in clusterDictionary["clusterNodes"]:
        print "Testing SSH  nodes"
        sshOnline = False
        while not sshOnline:
            sshOnline = check_ssh(node["externalIP"])
        print "Node online"
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(WarningPolicy())
        ssh.connect(node["externalIP"], 22, "gpadmin", GPADMIN_PW)
        sftp = ssh.open_sftp()
        sftp.put("hosts","/tmp/hosts")
        sftp.put("allhosts","/tmp/allhosts")
        sftp.put("workers", "/tmp/workers")
        ssh.exec_command("sudo sh -c 'cat /tmp/hosts >> /etc/hosts'")
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
    for nodeCnt in range(int(clusterDictionary["nodes"])):
        clusterNode = {}
        nodeName = clusterDictionary["clusterName"]+"-"+str(nodeCnt+1).zfill(3)

        gce_disk_struct = [
                {
                    "kind": "compute#attachedDisk",
                    "boot": True,
                    "autoDelete": True,

                    'initializeParams': {
                        'sourceImage': "/projects/centos-cloud/global/images/"+image,
                        "diskName": nodeName+"-boot-disk",
                        "diskSizeGb": 20,
                        "diskStorageType": diskType,
                        "diskType": "/compute/v1/projects/"+project+"/zones/"+zone+"/diskTypes/"+diskType
                    },
                },
                 {
                    "source": "projects/"+project+"/zones/"+zone+"/disks/"+nodeName+"-data-disk",
                     "autoDelete": True
                 }

            ]
        print nodeName+": Creating Disk Volume"
        volume = driver.create_volume(DISK_SIZE,nodeName+"-data-disk",None,None,None,False,"pd-standard")
        print nodeName+": Creating Compute Instance"
        node = driver.create_node(nodeName,size=serverType,image=None,location=zone,ex_disks_gce_struct=gce_disk_struct)

        #clusterNode["uuid"] = str(node).split(",")[0].split("=")[1]
        clusterNode["externalIP"] = str(node).split(",")[3].split("'")[1]
        clusterNode["internalIP"] = str(node).split(",")[4].split("'")[1]
        clusterNode["nodeName"] = nodeName
        #print nodeName+": UUID       : "+clusterNode["uuid"]
        print nodeName+": External IP: "+clusterNode["externalIP"]
        print nodeName+": Internal IP: "+clusterNode["internalIP"]
        # Set Server Role

        if (nodeCnt + 1) == 1:
            clusterNode["role"] = "master"
        elif (nodeCnt + 1) == 2:
            clusterNode["role"] = "standby"
        else:
            clusterNode["role"] = "worker"

        clusterNodes.append(clusterNode)
        print nodeName+": Prepping Host"

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(WarningPolicy())
        connected = False
        while not connected:
            try:
                ssh.connect(clusterNode["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH, timeout=120)
                connected = True
            except (BadHostKeyException, AuthenticationException,SSHException, socket.error) as e:
                time.sleep(3)
                print clusterNode["nodeName"]+ ": Waiting for SSH Connection"

        sftp = ssh.open_sftp()
        sftp.put("../configs/sysctl.conf.gpdb", "/tmp/sysctl.conf.gpdb")
        sftp.put("../configs/limits.conf.gpdb", "/tmp/limits.conf.gpdb")

        sftp.put("../scripts/prepareHost.sh","/tmp/prepareHost.sh")
        # client.close()
        #
        # #  MOVE THIS TO NEW METHOD   def preInstallPrep
        # client = paramiko.SSHClient()
        # client.set_missing_host_key_policy(WarningPolicy())
        # client.connect(clusterNode["externalIP"],22,SSH_USERNAME,None,pkey=None,key_filename=SSH_KEY_PATH,timeout=120)

        ssh.exec_command("sudo sed -i 's|[#]*PasswordAuthentication no|PasswordAuthentication yes|g' /etc/ssh/sshd_config")
        ssh.exec_command("sudo sed -i 's|UsePAM no|UsePAM yes|g' /etc/ssh/sshd_config")
        ssh.exec_command("sudo sh -c 'echo Defaults !requiretty\n > /etc/sudoers.d/888-dont-requiretty'")
        ssh.exec_command("sudo sh -c 'cat /tmp/sysctl.conf.gpdb >> /etc/sysctl.conf'")
        ssh.exec_command("sudo sh -c 'cat /tmp/limits.conf.gpdb >> /etc/security/limits.conf'")
        ssh.exec_command("sudo chmod +x /tmp/prepareHost.sh")



        # client = paramiko.SSHClient()
        # client.set_missing_host_key_policy(WarningPolicy())
        # driver.wait_until_running([node],wait_period=3, timeout=600, ssh_interface='public_ips')
        # client.connect(clusterNode["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH,timeout=120)
        print nodeName+": Running PrepareHost"
        (stdin, stdout, stderr)=ssh.exec_command("/tmp/prepareHost.sh")
        output = stdout.readlines()
        error = stderr.readlines
        # TRY REBOOT HERE...THEN ONLY WAITING ON ONE.
        print  clusterNode["nodeName"] + ": Rebooting to make System Config Changes"
        ssh.exec_command("sudo reboot -fq")
    clusterDictionary["clusterNodes"]=clusterNodes

    pprint.pprint(clusterNodes)
    #rebootCluster(clusterDictionary,driver)
    hostsFiles(clusterDictionary)

    hostPrep(clusterDictionary)
    initGPDB(clusterDictionary)





def rebootCluster(clusterDictionary,driver):

    for node in clusterDictionary["clusterNodes"]:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(WarningPolicy())
        client.connect(node["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH,timeout=120)
        print  node["nodeName"] + ": Rebooting"
        client.exec_command("sudo reboot -fq")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(WarningPolicy())
        sshOnline = False
        while not sshOnline:
            sshOnline = check_ssh(node["externalIP"])







def check_ssh(ip):
    initial_wait=5
    interval=3
    retries=20
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(WarningPolicy())

    time.sleep(initial_wait)

    for x in range(retries):
        try:

            print ssh.connect(ip, 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH,timeout=120)
            ssh.close()
            return True
        except Exception as e:
            time.sleep(interval)
            print "Testing SSH Connectivity"
    return False




if __name__ == '__main__':
    cliParse()

# CLASS SETUP
#https://github.com/mgoddard-pivotal/dsip_ec2_setupdard-pivotal/dsip_ec2_setup