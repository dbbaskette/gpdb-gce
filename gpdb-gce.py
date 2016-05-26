__author__ = 'dbaskette'

import argparse
import vagrant
import os
from common import ssh
import pprint
import shutil


def cliParse():
    VALID_ACTION = ["create","destroy","query"]
    parser = argparse.ArgumentParser(description='Pivotal Education Lab Builder')
    subparsers = parser.add_subparsers(help='sub-command help', dest="subparser_name")
    parser_create = subparsers.add_parser("create", help="Create a Cluster")
    parser_destroy = subparsers.add_parser("destroy", help="Destroy a Cluster")
    parser_query = subparsers.add_parser("query", help="Destroy a Cluster")

    parser_create.add_argument("--clustername", dest='clustername', action="store",help="Name of Cluster to be Created",required=True)
    parser_create.add_argument("--nodes", dest='nodes', default=1, action="store", help="Number of Nodes to be Created",required=True)
    parser_destroy.add_argument("--clustername", dest='clustername', action="store",help="Name of Cluster to be Deleted",required=True)
    args = parser.parse_args()
    clusterDictionary = {}
    if (args.subparser_name == "create"):
        clusterDictionary["clustername"] = args.clustername
        clusterDictionary["nodes"] = args.nodes
        createCluster(clusterDictionary)
    elif (args.subparser_name == "destroy"):
        clusterDictionary["clustername"] = args.clustername
        destroyCluster(clusterDictionary)
    elif (args.subparser_name == "query"):
        clusterDictionary["clustername"] = args.clustername
        queryCluster(clusterDictionary)


def queryCluster():
    # Test a single known VM
    v = vagrant.Vagrant(quiet_stdout=False)
    sshConfig = v.ssh_config("gpdb-1-db1")
    ipAddress = sshConfig.splitlines()[1].split()[1]


def hostPrep(clusterDictionary):
    print "Creating Data Directories and Sharing gpadmin keys across Cluster for passwordless ssh"

    for node in clusterDictionary["nodeInfo"]:
        if ("master" in node["role"] or ("standby" in node["role"])):
            print "Sharing gpadmin public key across cluster"
            ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin", "ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''")
            print " - Install Software for Passwordless SSH"
            ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin","sudo mv /etc/yum.repos.d/CentOS-SCL.repo /etc/yum.repos.d/CentOS-SCL.repo.old;sudo yum clean all;sudo yum install -y epel-release;sudo yum install -y sshpass")
            ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin","echo 'Host *\nStrictHostKeyChecking no' >> ~/.ssh/config;chmod 400 ~/.ssh/config")
            masterIP = node["ipAddress"]
            for node1 in clusterDictionary["nodeInfo"]:
                ssh.exec_command2(masterIP, "gpadmin", "gpadmin", "sshpass -p gpadmin  ssh-copy-id  gpadmin@" + node1["nodeName"])
            ssh.exec_command2(masterIP, "gpadmin", "gpadmin", "sudo mkdir -p /data/master;sudo chown -R gpadmin: /data")

            with open("./gpinitsystem_config", 'r+') as gpConfigFile:
                content = gpConfigFile.read()
                gpConfigFile.seek(0)
                gpConfigFile.truncate()
                gpConfigFile.write(content.replace("%MASTER%", node["nodeName"]))
            ssh.putFile(masterIP, "gpinitsystem_config", "gpadmin", "gpadmin")

        else:
            ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin", "sudo mkdir -p /data/primary /data/mirror;sudo chown -R gpadmin: /data")



        # elif "standby" in node["role"]:
        #     print "Standby ("+ node["ipAddress"] + "):   Sharing gpadmin public key across cluster"
        #
        #
        #     ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin", "ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''")
        #     print " - Install Software for Passwordless SSH"
        #     ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin", "sudo mv /etc/yum.repos.d/CentOS-SCL.repo /etc/yum.repos.d/CentOS-SCL.repo.old;sudo yum clean all;sudo yum install -y epel-release;sudo yum install -y sshpass")
        #     ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin","echo 'Host *\nStrictHostKeyChecking no' >> ~/.ssh/config;chmod 400 ~/.ssh/config")
        #     standbyIP = node["ipAddress"]
        #
        #
        #     for node in clusterDictionary["nodeInfo"]:
        #         ssh.exec_command2(standbyIP, "gpadmin", "gpadmin","sshpass -p gpadmin  ssh-copy-id  gpadmin@" + node["nodeName"])
        #
        #     ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin", "mkdir -p /data/master;chown -R gpadmin: /data")





def clusterStatus(clusterDictionary):

    v = vagrant.Vagrant(quiet_stdout=True)
    nodeInfo = []
    for nodecnt in range(int(clusterDictionary["nodes"])):
        hostName = "gpdb-"+str(nodecnt+1)+"-"+clusterDictionary["clustername"]
        status =  v.status(hostName)
        ipAddress = v.ssh_config(hostName).splitlines()[1].split()[1]
        clusterNode = {}
        if int(nodecnt+1)==1:
            clusterNode["role"]="master"
        elif int(nodecnt+1)==2:
            clusterNode["role"]="standby"
        else:
            clusterNode["role"]="worker"
        clusterNode["nodeName"] = hostName
        clusterNode["ipAddress"] = ipAddress
        clusterNode["status"] = status
        nodeInfo.append(clusterNode)
        clusterNode["internalIPAddress"] = (ssh.exec_command2(ipAddress, "gpadmin", "gpadmin", cmd="hostname --all-ip-address"))[1].strip('\n')
        #ssh.exec_command(clusterNode["ipAddress"], "gpadmin", "gpadmin", "sudo service sshd reload")

    clusterDictionary["nodeInfo"] = nodeInfo
    pprint.pprint(clusterDictionary)


def hostsFiles(clusterDictionary):
    print "Build /etc/hosts file for Cluster Nodes"

    clusterPath = "./" + clusterDictionary["clustername"]

    with open ("hosts","w") as hostsFile:
        hostsFile.write("######  GPDB-GCE ENTRIES #######\n")
        for node in clusterDictionary["nodeInfo"]:
            hostsFile.write(node["internalIPAddress"]+"  "+node["nodeName"]+"\n")

    with open ("workers","w") as workersFile:
        with open("allhosts", "w") as allhostsFile:
            for node in clusterDictionary["nodeInfo"]:
                if "master" in node["role"] :
                    allhostsFile.write(node["nodeName"] + "\n")
                elif "standby" in node["role"]:
                    allhostsFile.write(node["nodeName"] + "\n")
                else:
                    workersFile.write(node["nodeName"] + "\n")
                    allhostsFile.write(node["nodeName"] + "\n")


    for node in clusterDictionary["nodeInfo"]:
        ssh.putFile(node["ipAddress"],"hosts","gpadmin","gpadmin")
        ssh.exec_command2(node["ipAddress"],"gpadmin","gpadmin","sudo sh -c 'cat /tmp/hosts >> /etc/hosts'")
        ssh.putFile(node["ipAddress"],"allhosts","gpadmin","gpadmin")
        ssh.putFile(node["ipAddress"], "workers", "gpadmin", "gpadmin")


def createCluster(clusterDictionary):
    if not os.path.exists(clusterDictionary["clustername"]):
        os.makedirs(clusterDictionary["clustername"])
    clusterPath = "./" + clusterDictionary["clustername"]
    with open(clusterPath + "/Vagrantfile", "w") as vagrantfile:
        with open("./Vagrantfile.master") as master:
            for line in master:
                if "%CLUSTERNAME%" in line:
                    vagrantfile.write(line.replace("%CLUSTERNAME%", clusterDictionary["clustername"]))
                elif "%CLUSTERNODES%" in line:
                    vagrantfile.write(line.replace("%CLUSTERNODES%", clusterDictionary["nodes"]))
                else:
                    vagrantfile.write(line)

    os.chdir(clusterPath)
    shutil.copyfile("../gpinitsystem_config","./gpinitsystem_config")


    v = vagrant.Vagrant(quiet_stdout=True)
    v.up(provider="google")
    clusterStatus(clusterDictionary)
    hostsFiles(clusterDictionary)
    hostPrep(clusterDictionary)

def destroyCluster(clusterDictionary):
    clusterPath = "./" + clusterDictionary["clustername"]
    os.chdir(clusterPath)
    v = vagrant.Vagrant(quiet_stdout=True)
    v.destroy()
    os.chdir("../.")
    shutil.rmtree(clusterPath)




if __name__ == '__main__':
    cliParse()
