__author__ = 'dbaskette'

import argparse
import vagrant
import os
from common import ssh

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
    print v.status("gpdb-1-db1")
    sshConfig = v.ssh_config("gpdb-1-db1")
    ipAddress = sshConfig.splitlines()[1].split()[1]
    print ipAddress
    print ssh.exec_command2(ipAddress,cmd="ls")


def clusterStatus(clusterDictionary):

    v = vagrant.Vagrant(quiet_stdout=True)
    nodeInfo = []
    for nodecnt in range(int(clusterDictionary["nodes"])):
        hostName = "gpdb-"+str(nodecnt+1)+"-"+clusterDictionary["clustername"]
        status =  v.status(hostName)
        ipAddress = v.ssh_config(hostName).splitlines()[1].split()[1]
        print status
        clusterNode = {}
        clusterNode["ipAddress"] = ipAddress
        clusterNode["status"] = status
        nodeInfo.append(clusterNode)
        clusterNode["internalIPAddress"] = (ssh.exec_command2(ipAddress, "gpadmin", "gpadmin", cmd="hostname --all-ip-address"))[1].strip('\n')

    clusterDictionary["nodeInfo"] = nodeInfo
    print clusterDictionary


def etcHosts(clusterDictionary):
    print "Build ETC HOSTS and UPLOAD"


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
    v = vagrant.Vagrant(quiet_stdout=True)
    v.up(provider="google")
    clusterStatus(clusterDictionary)




def destroyCluster(clusterDictionary):
    clusterPath = "./" + clusterDictionary["clustername"]
    os.chdir(clusterPath)
    v = vagrant.Vagrant(quiet_stdout=True)
    v.destroy()




if __name__ == '__main__':
    cliParse()
