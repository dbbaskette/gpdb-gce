__author__ = 'dbaskette'

import argparse
import vagrant
import os
#from common import ssh
import pprint
import shutil
from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver
from libcloud.compute.ssh import ParamikoSSHClient
import time
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
        destroyCluster(clusterDictionary)
    elif (args.subparser_name == "query"):
        clusterDictionary["clusterName"] = args.clustername
        queryCluster(clusterDictionary)


def queryCluster():
    # Test a single known VM
    v = vagrant.Vagrant(quiet_stdout=False)
    sshConfig = v.ssh_config("gpdb-1-db1")
    ipAddress = sshConfig.splitlines()[1].split()[1]

# def initGPDB(clusterDictionary):
#     for node in clusterDictionary["nodeInfo"]:
#         if ("master" in node["role"]):
#             print "Initializing Greenplum Database"
#             ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin", "gpinitsystem -c /tmp/gpinitsystem_config -a")
#



#
# def hostPrep(clusterDictionary):
#     print "Creating Data Directories and Sharing gpadmin keys across Cluster for passwordless ssh"
#
#     for node in clusterDictionary["nodeInfo"]:
#         ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin","echo 'source /usr/local/greenplum-db/greenplum_path.sh\n' >> ~/.bashrc")
#         ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin","echo 'export MASTER_DATA_DIRECTORY=/data/master/gpseg-1\n' >> ~/.bashrc")
#         if ("master" in node["role"] or ("standby" in node["role"])):
#             print "Sharing gpadmin public key across cluster"
#             ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin", "ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''")
#             print " - Install Software for Passwordless SSH"
#             ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin","sudo mv /etc/yum.repos.d/CentOS-SCL.repo /etc/yum.repos.d/CentOS-SCL.repo.old;sudo yum clean all;sudo yum install -y epel-release;sudo yum install -y sshpass")
#             ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin","echo 'Host *\nStrictHostKeyChecking no' >> ~/.ssh/config;chmod 400 ~/.ssh/config")
#             masterIP = node["ipAddress"]
#             for node1 in clusterDictionary["nodeInfo"]:
#                 ssh.exec_command2(masterIP, "gpadmin", "gpadmin", "sshpass -p gpadmin  ssh-copy-id  gpadmin@" + node1["nodeName"])
#             ssh.exec_command2(masterIP, "gpadmin", "gpadmin", "sudo mkdir -p /data/master;sudo chown -R gpadmin: /data")
#             with open("./gpinitsystem_config", 'r+') as gpConfigFile:
#                 content = gpConfigFile.read()
#                 gpConfigFile.seek(0)
#                 gpConfigFile.truncate()
#                 gpConfigFile.write(content.replace("%MASTER%", node["nodeName"]))
#             ssh.putFile(masterIP, "gpinitsystem_config", "gpadmin", "gpadmin")
#         else:
#             ssh.exec_command2(node["ipAddress"], "gpadmin", "gpadmin", "sudo mkdir -p /data/primary /data/mirror;sudo chown -R gpadmin: /data")
#





# def clusterStatus(clusterDictionary):
#
#     v = vagrant.Vagrant(quiet_stdout=True)
#     nodeInfo = []
#     for nodecnt in range(int(clusterDictionary["nodes"])):
#         hostName = "gpdb-"+str(nodecnt+1)+"-"+clusterDictionary["clustername"]
#         status =  v.status(hostName)
#         ipAddress = v.ssh_config(hostName).splitlines()[1].split()[1]
#         clusterNode = {}
#         if int(nodecnt+1)==1:
#             clusterNode["role"]="master"
#         elif int(nodecnt+1)==2:
#             clusterNode["role"]="standby"
#         else:
#             clusterNode["role"]="worker"
#         clusterNode["nodeName"] = hostName
#         clusterNode["ipAddress"] = ipAddress
#         clusterNode["status"] = status
#         nodeInfo.append(clusterNode)
#         clusterNode["internalIPAddress"] = (ssh.exec_command2(ipAddress, "gpadmin", "gpadmin", cmd="hostname --all-ip-address"))[1].strip('\n')
#         #ssh.exec_command(clusterNode["ipAddress"], "gpadmin", "gpadmin", "sudo service sshd reload")
#
#     clusterDictionary["nodeInfo"] = nodeInfo
#     pprint.pprint(clusterDictionary)


# def hostsFiles(clusterDictionary):
#     print "Build /etc/hosts file for Cluster Nodes"
#
#     clusterPath = "./" + clusterDictionary["clustername"]
#
#     with open ("hosts","w") as hostsFile:
#         hostsFile.write("######  GPDB-GCE ENTRIES #######\n")
#         for node in clusterDictionary["nodeInfo"]:
#             hostsFile.write(node["internalIPAddress"]+"  "+node["nodeName"]+"\n")
#
#     with open ("workers","w") as workersFile:
#         with open("allhosts", "w") as allhostsFile:
#             for node in clusterDictionary["nodeInfo"]:
#                 if "master" in node["role"] :
#                     allhostsFile.write(node["nodeName"] + "\n")
#                 elif "standby" in node["role"]:
#                     allhostsFile.write(node["nodeName"] + "\n")
#                 else:
#                     workersFile.write(node["nodeName"] + "\n")
#                     allhostsFile.write(node["nodeName"] + "\n")
#
#
#     for node in clusterDictionary["nodeInfo"]:
#         ssh.putFile(node["ipAddress"],"hosts","gpadmin","gpadmin")
#         ssh.exec_command2(node["ipAddress"],"gpadmin","gpadmin","sudo sh -c 'cat /tmp/hosts >> /etc/hosts'")
#         ssh.putFile(node["ipAddress"],"allhosts","gpadmin","gpadmin")
#         ssh.putFile(node["ipAddress"], "workers", "gpadmin", "gpadmin")


# def createCluster(clusterDictionary,verbose):
#
#
#     if not os.path.exists(clusterDictionary["clustername"]):
#         os.makedirs(clusterDictionary["clustername"])
#     clusterPath = "./" + clusterDictionary["clustername"]
#     with open(clusterPath + "/Vagrantfile", "w") as vagrantfile:
#         with open("./Vagrantfile.master") as master:
#             for line in master:
#                 if "%CLUSTERNAME%" in line:
#                     vagrantfile.write(line.replace("%CLUSTERNAME%", clusterDictionary["clustername"]))
#                 elif "%CLUSTERNODES%" in line:
#                     vagrantfile.write(line.replace("%CLUSTERNODES%", clusterDictionary["nodes"]))
#                 else:
#                     vagrantfile.write(line)
#
#     os.chdir(clusterPath)
#     shutil.copyfile("../configs/gpinitsystem_config","./gpinitsystem_config")
#
#     v = vagrant.Vagrant(quiet_stdout=verbose)
#     v.up(provider="google")
#     clusterStatus(clusterDictionary)
#     hostsFiles(clusterDictionary)
#     hostPrep(clusterDictionary)
#     initGPDB(clusterDictionary)





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
    gpadminPW = "gpadmin"
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
        volume = driver.create_volume(500,nodeName+"-data-disk",None,None,None,False,"pd-standard")
        print nodeName+": Creating Compute Instance"
        node = driver.create_node(nodeName,size=serverType,image=None,location=zone,ex_disks_gce_struct=gce_disk_struct)
        clusterNode["externalIP"] = str(node).split(",")[3].split("'")[1]
        clusterNode["internalIP"] = str(node).split(",")[4].split("'")[1]
        clusterNode["nodeName"] = nodeName
        print nodeName+": External IP: "+clusterNode["externalIP"]
        print nodeName+": Internal IP: "+clusterNode["internalIP"]
        clusterNodes.append(clusterNode)
        print nodeName+": Prepping Host"
        sshClient=ParamikoSSHClient(clusterNode["externalIP"],22,SSH_USERNAME,None,key=None,key_files=SSH_KEY_PATH,timeout=120)

       # Do something more elaborate here.  Perhaps Build all server then do the SSH stuff

        time.sleep(60)

        sshClient.connect()


        print sshClient.run("sudo sed -i 's|[#]*PasswordAuthentication no|PasswordAuthentication yes|g' /etc/ssh/sshd_config")
        print sshClient.run("sudo sed -i 's|UsePAM no|UsePAM yes|g' /etc/ssh/sshd_config")
        print sshClient.run("sudo sh -c 'echo Defaults !requiretty\n > /etc/sudoers.d/888-dont-requiretty'")

        path, filename = os.path.split("../configs/sysctl.conf.gpdb")
        print os.getcwd()

        #FIX THESE PUTS!!   THEY ARE PUTTING THIS STRING IN instead of the file

        print sshClient.put("/tmp/sysctl.conf.gpdb","../configs/sysctl.conf.gpdb")
        print sshClient.put("/tmp/limits.conf.gpdb","../configs/limits.conf.gpdb")
        print sshClient.run("sudo sh -c 'cat /tmp/sysctl.conf.gpdb >> /etc/sysctl.conf'")
        print sshClient.run("sudo sh -c 'cat /tmp/limits.conf.gpdb >> /etc/security/limits.conf'")



    pprint.pprint(clusterNodes)


 #
# "shell", inline: "cat '/vagrant/configs/sysctl.conf.gpdb' >> /etc/sysctl.conf"
# node.vm.provision
# "shell", inline: "cat '/vagrant/configs/limits.conf.gpdb' >> /etc/security/limits.conf"
#
# node.vm.provision
# "shell", path: "../scripts/prepareHost.sh"




def destroyCluster(clusterDictionary):
    clusterPath = "./" + clusterDictionary["clustername"]
    os.chdir(clusterPath)
    v = vagrant.Vagrant(quiet_stdout=True)
    v.destroy()
    os.chdir("../.")
    shutil.rmtree(clusterPath)




if __name__ == '__main__':
    cliParse()
