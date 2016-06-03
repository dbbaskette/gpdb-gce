import time

import paramiko
from paramiko import WarningPolicy
from passlib.hash import md5_crypt
import warnings
import pprint

BASE_USERNAME="student"
BASE_PASSWORD="student"
BASE_HOME="/data"
NUM_USERS = 4

SSH_USERNAME = "dbaskette"
SSH_KEY_PATH = "/Users/dbaskette/.ssh/google_compute_engine"
KEY = "/Users/dbaskette/Downloads/Pivotal-8b6ceb84c23f.json"
SVC_ACCOUNT = "libcloud@pivotal-1211.iam.gserviceaccount.com"
GPADMIN_PW = "p1v0tal"


def create(clusterInfo):
    print "Creating "+ str(NUM_USERS) + " Users on all Cluster Nodes"
    warnings.simplefilter("ignore")

    for node in clusterInfo["clusterNodes"]:
        print node["nodeName"]+": Creating Users"
        connected = False
        attemptCount = 0
        while not connected:
            try:
                attemptCount += 1
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(WarningPolicy())
                ssh.connect(node["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH, timeout=120)
                for userNum in range(1,NUM_USERS+1):
                    userName = BASE_USERNAME + str(userNum).zfill(2)
                    homeDir = BASE_HOME + "/home"
                    pw = BASE_PASSWORD + str(userNum).zfill(2)
                    (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p " + homeDir+";sudo useradd -b "+homeDir+" -s "+ "/bin/bash -m "+userName)
                    (stdin, stdout, stderr) = ssh.exec_command("sudo sh -c 'echo "+ pw + " | passwd --stdin " + userName+"'")
                    stderr.readlines()
                    stdout.readlines()
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


def moveGPADMIN(clusterInfo):
    print "Moving GPADMIN Home Directory to Data Drive"
    warnings.simplefilter("ignore")

    gpControl(clusterInfo,"stop")

    for node in clusterInfo["clusterNodes"]:
        print node["nodeName"] + ": Moving GPADMIN to "+BASE_HOME+"/home"
        connected = False
        attemptCount = 0
        while not connected:
            try:
                attemptCount += 1


                homeDir = BASE_HOME + "/home"

                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(WarningPolicy())
                ssh.connect(node["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH,
                            timeout=120)
                print "Moving config"
                print "sudo usermod -d " + homeDir + "/gpadmin -m gpaddmin"
                (stdin, stdout, stderr) = ssh.exec_command("sudo usermod -d " + homeDir + "/gpadmin -m gpadmin")
                stderr.readlines()
                stdout.readlines()
                print "Moving Files"
                (stdin, stdout, stderr) = ssh.exec_command("sudo rsync -av /home/gpadmin "+ homeDir)

                ssh.close()

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
    gpControl(clusterInfo,"start")

def gpControl(clusterInfo,action):
    warnings.simplefilter("ignore")
    print "GPDB Controller Initiated: "+action
    for node in clusterInfo["clusterNodes"]:
        connected = False
        attemptCount = 0
        if "master" in node["role"]:
            while not connected:
                try:
                    attemptCount += 1
                    gpssh = paramiko.SSHClient()
                    gpssh.set_missing_host_key_policy(WarningPolicy())
                    gpssh.connect(node["externalIP"], 22, "gpadmin", GPADMIN_PW, pkey=None, key_filename=None,
                                    timeout=120)
                    (stdin, stdout, stderr) = gpssh.exec_command("gp"+action+ " -a")
                    stderr.readlines()
                    #pprint.pprint(stdout.readlines())
                    print stdout.read()
                    connected = True
                except Exception as e:
                    print node["nodeName"] + ": Attempting SSH Connection"
                    time.sleep(3)
                    if attemptCount > 40:
                        print "Failing Process"
                        exit()
                finally:
                    gpssh.close()
