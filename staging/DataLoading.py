import time

import paramiko
from paramiko import WarningPolicy
from passlib.hash import md5_crypt
import warnings
import pprint

BASE_USERNAME="student"
BASE_PASSWORD="student"
BASE_HOME="/data"


SSH_USERNAME = "dbaskette"
SSH_KEY_PATH = "/Users/dbaskette/.ssh/google_compute_engine"
KEY = "/Users/dbaskette/Downloads/Pivotal-8b6ceb84c23f.json"
SVC_ACCOUNT = "libcloud@pivotal-1211.iam.gserviceaccount.com"
GPADMIN_PW = "p1v0tal"
LABS="https://storage.googleapis.com/pivedu-bins/labs_dir_2015_11_13.tar.bz2"




def setup(clusterInfo):
    warnings.simplefilter("ignore")
    print "Setup Data Loading for Student Accounts"
    for node in clusterInfo["clusterNodes"]:
        connected = False
        attemptCount = 0
        if "etl" in node["role"]:
            while not connected:
                try:
                    print "Pulling Data to ETL Node"
                    attemptCount += 1
                    ssh = paramiko.SSHClient()
                    ssh.set_missing_host_key_policy(WarningPolicy())
                    labsDir=BASE_HOME+"/labs"
                    ssh.connect(node["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH,
                                timeout=120)
                    (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p "+labsDir+";sudo curl "+LABS +" | sudo tar -C "+labsDir+" -xjvf -")

                    stderr.readlines()
                    # pprint.pprint(stdout.readlines())
                    print stdout.read()
                    connected = True
                except Exception as e:
                    print node["nodeName"] + ": Attempting SSH Connection"
                    time.sleep(3)
                    if attemptCount > 40:
                        print "Failing Process"
                        exit()
                finally:
                    ssh.close()
