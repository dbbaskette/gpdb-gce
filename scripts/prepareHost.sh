#!/usr/bin/env bash

# This script runs as personal user with sudo privs.

GPZIP=greenplum-db-4.3.8.2-build-1-RHEL5-x86_64.zip

userSetup(){
    echo "Setting password for GPADMIN on all Nodes"
    sudo echo "p1v0tal"|sudo passwd --stdin gpadmin

}


setupDisk(){
echo "Setup Disk"
sudo yum -y install xfsprogs xfsdump
sudo fdisk /dev/sdb <<EOF
n
p
1
1

w
EOF
sudo mkfs.xfs -f /dev/sdb1 -L gpdbdata
#sudo e2label /dev/sdb1 gpdbdata
sudo mkdir /data
sudo sh -c "echo 'LABEL=gpdbdata /data xfs rw,noatime,inode64,allocsize=16m 0 0' >> /etc/fstab"

}

securitySetup(){
    echo "Disabling IP Tables"
    sudo /etc/init.d/iptables stop
    sudo /sbin/chkconfig iptables off
    echo "Disabling SELinux"
    sudo setenforce 0
    sudo sed -i "s/SELINUX=enforcing/SELINUX=disabled/" /etc/selinux/config
    sudo service sshd reload

}

networkSetup(){
    sudo sed -i 's|[#]*PasswordAuthentication no|PasswordAuthentication yes|g' /etc/ssh/sshd_config
    sudo sed -i 's|UsePAM no|UsePAM yes|g' /etc/ssh/sshd_config
    #sudo sh -c "echo 'Defaults \!requiretty' > /etc/sudoers.d/888-dont-requiretty"
    sudo sh -c "cat '/tmp/sysctl.conf.gpdb' >> /etc/sysctl.conf"
    sudo sh -c "cat '/tmp/limits.conf.gpdb' >> /etc/security/limits.conf"
}


installGPDBbins(){
    wget https://storage.googleapis.com/pivedu-bins/$GPZIP
    unzip $GPZIP
    GPBIN="${GPZIP%.*}.bin"
    sed -i 's/more <</cat <</g' ./$GPBIN
    sed -i 's/agreed=/agreed=1/' ./$GPBIN
    sed -i 's/pathVerification=/pathVerification=1/' ./$GPBIN
    sed -i '/defaultInstallPath=/a installPath=${defaultInstallPath}' ./$GPBIN
    sudo ./$GPBIN
}



_main() {
    userSetup
    securitySetup
    networkSetup
    installGPDBbins
    setupDisk



}


_main "$@"


# client.exec_command("sudo sed -i 's|[#]*PasswordAuthentication no|PasswordAuthentication yes|g' /etc/ssh/sshd_config")
#        client.exec_command("sudo sed -i 's|UsePAM no|UsePAM yes|g' /etc/ssh/sshd_config")
#        client.exec_command("sudo sh -c 'echo Defaults !requiretty\n > /etc/sudoers.d/888-dont-requiretty'")
#        client.exec_command("sudo sh -c 'cat /tmp/sysctl.conf.gpdb >> /etc/sysctl.conf'")
#        client.exec_command("sudo sh -c 'cat /tmp/limits.conf.gpdb >> /etc/security/limits.conf'")