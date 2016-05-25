#!/usr/bin/env bash

# This script runs as personal user with sudo privs.

GPZIP=greenplum-db-4.3.8.2-build-1-RHEL5-x86_64.zip

gpadminUser(){
    echo "Setting password for GPADMIN on all Nodes"
    sudo echo "gpadmin"|sudo passwd --stdin gpadmin

}

setSystemParams(){
    echo "Setting System Params"

}

securityChanges(){
    echo "Disabling IP Tables"
    sudo /etc/init.d/iptables stop
    sudo /sbin/chkconfig iptables off
    echo "Disabling SELinux"
    sudo setenforce 0
    sudo sed -i "s/SELINUX=enforcing/SELINUX=disabled/" /etc/selinux/config

}

installGPDBbins(){
    wget https://storage.googleapis.com/pivedu-bins/greenplum-db-4.3.8.2-build-1-RHEL5-x86_64.zip
    #cd /vagrant
    unzip $GPZIP
    GPBIN="${GPZIP%.*}.bin"
    sed -i 's/more <</cat <</g' ./$GPBIN
    sed -i 's/agreed=/agreed=1/' ./$GPBIN
    sed -i 's/pathVerification=/pathVerification=1/' ./$GPBIN
    sed -i '/defaultInstallPath=/a installPath=${defaultInstallPath}' ./$GPBIN
    sudo ./$GPBIN
}

buildGPHosts(){

    echo "TEST"


}

_main() {
    gpadminUser
    securityChanges
    #installGPDBbins


}


_main "$@"