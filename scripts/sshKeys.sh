#!/usr/bin/env bash

# This script runs as gpadmin

createKey(){
    echo "Creating Public/Private KeyPair for User: gpdmin"
    ssh-keygen -t rsa -f ~/.ssh/gpadmin.key -N ''
    echo $1
    echo $2
    # Build a hosts file from the hosts file


}


shareKey(){
    echo "Sharing Public Key with all cluster nodes"



}






_main() {
    createKey
    #shareKey

}


_main "$@"