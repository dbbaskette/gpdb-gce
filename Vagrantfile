VAGRANTFILE_API_VERSION = "2"
NODES = 2
#WORKERS = 4
#MASTERS = 2
BASENAME = "gpdb"
CLUSTER_ID = "db1"
PROJECT_ID = "pivotal-1211"
EMAIL = "dbaskette@pivotal.io"
KEY = "/Users/dbaskette/Downloads/Pivotal-8b6ceb84c23f.json"
ZONE = "us-east1-d"
IMAGE = "centos-6-v20160511"
SSH_USERNAME = "dbaskette"
SSH_KEY_PATH = "~/.ssh/google_compute_engine"


if Gem::Version.new(::Vagrant::VERSION) < Gem::Version.new('1.5')
  Vagrant.require_plugin('vagrant-hostmanager')
end


# Modified from original @ https://thisdataguy.com/2015/11/12/vagrant-hostmanager-virtualbox-and-aws/
$cached_addresses = {}
$ip_resolver = proc do |vm, resolving_vm|
  # For aws, we should use private IP on the guests, public IP on the host
  if vm.provider_name == :aws
    if resolving_vm.nil?
      used_name = vm.name.to_s + '--host'
    else
      used_name = vm.name.to_s + '--guest'
    end
  else
    used_name= vm.name.to_s
  end

  if $cached_addresses[used_name].nil?
    if hostname = (vm.ssh_info && vm.ssh_info[:host])

      # getting aws guest ip *for the host*, we want the public IP in that case.
      if vm.provider_name == :google and resolving_vm.nil?
        vm.communicate.execute('curl http://169.254.169.254/latest/meta-data/public-ipv4') do |type, pubip|
          $cached_addresses[used_name] = pubip
        end
      else

        vm.communicate.execute('uname -o') do |type, uname|
          unless uname.downcase.include?('linux')
            warn("Guest for #{vm.name} (#{vm.provider_name}) is not Linux, hostmanager might not find an IP.")
          end
        end

        vm.communicate.execute('hostname --all-ip-addresses') do |type, hostname_i|
          # much easier (but less fun) to work in ruby than sed'ing or perl'ing from shell

          allips = hostname_i.strip().split(' ')

          if allips.size() == 0
            warn("Trying to find out ip for #{vm.name} (#{vm.provider_name}), found none useable: #{allips}.")
          else
            if allips.size() > 1
              warn("Trying to find out ip for #{vm.name} (#{vm.provider_name}), found too many: #{allips} and I cannot choose cleverly. Will select the first one.")
            end
            $cached_addresses[used_name] = allips[0]
          end
        end
      end
    end
  end
  $cached_addresses[used_name]
end


Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
    config.vm.box = "gce"
    config.ssh.pty = true
    config.ssh.forward_agent = false
    config.hostmanager.enabled = false
    config.hostmanager.ip_resolver = $ip_resolver



# Build  Nodes

    (1..NODES).each do |node_num|
        node_name = "#{BASENAME}-#{node_num}-#{CLUSTER_ID}"
        config.vm.define node_name do |node|
            node.vm.provision "shell",inline: "sed -i 's|[#]*PasswordAuthentication no|PasswordAuthentication yes|g' /etc/ssh/sshd_config"
            node.vm.provision "shell",inline: "sed -i 's|UsePAM no|UsePAM yes|g' /etc/ssh/sshd_config"
            node.vm.provision "shell", path: "./scripts/prepareHost.sh"
            node.vm.provision "shell",inline: "/etc/init.d/sshd reload"
            #node.vm.provision :hostmanager do |hm|
            #    hm.manage_host = false
            #    hm.manage_guest = true
            #    hm.ignore_private_ip = false
            #end

            node.vm.provider :google do |google, override|
                google.google_project_id = PROJECT_ID
                google.google_client_email = EMAIL
                google.google_json_key_location = KEY
                google.zone = ZONE
                google.image = IMAGE
                google.instance_ready_timeout = 600
                override.ssh.username = SSH_USERNAME
                override.ssh.private_key_path = SSH_KEY_PATH
                google.name = node_name
                google.machine_type = "n1-standard-1"
                google.metadata = {'type' => 'master'}
                google.tags = ['pivotal', 'edu','master']
            end

  


        end
    end

end


