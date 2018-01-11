# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure("2") do |config|

  config.vm.define "zendirVM", primary: true do |zendirVM|

    zendirVM.vm.box = "bento/centos-7.4"
    zendirVM.vm.hostname = 'zendirVM'

    zendirVM.vm.network :private_network, ip: "192.168.56.56"

    zendirVM.vm.synced_folder "/Users/kristopherfield", "/mnt/localdirectory", type: "virtualbox"

    zendirVM.vm.provider :virtualbox do |v|
      v.customize ["modifyvm", :id, "--natdnshostresolver1", "on"]
      v.customize ['modifyvm', :id, '--cableconnected1', 'on']
      v.customize ["modifyvm", :id, "--memory", 4096 ]
      v.customize ["modifyvm", :id, "--cpus", 2 ]
      v.customize ["modifyvm", :id, "--name", "zendirVM"]
    end

    #
    # Bootstrap meza on the controlling VM
    #
    zendirVM.vm.provision "getmeza", type: "shell", preserve_order: true, inline: <<-SHELL

      echo "Yum installs"
      yum install -y epel-release
      yum install -y git ansible libselinux-python
      ansible-galaxy install geerlingguy.mysql
      ansible-playbook /vagrant/setup.yml
      python /vagrant/setup-db.py

      # bash /opt/meza/src/scripts/getmeza.sh
      # rm -rf /opt/conf-meza/users/meza-ansible/.ssh/id_rsa
      # rm -rf /opt/conf-meza/users/meza-ansible/.ssh/id_rsa.pub
      # mv /tmp/meza-ansible.id_rsa /opt/conf-meza/users/meza-ansible/.ssh/id_rsa
      # mv /tmp/meza-ansible.id_rsa.pub /opt/conf-meza/users/meza-ansible/.ssh/id_rsa.pub

      # chmod 600 /opt/conf-meza/users/meza-ansible/.ssh/id_rsa
      # chown meza-ansible:meza-ansible /opt/conf-meza/users/meza-ansible/.ssh/id_rsa
      # chmod 644 /opt/conf-meza/users/meza-ansible/.ssh/id_rsa.pub
      # chown meza-ansible:meza-ansible /opt/conf-meza/users/meza-ansible/.ssh/id_rsa.pub

      # cat /opt/conf-meza/users/meza-ansible/.ssh/id_rsa.pub >> /opt/conf-meza/users/meza-ansible/.ssh/authorized_keys
    SHELL


  end

end
