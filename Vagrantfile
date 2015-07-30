Vagrant.configure("2") do |config|
    config.berkshelf.enabled = true

    config.vm.box = "opscode-ubuntu-12.04"
    config.vm.box_url = "http://opscode-vm-bento.s3.amazonaws.com/vagrant/virtualbox/opscode_ubuntu-12.04_chef-provisionerless.box"
    # config.omnibus.chef_version = "latest"

    config.vm.provider "virtualbox" do |v|
        v.memory = 1024
    end

    config.vm.provision :shell, :inline => "apt-get update"
end