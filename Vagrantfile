
Vagrant.configure("2") do |config|

 config.vm.define "db" do |app|
   app.vm.provider "docker" do |d|	
     config.vm.boot_timeout = 900
     d.cmd     = ["/sbin/my_init", "--enable-insecure-key"]
     d.build_dir = "manifests/development/db"
     d.has_ssh = true
     d.name = "db-x"
     d.ports = ["6432:5432"]
     #d.volumes = ["data/postgres:/var/lib/postgresql/data", "data/mongodb:/data/db"]
     #d.create_args = ["--add-host", "dockerhost:" + `ip route | awk '/docker0/ { print $NF }'`.strip]
     #d.create_args = ["--add-host", "dockerhost:" + "131.142.152.246"]	
     d.create_args = ["--add-host", "dockerhost:" + "192.168.99.100"]	
   end		    
 end

end
