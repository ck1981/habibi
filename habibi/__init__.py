# pylint: disable=R0902, W0613, R0913, R0914, R0201, R0904
"""Habibi is a testing tool which scalarizr team uses to mock scalr's side of communication.
It allowes to test scalarizr behavior on real virtual machines,
without implementing it by scalr team. Habibi uses lxc containers as instances, which
makes habibi tests incredibly fast and totally free (no cloud providers involved).

Habibi consists of several modules:

- Habibi - represents scalr farm, could contain zero or more Roles.
- Storage - persistent storage service, based on lvm. Replaces EBS' and similar services.
- Events system, which connects framework parts and test code together.

Prerequisites:
    Ubuntu 12.04 or higher as host machine
    lvm2


Howto:
farm = habibi.Habibi(testfarm)
farm.save()
role = farm.add_role('myrole', ['app','www'])

farm.start()

server = role.add_server(volumes={'/host/path': '/guest/path'})
server.run(broker_url='amqp://real/url', cmd='python /path/to/mounted/fatmouse/app.py')
print server.status # => "pending"

server.terminate() # Kills docker container

habibi.Habibi.objects # => [<Farm: testfarm>], finds all farms
habibi.FarmRole.objects # => [<FarmRole: myrole>]

habibi.Server.objects(server_id='1b15db6e-6369-4a19-9aee-abccde1c1d75') # => [<Server: ...>], filter search results
"""
