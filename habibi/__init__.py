# pylint: disable=R0902, W0613, R0913, R0914, R0201, R0904

"""
Habibi is a testing tool which scalarizr team uses to mock scalr's side of communication.
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

import os
import sys
import copy
import uuid
import random
import socket
import logging
import itertools
import subprocess

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import mongoengine as me

from habibi import crypto

THIS_MACHINE = socket.gethostname()

logging.basicConfig(
        stream=sys.stderr, 
        level=logging.DEBUG, 
        format='%(asctime)s %(name)-20s %(levelname)-8s - %(message)s')
LOG = logging.getLogger('habibi')


ROUTER_IP = '10.0.3.1'
ROUTER_PORT = 10001
DEFAULT_BROKER_URL = 'amqp://guest:guest@localhost:5672//'


def configure(mongo_url=None):
    if not mongo_url:
        mongo_url = os.environ.get("HABIBI_MONGO_URL", "mongodb://localhost:27017/habibi")
    parsed_url = urlparse(mongo_url)
    db_name = os.path.basename(parsed_url.path)
    me.connect(db_name, host=parsed_url.hostname, port=parsed_url.port or 27017,
                        username=parsed_url.username, password=parsed_url.password)


def get_unique_id(name):
    db = me.connection.get_db()
    if not list(db.counters.find(dict(name=name))):
        db.counters.insert(dict(name=name, count=random.randint(1, 999999)))
    res = db.counters.find_and_modify(query=dict(name=name), update={"$inc": {"count": 1}})
    return res["count"]


class Farm(me.Document):

    base_dir = '.habibi'
    id = me.IntField(required=True, unique=True, primary_key=True)
    status = me.StringField(choices=('running', 'terminated'), required=True)
    farm_crypto_key = me.StringField(required=True)
    farm_roles = me.ListField(me.ReferenceField('FarmRole'))
    name = me.StringField(required=True, unique=True)

    def __init__(self, name=name, farm_roles=None, farm_crypto_key=None, status='terminated', id=None, **kwargs):
        if not id:
            id = get_unique_id('farm')
        kwargs2 = dict(
            farm_roles=farm_roles or list(),
            farm_crypto_key=farm_crypto_key or crypto.keygen(),
            status=status,
            id=id
        )
        kwargs.update(kwargs2)
        super(Farm, self).__init__(name=name, **kwargs)


    def add_farm_role(self, role, orchestration=None):
        farm_role = FarmRole(farm=self, role=role, orchestration=orchestration)
        farm_role.save()
        self.update(add_to_set__farm_roles=farm_role)
        self.reload()
        return farm_role

    @property
    def started(self):
        self.reload()
        return self.status == 'running'

    def remove_role(self, role):
        # TODO
        if role not in self.roles:
            raise Exception('Role %s not found' % role.name)
        self.roles.remove(role)
        for server in self.servers(role_name=role.name):
            try:
                server.destroy()
            except:
                LOG.debug('Failed to terminate server %s' % server.id, exc_info=sys.exc_info())

    def start(self):
        """
        Idempotent
        """
        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)
        self.update(set__status='running')
        self.reload()

    def stop(self):
        """
        Idempotent
        """
        self.update(set__status='terminated')
        self.reload()

    def servers(self, sid=None, role_name=None, **kwds):
        """
        @param sid: find server with specified id
        @param role: role name to search servers in
        @param kwds: filter found servers by attribute values (see examples)

        keyword arguments are additional filters for servers, where key is server's attribute name,
        and value either attribute value or validator function, which accepts single argument (attribute value):

            # find pending and initilizing servers across all roles
            servers(status=lambda s: s.status in ('pending', 'initializing'))

            # find server with index=3 in percona55 role
            third = servers(role_name='percona55', index=3)[0]
        """
        self.reload()
        search_res = []
        if role_name is not None:
            for role in self.roles:
                if role.name == role_name:
                    search_res = copy.copy(role.servers)
                    break
        else:
            search_res = list(itertools.chain(*[r.servers for r in self.roles]))

        if sid is not None:
            search_res = filter(lambda x: x.id == sid, search_res)

        if kwds:
            def filter_by_attrs(server):
                for find_attr, find_value in kwds.iteritems():
                    real_value = getattr(server, find_attr)
                    if callable(find_value):
                        if not find_value(real_value):
                            return False
                    else:
                        if real_value != find_value:
                            return False
                else:
                    return True

            search_res = filter(filter_by_attrs, search_res)

        if search_res:
            return ServerSet(search_res)
        else:
            raise LookupError('No servers were found')


    def orchestrate_event(self, event_id):
        pass


class Role(me.Document):

    id = me.IntField(unique=True, required=True, primary_key=True)
    name = me.StringField(unique=True, required=True)
    image = me.StringField(required=True)
    behaviors = me.ListField(me.StringField(), required=True)

    def __init__(self, image=image, behaviors=behaviors, name=name, id=None, **kwargs):
        if not id:
            id = get_unique_id('role')
        kwargs.update(dict(
            behaviors=(isinstance(behaviors, (list, tuple)) and behaviors) or [behaviors],
            id=id,
            image=image,
            name=name
        ))
        super(Role, self).__init__(**kwargs)


class FarmRole(me.Document):

    id = me.IntField(unique=True, required=True, primary_key=True)
    servers = me.ListField(me.ReferenceField('Server'))
    farm = me.ReferenceField(Farm, required=True, reverse_delete_rule=me.NULLIFY)
    role = me.ReferenceField('Role', required=True, dbref=True)
    orchestration = me.DictField()

    def __init__(self, farm=farm, role=role, orchestration=None, servers=None, id=None, **kwargs):
        """
        Creates and saves (or updates) role to mongo backend

        @param force_insert: If set, will raise exception if role with the same name exists in the farm

        @param farm: Farm object (habibi)
        @type farm: Farm
        """
        if not id:
            id = get_unique_id('farmrole')
        kwargs2 = dict(
            id=id,
            orchestration=orchestration or dict(),
            servers=servers or list()
        )
        kwargs.update(kwargs2)
        super(FarmRole, self).__init__(farm=farm, role=role, **kwargs)


    def _next_server_index(self):
        """
        self.reload()
        last_server_index = 1
        for server in self.servers:
            if server.status == 'terminated':
                continue
            if server.index > last_server_index:
                return last_server_index
            last_server_index += 1
        return last_server_index
        """
        return random.randint(1, 10000)

    def _pack_user_data(self, user_data):
        return ';'.join(['{0}={1}'.format(k, v) for k, v in user_data.items()])

    def add_server(self, zone='lxc-zone', volumes=None):
        """
        @param volumes: dirs to share with container, host_path: container_path
        @type volumes: dict
        """
        self.reload()
        if not self.farm.started:
            raise Exception("You should start your farm first.")

        server = Server(farm_role=self,
                        index=self._next_server_index(),
                        zone=zone,
                        role=self,
                        host_machine=THIS_MACHINE,
                        volumes=volumes)
        server.save()

        self.update(add_to_set__servers=server)
        self.reload()
        return server


class Server(me.Document):

    id = me.StringField(min_length=36, max_length=36, required=True, unique=True, primary_key=True)
    index = me.IntField(min_value=0, required=True)
    farm_role = me.ReferenceField(FarmRole)
    crypto_key = me.StringField(min_length=40, max_length=40, required=True)
    farm_hash = me.StringField(min_length=10, max_length=10, required=True)
    public_ip = me.StringField()
    private_ip = me.StringField()
    status = me.StringField(choices=['running', 'pending launch', 'pending', 'initializing', 'pending terminate',
                                     'terminated'], required=True)
    zone = me.StringField(required=True)
    host_machine = me.StringField(required=True)
    container_id = me.StringField()
    volumes = me.DictField()


    def __init__(self,
                 farm_role=None,
                 id=None,
                 index=0,
                 crypto_key=None,
                 farm_hash=None,
                 public_ip=None,
                 private_ip=None,
                 status='pending launch',
                 zone=None,
                 host_machine=None,
                 volumes=None, **kwargs):
        """
        @param role: Role object
        @type role: FarmRole
        """
        kwargs2 = dict(
            id=id or str(uuid.uuid4()),
            index=index,
            farm_role=farm_role,
            crypto_key=crypto_key or crypto.keygen(40),
            farm_hash=farm_hash or crypto.keygen(10),
            public_ip=public_ip,
            private_ip=private_ip,
            status=status,
            zone=zone,
            _rootfs_path=None,
            host_machine=host_machine,
            volumes=volumes or dict()
        )
        kwargs.update(kwargs2)
        super(Server, self).__init__(**kwargs)


    """
    @property
    def rootfs_path(self):
        if not self._rootfs_path:
            lxc_containers_path = '/var/lib/lxc'

            # REVIEW: why loop? server-id -> lxc mapping is 1-1
            # Container directory name and server id are not the same. We need to find
            # exact path using loop or glob.
            # Rewrited with glob
            try:
                server_dir_path = glob.glob(os.path.join(lxc_containers_path, str(self.id) + '*'))[0]
                self._rootfs_path = os.path.join(server_dir_path, 'rootfs')
            except KeyError:
                raise BaseException("Can't find server with id: %s" % self.id)
                
        return self._rootfs_path
    """

    def run(self, cmd, env, cwd=None):
        """
        Run server as prepared docker image
        @param cmd: command to run into container
        """

        self.reload(max_depth=5)
        """
        env = dict(
            FAM_AGENT_CFG=json.dumps(dict(SERVER_ID=self.server_id)),
            FAM_CELERY_CFG=json.dumps(dict(BROKER_URL=broker_url))
        )
        """
        server_dir = os.path.join(self.farm_role.farm.base_dir, self.id)
        if not os.path.isdir(server_dir):
            os.makedirs(server_dir)
        run_cmd = ['docker', 'run', '-t', '-i', '-d', '--name=%s' % self.id]
        for k, v in self.volumes.items():
            run_cmd.extend(['-v', '%s:%s:rw' % (k, v)])
        for k, v in env.items():
            run_cmd.extend(['-e', "%s='%s'" % (k, v)])
        if cwd:
            run_cmd.extend(['-w', cwd])
        run_cmd.extend([self.farm_role.role.image, cmd])
        run_cmd = map(str, run_cmd)
        lxc_start = subprocess.Popen(" ".join(run_cmd),
                                     shell=True,
                                     cwd=server_dir,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = lxc_start.communicate()

        if lxc_start.returncode:
            self.update(set__status='terminated')
            self.reload()
            raise Exception('Container start or provisioning failed. ret code: %s' % lxc_start.returncode)
        else:
            self.update(set__status='pending', set__container_id=stdout.strip())
            self.reload()

    def terminate(self):
        self.reload()
        if self.status != 'terminated':
            if self.host_machine != THIS_MACHINE:
                raise Exception('Server %s was started on another host machine' % self.id)
            subprocess.call('docker kill %s' % self.container_id, shell=True)
            subprocess.call('docker rm %s' % self.container_id, shell=True)
            self.status = 'terminated'
            self.save()

    def stop(self):
        p = subprocess.Popen('vagrant halt', shell=True, cwd=self.server_dir)
        p.communicate()

    def get_output(self):
        self.reload()
        if self.status != 'pending launch':
            p = subprocess.Popen(('docker', 'logs', self.container_id), shell=True, stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout = p.communicate()[0]
            return stdout.strip()
        else:
            raise Exception('Server hasn ot been started yet')

    def wait(self):
        p = subprocess.Popen(('docker', 'wait', self.container_id), shell=True, stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p.communicate()

    @property
    def server_dir(self):
        return os.path.join(self.farm_role.farm.base_dir, self.id)


class ServerSet(object):
    """
    Represents list of servers. Every action on this object will be performed consequently.
    Also supports iteration, if you want to get one server or subset:

        s = ServerSet([server1, server2, server3])
        s.block_network() # kill network to all servers in set
        s.terminate() # terminate all servers in set

        s[0] # first server in set
        s[:2] # ServerSet object with first and second servers of current ServerSet
    """

    def __init__(self, servers):
        self._servers = servers

    def __getattr__(self, item):
        return self._wrapper(self._servers, item)

    def __iter__(self):
        for server in self._servers:
            yield server

    def __getitem__(self, item):
        if not isinstance(item, (int, slice)):
            raise TypeError('Indicies must be of int type, not %s' % type(item))
        ret = self._servers[item]
        if isinstance(ret, list):
            return ServerSet(ret)
        else:
            return ret

    class _wrapper(object):

        def __init__(self, servers, attr):
            self.attr_name = attr
            self.servers = servers

        def __call__(self, *args, **kwargs):
            ret = []
            for server in self.servers:
                attr = getattr(server, self.attr_name)
                ret.append(attr(*args, **kwargs))
            return ret


class Event(me.Document):
    id = me.StringField(primary_key=True, min_length=36, max_length=36)
    triggering_server = me.ReferenceField(Server)
    name = me.StringField(required=True)

    def __init__(self, id=None, triggering_server=None, name=None, **kwargs):
        if id is None:
            id = str(uuid.uuid4())
        super(Event, self).__init__(id=id, triggering_server=triggering_server, name=name, **kwargs)



class GlobalVariable(me.Document):
    name = me.StringField(primary_key=True, required=True, min_length=1)
    scopes = me.DictField()
