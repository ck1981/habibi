import uuid

import peewee
from playhouse import db_url

from habibi.utils import crypto

DB_PROXY = peewee.Proxy()

def connect_to_db(url):
    database = db_url.connect(url)
    DB_PROXY.initialize(database)
    for model in SCALR_ENTITIES:
        model.create_table(fail_silently=True)


"""Peewee models for Scalr entities."""

class HabibiModel(peewee.Model):
    class Meta:

        def get_table_name(model):
             return "{}s".format(model.__class__.__name__.lower())

        database = DB_PROXY
        db_table_func = get_table_name

class Farm(HabibiModel):
    name = peewee.CharField(unique=True)
    farm_crypto_key = peewee.CharField()
    status = peewee.CharField(default='terminated',
                              choices=(('running', 'Running'), ('terminated', 'Terminated')))

    '''
    def add_farm_role(self, role, orchestration=None):
        if isinstance(role, int):
            role = Role.get(id=role)
        return FarmRole.create(farm=self, role=role, orchestration=orchestration)

    @property
    def started(self):
        return self.status == 'running'

    def remove_role(self, role):
        if role not in self.roles:
            raise Exception('Role %s not found' % role.name)
        self.roles.remove(role)
        for server in self.servers(role_name=role.name):
            try:
                server.destroy()
            except:
                LOG.debug('Failed to terminate server %s' % server.id, exc_info=sys.exc_info())

    def start(self):
        if not os.path.isdir(self.base_dir):
            os.makedirs(self.base_dir)
        self.update(status='running').execute()

    def stop(self):
        self.update(status='terminated').execute()

    def servers(self, server_id=None, role_name=None, **kwds):
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

        if server_id is not None:
            search_res = filter(lambda x: x.id == server_id, search_res)

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
    '''

class Role(HabibiModel):
    name = peewee.CharField(unique=True)
    image = peewee.CharField()
    behaviors = peewee.CharField()


class FarmRole(HabibiModel):
    farm = peewee.ForeignKeyField(Farm, related_name='farm_roles')
    role = peewee.ForeignKeyField(Role)
    orchestration = peewee.TextField()

'''
    def _next_server_index(self):
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
                        behaviors=self.role.behaviors,
                        index=self._next_server_index(),
                        zone=zone,
                        role=self,
                        host_machine=THIS_MACHINE,
                        volumes=volumes)
        server.save()

        self.update(add_to_set__servers=server)
        self.reload()
        return server
'''

class Server(HabibiModel):

    index = peewee.IntegerField()
    zone = peewee.CharField()
    farm_role = peewee.ForeignKeyField(FarmRole, related_name='servers')
    public_ip = peewee.CharField()
    private_ip = peewee.CharField()
    crypto_key = peewee.CharField()
    host_machine = peewee.CharField()
    container_id = peewee.CharField()
    volumes = peewee.TextField()
    status = peewee.CharField(default='pending launch',
                choices=(('running', 'Running'), ('pending launch', 'Pending launch'),
                    ('pending', 'Pending'),('initializing', 'Initializing'),
                    ('pending terminate', 'Pending terminate'),
                    ('terminated', 'Terminated')))

    def __init__(self, farm_role=None, id=None, index=0, crypto_key=None, farm_hash=None,
                 public_ip=None, private_ip=None, status='pending launch', zone=None,
                 host_machine=None, volumes=None, **kwargs):
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

'''
    def run(self, cmd, env, cwd=None):
        """Run server as prepared docker image

        :param cmd: command to run into container

        """

        self.reload(max_depth=5)
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
        run_cmd = list(map(str, run_cmd))
        lxc_start = subprocess.Popen(" ".join(run_cmd),
                                     shell=True,
                                     cwd=server_dir,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = lxc_start.communicate()

        if lxc_start.returncode:
            self.update(set__status='terminated')
            self.reload()
            raise Exception('Container start or provisioning failed. '
                    'ret code: {retcode}\nSTDERR: {stderr}\nSTDOUT: {stdout}' .format(retcode=lxc_start.returncode, stderr=stderr, stdout=stdout))
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
'''

'''
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
'''

class Event(HabibiModel):
    class Meta:
        db_table = 'events'

    name = peewee.CharField()
    event_id = peewee.CharField()
    triggering_server = peewee.ForeignKeyField(Server, related_name='sent_events')

    def __init__(self, id=None, triggering_server=None, name=None, **kwargs):
        if id is None:
            id = str(uuid.uuid4())
        super(Event, self).__init__(id=id, triggering_server=triggering_server, name=name, **kwargs)



class GlobalVariable(HabibiModel):
    scopes = ('farm', 'role', 'farm_role', 'server')
    name = peewee.CharField()
    scopes = peewee.CharField()

SCALR_ENTITIES = (Farm, Role, FarmRole, Server, Event, GlobalVariable)
