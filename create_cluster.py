#!/usr/bin/env python3
import os
import json
import yaml


CLUSTER_INSTANCE_FILENAME = 'all.json'

CLUSTER_ROLE_CONFIG = 'configsvr'
CLUSTER_ROLE_SHARD = 'shardsvr'
CLUSTER_ROLE_MONGOS = 'mongossvr'


class CfgField(object):
    def to_dict(self):
       res = {}
       for key, val in self.__dict__.items():
          if not val:
             continue
          res[key] = val
       return res if res else None


class CfgShard(CfgField):
    def __init__(self, role=None, config_db=None):
        self.clusterRole = role
        self.configDb = config_db


class CfgNet(CfgField):
    def __init__(self, bind_ip=None, port=0):
        self.bindIp = bind_ip
        self.port = port


class CfgStorage(CfgField):
    def __init__(self, db_path):
        self.dbPath = db_path


class CfgReplication(CfgField):
    def __init__(self, repl_set_name):
        self.replSetName = repl_set_name


class CfgSystemLog(CfgField):
    def __init__(self, destination=None, path=None, log_append=None):
        self.destination = destination
        self.path = path
        self.logAppend = log_append # type: bool


class MongoCfg(object):
    def __init__(self):
        pass
    
    def is_not_output(self, key_name):
       return key_name.startswith('_')

    def to_dict(self):
       res = {}
       for key, cfg_field in self.__dict__.items():
          if self.is_not_output(key):
             continue

          field_dict = cfg_field.to_dict()
          if field_dict is None:
             continue
          res[key] = field_dict
       return res


class CfgCreatorBase(object):

    ROLE = None

    def __init__(self, config_dict):
        self.db_path = config_dict['dbDir']
        self.log_path = config_dict['logDir']

    def generate(self, **kwargs):
        self.create(**kwargs)

    def _save(self, inst_name, content_dict):
        with open(os.path.join('./etc', inst_name + '.conf'), 'w') as f:
            data = yaml.dump(content_dict)
            f.write(data)

    def create(self, **kwargs):
        raise NotImplementedError


class CreatorConfig(CfgCreatorBase):

    ROLE = CLUSTER_ROLE_CONFIG

    def create(self, **kwargs):
        inst_info_dict = kwargs[self.ROLE]
        bind_ip = inst_info_dict['bindIp']
        for i, inst_dict in enumerate(inst_info_dict['instances']):
            cfg = MongoCfg()
            inst_name = '{}{}'.format(self.ROLE, i+1)

            cfg.net = CfgNet(bind_ip=bind_ip, port=inst_dict['port'])
            cfg.storage = CfgStorage(db_path=os.path.join(self.log_path, inst_name))
            cfg.sharding = CfgShard(role=self.ROLE)
            cfg.replication = CfgReplication(repl_set_name=self.ROLE)
            cfg.systemLog = CfgSystemLog(destination='file',
                                           path=os.path.join(self.log_path, inst_name + '.log'),
                                           log_append=True)
            info = cfg.to_dict()
            self._save(inst_name, info)


class CreatorShard(CfgCreatorBase):

    ROLE = CLUSTER_ROLE_SHARD

    def create(self, **kwargs):
        inst_info_dict = kwargs[self.ROLE]
        bind_ip = inst_info_dict['bindIp']
        for i, inst_dict in enumerate(inst_info_dict['instances']):
            cfg = MongoCfg()
            inst_name = '{}{}'.format(self.ROLE, i+1)

            cfg.sharding = CfgShard(role=self.ROLE)
            cfg.net = CfgNet(bind_ip=bind_ip, port=inst_dict['port'])
            cfg.storage = CfgStorage(db_path=os.path.join(self.log_path, inst_name))
            cfg.replication = CfgReplication(repl_set_name=inst_name)
            cfg.systemLog = CfgSystemLog(destination='file',
                                         path=os.path.join(self.log_path, inst_name + '.log'),
                                         log_append=True)

            info = cfg.to_dict()
            self._save(inst_name, info)


class CreatorMongos(CfgCreatorBase):

    ROLE = CLUSTER_ROLE_MONGOS

    def create(self, **kwargs):
        config_info_dict = kwargs[CreatorConfig.ROLE]
        mongos_info_dict = kwargs[self.ROLE]
        bind_ip = mongos_info_dict['bindIp']
        def _make_url(ip, port):
            return '{bind_ip}:{port}'.format(bind_ip=ip,
                                             port=port)

        config_bind_ip = config_info_dict['bindIp']
        url_list_str = ','.join([_make_url(config_bind_ip, x['port']) for x in config_info_dict['instances']])
        config_db = '{role}/{url_list_str}'.format(role=CreatorConfig.ROLE,
                                               url_list_str=url_list_str)

        for i, inst_dict in enumerate(mongos_info_dict['instances']):
            cfg = MongoCfg()
            inst_name = '{}{}'.format(self.ROLE, i+1)

            cfg.net = CfgNet(bind_ip=bind_ip, port=inst_dict['port'])
            cfg.sharding = CfgShard(config_db=config_db)
            cfg.systemLog = CfgSystemLog(destination='file',
                                         path=os.path.join(self.log_path, inst_name + '.log'),
                                         log_append=True)

            info = cfg.to_dict()
            self._save(inst_name, info)


CREATOR_CLAZZ = {
    CreatorConfig.ROLE: CreatorConfig,
    CreatorShard.ROLE: CreatorShard,
    CreatorMongos.ROLE: CreatorMongos,
}


class ClusterCreator(object):
    CREATORS = {}

    def __init__(self, config_dict):
        for name, clz in CREATOR_CLAZZ.items():
            self.CREATORS[name] = clz(config_dict)


    def check_role_creator(self, instance_dict):
        for role_name in instance_dict.keys():
            if role_name not in self.CREATORS:
                raise ValueError("unrecognize cluster role:%s" % role_name)


    def create_cluster_cfg(self, instance_dict):
        for role_name in instance_dict.keys():
            ctor = self.CREATORS[role_name]
            ctor.generate(**instance_dict)


def main():
    with open(CLUSTER_INSTANCE_FILENAME, 'r') as f:
        d = json.load(f)
        cluster_creator = ClusterCreator(d)
        instance_dict = d['cluster']
        cluster_creator.check_role_creator(instance_dict)
        cluster_creator.create_cluster_cfg(instance_dict)


if __name__ == '__main__':
    main()
