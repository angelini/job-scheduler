from collections import namedtuple

STORES = ('file', 'hdfs')
STATUSES = ('pending', 'running', 'failed', 'successful')

Dataset = namedtuple('Dataset',
                     ['id', 'store', 'path', 'start', 'stop'])

Job = namedtuple('Job',
                 ['id', 'ds_id', 'path'])

Relation = namedtuple('Relation',
                      ['id', 'from_id', 'to_id'])

Change = namedtuple('Change',
                    ['id', 'ds_id', 'status', 'start', 'stop', 'created_at'])

Execution = namedtuple('Execution',
                       ['id', 'ds_id', 'status', 'created_at'])
