from datetime import datetime, timedelta
import os
import random
import string

from js.types import Change, Dataset, Execution, Job, Relation, STORES

START = datetime(year=2016, month=1, day=1)

def rand_select(l):
    return l[random.randint(0, len(l) - 1)]


def rand_word():
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(6))


def gen_path(prefix):
    return os.path.join(prefix, rand_word())


def gen_span():
    return (START, START + timedelta(days=random.randint(0, 5)))


def gen_dataset():
    start, stop = gen_span()
    return Dataset(None, rand_select(STORES), gen_path('data'), start, stop)


def gen_job(ds_id):
    return Job(None, ds_id, gen_path('jobs') + '.sh')

def gen_relationships(datasets):
    relations = []

    for i, dataset in enumerate(datasets):
        if i > 0 and random.randint(1, 10) <= 4:
            relations.append((datasets[i - 1].id, dataset.id))
        elif i > 1 and random.randint(1, 10) <= 2:
            relations.append((datasets[i - 1].id, dataset.id))
            relations.append((datasets[i - 2].id, dataset.id))

    return list(map(lambda t: Relation(None, t[0], t[1]), relations))

def gen_changes(datasets):
    changes = []

    for dataset in datasets:
        if random.randint(1, 10) <= 8:
            new_stop = dataset.stop + timedelta(days=random.randint(0, 10))
            changes.append(
                Change(None, dataset.id, 'pending', dataset.stop, new_stop, datetime.now()))

    return changes