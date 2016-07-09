from datetime import datetime
import networkx as nx
import psycopg2

from js import db
from js.generator import gen_dataset, gen_job, gen_relationships, gen_changes
from js.types import Change, Dataset, Execution, Job, Relation


def process_change(cur, change):
    print('processing:\t{}'.format(change))
    db.update_dataset_stop(cur, change.ds_id, change.stop)

    for child in db.load_children_datasets(cur, change.ds_id):
        db.create_execution(cur,
            Execution(None, child.id, 'pending', datetime.now()))

    db.update_change_status(cur, change.id, 'successful')


def start_execution(cur, execution):
    db.update_execution_status(cur, execution.id, 'running')

    dataset = db.load_datasets(cur, id=execution.ds_id)[0]
    inputs = db.load_parent_datasets(cur, dataset.id)

    assert inputs

    motm = None
    for input in inputs:
        if not motm:
            motm = input.stop
        elif input.stop < motm:
            motm = input.stop

    if dataset.stop < motm:
        job = db.load_jobs(cur, ds_id=dataset.id)[0]
        print('kickoff:\t{}'.format(job))
        db.create_change(cur,
            Change(None, dataset.id, 'pending', dataset.stop, motm, datetime.now()))

    db.update_execution_status(cur, execution.id, 'successful')


def fetch_and_process_change(cur):
    change = db.pop_change(cur)

    if not change:
        return False

    process_change(cur, change)

    execution = db.pop_execution(cur)
    while execution:
        start_execution(cur, execution)
        execution = db.pop_execution(cur)

    return True


def generate_initial_state(cur):
    for _ in range(10):
        db.create_dataset(cur, gen_dataset())
    datasets = db.load_datasets(cur)

    for i in range(10):
        db.create_job(cur, gen_job(datasets[i].id))

    set(map(lambda r: db.create_relation(cur, r), gen_relationships(datasets)))


def add_new_changes(cur):
    root_datasets = db.load_root_datasets(cur)
    set(map(lambda c: db.create_change(cur, c), gen_changes(root_datasets)))


def process_pending_changes(cur):
    while True:
        if not fetch_and_process_change(cur):
            return


def build_relations_graph(datasets, relations):
    graph = nx.DiGraph()

    dataset_ids = set(map(lambda d: d.id, datasets))
    graph.add_nodes_from(dataset_ids)

    for relation in relations:
        graph.add_edge(relation.from_id, relation.to_id)

    return graph


def print_graph(graph):
    print()
    print(nx.nx_agraph.to_agraph(graph), end='')


def reset_db(cur, drop=False):
    command = 'DROP TABLE IF EXISTS' if drop else 'TRUNCATE TABLE'
    for table in ['executions', 'changes', 'relations', 'jobs', 'datasets']:
        cur.execute('{} {} CASCADE;'.format(command, table))
        if not drop:
            cur.execute('ALTER SEQUENCE {}_id_seq RESTART WITH 1;'.format(table))


# ------------------------------------------------------------------------------


conn = psycopg2.connect("dbname=schedule user=ubuntu")
conn.autocommit = True

cur = conn.cursor()

reset_db(cur)
generate_initial_state(cur)

graph = build_relations_graph(db.load_datasets(cur), db.load_relations(cur))
print_graph(graph)
assert nx.algorithms.dag.is_directed_acyclic_graph(graph)

add_new_changes(cur)
process_pending_changes(cur)