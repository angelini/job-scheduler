from datetime import timedelta
import networkx as nx

from js import db, stream
from js.generator import gen_dataset, gen_job, gen_relationships, gen_changes, now_seconds
from js.types import Change, Execution, Timer

MINIMUM_WAIT = timedelta(seconds=30)


def _p(prefix, template, obj):
    print('{}\t'.format(prefix + '\t' if len(prefix) < 8 else prefix),
          template.format(**obj._asdict()))


def p_job(prefix, job):
    _p(prefix, 'job({id}, {ds_id}, {path})', job)


def p_change(prefix, change):
    _p(prefix, 'change({id}, {ds_id}, {status}, {start} - {stop})', change)


def p_execution(prefix, execution):
    _p(prefix, 'execution({id}, {ds_id}, {status}, {skip_reason})', execution)


def p_timer(prefix, timer):
    _p(prefix, 'timer({id}, {ds_id}, {status}, {start})', timer)


def process_change(cur, change):
    p_change('run', change)
    db.update_dataset_stop(cur, change.ds_id, change.stop)

    for child in db.load_children_datasets(cur, change.ds_id):
        db.create_execution(
            cur,
            Execution(None, child.id, 'pending', now_seconds(), None))

    db.update_change_status(cur, change.id, 'successful')


def process_timer(cur, timer):
    p_timer('run', timer)
    db.create_execution(
        cur,
        Execution(None, timer.ds_id, 'pending', now_seconds(), None))

    db.update_timer_status(cur, timer.id, 'successful')


def should_skip(dataset, motm, previous_execution):
    if dataset.stop > motm:
        return 'MOTM earlier than current stop'

    if previous_execution and previous_execution.created_at + MINIMUM_WAIT > now_seconds():
        db.create_timer(
            cur,
            Timer(None, dataset.id, 'pending', previous_execution.created_at + MINIMUM_WAIT, now_seconds()))
        return 'Previous execution within the minimum wait time'

    return False


def start_execution(cur, execution):
    p_execution('  run', execution)
    db.update_execution_status(cur, execution.id, 'running')

    dataset = db.load_datasets(cur, id=execution.ds_id)[0]
    inputs = db.load_parent_datasets(cur, dataset.id)
    previous_execution = db.previous_successful_execution(cur, execution)

    assert inputs

    motm = None
    for input in inputs:
        if not motm:
            motm = input.stop
        elif input.stop < motm:
            motm = input.stop

    skip_reason = should_skip(dataset, motm, previous_execution)

    if skip_reason:
        db.update_execution_status(cur, execution.id, 'skipped', skip_reason=skip_reason)
        p_execution('  skip', db.load_executions(cur, id=execution.id)[0])
    else:
        job = db.load_jobs(cur, ds_id=dataset.id)[0]
        p_job('    launch', job)
        db.create_change(
            cur,
            Change(None, dataset.id, 'pending', dataset.stop, motm, now_seconds()))

        db.update_execution_status(cur, execution.id, 'successful')
        p_execution('  success', db.load_executions(cur, id=execution.id)[0])


def generate_initial_state(cur):
    for _ in range(10):
        db.create_dataset(cur, gen_dataset())
    datasets = db.load_datasets(cur)

    for i in range(10):
        db.create_job(cur, gen_job(datasets[i].id))

    set(map(lambda r: db.create_relation(cur, r), gen_relationships(datasets)))


def pull_new_changes(cur, consumer):
    changes = stream.poll_for_changes(consumer)
    set(map(lambda c: db.create_change(cur, c), changes))


def process_pending_executions(cur):
    while True:
        oldest_pending_execution = db.pop_execution(cur)
        if not oldest_pending_execution:
            return

        start_execution(cur, oldest_pending_execution)


def process_pending_changes(cur, start_executions=True):
    while True:
        oldest_pending_change = db.pop_change(cur)
        if not oldest_pending_change:
            return

        process_change(cur, oldest_pending_change)
        if start_executions:
            process_pending_executions(cur)


def process_pending_timers(cur, start_executions=True):
    while True:
        oldest_valid_timer = db.pop_valid_timer(cur, now_seconds())
        if not oldest_valid_timer:
            return

        process_timer(cur, oldest_valid_timer)
        if start_executions:
            process_pending_executions(cur)


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


if __name__ == '__main__':
    cur = db.create_cursor()
    consumer = stream.create_consumer()
    producer = stream.create_producer()

    db.reset(cur)
    generate_initial_state(cur)

    graph = build_relations_graph(db.load_datasets(cur), db.load_relations(cur))
    print_graph(graph)
    assert nx.algorithms.dag.is_directed_acyclic_graph(graph)

    root_datasets = db.load_root_datasets(cur)
    changes = gen_changes(root_datasets)
    stream.push_changes(producer, changes)

    pull_new_changes(cur, consumer)
    process_pending_changes(cur)
