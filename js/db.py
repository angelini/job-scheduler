import psycopg2

from js.types import Change, Dataset, Execution, Job, Relation


def create_cursor():
    conn = psycopg2.connect('dbname=schedule user=alexangelini')
    conn.autocommit = True
    return conn.cursor()


def reset(cur, drop=False):
    command = 'DROP TABLE IF EXISTS' if drop else 'TRUNCATE TABLE'
    for table in ('executions', 'changes', 'relations', 'jobs', 'datasets'):
        cur.execute('{} {} CASCADE;'.format(command, table))
        if not drop:
            cur.execute('ALTER SEQUENCE {}_id_seq RESTART WITH 1;'.format(table))

    if drop:
        for t in ('STORE', 'STATUS'):
            cur.execute('DROP TYPE IF EXISTS {}'.format(t))


def _load(cur, clazz, wheres):
    table = clazz.__name__.lower() + 's'
    columns = clazz._fields
    column_names = ', '.join(columns)

    where_items = wheres.items()
    where_values = tuple(i[1] for i in where_items)

    where_predicates = ' AND '.join(
        map(lambda t: '{} = %s'.format(t[0]), where_items))

    if where_predicates:
        where_predicates = "WHERE " + where_predicates

    cur.execute('''
        SELECT {}
        FROM {} {};
    '''.format(column_names, table, where_predicates), where_values)
    return [clazz(*row) for row in cur.fetchall()]


def _create(cur, clazz, instance):
    assert instance.id is None

    table = clazz.__name__.lower() + 's'
    columns = clazz._fields[1:]
    column_names = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))

    cur.execute(
        'INSERT INTO {} ({}) VALUES ({});'.format(table, column_names, placeholders),
        tuple(getattr(instance, k) for k in columns))


def load_datasets(cur, **kwargs):
    return _load(cur, Dataset, kwargs)


def create_dataset(cur, new_dataset):
    _create(cur, Dataset, new_dataset)


def load_jobs(cur, **kwargs):
    return _load(cur, Job, kwargs)


def create_job(cur, new_job):
    _create(cur, Job, new_job)


def load_relations(cur, **kwargs):
    return _load(cur, Relation, kwargs)


def create_relation(cur, new_relation):
    _create(cur, Relation, new_relation)


def load_changes(cur, **kwargs):
    return _load(cur, Change, kwargs)


def create_change(cur, new_change):
    _create(cur, Change, new_change)


def load_executions(cur, **kwargs):
    return _load(cur, Execution, kwargs)


def create_execution(cur, new_execution):
    _create(cur, Execution, new_execution)


def load_root_datasets(cur):
    cur.execute('''
        SELECT {}
        FROM datasets
        WHERE id NOT IN (
            SELECT distinct(to_id)
            FROM relations
        );
    '''.format(', '.join(Dataset._fields)))
    return [Dataset(*row) for row in cur.fetchall()]


def load_parent_datasets(cur, ds_id):
    cur.execute('''
        SELECT {}
        FROM datasets
        WHERE id IN (
            SELECT from_id
            FROM relations
            WHERE to_id = %s
        )
    '''.format(', '.join(Dataset._fields)), (ds_id,))
    return [Dataset(*row) for row in cur.fetchall()]


def load_children_datasets(cur, ds_id):
    cur.execute('''
        SELECT {}
        FROM datasets
        WHERE id IN (
            SELECT to_id
            FROM relations
            WHERE from_id = %s
        )
    '''.format(', '.join(Dataset._fields)), (ds_id,))
    return [Dataset(*row) for row in cur.fetchall()]


def update_dataset_stop(cur, ds_id, stop):
    cur.execute('''
        UPDATE datasets
        SET stop = %s
        WHERE id = %s
    ''', (stop, ds_id))


def update_execution_status(cur, id, status, skip_reason=None):
    cur.execute('''
        UPDATE executions
        SET status = %s, skip_reason = %s
        WHERE id = %s
    ''', (status, skip_reason, id))


def update_change_status(cur, id, status):
    cur.execute('''
        UPDATE changes
        SET status = %s
        WHERE id = %s
    ''', (status, id))


def _pop(cur, clazz):
    table = clazz.__name__.lower() + 's'
    columns = clazz._fields
    column_names = ', '.join(columns)

    cur.execute('''
        UPDATE {}
        SET status = 'running'
        WHERE id IN (
            SELECT id
            FROM {}
            WHERE status = 'pending'
            ORDER BY created_at
            LIMIT 1
        )
        RETURNING {}
    '''.format(table, table, column_names))

    results = cur.fetchone()
    return clazz(*results) if results else None


def pop_change(cur):
    return _pop(cur, Change)


def pop_execution(cur):
    return _pop(cur, Execution)
