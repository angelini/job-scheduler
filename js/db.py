import psycopg2

from js.types import Change, Dataset, Execution, Job, Relation, Timer


def create_cursor():
    conn = psycopg2.connect('dbname=schedule user=alexangelini')
    conn.autocommit = True
    return conn.cursor()


def reset(cur, drop=False):
    command = 'DROP TABLE IF EXISTS' if drop else 'TRUNCATE TABLE'
    for table in ('timers', 'executions', 'changes', 'relations', 'jobs', 'datasets'):
        cur.execute('{} {} CASCADE;'.format(command, table))
        if not drop:
            cur.execute('ALTER SEQUENCE {}_id_seq RESTART WITH 1;'.format(table))

    if drop:
        for t in ('STORE', 'STATUS'):
            cur.execute('DROP TYPE IF EXISTS {} CASCADE'.format(t))


def _wheres_to_predicate_and_values(wheres):
    items = wheres.items()
    values = tuple(i[1][1] if isinstance(i[1], tuple) else i[1] for i in items)

    def format_item(item):
        name, value = item
        if isinstance(value, tuple):
            return '{} {} %s'.format(name, value[0])
        else:
            return '{} = %s'.format(name)

    predicate = ' AND '.join(map(format_item, items))

    if predicate:
        predicate = 'WHERE ' + predicate

    return (predicate, values)


def _load(cur, clazz, wheres):
    table = clazz.__name__.lower() + 's'
    columns = clazz._fields
    column_names = ', '.join(columns)

    where_predicate, where_values = _wheres_to_predicate_and_values(wheres)

    cur.execute('''
        SELECT {}
        FROM {}
        {};
    '''.format(column_names, table, where_predicate), where_values)
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


def load_timers(cur, **kwargs):
    return _load(cur, Timer, kwargs)


def create_timer(cur, new_timer):
    _create(cur, Timer, new_timer)


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


def update_timer_status(cur, id, status):
    cur.execute('''
        UPDATE timers
        SET status = %s
        WHERE id = %s
    ''', (status, id))


def _pop(cur, clazz, wheres):
    table = clazz.__name__.lower() + 's'
    columns = clazz._fields
    column_names = ', '.join(columns)

    wheres['status'] = 'pending'
    where_predicate, where_values = _wheres_to_predicate_and_values(wheres)

    cur.execute('''
        UPDATE {}
        SET status = 'running'
        WHERE id IN (
            SELECT id
            FROM {}
            {}
            ORDER BY created_at
            LIMIT 1
        )
        RETURNING {}
    '''.format(table, table, where_predicate, column_names),
        where_values)

    results = cur.fetchone()
    return clazz(*results) if results else None


def pop_change(cur):
    return _pop(cur, Change, {})


def pop_execution(cur):
    return _pop(cur, Execution, {})


def pop_valid_timer(cur, now):
    return _pop(cur, Timer, {'start': ('<=', now)})


def previous_successful_execution(cur, current_exec):
    cur.execute('''
        SELECT {}
        FROM executions
        WHERE ds_id = %s
        AND created_at < %s
        AND status = 'successful'
    '''.format(', '.join(Execution._fields)),
        (current_exec.ds_id, current_exec.created_at))

    previous = cur.fetchone()
    return Execution(*previous) if previous else None
