### Usage

```
# Start Postgres
$ sudo /etc/init.d/postgres start

# Create tables
$ createdb schedule
$ psql -d schedule -f tables.sql

# Start a REPL
$ ipython3 -i js/main.py
```

```python
add_new_changes(cur)
process_pending_changes(cur)
```