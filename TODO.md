
# TODO

## Good Things TODO:
* Support more plain types.
* Add @sql_str so you could do something like `sql"select * from x where stuff=$unsafe_user_input"`.
* Add macro for adding custom type handling.
* Parallelize column fetching.
* Type rows with something like *NamedTuple*.
* Add more tests.

## Things TODO:
* Add support for Postgres arrays.
* Add support for composite types.
* Support some kind of DBI interface.

## Things to Shrug Indifferently At:
* Supporting Prepared statements.
    Postgres supports prepared statements at the sql level.
    ```PREPARE fooplan (int, text, bool, numeric) AS
        INSERT INTO foo VALUES($1, $2, $3, $4);
        EXECUTE fooplan(1, 'Hunter Valley', 't', 200.00);
    ```
* Libpq event interface.
    Not sure if that many people really use it.
* Implementing a copyfrom function.
    Best would probably be to dump to a csv file and fetch it from there.

## Things to Run Screaming Away from:
* Interfaces that force you to prepare your query before you execute it.
* Command interface is different from select one.
* Spelling Postgres as PostgreSQL.

