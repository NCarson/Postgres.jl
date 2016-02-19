
# Release

## 0.1.0
* Returns results as *DataFrame*.
* Result interface allows for array, column, or row access.
* Transaction support.
* DataFrame can be dumped to database via *copyto*.
    On-the-fly table creation supported.
* Ctl-C cancels queries at the server.
* *Most* 'plain types' supported by comparable Julia type.
* Automatic support for user-defined enums and domains.
    Enum type columns automatically converted to *PooledDataArray*.
* Custom types easily supported by 'injecting' in and out functions.
* Supports the DBI cursor interface including 'fixed-size' paged cursors.
* Server errors expose full information via PostgresServerError.
