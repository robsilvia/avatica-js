# Avatica JS
JavaScript connector to [Calcite Avatica Server](https://calcite.apache.org/avatica/)

Missing Avatica API Features:
- There is no support for array/component columns
- There is no support for BigInteger

Implementaiton Notes:
- There is no way to configure frame batch sizes, hardcoded to 100 at the moment
- The mapping algorithm prefers strings to byte arrays

### What is this?

Avatica JS is a connection wrapper around, [Calcite Avatica Server](https://calcite.apache.org/avatica/). Which itself provides an api for remote jdbc datasources. Effectively Avatica JS lets you use any java datasource as a javascript datasources.

### Disclaimer

I wrote this because it looks neat. There are no tests, and I am not currently using this in any live setting. Use at your own risk.

### Example
```
const {StatementParameter,ConnectionFactory} = require('avaticajs');

const factory = new ConnectionFactory('https://avatica-host/', "user", "pass")

factory.connect()
    .then(conn => {
        function printResults(resultSet) {
            console.dir(resultSet, { depth: null })
        }

        function dbInfo() {
            return conn.dbInfo().then(printResults)
        }

        function listTableTypes() {
            return conn.tableTypes().then(printResults)
        }

        function listCatalogs() {
            return conn.catelogs().then(printResults)
        }

        function listSchemas() {
            return conn.schemas({catalog: "test"}).then(printResults)
        }

        function listTables() {
            return conn.tables({catalog: "testdb", schema: "test"}).then(printResults)
        }

        function listColumns() {
            return conn.columns({catalog: "testdb", schema: "test", table: "user"}).then(printResults)
        }

        function query() {
            return conn.query("select * from testdb.test.user").then(printResults)
        }

        function execute() {
            return conn.execute("select * from testdb.test.user where user_id = ?" , [StatementParameter.str("REID")]).then(printResults)
        }

        dbInfo()
            .then(listTableTypes)
            .then(listCatalogs)
            .then(listSchemas)
            .then(listTables)
            .then(listColumns)
            .then(query)
            .then(execute)
            .then(() => {return conn.close()})
            .catch(err => {
                conn.close()
                throw err
            })
    })
    .catch(err => {
        console.log("Got error: ", err)
    })
```
