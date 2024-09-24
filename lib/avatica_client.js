const axios = require('axios')
const uuid = require('uuid')
const os = require('os')

const {ResultSet, rootModel} = require("./model");


// TODO bytesValue and BigInteger support
// TODO add configuration for determining which value type to retrieve if there are multples (like bytes)

/**
 * Internal client that handles protobuf encoding/decoding and HTTP communication.
 */
class ProtobufClient {
    constructor(url) {
        this._url = url
    }

    static _encode(payload, messageTypeName) {
        const MessageType = rootModel.lookup(messageTypeName)
        const verifyError = MessageType.verify(payload)
        if (verifyError) {
            throw new Error(verifyError)
        }
        return MessageType.encode(payload).finish()
    }

    static _encodeAsWireMessage(payload, messageTypeName) {
        const encodedPayload = ProtobufClient._encode(payload, messageTypeName)
        const WireMessage = rootModel.WireMessage
        const wireMessagePayload = {
            name: `org.apache.calcite.avatica.proto.Requests$${messageTypeName}`,
            wrappedMessage: encodedPayload
        }
        const verifyError = WireMessage.verify(wireMessagePayload)
        if (verifyError) {
            throw new Error(verifyError)
        }
        return WireMessage.encode(wireMessagePayload).finish()
    }

    static _decode(payload, messageTypeName) {
        const MessageType = rootModel.lookup(messageTypeName)
        return MessageType.decode(payload)
    }

    static _decodeWireMessage(payload, messageTypeName) {
        const WireMessage = rootModel.WireMessage
        const decodedWireMessage = WireMessage.decode(payload)
        return ProtobufClient._decode(decodedWireMessage.wrappedMessage, messageTypeName)
    }

    post(messageAsJson, requestMessageTypeName, responseMessageTypeName) {
        const wireMessage = ProtobufClient._encodeAsWireMessage(messageAsJson, requestMessageTypeName, responseMessageTypeName)
        return axios.post(
            this._url,
            wireMessage,
            {
                headers: {
                    'Content-Yype': 'application/x-google-protobuf'
                },
                responseType: 'arraybuffer'
            })
            .then(response => {
                return ProtobufClient._decodeWireMessage(response.data, responseMessageTypeName)
            })
            .catch(err => {
                const errorResponse = ProtobufClient._decodeWireMessage(err.response.data, 'ErrorResponse')
                throw new Error(errorResponse.errorMessage)
            })
    }
}


// https://calcite.apache.org/avatica/docs/protobuf_reference.html
/**
 * Remote avatica jdbc connection
 */
class Connection {
    constructor(connectionId, protobufClient) {
        this._connectionId = connectionId
        this._protobufClient = protobufClient
        this._maxFrameSize = 100 // TODO Make Conection max frame size configurable
        this._processFrame = function(statementId, offset, frame, resultSet) {
            frame.rows.forEach(r => {
                const mappedRow = r.value.map(columnValue => ConnectionFactory._mapColumnValue(columnValue))
                resultSet.rows.push(mappedRow)
            })

            if (frame.done) {
                this._protobufClient.post({
                    connectionId: this._connectionId,
                    statementId: statementId
                }, 'CloseStatementRequest', 'CloseStatementResponse')
                return resultSet
            }

            offset = offset + frame.rows.length
            const fetchRequest = {
                connectionId: this._connectionId,
                statementId: statementId,
                offset: offset,
                frameMaxSize: this._maxFrameSize
            }

            return this._protobufClient.post(
                fetchRequest,
                'FetchRequest',
                'FetchResponse'
            ).then(fetchResponse => {
                return this._processFrame(statementId, offset, fetchResponse.frame, resultSet)
            })
        }
    }

    /**
     * Close this connection.
     *
     * Should be called on a connection once it is no longer needed.
     */
    close() {
        return this._protobufClient.post({
            connectionId: this._connectionId
        }, 'CloseConnectionRequest', 'CloseConnectionResponse')
    }

    dbInfo() {
        return this._protobufClient.post({
                connectionId: this._connectionId
            }, 'DatabasePropertyRequest', 'DatabasePropertyResponse')
            .then(response => {
                return response.props
            })
    }

    tableTypes() {
        return this._protobufClient.post({
                connectionId: this._connectionId
            }, 'TableTypesRequest', 'ResultSetResponse')
            .then(resultSet => {
                const columns = resultSet.signature.columns
                return this._processFrame(resultSet.statementId, 0, resultSet.firstFrame, new ResultSet(columns, []))
            })
    }

    rollback() {
        return this._protobufClient.post({
                connectionId: this._connectionId
            }, 'RollbackRequest', 'RollbackResponse')
            .then(response => {
                return response
            })
    }

    commit() {
        return this._protobufClient.post({
                connectionId: this._connectionId
            }, 'CommitRequest', 'CommitResponse')
            .then(response => {
                return response
            })
    }

    /**
     * Search databse for catalogs
     * @returns {PromiseLike<ResultSet>} promimse containing a ResultSet
     */
    catelogs() {
        return this._protobufClient.post({
                connectionId: this._connectionId
            }, 'CatalogsRequest', 'ResultSetResponse')
            .then(resultSet => {
                const columns = resultSet.signature.columns
                return this._processFrame(resultSet.statementId, 0, resultSet.firstFrame, new ResultSet(columns, []))
            })
    }

    /**
     * @typedef SchemaFilter
     * @property {string} catalog - Category name
     * @property {string} schema - Schema search pattern
     */

    /**
     * Search databse for schema information
     * @param filter {SchemaFilter}
     * @returns {PromiseLike<ResultSet>} promimse containing a ResultSet
     */
    schemas(filter) {
        const request = {
            catalog: Object.hasOwn(filter, 'catalog') ? filter.catalog : ''
            , schemaPattern: Object.hasOwn(filter, 'schema') ? filter.schema : ''
            , connectionId: this._connectionId
            , hasCatalog: Object.hasOwn(filter, 'catalog')
            , hasSchemaPattern: Object.hasOwn(filter, 'schema')
        }
        return this._protobufClient.post(request, 'SchemasRequest', 'ResultSetResponse')
            .then(resultSet => {
                const columns = resultSet.signature.columns
                return this._processFrame(resultSet.statementId, 0, resultSet.firstFrame, new ResultSet(columns, []))
            })
    }

    /**
     * @typedef TableFilter
     * @property {string} catalog - Category name
     * @property {string} schema - Schema search pattern
     * @property {string} table - Table search pattern
     */

    /**
     * Search databse for table information
     * @param filter {TableFilter}
     * @returns {PromiseLike<ResultSet>} promimse containing a ResultSet
     */
    tables(filter) {
        const request = {
            catalog: Object.hasOwn(filter, 'catalog') ? filter.catalog : ''
            , schemaPattern: Object.hasOwn(filter, 'schema') ? filter.schema : ''
            , tableNamePattern: Object.hasOwn(filter, 'table') ? filter.table : ''
            , typeList: []
            , hasTypeList: false
            , connectionId: this._connectionId
            , hasCatalog: Object.hasOwn(filter, 'catalog')
            , hasSchemaPattern: Object.hasOwn(filter, 'schema')
            , hasTableNamePattern: Object.hasOwn(filter, 'table')
        }
        return this._protobufClient.post(request, 'TablesRequest', 'ResultSetResponse')
            .then(resultSet => {
                const columns = resultSet.signature.columns
                return this._processFrame(resultSet.statementId, 0, resultSet.firstFrame, new ResultSet(columns, []))
            })
    }

    /**
     * @typedef ColumnFilter
     * @property {string} catalog - Category name
     * @property {string} schema - Schema search pattern
     * @property {string} table - Table search pattern
     * @property {string} column - Column search pattern
     */

    /**
     * Search databse for column information
     * @param filter {ColumnFilter}
     * @returns {PromiseLike<ResultSet>} promimse containing a ResultSet
     */
    columns(filter) {
        const request = {
            catalog: Object.hasOwn(filter, 'catalog') ? filter.catalog : ''
            , schemaPattern: Object.hasOwn(filter, 'schema') ? filter.schema : ''
            , tableNamePattern: Object.hasOwn(filter, 'table') ? filter.table : ''
            , columnNamePattern: Object.hasOwn(filter, 'column') ? filter.column : ''
            , connectionId: this._connectionId
            , hasCatalog: Object.hasOwn(filter, 'catalog')
            , hasSchemaPattern: Object.hasOwn(filter, 'schema')
            , hasTableNamePattern: Object.hasOwn(filter, 'table')
            , hasColumnNamePattern: Object.hasOwn(filter, 'column')
        }
        return this._protobufClient.post(request, 'ColumnsRequest', 'ResultSetResponse')
            .then(resultSet => {
                const columns = resultSet.signature.columns
                return this._processFrame(resultSet.statementId, 0, resultSet.firstFrame, new ResultSet(columns, []))
            })
    }

    /**
     * Creates and executes a prepared statement
     * @param sql statement to be executed
     * @param parameters {Array.<StatementParameter>}
     * @returns {PromiseLike<ResultSet>} promimse containing the ResultSet from the statement
     */
    execute(sql, parameters) {
        const pValues = (parameters && parameters.constructor === Array) ? parameters : [];

        return this._protobufClient.post(
                {
                    connectionId: this._connectionId,
                    sql: sql,
                    maxRowsTotal: 9999999
                },
                'PrepareRequest', 'PrepareResponse'
            )
            .then(prepareResponse => {
                const executeRequest = {
                    statementHandle: prepareResponse.statement,
                    parameterValues: pValues,
                    hasParameterValues: pValues.length > 0,
                    firstFrameMaxSize: this._maxFrameSize
                }
                return this._protobufClient.post(executeRequest, 'ExecuteRequest', 'ExecuteResponse')
                    .then(executeResponse => {
                        const columns = executeResponse.results[0].signature.columns
                        return this._processFrame(prepareResponse.statementId, 0,
                            executeResponse.results[0].firstFrame,
                            new ResultSet(columns, []))
                    })
            })
    }

    /**
     * Execute a SQL query.
     *
     * Returns a Promise to the ResultSet containing the results of the query.
     *
     * @param sql query to be executed
     * @returns {PromiseLike<ResultSet>} promimse containing the ResultSet from the query
     */
    query(sql) {
        return this._protobufClient.post(
                {
                    connectionId: this._connectionId
                },
                'CreateStatementRequest', 'CreateStatementResponse'
            )
            .then(createStatementResponse => {
            const prepareAndExecuteRequest = {
                connectionId: this._connectionId,
                statementId: createStatementResponse.statementId,
                sql: sql,
                maxRowsTotal: 9999999
            }
            return this._protobufClient.post(
                prepareAndExecuteRequest,
                'PrepareAndExecuteRequest', 'ExecuteResponse'
            )
            .then(prepareAndExecuteResponse => {
                const columns = prepareAndExecuteResponse.results[0].signature.columns
                return this._processFrame(createStatementResponse.statementId, 0,
                    prepareAndExecuteResponse.results[0].firstFrame,
                    new ResultSet(columns, []))
            })
        })
    }
}

class ConnectionFactory {
    /**
     * The main factory for the Connection object.
     *
     * The Connection object returned in the promise should be closed after it is no longer needed.
     *
     * @param url url of the Avatica server to connect to
     * @param apiKey user api key for connecting to Avatica
     * @param apiSecret user api secret for connecting to Avatica
     * @returns {ConnectionFactory} a promise to a Connection object which allows querying
     */
    constructor(url, apiKey, apiSecret) {
        this.url = url
        this.apiKey = apiKey
        this.apiSecret = apiSecret
    }

    /**
     * Create a promise of Connection object.
     *
     * The Connection object returned in the promise should be closed after it is no longer needed.
     *
     * @returns {Promise<Connection | never>} a promise to a Connection object which allows querying
     */
    connect() {
        const protobufClient = new ProtobufClient(this.url)
        const connectionId = `${uuid.v1()}@${os.hostname()}`
        const openConnectionPayload = {
            connectionId: connectionId,
            info: {
                user: this.apiKey,
                password: this.apiSecret
            }
        }

        return protobufClient.post(
            openConnectionPayload,
            'OpenConnectionRequest',
            'OpenConnectionResponse')
            .then(response => {
                return new Connection(connectionId, protobufClient)
            })
    }

    static _mapColumnValue(columnValue) {
        const scalarValue = columnValue.scalarValue
        if (scalarValue.null)
            return null
        if (scalarValue.type === 8 || scalarValue.boolValue)
            return scalarValue.boolValue
        if (scalarValue.stringValue)
            return scalarValue.stringValue
        if (scalarValue.doubleValue)
            return scalarValue.doubleValue
        if (scalarValue.numberValue) {
            if (scalarValue.numberValue.toNumber)
                return scalarValue.numberValue.toNumber()
            return scalarValue.numberValue
        }
        if (scalarValue.bytesValue)
            return scalarValue.bytesValue

        throw new Error(`Unsupported type ${scalarValue.type} -> ${JSON.stringify(scalarValue)}`)
    }

}

module.exports = ConnectionFactory