const protobuf = require('protobufjs')

// Load all the protobuf message definitions
const rootModel = protobuf.Root.fromJSON(require('./protobuf_bundle.json'))

/**
 * The return value for database results.
 */
class ResultSet {
    constructor(columns, rows) {
        this.columns = columns
        this.rows = rows
    }
}

/**
 * Prepared statement parameter, use static helper methods to create appropriate parameter type
 */
class StatementParameter {
    constructor(type) {
        this.type = type
    }

    /**
     * Create string parameter
     * @param value {string}
     * @returns {StatementParameter}
     */
    static str(value) {
        return new StringStatementParameter(rootModel.Rep.STRING, value)
    }

    /**
     * Create char parameter
     * @param value {string}
     * @returns {StatementParameter}
     */
    static char(value) {
        return new StringStatementParameter(rootModel.Rep.CHARACTER, value)
    }

    /**
     * Create BigDecimal parameter
     * @param value {string|number}
     * @returns {StatementParameter}
     */
    static bigDecimal(value) {
        return new StringStatementParameter(rootModel.Rep.BIG_DECIMAL, value.toString())
    }

    /**
     * Create pre-encoded byte string parameter
     * @param value {string}
     * @returns {StatementParameter}
     */
    static encoded(value) {
        return new StringStatementParameter(rootModel.Rep.BYTE_STRING, value)
    }

    /**
     * Create null parameter
     * @returns {StatementParameter}
     */
    static null() {
        return new NullStatementParameter()
    }

    /**
     * Create primative boolean parameter
     * @param value {boolean}
     * @returns {StatementParameter}
     */
    static primativeBool(value) {
        return new BooleanStatementParameter(rootModel.Rep.PRIMITIVE_BOOLEAN, value === true)
    }

    /**
     * Create boolean parameter
     * @param value {boolean}
     * @returns {StatementParameter}
     */
    static bool(value) {
        return new BooleanStatementParameter(rootModel.Rep.BOOLEAN, value === true)
    }

    /**
     * Create number parameter
     * @param value {number}
     * @returns {StatementParameter}
     */
    static genericNumber(value) {
        return new NumberStatementParameter(rootModel.Rep.NUMBER, value)
    }

    /**
     * Create primative
     * @param value {number}
     * @returns {StatementParameter}
     */
    static primativeByte(value) {
        return new NumberStatementParameter(rootModel.Rep.PRIMITIVE_BYTE, value)
    }

    /**
     * Create primative
     * @param value {number}
     * @returns {StatementParameter}
     */
    static primativeShort(value) {
        return new NumberStatementParameter(rootModel.Rep.PRIMITIVE_SHORT, value)
    }

    /**
     * Create primative
     * @param value {number}
     * @returns {StatementParameter}
     */
    static primativeInt(value) {
        return new NumberStatementParameter(rootModel.Rep.PRIMITIVE_INT, value)
    }

    /**
     * Create primative
     * @param value {number}
     * @returns {StatementParameter}
     */
    static primativeLong(value) {
        return new NumberStatementParameter(rootModel.Rep.PRIMITIVE_LONG, value)
    }

    /**
     * Create primative
     * @param value {number}
     * @returns {StatementParameter}
     */
    static primativeFloat(value) {
        return new NumberStatementParameter(rootModel.Rep.PRIMITIVE_FLOAT, value)
    }

    /**
     * Create primative
     * @param value {number}
     * @returns {StatementParameter}
     */
    static primativeDouble(value) {
        return new NumberStatementParameter(rootModel.Rep.PRIMITIVE_DOUBLE, value)
    }

    /**
     * Create byte
     * @param value {number}
     * @returns {StatementParameter}
     */
    static byte(value) {
        return new NumberStatementParameter(rootModel.Rep.BYTE, value)
    }

    /**
     * Create short
     * @param value {number}
     * @returns {StatementParameter}
     */
    static short(value) {
        return new NumberStatementParameter(rootModel.Rep.SHORT, value)
    }

    /**
     * Create int
     * @param value {number}
     * @returns {StatementParameter}
     */
    static int(value) {
        return new NumberStatementParameter(rootModel.Rep.INT, value)
    }

    /**
     * Create long
     * @param value {number}
     * @returns {StatementParameter}
     */
    static long(value) {
        return new NumberStatementParameter(rootModel.Rep.LONG, value)
    }

    /**
     * Create float
     * @param value {number}
     * @returns {StatementParameter}
     */
    static float(value) {
        return new NumberStatementParameter(rootModel.Rep.FLOAT, value)
    }

    /**
     * Create double
     * @param value {number}
     * @returns {StatementParameter}
     */
    static double(value) {
        return new NumberStatementParameter(rootModel.Rep.DOUBLE, value)
    }

    /**
     * @param value As an integer, milliseconds since midnight
     * @returns {StatementParameter}
     */
    static sqlTime(value) {
        return new NumberStatementParameter(rootModel.Rep.JAVA_SQL_TIME, value)
    }

    /**
     * @param value As an integer, the number of days since the epoch.
     * @returns {StatementParameter}
     */
    static sqlDate(value) {
        return new NumberStatementParameter(rootModel.Rep.JAVA_SQL_DATE, value)
    }

    /**
     * @param value As a long, milliseconds since the epoch.
     * @returns {StatementParameter}
     */
    static sqlTimestamp(value) {
        return new NumberStatementParameter(rootModel.Rep.JAVA_SQL_TIMESTAMP, value)
    }

    /**
     * @param value As a long, milliseconds since the epoch.
     * @returns {StatementParameter}
     */
    static javaDate(value) {
        return new NumberStatementParameter(rootModel.Rep.JAVA_UTIL_DATE, value)
    }
}


class StringStatementParameter extends StatementParameter {
    constructor(type,value) {
        super(type);
        this.stringValue = value
    }
}

class NullStatementParameter extends StatementParameter {
    constructor() {
        super(rootModel.Rep.NULL);
        this.null = true
    }
}

class BooleanStatementParameter extends StatementParameter {
    constructor(type,value) {
        super(type);
        this.boolValue = value
    }
}

class NumberStatementParameter extends StatementParameter {
    constructor(type,value) {
        super(type);
        this.numberValue = value
    }
}

module.exports = {
    ResultSet
    , StatementParameter
    , rootModel
}
