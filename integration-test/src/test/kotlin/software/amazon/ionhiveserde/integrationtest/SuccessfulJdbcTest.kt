package software.amazon.ionhiveserde.integrationtest

import junitparams.JUnitParamsRunner
import junitparams.Parameters
import junitparams.naming.TestCaseName
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ion.IonSequence
import software.amazon.ion.IonTimestamp
import software.amazon.ion.IonValue
import software.amazon.ion.Timestamp
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.arrays
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.bigInts
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.binaries
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.booleans
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.chars
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.decimals
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.doubles
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.floats
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.ints
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.maps
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.strings
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.structs
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.tinyInts
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.varchars
import java.sql.ResultSet
import java.sql.Types
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

@RunWith(JUnitParamsRunner::class)
class SuccessfulJdbcTest : SuccessfulSingleValueMappingBaseTest() {
    override val tableNamePrefix = "jdbc_test"

    private fun queryAndAssert(hiveType: String,
                               ionValue: String,
                               assertions: (expected: IonValue, actual: ResultSet) -> Unit) {
        val tableName = loadDataIntoNewTable(hiveType, ionValue)
        val expected = ion.singleValue(ionValue)

        hive().query("SELECT * FROM $tableName") { rs ->
            assertEquals(1, rs.metaData.columnCount, message = "ResultSet has more than one column")

            rs.next()
            assertions(expected, rs)

            assertFalse(rs.next(), message = "ResultSet has more than one row")
        }

    }

    fun basicTypesTestCases() = booleans +
            tinyInts +
            ints +
            bigInts +
            floats +
            doubles +
            decimals +
            binaries +
            chars +
            varchars +
            strings +
            arrays +
            maps +
            structs

    @Test
    @Parameters(method = "basicTypesTestCases")
    @TestCaseName("[{index}] {method}: {0} - {1}")
    fun basicTypes(hiveType: String, ionValue: String) {
        queryAndAssert(hiveType, ionValue) { expected, rs ->

            val actual = when (rs.metaData.getColumnType(1)) {
                Types.BOOLEAN -> ion.newBool(rs.getBoolean(1))
                Types.TINYINT, Types.INTEGER -> ion.newInt(rs.getInt(1))
                Types.BIGINT -> ion.newInt(rs.getLong(1))
                Types.FLOAT, Types.DOUBLE -> ion.newFloat(rs.getDouble(1))
                Types.DECIMAL -> ion.newDecimal(rs.getBigDecimal(1).stripTrailingZeros())
                Types.BINARY -> ion.newBlob(rs.getBinaryStream(1).readBytes())
                Types.CHAR, Types.VARCHAR -> ion.newString(rs.getString(1).trim())
                Types.DATE -> ion.newTimestamp(Timestamp.forDateZ(rs.getDate(1)))
                Types.TIMESTAMP -> ion.newTimestamp(Timestamp.forSqlTimestampZ(rs.getTimestamp(1)))

                // java object is used for maps
                Types.ARRAY, Types.JAVA_OBJECT, Types.STRUCT  -> {
                    // hive jdbc driver represents this types a json string
                    ion.singleValue(rs.getString(1))
                }
                else -> {
                    throw IllegalArgumentException("Unknown sql column type: ${rs.metaData.getColumnType(1)}")
                }
            }

            assertEquals(expected, actual)
        }
    }

    fun timestamps() = TestCases.timestamps

    /**
     * Timestamps coming from Hive use the highest precision, loosing their original precision so we assert on the
     * milliseconds from epoch to ignore precision
     */
    @Test
    @Parameters(method = "timestamps")
    @TestCaseName("[{index}] {method}: {0} - {1}")
    fun timestamps(hiveType: String, ionValue: String) {
        queryAndAssert(hiveType, ionValue) { expected, rs ->
            val expectedTimestamp = (expected as IonTimestamp).timestampValue()
            // java.sql.Timestamp are created on the local timezone, need to convert back into UTC before asserting
            val actual = rs.getTimestamp(1).toLocalDateTime().atOffset(ZoneOffset.UTC)


            // convert SQL timestamp to Ion timestamp to normalize assertions
            assertEquals(
                    expectedTimestamp.millis,
                    actual.toInstant().toEpochMilli(),
                    message = """|Expected: $expectedTimestamp
                                 |Actual:   $actual
                                 |""".trimMargin())
        }
    }

    fun dates() = TestCases.dates

    /**
     * Timestamps stored as Dates loose precision higher than day
     */
    @Test
    @Parameters(method = "dates")
    @TestCaseName("[{index}] {method}: {0} - {1}")
    fun roundTripDates(hiveType: String, ionValue: String) {
        queryAndAssert(hiveType, ionValue) { expected, rs ->
            val expectedTimestamp = (expected as IonTimestamp).timestampValue()
            val actualDate = rs.getDate(1).toLocalDate()

            val assertErrorMessage = """|Expected: $expectedTimestamp
                                        |Actual:   $actualDate
                                        |""".trimMargin()

            assertEquals(expectedTimestamp.year, actualDate.year, message = assertErrorMessage)
            assertEquals(expectedTimestamp.month, actualDate.monthValue, message = assertErrorMessage)
            assertEquals(expectedTimestamp.day, actualDate.dayOfMonth, message = assertErrorMessage)
        }
    }

    fun hiveTypes() = TestCases.hiveTypes

    @Test
    @Parameters(method = "hiveTypes")
    @TestCaseName("[{index}] {method}: {0}")
    fun roundTripNull(hiveType: String) {
        queryAndAssert(hiveType, "null") { _, actual ->
            val obj = actual.getObject(1)
            assertNull(obj)
        }
    }
}