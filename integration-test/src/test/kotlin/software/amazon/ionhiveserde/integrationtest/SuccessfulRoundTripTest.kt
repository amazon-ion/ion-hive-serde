package software.amazon.ionhiveserde.integrationtest

import junitparams.JUnitParamsRunner
import junitparams.Parameters
import junitparams.naming.TestCaseName
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ion.IonStruct
import software.amazon.ion.IonTimestamp
import software.amazon.ion.IonValue
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.arrays
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.bigInts
import software.amazon.ionhiveserde.integrationtest.TestCases.Companion.binaries
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
import kotlin.test.assertEquals

/**
 * Round trips test.
 * 1. Loads Ion data into Hive
 * 1. Queries Hive table writing output as Ion into filesystem
 */
@RunWith(JUnitParamsRunner::class)
class SuccessfulRoundTripTest : SuccessfulSingleValueMappingBaseTest() {
    override val tableNamePrefix = "round_trip_test"

    fun basicTypesTestCases() = TestCases.booleans +
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
        queryAndAssert(hiveType, ionValue) { expected, actual -> assertEquals(expected, actual) }
    }

    fun timestamps() = TestCases.timestamps

    /**
     * Timestamps coming from Hive use the highest precision, loosing their original precision so we assert on the
     * milliseconds from epoch to ignore precision
     */
    @Test
    @Parameters(method = "timestamps")
    @TestCaseName("[{index}] {method}: {0} - {1}")
    fun timestamps(ionValue: String) {
        queryAndAssert("TIMESTAMP", ionValue) { expected, actual ->
            val expectedTimestamp = (expected as IonTimestamp).timestampValue()
            val actualTimestamp = (actual as IonTimestamp).timestampValue()

            assertEquals(
                    expectedTimestamp.millis,
                    actualTimestamp.millis,
                    message = """|Expected: $expectedTimestamp
                                 |Actual:   $actualTimestamp
                                 |""".trimMargin()
            )

            // stored timestamps are always converted to Z
            assertEquals(0, actualTimestamp.localOffset)
        }
    }

    fun dates() = TestCases.dates

    /**
     * Timestamps stored as Dates loose precision higher than day
     */
    @Test
    @Parameters(method = "timestamps")
    @TestCaseName("[{index}] {method}: {0} - {1}")
    fun roundTripDates(ionValue: String) {
        queryAndAssert("DATE", ionValue) { expected, actual ->
            val expectedTimestamp = (expected as IonTimestamp).timestampValue()
            val actualTimestamp = (actual as IonTimestamp).timestampValue()

            val assertErrorMessage = """|Expected: $expectedTimestamp
                                        |Actual:   $actualTimestamp
                                        |""".trimMargin()

            assertEquals(expectedTimestamp.year, actualTimestamp.year, message = assertErrorMessage)
            assertEquals(expectedTimestamp.month, actualTimestamp.month, message = assertErrorMessage)
            assertEquals(expectedTimestamp.day, actualTimestamp.day, message = assertErrorMessage)

            assertEquals(0, actualTimestamp.hour, message = assertErrorMessage)
            assertEquals(0, actualTimestamp.minute, message = assertErrorMessage)
            assertEquals(0, actualTimestamp.second, message = assertErrorMessage)
            assertEquals(0, actualTimestamp.localOffset, message = assertErrorMessage)
        }
    }

    fun hiveTypes() = TestCases.hiveTypes

    @Test
    @Parameters(method = "hiveTypes")
    @TestCaseName("[{index}] {method}: {0}")
    fun roundTripNull(hiveType: String) {
        val tableName = loadDataIntoNewTable(hiveType, "null")

        val actualDatagram = queryDataIntoDatagram(tableName)
        assertEquals(1, actualDatagram.size)

        val actual = actualDatagram.first() as IonStruct
        assertEquals(0, actual.size())
    }

    private fun queryDataIntoDatagram(tableName: CharSequence) =
            ion.loader.load(hive().queryToFileAndRead("SELECT * FROM $tableName"))

    private fun queryAndAssert(hiveType: String,
                               ionValue: String,
                               extraAssertions: (expected: IonValue, actual: IonValue) -> Unit = { _, _ -> }) {
        val tableName = loadDataIntoNewTable(hiveType, ionValue)

        val expected = ion.singleValue(ionValue)
        val actualDatagram = queryDataIntoDatagram(tableName)

        assertEquals(1, actualDatagram.size)

        val actual = actualDatagram.first() as IonStruct

        assertEquals(1, actual.size())
        extraAssertions(expected, actual.first())
    }
}
