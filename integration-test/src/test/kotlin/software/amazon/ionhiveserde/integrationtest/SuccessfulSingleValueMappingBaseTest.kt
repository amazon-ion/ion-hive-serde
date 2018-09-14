package software.amazon.ionhiveserde.integrationtest

import junitparams.JUnitParamsRunner
import org.junit.runner.RunWith
import software.amazon.ionhiveserde.integrationtest.hive.Hive
import software.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector
import software.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector
import software.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector
import java.util.*

/**
 * Base class for tests that verify the mapping of a single value between Ion and Hive
 */
@RunWith(JUnitParamsRunner::class)
abstract class SuccessfulSingleValueMappingBaseTest : Base() {
    companion object {
        const val COLUMN_NAME = "field"
    }

    protected abstract val tableNamePrefix: String

    private fun makeStruct(ionValue: String): String = "{$COLUMN_NAME: $ionValue}"

    protected fun loadDataIntoNewTable(hiveType: String, ionValue: String): String {
        val data = makeStruct(ionValue)
        val tableName = randomTableName() // randomize table names to allow for parallelization

        hive().execute("CREATE TABLE $tableName (field $hiveType) ${Hive.ROW_FORMAT_SERDE_STATEMENT}")
        hive().loadData(data, tableName)

        return tableName
    }

    private fun randomTableName() = "${tableNamePrefix}_${UUID.randomUUID().toString().replace('-', '_')}"
}