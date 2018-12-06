/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.integrationtest.docker

import org.apache.hadoop.mapred.InputFormat
import software.amazon.ionhiveserde.IonHiveSerDe
import software.amazon.ionhiveserde.formats.IonInputFormat
import software.amazon.ionhiveserde.formats.IonOutputFormat
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.File
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.*
import kotlin.reflect.KClass

/** local path for the directory shared between local fs and hive docker container. */
const val SHARED_DIR = "docker-tmp"

// path in the hive server docker container
private const val SERDE_PATH = "/docker-tmp/serde/serde-all.jar"
private const val JDBC_CONNECTION_STRING = "jdbc:hive2://0.0.0.0:10000/"

/**
 * Encapsulates Hive interactions with tables using the Ion SerDe.
 */
class Hive : Closeable {
    companion object {
        init {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
    }

    private val connection by lazy { DriverManager.getConnection(JDBC_CONNECTION_STRING) }
    private val statement by lazy {
        // Adds the SerDe on creation to ensure we'll have access to it
        connection.createStatement().apply { execute("ADD JAR $SERDE_PATH") }
    }

    override fun close() {
        statement.close()
        connection.close()
    }

    /**
     * Creates an external table using the Ion Hive SerDe.
     *
     * @param tableName Table name, must not exist.
     * @param columns table columns represented as a column name to hive type map.
     * @param location HDFS path where the table data is stored.
     * @param serdeProperties SerDe properties represented as a property name to property value map.
     */
    fun createExternalTable(tableName: String,
                            columns: Map<String, String>,
                            location: String,
                            serdeProperties: Map<String, String> = emptyMap(),
                            inputFormatClass: KClass<*> = IonInputFormat::class) {
        val columnsAsString = columns.entries.joinToString(",") { "${it.key} ${it.value}" }

        execute("""
            CREATE EXTERNAL TABLE $tableName ($columnsAsString)
            ${serDeStatement(serdeProperties, inputFormatClass)}
            LOCATION '$location'
        """)
    }

    /**
     * Executes the query and operates on the result set. Correctly closes the result set after block is executed.
     *
     * @param sql sql statement.
     * @param block lambda to operate on the result set.
     */
    fun query(sql: String, block: (ResultSet) -> Unit = {}) {
        statement.executeQuery(sql).use(block)
    }

    /**
     * Executes the query storing the output on the file system using the SerDe. Reads the output back as text.
     *
     * @param sql sql statement.
     * @param serdeProperties SerDe properties represented as a property name to property value map.
     * @return query output read as text.
     */
    fun queryToFileAndRead(sql: String, serdeProperties: Map<String, String> = emptyMap()): ByteArray {
        val fileName = UUID.randomUUID().toString()
        val path = "docker-tmp/output/$fileName"

        val queryString = """
            INSERT OVERWRITE LOCAL DIRECTORY '/$path'
            ${serDeStatement(serdeProperties)}
            $sql
        """

        statement.execute(queryString)

        val dir = File(path)

        return dir.listFiles()
                .filterNot { it.isHidden }
                .fold(ByteArrayOutputStream()) { acc, file -> acc.apply { write(file.readBytes()) } }
                .toByteArray()
    }

    /**
     * Checks if the hive connection is closed.
     *
     * @return true if the connection is closed, false otherwise.
     */
    fun isClosed() = connection.isClosed

    /**
     * Drops all hive tables in the current database.
     *
     * **Note**: External tables don't their data destroyed.
     */
    fun dropAllTables() {
        // Table drop is done in two steps as dropping a table while having the SHOW TABLES result set open resulted
        // in an exception.
        val tableNames = mutableListOf<String>()
        query("SHOW TABLES") { rs ->
            while (rs.next()) {
                tableNames.add(rs.getString(1))
            }
        }

        tableNames.forEach { execute("DROP TABLE $it") }
    }

    /**
     * Execute a single statement. Useful for statements that don't expect a return such as `CREATE TABLE`.
     *
     * @param sql sql statement.
     */
    private fun execute(sql: String) {
        this.statement.execute(sql)
    }

    /**
     * Builds a SerDe statement including SerDe properties, input and output format.
     *
     * @param properties property name to property value map
     */
    private fun serDeStatement(
            properties: Map<String, String> = emptyMap(),
            inputFormatClass: KClass<*> = IonInputFormat::class): String {
        val serdeProperties = if (properties.isEmpty()) {
            ""
        } else {
            """WITH SERDEPROPERTIES (${properties.entries.joinToString(",") { """ "${it.key}" = "${it.value}" """ }})"""
        }

        return """
            ROW FORMAT SERDE '${IonHiveSerDe::class.java.name}'
            $serdeProperties
            STORED AS
                INPUTFORMAT '${inputFormatClass.java.name}'
                OUTPUTFORMAT '${IonOutputFormat::class.java.name}'
        """
    }
}
