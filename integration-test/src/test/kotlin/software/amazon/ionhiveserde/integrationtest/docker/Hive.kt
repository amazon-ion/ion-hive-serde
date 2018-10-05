/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.integrationtest.docker

import software.amazon.ionhiveserde.IonHiveSerDe
import software.amazon.ionhiveserde.storagehandlers.IonStorageHandler
import java.io.Closeable
import java.io.File
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.*

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
        val ROW_FORMAT_SERDE_STATEMENT = "ROW FORMAT SERDE '${IonHiveSerDe::class.java.name}'"
        val STORAGE_HANDLER_STATEMENT = "STORED BY '${IonStorageHandler::class.java.name}'"

        init {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
    }

    private val connection by lazy { DriverManager.getConnection(JDBC_CONNECTION_STRING) }
    private val statement by lazy {
        val stmt = connection.createStatement()
        // Adds the SerDe on creation to ensure we'll have access to it
        stmt.execute("ADD JAR $SERDE_PATH")

        stmt
    }

    override fun close() {
        statement.close()
        connection.close()
    }

    /**
     * Execute a single statement. Useful for statements that don't expect a return such as `CREATE TABLE`.
     *
     * @param sql sql statement.
     */
    fun execute(sql: String) {
        statement.execute(sql)
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
     * @return query output read as text.
     */
    fun queryToFileAndRead(sql: String): String {
        val fileName = UUID.randomUUID().toString()
        val path = "docker-tmp/output/$fileName"

        val queryString = """
            INSERT OVERWRITE LOCAL DIRECTORY '/$path'
            $ROW_FORMAT_SERDE_STATEMENT
            $sql
        """

        statement.execute(queryString)

        val dir = File(path)

        // TODO add support for binary, maybe return an InputStream instead of raw text
        return dir.listFiles().filterNot { it.isHidden }.fold("") { acc, file ->
            acc + file.readText(Charsets.UTF_8)
        }
    }

    fun isClosed() = connection.isClosed

    /**
     * Drops all hive tables in the current database.
     */
    fun dropAllTables() {
        // Table drop is done in two steps as dropping a table while having the SHOW TABLES result set open resulted
        // in an exception
        val tableNames = mutableListOf<String>()
        query("SHOW TABLES") { rs ->
            while (rs.next()) {
                tableNames.add(rs.getString(1))
            }
        }

        tableNames.forEach { execute("DROP TABLE $it") }
    }
}

