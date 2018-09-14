package software.amazon.ionhiveserde.integrationtest.hive

import software.amazon.ionhiveserde.IonHiveSerDe
import java.io.Closeable
import java.io.File
import java.sql.DriverManager
import java.sql.ResultSet
import java.util.*

// path in the hive server docker container
private const val SERDE_PATH = "/docker-tmp/serde/serde-all.jar"

class Hive : Closeable {
    companion object {
        val ROW_FORMAT_SERDE_STATEMENT = "ROW FORMAT SERDE '${IonHiveSerDe::class.java.name}'"

        init {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
    }

    private val connection by lazy { DriverManager.getConnection("jdbc:hive2://0.0.0.0:10000/default", "", "") }
    private val statement by lazy {
        val stmt = connection.createStatement()
        stmt.execute("ADD JAR $SERDE_PATH")

        stmt
    }

    override fun close() {
        statement.close()
        connection.close()
    }

    fun execute(q: String) {
        statement.execute(q)
    }

    fun query(q: String, block: (ResultSet) -> Unit = {}) {
        statement.executeQuery(q).use(block)
    }

    fun queryToFileAndRead(q: String): String {
        val fileName = UUID.randomUUID().toString()
        val path = "docker-tmp/output/$fileName"

        val queryString = """
            INSERT OVERWRITE LOCAL DIRECTORY '/$path'
            $ROW_FORMAT_SERDE_STATEMENT
            $q
        """
        statement.execute(queryString)

        val dir = File(path)

        return dir.listFiles().filterNot { it.isHidden }.fold("") { acc, file ->
            acc + file.readText(Charsets.UTF_8)
        }
    }

    fun isClosed() = connection.isClosed

    fun loadDataFromFile(filePath: String, tableName: String) {
        execute("LOAD DATA LOCAL INPATH '$filePath' OVERWRITE INTO TABLE $tableName")
    }

    fun loadData(data: String, tableName: String) {
        val fileName = UUID.randomUUID().toString()
        val path = "docker-tmp/input/$fileName"

        File(path).writeText(data, Charsets.UTF_8)
        execute("LOAD DATA LOCAL INPATH '/$path' OVERWRITE INTO TABLE $tableName")
    }

    fun dropAllTables() {
        val tableNames = mutableListOf<String>()
        query("SHOW TABLES") { rs ->
            while (rs.next()) {
                tableNames.add(rs.getString(1))
            }
        }

        tableNames.forEach { execute("DROP TABLE $it") }
    }
}

