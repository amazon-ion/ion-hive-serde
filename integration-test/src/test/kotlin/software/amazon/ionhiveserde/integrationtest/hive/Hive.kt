package software.amazon.ionhiveserde.integrationtest.hive

import java.io.Closeable
import java.sql.DriverManager
import java.sql.ResultSet

// hive docker path
private const val SERDE_PATH = "/shared/serde/serde-all.jar"

class Hive : Closeable {
    companion object {
        init {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
    }

    private val connection by lazy { DriverManager.getConnection("jdbc:hive2://0.0.0.0:10000/default", "", "") }
    private val statement by lazy { connection.createStatement() }

    override fun close() {
        statement.close()
        connection.close()
    }

    fun query(q: String, block: (ResultSet) -> Unit = {}) {
        statement.executeQuery(q).use(block)
    }
}