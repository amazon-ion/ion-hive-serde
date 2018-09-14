package software.amazon.ionhiveserde.integrationtest

import software.amazon.ionhiveserde.objectinspectors.IonIntToBigIntObjectInspector
import software.amazon.ionhiveserde.objectinspectors.IonIntToIntObjectInspector
import software.amazon.ionhiveserde.objectinspectors.IonIntToTinyIntObjectInspector

// FIXME make it a java class
class TestCases {
    companion object {
        val booleans = listOf(
                listOf("BOOLEAN", "true"),
                listOf("BOOLEAN", "false")
        )

        val tinyInts = listOf(
                listOf("TINYINT", IonIntToTinyIntObjectInspector.MIN_VALUE.toString()),
                listOf("TINYINT", "-1"),
                listOf("TINYINT", "1"),
                listOf("TINYINT", "0"),
                listOf("TINYINT", IonIntToTinyIntObjectInspector.MAX_VALUE.toString())
        )

        val ints = listOf(
                listOf("INT", IonIntToIntObjectInspector.MIN_VALUE.toString()),
                listOf("INT", "-1"),
                listOf("INT", "1"),
                listOf("INT", "0"),
                listOf("INT", IonIntToIntObjectInspector.MAX_VALUE.toString())
        )

        val bigInts = listOf(
                listOf("BIGINT", IonIntToBigIntObjectInspector.MIN_VALUE.toString()),
                listOf("BIGINT", "-1"),
                listOf("BIGINT", "1"),
                listOf("BIGINT", "0"),
                listOf("BIGINT", IonIntToBigIntObjectInspector.MAX_VALUE.toString())

                // TODO Decimal for arbitrary precision integers https://github.com/amzn/ion-hive-serde/issues/8
        )

        val floats = listOf(
                listOf("FLOAT", "-inf"),
                listOf("FLOAT", "-1e0"),
                listOf("FLOAT", "0e0"),
                listOf("FLOAT", "1e0"),
                listOf("FLOAT", "+inf"),
                listOf("FLOAT", "nan")
        )

        val doubles = listOf(
                listOf("DOUBLE", "-1e0"),
                listOf("DOUBLE", "0e0"),
                listOf("DOUBLE", "1e0")
        )

        val decimals = listOf(
                listOf("DECIMAL", "-1."),
                listOf("DECIMAL", "0."),
                listOf("DECIMAL", "1.")
        )

        val binaries = listOf(
                listOf("BINARY", "{{ VG8gaW5maW5pdHkuLi4gYW5kIGJleW9uZCE= }}")
                // TODO CLOB https://github.com/amzn/ion-hive-serde/issues/8
        )

        val chars = listOf(
                listOf("CHAR(10)", """ "char" """),
                listOf("CHAR(10)", """ "0123456789" """),
                listOf("CHAR(10)", """ "💩" """)
        )

        val varchars = listOf(
                listOf("VARCHAR(10)", """ "varchar" """),
                listOf("VARCHAR(10)", """ "0123456789" """),
                listOf("VARCHAR(10)", """ "💩" """)
        )

        val strings = listOf(
                listOf("STRING", """ "string" """),
                listOf("STRING", """ "longer string" """),
                listOf("STRING", """ "💩💩💩💩" """)
        )

        val timestamps = listOf(
                listOf("TIMESTAMP", "2018T"),
                listOf("TIMESTAMP", "2018-09T"),
                listOf("TIMESTAMP", "2018-09-14T"),
                listOf("TIMESTAMP", "2018-09-14T10:55Z"),
                listOf("TIMESTAMP", "2018-09-14T10:55:10Z"),
                listOf("TIMESTAMP", "2018-09-14T10:55:10.001Z"),
                listOf("TIMESTAMP", "2018-09-14T10:55:10+10:00")
        )

        // use the same values as timestamp, but mapping to hive DATE
        val dates = timestamps.map {
            listOf("DATE", it[1])
        }

        val arrays = listOf(
                listOf("array<int>", "[1,2,3,4]"),
                listOf("array<int>", "[]"),
                listOf("array<boolean>", "[]"),
                listOf("array<boolean>", "[true, false]")

                // TODO SEXP https://github.com/amzn/ion-hive-serde/issues/8
        )

        val maps = listOf(
                listOf("map<string, int>", "{foo: 1, bar: 2}"),
                listOf("map<string, int>", "{}")
        )

        val structs = listOf(
                listOf("struct<foo: INT, bar: BOOLEAN>", "{foo: 1, bar: true}"),
                listOf("struct<foo: INT, bar: BOOLEAN>", "{foo: 2, bar: false}")
        )

        val hiveTypes = listOf(
                "BOOLEAN",
                "INT",
                "TINYINT",
                "SMALLINT",
                "BIGINT",
                "FLOAT",
                "DOUBLE",
                "DECIMAL",
                "BINARY",
                "CHAR(10)",
                "VARCHAR(10)",
                "STRING",
                "DATE",
                "TIMESTAMP"
        )
    }
}