/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.amazon.ionhiveserde

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.junit.Assert.assertEquals
import org.junit.Test
import software.amazon.ion.system.IonSystemBuilder
import java.util.*

private val EMPTY_PROPERTIES = Properties()

class IonHiveSerDeTest {
    private val ion = IonSystemBuilder.standard().build();
    private val instance = IonHiveSerDe()

    @Test
    fun roundTrip() {
        val tbl = makeProperties(listOf(
                "cBoolean" to "boolean",
                "cDecimal" to "decimal",
                "cFloat" to "float",
                "cDouble" to "double",
                "cBigInt" to "bigint",
                "cInt" to "int",
                "cSmallInt" to "smallint",
                "cTinyInt" to "tinyint",
                "cChar" to "char(10)",
                "cVarchar" to "varchar(100)",
                "cString" to "string",
                "cClob" to "binary",
                "cBlob" to "binary",
                "cDate" to "date",
                "cTimestamp" to "timestamp"
        ))

        val ionText = """
            {
                cBoolean: true,
                cDecimal: 1.0,
                cFloat: 1.2e0,
                cDouble: 1.2e0,
                cBigInt: ${Int.MAX_VALUE.toLong() + 10L},
                cInt: ${Short.MAX_VALUE.toInt() + 10},
                cSmallInt: 300,
                cTinyInt: 1,
                cChar: "char",
                cVarchar: "varchar",
                cString: "string",
                cClob: {{ "This is a CLOB of text." }},
                cBlob: {{ VG8gaW5maW5pdHkuLi4gYW5kIGJleW9uZCE= }},
                cDate: 2018-01-01T,
                cTimestamp: 2018-01-01T10:10:01Z,
            }
        """

        instance.initialize(null, tbl, EMPTY_PROPERTIES)

        val expected = Text(ionText)

        val result = instance.deserialize(expected)
        val actual = instance.serialize(result, instance.objectInspector)

        assertIonEquals(expected, actual)
    }

    private fun assertIonEquals(expected: Writable, actual: Writable) {
        // FIXME add support for binary
        val actualIon = ion.loader.load((actual as Text).toString())[0]
        val expectedIon = ion.loader.load((expected as Text).toString())[0]

        assertEquals(expectedIon, actualIon)
    }

    private fun makeProperties(columns: List<Pair<String, String>>): Properties {

        val prop = Properties()
        prop.setProperty("columns", columns.joinToString(",") { it.first })
        prop.setProperty("columns.types", columns.joinToString(",") { it.second })

        return prop
    }
}