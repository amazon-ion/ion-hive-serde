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

package software.amazon.ionhiveserde.objectinspectors

import junitparams.JUnitParamsRunner
import junitparams.NamedParameters
import junitparams.Parameters
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ion.Timestamp
import software.amazon.ionhiveserde.ION
import java.sql.Date
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class IonTimestampToDateObjectInspectorTest : AbstractIonPrimitiveJavaObjectInspectorTest() {

    override val subject = IonTimestampToDateObjectInspector()

    @NamedParameters("parameters")
    fun parameters(): Array<Array<Any>> {
        return arrayOf(
                arrayOf(Date(1514764800000L), Timestamp.valueOf("2018T")),
                arrayOf(Date(1533081600000L), Timestamp.valueOf("2018-08T")),
                arrayOf(Date(1534291200000L), Timestamp.valueOf("2018-08-15T")),
                arrayOf(Date(1534291200000L), Timestamp.valueOf("2018-08-15T01:01Z")),
                arrayOf(Date(1534291200000L), Timestamp.valueOf("2018-08-15T01:01:01Z"))
        )
    }

    @Test
    @Parameters(named = "parameters")
    fun getPrimitiveWritableObject(date: Date, timestamp: Timestamp) {
        val ionTimestamp = ION.newTimestamp(timestamp)

        val actual = subject.getPrimitiveWritableObject(ionTimestamp)
        assertEquals(DateWritable(date), actual, message = "for $timestamp")
    }
}