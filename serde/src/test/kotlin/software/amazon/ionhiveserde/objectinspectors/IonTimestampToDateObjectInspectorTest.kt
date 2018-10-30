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
import junitparams.Parameters
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ion.IonTimestamp
import software.amazon.ion.Timestamp
import software.amazon.ionhiveserde.ION
import java.sql.Date
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class IonTimestampToDateObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonTimestamp, DateWritable, Date>() {

    override val subject = IonTimestampToDateObjectInspector()
    override fun validTestCases() = listOf(
            ValidTestCase(ION.newTimestamp(Timestamp.valueOf("2018T")), Date(1514764800000L), DateWritable(Date(1514764800000L))),
            ValidTestCase(ION.newTimestamp(Timestamp.valueOf("2018-08T")), Date(1533081600000L), DateWritable(Date(1533081600000L))),
            ValidTestCase(ION.newTimestamp(Timestamp.valueOf("2018-08-15T")), Date(1534291200000L), DateWritable(Date(1534291200000L))),
            ValidTestCase(ION.newTimestamp(Timestamp.valueOf("2018-08-15T01:01Z")), Date(1534291200000L), DateWritable(Date(1534291200000L))),
            ValidTestCase(ION.newTimestamp(Timestamp.valueOf("2018-08-15T01:01:01Z")), Date(1534291200000L), DateWritable(Date(1534291200000L)))
    )

    @Test
    @Parameters(method = "validTestCases")
    override fun getPrimitiveJavaObject(testCase: ValidTestCase<out IonTimestamp, DateWritable, Date>) {
        val actualDate = subject.getPrimitiveJavaObject(testCase.ionValue) as Date
        // assert on LocalDate because Date keeps the time component internally...
        assertEquals(testCase.expectedPrimitive.toLocalDate(), actualDate.toLocalDate())
    }
}