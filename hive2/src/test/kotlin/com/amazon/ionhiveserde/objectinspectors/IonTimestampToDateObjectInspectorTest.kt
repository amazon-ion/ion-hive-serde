// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonTimestamp
import com.amazon.ion.Timestamp
import com.amazon.ionhiveserde.ION
import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.junit.Test
import org.junit.runner.RunWith
import java.sql.Date
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class IonTimestampToDateObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonTimestamp, DateWritable, Date>() {

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonTimestampToDateObjectInspector()
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
