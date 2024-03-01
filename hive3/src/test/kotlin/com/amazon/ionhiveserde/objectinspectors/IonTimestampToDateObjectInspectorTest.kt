// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonTimestamp
import com.amazon.ion.Timestamp
import com.amazon.ionhiveserde.ION
import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.apache.hadoop.hive.common.type.Date
import org.apache.hadoop.hive.serde2.io.DateWritableV2
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class IonTimestampToDateObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonTimestamp, DateWritableV2, Date>() {

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonTimestampToDateObjectInspector()
    override fun validTestCases() = listOf(
            validTestCase("2018T",               1514764800000L,1514764800000L),
            validTestCase("2018-08T",            1533081600000L,1533081600000L),
            validTestCase("2018-08-15T",         1534291200000L,1534291200000L),
            validTestCase("2018-08-15T01:01Z",   1534291200000L,1534291200000L),
            validTestCase("2018-08-15T01:01:01Z",1534291200000L,1534291200000L)
    )

    private fun validTestCase(ionFormattedTimestamp: String, dateEpochMilli: Long, dateWritableEpochMilli: Long) =
            ValidTestCase(
                    ION.newTimestamp(Timestamp.valueOf(ionFormattedTimestamp)),
                    Date.ofEpochMilli(dateEpochMilli),
                    DateWritableV2(Date.ofEpochMilli(dateWritableEpochMilli)))

    @Test
    @Parameters(method = "validTestCases")
    override fun getPrimitiveJavaObject(testCase: ValidTestCase<out IonTimestamp, DateWritableV2, Date>) {
        val actualDate = subject.getPrimitiveJavaObject(testCase.ionValue) as Date
        // assert on epoch milli because Date keeps the time component internally...
        assertEquals(testCase.expectedPrimitive.toEpochMilli(), actualDate.toEpochMilli())
    }
}
