/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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