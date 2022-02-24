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
import com.amazon.ionhiveserde.ION
import junitparams.JUnitParamsRunner
import org.apache.hadoop.hive.common.type.Timestamp
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2
import org.junit.runner.RunWith

@RunWith(JUnitParamsRunner::class)
class IonTimestampToTimestampObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonTimestamp, TimestampWritableV2, Timestamp>() {

    override val subject = IonTimestampToTimestampObjectInspector()
    override fun validTestCases() = listOf(
            validTestCase("2018T"               , 1514764800000L, 1514764800000L),
            validTestCase("2018-08T"            , 1533081600000L, 1533081600000L),
            validTestCase("2018-08-15T"         , 1534291200000L, 1534291200000L),
            validTestCase("2018-08-15T01:01Z"   , 1534294860000L, 1534294860000L),
            validTestCase("2018-08-15T01:01:01Z", 1534294861000L, 1534294861000L)
    )

    private fun validTestCase(ionFormattedTimestamp: String, timestampEpochMilli: Long, timestampWritableEpochMilli: Long) =
            ValidTestCase(
                    ION.newTimestamp(com.amazon.ion.Timestamp.valueOf(ionFormattedTimestamp)),
                    Timestamp.ofEpochMilli(timestampEpochMilli),
                    TimestampWritableV2(Timestamp.ofEpochMilli(timestampWritableEpochMilli)))
}