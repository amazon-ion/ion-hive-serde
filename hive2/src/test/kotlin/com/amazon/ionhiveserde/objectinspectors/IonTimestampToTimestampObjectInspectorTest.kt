// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonTimestamp
import com.amazon.ion.Timestamp
import com.amazon.ionhiveserde.ION
import junitparams.JUnitParamsRunner
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.junit.runner.RunWith

@RunWith(JUnitParamsRunner::class)
class IonTimestampToTimestampObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonTimestamp, TimestampWritable, java.sql.Timestamp>() {

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonTimestampToTimestampObjectInspector()
    override fun validTestCases() = listOf(
            ValidTestCase(ION.newTimestamp(Timestamp.valueOf("2018T")), java.sql.Timestamp(1514764800000L), TimestampWritable(java.sql.Timestamp(1514764800000L))),
            ValidTestCase(ION.newTimestamp(Timestamp.valueOf("2018-08T")), java.sql.Timestamp(1533081600000L), TimestampWritable(java.sql.Timestamp(1533081600000L))),
            ValidTestCase(ION.newTimestamp(Timestamp.valueOf("2018-08-15T")), java.sql.Timestamp(1534291200000L), TimestampWritable(java.sql.Timestamp(1534291200000L))),
            ValidTestCase(ION.newTimestamp(Timestamp.valueOf("2018-08-15T01:01Z")), java.sql.Timestamp(1534294860000L), TimestampWritable(java.sql.Timestamp(1534294860000L))),
            ValidTestCase(ION.newTimestamp(Timestamp.valueOf("2018-08-15T01:01:01Z")), java.sql.Timestamp(1534294861000L), TimestampWritable(java.sql.Timestamp(1534294861000L)))
    )
}
