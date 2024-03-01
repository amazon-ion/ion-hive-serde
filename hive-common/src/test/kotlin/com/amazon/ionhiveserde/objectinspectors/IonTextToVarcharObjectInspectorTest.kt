// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonText
import com.amazon.ionhiveserde.ION
import org.apache.hadoop.hive.common.type.HiveVarchar
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable

class IonTextToVarcharObjectInspectorTest
    : AbstractOverflowablePrimitiveObjectInspectorTest<IonText, HiveVarcharWritable, HiveVarchar>() {

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonTextToVarcharObjectInspector(5, true)
    override fun validTestCases() = listOf(
            ValidTestCase(ION.newString("1234"), HiveVarchar("1234", 5), HiveVarcharWritable(HiveVarchar("1234", 5))),
            ValidTestCase(ION.newSymbol("1234"), HiveVarchar("1234", 5), HiveVarcharWritable(HiveVarchar("1234", 5)))
    )

    override val subjectOverflow = com.amazon.ionhiveserde.objectinspectors.IonTextToVarcharObjectInspector(5, false)

    override fun overflowTestCases() = listOf(
            OverflowTestCase(ION.newString("1234567"), HiveVarchar("12345", 5), HiveVarcharWritable(HiveVarchar("12345", 5))),
            OverflowTestCase(ION.newSymbol("1234567"), HiveVarchar("12345", 5), HiveVarcharWritable(HiveVarchar("12345", 5)))
    )
}
