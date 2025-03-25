// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonValue
import com.amazon.ionhiveserde.ION
import org.apache.hadoop.hive.common.type.HiveDecimal
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
import org.junit.Test
import org.junit.jupiter.api.assertThrows
import java.lang.NumberFormatException

class IonNumberToDecimalObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonValue, HiveDecimalWritable, HiveDecimal>() {

    override fun validTestCases() = listOf(
            ValidTestCase(ION.newDecimal(1), HiveDecimal.ONE, HiveDecimalWritable(HiveDecimal.ONE)),
            ValidTestCase(ION.newDecimal(0), HiveDecimal.ZERO, HiveDecimalWritable(HiveDecimal.ZERO)),
            ValidTestCase(ION.newFloat(1), HiveDecimal.ONE, HiveDecimalWritable(HiveDecimal.ONE)),
            ValidTestCase(ION.newFloat(0), HiveDecimal.ZERO, HiveDecimalWritable(HiveDecimal.ZERO)),
            ValidTestCase(ION.newInt(1), HiveDecimal.ONE, HiveDecimalWritable(HiveDecimal.ONE)),
            ValidTestCase(ION.newInt(0), HiveDecimal.ZERO, HiveDecimalWritable(HiveDecimal.ZERO)),
    )

    @Test
    fun `non-numeric floats cannot be converted`() {
        assertThrows<NumberFormatException> { subject.getPrimitiveJavaObject(ION.newFloat(Double.NaN)) }
        assertThrows<NumberFormatException> { subject.getPrimitiveJavaObject(ION.newFloat(Double.NEGATIVE_INFINITY)) }
        assertThrows<NumberFormatException> { subject.getPrimitiveJavaObject(ION.newFloat(Double.POSITIVE_INFINITY)) }
    }

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonNumberToDecimalObjectInspector()
}

