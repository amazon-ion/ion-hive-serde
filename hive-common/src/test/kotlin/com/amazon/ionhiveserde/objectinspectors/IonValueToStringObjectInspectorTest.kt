// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonType
import com.amazon.ion.IonValue
import com.amazon.ion.impl._Private_Utils
import com.amazon.ionhiveserde.ION
import org.apache.hadoop.io.Text

class IonValueToStringObjectInspectorTest : AbstractIonPrimitiveJavaObjectInspectorTest<IonValue, Text, String>() {

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonValueToStringObjectInspector()
    override fun validTestCases() = listOf(
            ValidTestCase(ION.newString("some string"), "some string", Text("some string")),
            ValidTestCase(ION.newSymbol("some string"), "some string", Text("some string")),
            ValidTestCase(ION.newInt(12), "12", Text("12")),
            ValidTestCase(ION.newDecimal(12.1), "12.1", Text("12.1")),
            ValidTestCase(ION.newFloat(12345), "12345e0", Text("12345e0")),
            ValidTestCase(ION.newBool(true), "true", Text("true")),
            ValidTestCase(ION.newUtcTimestampFromMillis(999999999999), "2001-09-09T01:46:39.999Z", Text("2001-09-09T01:46:39.999Z")),
            ValidTestCase(generateTestIonStruct("field", ION.newInt(1)), "{field:1}", Text("{field:1}")),
            ValidTestCase(ION.newList(intArrayOf(1)), "[1]", Text("[1]")),
            ValidTestCase(ION.newSexp(intArrayOf(1)), "(1)", Text("(1)")),
            ValidTestCase(ION.newClob(_Private_Utils.utf8("test")), "{{\"test\"}}", Text("{{\"test\"}}")),
            ValidTestCase(ION.newBlob(byteArrayOf(1, 2, 3, 4, 5)), "{{AQIDBAU=}}", Text("{{AQIDBAU=}}")),
    )

    private fun generateTestIonStruct(field: String, value: IonValue): IonValue {
        val struct = ION.newEmptyStruct()
        struct.add(field, value)
        return struct
    }
}
