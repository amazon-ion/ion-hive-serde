// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonText
import com.amazon.ionhiveserde.ION
import org.apache.hadoop.io.Text

class IonTextToStringObjectInspectorTest : AbstractIonPrimitiveJavaObjectInspectorTest<IonText, Text, String>() {

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonTextToStringObjectInspector()
    override fun validTestCases() = listOf(
            ValidTestCase(ION.newString("some string"), "some string", Text("some string")),
            ValidTestCase(ION.newSymbol("some string"), "some string", Text("some string"))
    )

}
