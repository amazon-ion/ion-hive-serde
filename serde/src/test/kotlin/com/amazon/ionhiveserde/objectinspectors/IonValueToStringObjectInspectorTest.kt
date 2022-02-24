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

import com.amazon.ion.IonValue
import com.amazon.ionhiveserde.ION
import org.apache.hadoop.io.Text

class IonValueToStringObjectInspectorTest : AbstractIonPrimitiveJavaObjectInspectorTest<IonValue, Text, String>() {

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonValueToStringObjectInspector()
    override fun validTestCases() = listOf(
            ValidTestCase(ION.newString("some string"), "some string", Text("some string")),
            ValidTestCase(ION.newDecimal(12.1), "12.1", Text("12.1")),
            ValidTestCase(ION.newBool(true), "true", Text("true")),
    )
}
