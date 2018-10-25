/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.objectinspectors

import org.apache.hadoop.hive.common.type.HiveChar
import org.apache.hadoop.hive.serde2.io.HiveCharWritable
import org.apache.hadoop.io.Text
import org.junit.Test
import software.amazon.ion.IonText
import software.amazon.ionhiveserde.ION
import kotlin.test.assertEquals

class IonTextToCharObjectInspectorTest
    : AbstractOverflowablePrimitiveObjectInspectorTest<IonText, HiveCharWritable, HiveChar>() {

    override val subject = IonTextToCharObjectInspector(5, true)
    override fun validTestCases() = listOf(
            ValidTestCase(ION.newString("1234"), HiveChar("1234", 5), HiveCharWritable(HiveChar("1234", 5))),
            ValidTestCase(ION.newSymbol("1234"), HiveChar("1234", 5), HiveCharWritable(HiveChar("1234", 5)))
    )

    override val subjectOverflow = IonTextToCharObjectInspector(5, false)
    override fun overflowTestCases() = listOf(
        OverflowTestCase(ION.newString("1234567"), HiveChar("12345", 5), HiveCharWritable(HiveChar("12345", 5))),
        OverflowTestCase(ION.newSymbol("1234567"), HiveChar("12345", 5), HiveCharWritable(HiveChar("12345", 5)))
    )
}