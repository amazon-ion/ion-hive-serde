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

import junitparams.JUnitParamsRunner
import junitparams.NamedParameters
import junitparams.Parameters
import org.apache.hadoop.io.BytesWritable
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ion.IonLob
import software.amazon.ionhiveserde.ION
import software.amazon.ionhiveserde.ionNull
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@RunWith(JUnitParamsRunner::class)
class IonLobToBinaryObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonLob, BytesWritable, ByteArray>() {

    // CLOBs are encoded as 7 bit ASCII, see http://amzn.github.io/ion-docs/docs/spec.html#clob
    private val raw = "12345678".toByteArray(charset = Charsets.US_ASCII)

    override val subject = IonLobToBinaryObjectInspector()
    override fun validTestCases() = listOf(
            ValidTestCase(ION.newClob(raw), raw, BytesWritable(raw)),
            ValidTestCase(ION.newBlob(raw), raw, BytesWritable(raw))
    )

    @Test
    @Parameters(method = "validTestCases")
    override fun getPrimitiveJavaObject(testCase: ValidTestCase<out IonLob, BytesWritable, ByteArray>) {
        val actual = subject.getPrimitiveJavaObject(testCase.ionValue) as ByteArray

        assertTrue(byteArrayEquals(raw, actual))
    }

    private fun byteArrayEquals(lhs: ByteArray, rhs: ByteArray): Boolean {
        return lhs.size == rhs.size && lhs.foldIndexed(true) { index, acc, b -> acc && rhs[index] == b }
    }
}