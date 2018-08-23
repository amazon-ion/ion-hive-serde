/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ionhiveserde.ION
import junitparams.JUnitParamsRunner
import junitparams.NamedParameters
import junitparams.Parameters
import org.apache.hadoop.io.BytesWritable
import org.junit.Test
import org.junit.runner.RunWith
import software.amazon.ion.IonLob
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@RunWith(JUnitParamsRunner::class)
class IonLobToBinaryObjectInspectorTest : AbstractIonPrimitiveJavaObjectInspectorTest() {

    override val subject = IonLobToBinaryObjectInspector()

    // CLOBs are encoded as 7 bit ASCII, see http://amzn.github.io/ion-docs/docs/spec.html#clob
    private val raw = "12345678".toByteArray(charset = Charsets.US_ASCII)

    @NamedParameters("lobs")
    fun lobs(): Array<IonLob> = arrayOf(ION.newClob(raw), ION.newBlob(raw))

    @Test
    @Parameters(named = "lobs")
    fun getPrimitiveWritableObject(value: IonLob) {
        val actual = subject.getPrimitiveWritableObject(value)

        assertEquals(BytesWritable(raw), actual)
    }

    @Test
    @Parameters(named = "lobs")
    fun getPrimitiveJavaObject(value: IonLob) {
        val actual = subject.getPrimitiveJavaObject(value) as ByteArray

        assertTrue(byteArrayEquals(raw, actual))
    }

    private fun byteArrayEquals(lhs: ByteArray, rhs: ByteArray): Boolean {
        return lhs.size == rhs.size && lhs.foldIndexed(true, { index, acc, b -> acc && rhs[index] == b })
    }
}