// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonLob
import com.amazon.ionhiveserde.ION
import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.apache.hadoop.io.BytesWritable
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertTrue

@RunWith(JUnitParamsRunner::class)
class IonLobToBinaryObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonLob, BytesWritable, ByteArray>() {

    // CLOBs are encoded as 7 bit ASCII, see http://amazon-ion.github.io/ion-docs/docs/spec.html#clob
    private val raw = "12345678".toByteArray(charset = Charsets.US_ASCII)

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonLobToBinaryObjectInspector()
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
