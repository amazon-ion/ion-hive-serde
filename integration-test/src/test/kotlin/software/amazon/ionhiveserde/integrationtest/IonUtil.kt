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

/**
 * Ion related utility methods to be used in the test scope.
 */
@file:JvmName("IonUtil")

package software.amazon.ionhiveserde.integrationtest

import software.amazon.ion.IonStruct
import software.amazon.ion.IonSystem
import software.amazon.ion.IonValue
import software.amazon.ion.IonWriter
import software.amazon.ion.system.IonSystemBuilder
import java.io.File
import java.io.FileOutputStream

/** Standard Ion system with no catalog. */
val ION: IonSystem = IonSystemBuilder.standard().build()

// Extensions

/**
 * [IonSystem.singleValue] from a local path.
 */
fun IonSystem.singleValueFromPath(path: String): IonValue = this.singleValue(File(path).readBytes())

/** [IonSystem.newTextWriter] from the local path. */
fun IonSystem.newTextWriterFromPath(path: String): IonWriter = ION.newTextWriter(FileOutputStream(File(path)))

/** [IonSystem.newBinaryWriterFromPath] from the local path. */
fun IonSystem.newBinaryWriterFromPath(path: String): IonWriter = ION.newBinaryWriter(FileOutputStream(File(path)))

/** Struct keys as a sequence. */
fun IonStruct.keys(): Sequence<String> = this.asSequence().map { it.fieldName }
