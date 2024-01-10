// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Ion related utility methods to be used in the test scope.
 */
@file:JvmName("IonUtil")

package com.amazon.ionhiveserde.integrationtest

import com.amazon.ion.*
import com.amazon.ion.system.*
import java.io.File
import java.io.FileOutputStream

/** Standard Ion system with no catalog. */
val DOM_FACTORY: IonSystem = IonSystemBuilder.standard().build()

/** Creates Ion text writer from the local path. */
fun newTextWriterFromPath(path: String): IonWriter =
        IonTextWriterBuilder.standard().build(FileOutputStream(File(path)))

/** Creates Ion binary writer from the local path. */
fun newBinaryWriterFromPath(
        path: String,
        catalog: IonCatalog = SimpleCatalog(),
        vararg imports: SymbolTable): IonWriter {
    return IonBinaryWriterBuilder.standard()
            .withCatalog(catalog)
            .withImports(*imports)
            .build(FileOutputStream(File(path)))
}

// Extensions -------------------------------------------------------------------------------------------------------

/**
 * [IonSystem.singleValue] from a local path.
 */
fun IonSystem.singleValueFromPath(path: String): IonValue = this.singleValue(File(path).readBytes())

/** Struct keys as a sequence. */
fun IonStruct.keys(): Sequence<String> = this.asSequence().map { it.fieldName }

/** Write all values from a string containing an ion document */
fun IonWriter.writeValues(ionDocument: String) =
        IonReaderBuilder.standard().build(ionDocument).use { r -> this.writeValues(r) }
