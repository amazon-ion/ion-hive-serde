// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * Utility functions for dealing with the local file system.
 */
@file:JvmName("FileSystemUtil")

package com.amazon.ionhiveserde.integrationtest

import java.io.File

/**
 * Creates the directory including parent directories if necessary.
 *
 * @see [File.mkdirs]
 *
 * @param path path to create the directory.
 * @return true if the directory was created, false otherwise.
 */
fun mkdir(path: String): Boolean = File(path).mkdirs()

/**
 * Removes path recursively.
 *
 * @param path path to be removed.
 * @return true if the path was removed, false otherwise.
 */
fun rm(path: String) = File(path).deleteRecursively()

/**
 * Sanitizes a String to be a valid path by replacing invalid sequences with the replacement string.
 *
 * @param replacement String to be used, defaults to `"_"`.
 *
 * @return sanitized String.
 */
fun String.sanitize(replacement: String = "_") = this.replace(Regex("[<>:,()]"), replacement)
