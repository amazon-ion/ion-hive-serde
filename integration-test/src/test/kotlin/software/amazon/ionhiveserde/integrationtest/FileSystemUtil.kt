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
 * Utility functions for dealing with the local file system.
 */
@file:JvmName("FileSystemUtil")

package software.amazon.ionhiveserde.integrationtest

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
