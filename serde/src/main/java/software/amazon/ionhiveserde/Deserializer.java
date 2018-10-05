/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde;

import java.io.IOException;
import org.apache.hadoop.hive.serde2.SerDeException;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonNull;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolToken;

/**
 * Reads an Ion stream into the DOM model skipping null values instead of creating {@link IonNull} objects. This is
 * necessary for primitives since Hive handles the null check on their side and will not consider {@link IonNull}
 * as a Java null
 */
class Deserializer {

    private static final String ERROR_MESSAGE = "There must be a single ion value";

    /**
     * Deserialize a byte array containing a single Ion value encoded in either Ion binary or Ion text.
     *
     * @param ion Ion system used to create readers and writers
     * @param bytes byte array containing the single Ion value
     * @param length number of bytes to deserialize
     * @return the Ion value that was encoded in the bytes array
     */
    static IonValue deserialize(final IonSystem ion, final byte[] bytes, final int length) throws SerDeException {
        final IonDatagram datagram = ion.newDatagram();

        try (
            final IonReader reader = ion.newReader(bytes, 0, length);
            final IonWriter writer = ion.newWriter(datagram)
        ) {

            if (reader.next() == null) {
                throw new IllegalArgumentException(ERROR_MESSAGE);
            }

            deserializeValue(writer, reader);

            if (reader.next() != null) {
                throw new IllegalArgumentException(ERROR_MESSAGE);
            }

        } catch (IOException e) {
            throw new SerDeException(e);
        }

        return datagram.get(0);
    }

    // based on https://github.com/amzn/ion-java/blob/master/src/software/amazon/ion/impl/bin/AbstractIonWriter.java#L86
    private static void deserializeValue(final IonWriter writer, final IonReader reader) throws IOException {
        // skip nulls in structs instead of writing an IonNull
        if (reader.isInStruct() && reader.isNullValue()) {
            return;
        }

        final SymbolToken fieldName = reader.getFieldNameSymbol();
        if (fieldName != null && reader.isInStruct()) {
            writer.setFieldNameSymbol(fieldName);
        }

        final SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
        if (annotations.length > 0) {
            writer.setTypeAnnotationSymbols(annotations);
        }

        final IonType type = reader.getType();
        if (reader.isNullValue()) {
            writer.writeNull(type);
            return;
        }

        switch (type) {
            case BOOL:
                writer.writeBool(reader.booleanValue());
                break;

            case INT:
                switch (reader.getIntegerSize()) {
                    case INT:
                        writer.writeInt(reader.intValue());
                        break;

                    case LONG:
                        writer.writeInt(reader.longValue());
                        break;

                    case BIG_INTEGER:
                        writer.writeInt(reader.bigIntegerValue());
                        break;

                    default:
                        throw new IllegalStateException();
                }
                break;

            case FLOAT:
                writer.writeFloat(reader.doubleValue());
                break;

            case DECIMAL:
                writer.writeDecimal(reader.bigDecimalValue());
                break;

            case STRING:
                writer.writeString(reader.stringValue());
                break;

            case SYMBOL:
                writer.writeSymbolToken(reader.symbolValue());
                break;

            case CLOB:
                writer.writeClob(reader.newBytes());
                break;

            case BLOB:
                writer.writeClob(reader.newBytes());
                break;

            case TIMESTAMP:
                writer.writeTimestamp(reader.timestampValue());
                break;

            case SEXP:
            case LIST:
            case STRUCT:
                reader.stepIn();
                writer.stepIn(type);

                while (reader.next() != null) {
                    deserializeValue(writer, reader);
                }

                writer.stepOut();
                reader.stepOut();
                break;

            default:
                throw new IllegalStateException("Unexpected type: " + type);
        }
    }
}
