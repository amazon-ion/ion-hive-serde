package software.amazon.ionhiveserde;

import java.io.IOException;
import org.apache.hadoop.hive.serde2.SerDeException;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolToken;

/**
 * Reads an Ion stream into the DOM model skipping null values instead of creating IonNull objects. This is necessary
 * for primitives since Hive handles the null check on their side and will not consider IonNull as java null
 */
class Deserializer {
    private static final String ERROR_MESSAGE = "There must be a single ion value";

    static IonValue deserialize(final IonSystem ion, final byte[] bytes) throws SerDeException {
        final IonDatagram datagram = ion.newDatagram();

        try (
            final IonReader reader = ion.newReader(bytes);
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
