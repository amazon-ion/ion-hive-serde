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

package software.amazon.ionhiveserde.formats;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonWriter;
import software.amazon.ion.system.IonSystemBuilder;
import software.amazon.ion.util.IonStreamUtils;

/**
 * Hadoop input format for Ion files, works for text and binary. Splits are based on top-level Ion values.
 */
public class IonInputFormat extends FileInputFormat {

    // TODO create from job config https://github.com/amzn/ion-hive-serde/issues/15
    private static IonSystem ion = IonSystemBuilder.standard()
        .withStreamCopyOptimized(true) // use stream copy optimized to copy raw data when possible
        .build();

    @Override
    public RecordReader getRecordReader(final InputSplit split, final JobConf job, final Reporter reporter)
            throws IOException {
        final FileSplit fileSplit = (FileSplit) split;

        reporter.setStatus(fileSplit.toString());

        // Keep original file type to avoid converting from text <-> binary and allow the opportunity to use of stream
        // copy optimized writers
        if (isBinary(fileSplit, job)) {
            return new IonBinaryRecordReader(ion, fileSplit, job);
        }

        return new IonTextRecordReader(ion, fileSplit, job);
    }

    // FIXME avoid opening the file twice
    private boolean isBinary(final FileSplit fileSplit, final JobConf job) throws IOException {
        final Path path = fileSplit.getPath();
        final FileSystem fs = path.getFileSystem(job);

        try (FSDataInputStream inputStream = fs.open(path)) {
            byte[] bytes = new byte[4];

            inputStream.readFully(bytes, 0, 4);

            return IonStreamUtils.isIonBinary(bytes);
        }
    }

    private abstract static class IonRecordReaderBase<T extends Writable> implements RecordReader<LongWritable, T> {

        private final FSDataInputStream fsDataInputStream;
        private final IonReader reader;
        private final ByteArrayOutputStream out;

        IonRecordReaderBase(final IonSystem ion, final FileSplit fileSplit, final JobConf job)
                throws IOException {
            final Path path = fileSplit.getPath();
            final FileSystem fs = path.getFileSystem(job);

            fsDataInputStream = fs.open(path);
            fsDataInputStream.seek(fileSplit.getStart());

            reader = ion.newReader(fsDataInputStream.getWrappedStream());
            out = new ByteArrayOutputStream();
        }

        @Override
        public final LongWritable createKey() {
            return new LongWritable();
        }

        @Override
        public final long getPos() throws IOException {
            return fsDataInputStream.getPos();
        }

        @Override
        public final void close() throws IOException {
            this.fsDataInputStream.close();
        }

        @Override
        public final float getProgress() throws IOException {
            return 0;
        }

        @Override
        public final boolean next(final LongWritable key, final T value) throws IOException {
            if (reader.next() == null) {
                return false;
            }

            out.reset();

            try (final IonWriter writer = newWriter(out)) {
                writer.writeValue(reader);
            }

            writeValue(out.toByteArray(), value);

            return true;
        }

        abstract IonWriter newWriter(final OutputStream out);

        abstract void writeValue(final byte[] bytes, final T value);
    }

    private static class IonTextRecordReader extends IonRecordReaderBase<Text> {

        IonTextRecordReader(final IonSystem ion, final FileSplit fileSplit, final JobConf job) throws IOException {
            super(ion, fileSplit, job);
        }

        @Override
        public IonWriter newWriter(final OutputStream out) {
            return ion.newTextWriter(out);
        }

        @Override
        public void writeValue(final byte[] bytes, final Text value) {
            value.set(bytes);
        }

        @Override
        public Text createValue() {
            return new Text();
        }
    }

    private static class IonBinaryRecordReader extends IonRecordReaderBase<BytesWritable> {

        IonBinaryRecordReader(final IonSystem ion, final FileSplit fileSplit, final JobConf job) throws IOException {
            super(ion, fileSplit, job);
        }

        @Override
        public void writeValue(final byte[] bytes, final BytesWritable value) {
            value.set(bytes, 0, bytes.length);
        }

        @Override
        public BytesWritable createValue() {
            return new BytesWritable();
        }

        @Override
        public IonWriter newWriter(final OutputStream out) {
            return ion.newBinaryWriter(out);
        }
    }
}
