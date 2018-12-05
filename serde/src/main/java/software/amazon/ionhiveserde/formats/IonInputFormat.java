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

package software.amazon.ionhiveserde.formats;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonWriter;
import software.amazon.ion.util.IonStreamUtils;
import software.amazon.ionhiveserde.IonFactory;
import software.amazon.ionhiveserde.configuration.HadoopProperties;
import software.amazon.ionhiveserde.configuration.source.HadoopConfigurationAdapter;

/**
 * Hadoop input format for Ion files, works for text and binary. Splits are based on top-level Ion values.
 */
public class IonInputFormat extends FileInputFormat {

    private static final Log LOG = LogFactory.getLog(IonInputFormat.class);

    @Override
    public RecordReader getRecordReader(final InputSplit split, final JobConf job, final Reporter reporter)
        throws IOException {

        LOG.fatal(job);

        final FileSplit fileSplit = (FileSplit) split;

        reporter.setStatus(fileSplit.toString());

        return new IonRecordReader(fileSplit, job);
    }

    private static class IonRecordReader implements RecordReader<LongWritable, BytesWritable> {
        private boolean isBinary(final FileSplit fileSplit, final JobConf job) throws IOException {
            final Path path = fileSplit.getPath();
            final FileSystem fs = path.getFileSystem(job);

            try (FSDataInputStream inputStream = fs.open(path)) {
                byte[] bytes = new byte[4];

                inputStream.readFully(bytes, 0, 4);

                return IonStreamUtils.isIonBinary(bytes);
            }
        }

        private final FSDataInputStream fsDataInputStream;
        private final HadoopProperties properties;
        private final IonFactory ionFactory;
        private final IonReader reader;
        private final ByteArrayOutputStream out;
        private final boolean isBinary;
        private final long start;

        IonRecordReader(final FileSplit fileSplit, final JobConf job)
            throws IOException {
            final Path path = fileSplit.getPath();
            final FileSystem fs = path.getFileSystem(job);

            isBinary = isBinary(fileSplit, job);

            start = fileSplit.getStart();

            fsDataInputStream = fs.open(path);
            fsDataInputStream.seek(fileSplit.getStart());

            properties = new HadoopProperties(new HadoopConfigurationAdapter(job));

            ionFactory = new IonFactory(properties);

            reader = ionFactory.newReader(fsDataInputStream.getWrappedStream());

            out = new ByteArrayOutputStream();
        }

        @Override
        public final LongWritable createKey() {
            return new LongWritable();
        }

        @Override
        public BytesWritable createValue() {
            return new BytesWritable();
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
            return fsDataInputStream.getPos() - start;
        }

        @Override
        public final boolean next(final LongWritable key, final BytesWritable value) throws IOException {
            try {
                if (reader.next() == null) {
                    return false;
                }

                out.reset();

                try (final IonWriter writer = newWriter(out)) {
                    writer.writeValue(reader);
                }

                byte[] newData = out.toByteArray();
                value.set(newData, 0, newData.length);

                return true;
            } catch (IonException e) {
                // skips rest of the split if ignoring malformed
                if (properties.getIgnoreMalformed()) {
                    return false;
                }

                throw e;
            }
        }

        private IonWriter newWriter(final ByteArrayOutputStream out) {
            return isBinary
                ? ionFactory.newBinaryWriter(out)
                : ionFactory.newTextWriter(out);
        }
    }
}
