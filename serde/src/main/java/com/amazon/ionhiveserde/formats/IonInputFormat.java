/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.formats;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;
import com.amazon.ion.util.IonStreamUtils;
import com.amazon.ionhiveserde.IonFactory;
import com.amazon.ionhiveserde.configuration.HadoopProperties;
import com.amazon.ionhiveserde.configuration.source.HadoopConfigurationAdapter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Hadoop input format for Ion files, works for text and binary. Splits are based on top-level Ion values.
 */
public class IonInputFormat extends FileInputFormat {

    private static final Log LOG = LogFactory.getLog(IonInputFormat.class);

    @Override
    protected boolean isSplitable(final FileSystem fs, final Path filename) {
        return false;
    }

    @Override
    public RecordReader getRecordReader(final InputSplit split, final JobConf job, final Reporter reporter)
        throws IOException {

        LOG.fatal(job);

        final FileSplit fileSplit = (FileSplit) split;

        reporter.setStatus(fileSplit.toString());

        return new IonRecordReader(fileSplit, job);
    }

    private static class IonRecordReader implements RecordReader<LongWritable, BytesWritable> {
        private final InputStream in;

        private final HadoopProperties properties;
        private final IonFactory ionFactory;
        private final IonReader reader;
        private final ByteArrayOutputStream out;
        private final boolean isBinary;
        private final long start;
        private final long end;

        private boolean isBinary(final InputStream inputStream) throws IOException {
            try (InputStream input = inputStream) {
                byte[] bytes = new byte[4];

                input.read(bytes, 0, 4);

                return IonStreamUtils.isIonBinary(bytes);
            }
        }

        IonRecordReader(final FileSplit fileSplit, final JobConf job)
                throws IOException {
            start = fileSplit.getStart();
            // We don't handle the case that a FileSplit is starting at a position other than 0.
            if (start != 0) {
                throw new IOException(String.format("File split is at position %d, expected 0.", start));
            }
            end = start + fileSplit.getLength();

            try (InputStream binCheck = getInputStream(fileSplit, job)) {
                isBinary = isBinary(binCheck);
            }

            in = getInputStream(fileSplit, job);

            properties = new HadoopProperties(new HadoopConfigurationAdapter(job));

            ionFactory = new IonFactory(properties);

            reader = ionFactory.newReader(in);

            out = new ByteArrayOutputStream();
        }

        private InputStream getInputStream(final FileSplit fileSplit, final JobConf job) throws IOException {
            final Path path = fileSplit.getPath();
            final FileSystem fs = path.getFileSystem(job);

            CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
            final CompressionCodec codec = compressionCodecs.getCodec(path);

            FSDataInputStream inputStream = fs.open(path);

            return (codec == null) ? inputStream : codec.createInputStream(inputStream);
        }

        @Override
        public final LongWritable createKey() {
            return new LongWritable();
        }

        @Override
        public BytesWritable createValue() {
            return new BytesWritable();
        }

        // CompressionInputStream and FSDataInputStream are both Seekable, but have no common
        // Seekable parents. We can confidently cast our InputStream to Seekable.
        @Override
        public final long getPos() throws IOException {
            return ((Seekable)in).getPos();
        }

        @Override
        public final void close() throws IOException {
            this.in.close();
        }

        // We don't know why `progress` is equal to 0 instead of 1 when start == end, what Hadoop is doing since long
        // times ago is setting it to 0.
        //
        // References:
        // * The original split support issue: https://issues.apache.org/jira/browse/HADOOP-451
        // * The original patch that introduced this: https://svn.apache.org/viewvc/lucene/hadoop/trunk/src/java/org/apa
        // che/hadoop/mapred/TextInputFormat.java?r1=469596&r2=488438&pathrev=502021&diff_format=h
        @Override
        public final float getProgress() throws IOException {
            float size = (this.end - this.start);
            float progress = (this.getPos() - this.start);
            return this.end == this.start ? 0.0F : Math.min(1.0F, progress / size);
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
