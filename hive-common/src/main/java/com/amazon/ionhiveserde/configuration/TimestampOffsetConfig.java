// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.configuration;

import com.amazon.ionhiveserde.configuration.source.RawConfiguration;

/**
 * Encapsulates the timestamp.serialization_offset configuration.
 */
class TimestampOffsetConfig {
    private static final String DEFAULT_OFFSET_KEY = "ion.timestamp.serialization_offset";
    private static final String DEFAULT_OFFSET = "Z";
    private final int timestampOffsetInMinutes;

    /**
     * Constructor.
     *
     * @param configuration raw configration.
     */
    TimestampOffsetConfig(final RawConfiguration configuration) {
        timestampOffsetInMinutes = parseOffset(configuration.getOrDefault(DEFAULT_OFFSET_KEY, DEFAULT_OFFSET));
    }

    /**
     * Returns the timestamp timestampOffsetInMinutes in minutes to use when serializing and deserializing Ion
     * timestamps.
     *
     * @return timestamp timestampOffsetInMinutes in minutes to be used.
     */
    int getTimestampOffsetInMinutes() {
        return timestampOffsetInMinutes;
    }

    /**
     * Parses the string representation of an offset.
     *
     * @param offsetText text to parse.
     * @return offset in minutes.
     * @throws IllegalArgumentException if it cannot correctly parse offsetText.
     */
    private static int parseOffset(final String offsetText) {
        if (offsetText == null) {
            throw new IllegalArgumentException("offset text cannot be null");
        }

        int length = offsetText.length();

        if (length < 1) {
            throw new IllegalArgumentException("offset text cannot be empty");
        }

        char firstChar = offsetText.charAt(0);
        if (firstChar == 'Z' && length == 1) {
            return 0;
        }

        int offset = 0;
        int position = 0;
        int signal = 1;

        if (firstChar == '+' || firstChar == '-') {
            if (length != 6) {
                throw new IllegalArgumentException(
                    "wrong size for offset text, must be 6 characters when including the sign");
            }
            if (firstChar == '-') {
                signal = -1;
            }
            position += 1;
        } else if (length != 5) {
            throw new IllegalArgumentException(
                "wrong size for offset text, must be 5 characters when not including the sign");
        }

        // read hours
        int hours = readDigits(offsetText, position, position + 2);
        if (hours < 0 || hours > 23) {
            throw new IllegalArgumentException("offset text hours must be between 0 and 23 inclusive");
        }
        position += 2;
        offset = hours * 60;

        // check for ':'
        if (offsetText.charAt(position) != ':') {
            throw new IllegalArgumentException("missing : between hour and minute");
        }
        position += 1;

        // read minutes
        int minutes = readDigits(offsetText, position, position + 2);
        if (minutes < 0 || minutes > 59) {
            throw new IllegalArgumentException("offset text minutes must be between 0 and 59 inclusive");
        }
        offset += minutes;

        return offset * signal;
    }

    private static int readDigits(final String text, final int start, final int end) {
        try {
            return Integer.parseInt(text.substring(start, end));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("offset text hours is not a number");
        }
    }
}
