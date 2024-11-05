package com.pinterest.memq.commons.storage.s3express.keygenerator;

import java.text.SimpleDateFormat;

/**
 * Generate S3 object key with date and hour as prefix
 * The key format is: yyMMdd-HH/{path}/{firstMessageClientRequestId}_{firstMessageServerRequestId}_{attempt}
 * For example: 240101-01/test_topic/123_456_1
 *
 * The date-hour prefix can help the cleaning job to clean up the old data.
 * Until 2024/11/05, AWS S3Express does not support object lifecycle policy.
 * We need to clean up the old data manually or via scripts.
 * With this setup, we can easily clean up the old data by deleting the hourly prefix.
 */
public class DateHourKeyGenerator extends S3ExpressObjectKeyGenerator {

    private static final String DATE_HOUR_PATTERN = "yyMMdd-HH";

    public DateHourKeyGenerator(String path) {
        super(path);
    }

    @Override
    public String generateObjectKey(long firstMessageClientRequestId,
                                    long firstMessageServerRequestId,
                                    int attempt) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(getCurrentDateHr());
        keyBuilder.append(SLASH);
        keyBuilder.append(path);
        keyBuilder.append(SLASH);
        keyBuilder.append(firstMessageClientRequestId);
        keyBuilder.append(SEPARATOR);
        keyBuilder.append(firstMessageServerRequestId);
        keyBuilder.append(SEPARATOR);
        keyBuilder.append(attempt);
        return keyBuilder.toString();
    }

    protected static String getCurrentDateHr() {
        return new SimpleDateFormat(DATE_HOUR_PATTERN).format(new java.util.Date());
    }
}
