package com.pinterest.memq.commons.storage.s3express.keygenerator;

public abstract class S3ExpressObjectKeyGenerator {

    protected static final String SLASH = "/";
    protected static final String SEPARATOR = "_";

    protected String path;

    public S3ExpressObjectKeyGenerator(String path) {
        this.path = path;
    }

    /**
     * Generate the S3Express object key
     * @param firstMessageClientRequestId
     * @param firstMessageServerRequestId
     * @param attempt
     * @return
     */
    public abstract String generateObjectKey(long firstMessageClientRequestId,
                                             long firstMessageServerRequestId,
                                             int attempt);
}
