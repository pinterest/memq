package com.pinterest.memq.commons.storage.s3express;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class S3ExpressHelper {
    public static class S3ExpressParsingException extends Exception {
        public S3ExpressParsingException(String message) {
            super(message);
        }
    }

    /**
     * Map from region code to AWS region name
     * The region code is the second part of the bucket name, e.g. "use1" in "s3express--use1--us-east-1--x-s3"
     * The region name is the AWS region name, e.g. "us-east-1"
     */
    public static final Map<String, String> awsRegionMap = new HashMap<String, String>() {{
        put("use1", "us-east-1");
    }};

    /**
     * Validate the bucket name is a valid s3express bucket name
     * https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html#bucketnamingrules-directorybucket
     * @param bucketName the bucket name to validate
     * @throws S3ExpressParsingException
     */
    public static void validateS3ExpressBucketName(String bucketName) throws S3ExpressParsingException {
        if (!bucketName.matches(".*--.*-.*--x-s3")) {
            throw new S3ExpressParsingException("Invalid s3express bucket name: " + bucketName);
        }
    }

    /**
     * Generate the bucket URL from the bucket name
     * @param bucketName
     * @return the bucket URL
     * @throws S3ExpressParsingException
     */
    public static String generateBucketUrl(String bucketName) throws S3ExpressParsingException{
        validateS3ExpressBucketName(bucketName);
        String region = getRegionFromBucket(bucketName);
        String azName = bucketName.split("--")[1];
        return String.format("https://%s.s3express-%s.%s.amazonaws.com/", bucketName, azName, region);
    }

    /**
     * Get the region name from the bucket name
     * @param bucketName
     * @return the region name
     * @throws S3ExpressParsingException
     */
    public static String getRegionFromBucket(String bucketName) throws S3ExpressParsingException {
        validateS3ExpressBucketName(bucketName);
        String regionCode = bucketName.split("--")[1].split("-")[0];
        if (!awsRegionMap.containsKey(regionCode)) {
            throw new S3ExpressParsingException(
                    String.format("Unknown region code %s from bucket name %s", regionCode, bucketName));
        }
        return awsRegionMap.get(regionCode);
    }

    /**
     * Get the current date in the format of "yyMMdd-HH"
     * This is used to generate the hourly folder name. It helps the cleaning job to clean up the old data.
     * Until 2024/11/05, AWS S3Express does not support object lifecycle policy. We need to clean up the old data manually.
     * With this setup, we can easily clean up the old data by deleting the hourly folder.
     * @return the current date in the format of "yyMMdd-HH", e.g. "241105-12" for 2024/11/05 12:00 PM UTC.
     */
    public static String getCurrentDateHr() {
        return new SimpleDateFormat("yyMMdd-HH").format(new java.util.Date());
    }
}
