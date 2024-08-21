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

    public static final Map<String, String> awsRegionMap = new HashMap<String, String>() {{
        put("use1", "us-east-1");
    }};

    public static void validateS3ExpressBucketName(String bucketName) throws S3ExpressParsingException {
        if (!bucketName.matches(".*--.*-.*--x-s3")) {
            throw new S3ExpressParsingException("Invalid s3express bucket name: " + bucketName);
        }
    }

    public static String generateBucketUrl(String bucketName) throws S3ExpressParsingException{
        validateS3ExpressBucketName(bucketName);
        String region = getRegionFromBucket(bucketName);
        String azName = bucketName.split("--")[1];
        return String.format("https://%s.s3express-%s.%s.amazonaws.com/", bucketName, azName, region);
    }

    public static String getRegionFromBucket(String bucketName) throws S3ExpressParsingException {
        validateS3ExpressBucketName(bucketName);
        String regionCode = bucketName.split("--")[1].split("-")[0];
        if (!awsRegionMap.containsKey(regionCode)) {
            throw new S3ExpressParsingException(
                    String.format("Unknown region code %s from bucket name %s", regionCode, bucketName));
        }
        return awsRegionMap.get(regionCode);
    }

    public static String getCurrentDateHr() {
        return new SimpleDateFormat("yy-MM-dd-HH").format(new java.util.Date());
    }
}
