package com.pinterest.memq.commons.storage.s3express;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestS3ExpressHelper {
    @Test
    public void testGenerateBucketUrl() throws Exception {
        String bucketName = "testbucket--use1-az5--x-s3";
        assertEquals(
                "https://testbucket--use1-az5--x-s3.s3express-use1-az5.us-east-1.amazonaws.com",
                S3ExpressHelper.generateBucketUrl(bucketName)
        );
    }

    @Test
    public void testValidateS3ExpressBucketName() throws Exception {
        String bucketName = "testbucket--use1-az5--x-s3";
        S3ExpressHelper.validateS3ExpressBucketName(bucketName);
    }

    @Test (expected = S3ExpressHelper.S3ExpressParsingException.class)
    public void testValidateS3ExpressBucketNameInvalid() throws Exception {
        String bucketName = "test-bucket";
        S3ExpressHelper.validateS3ExpressBucketName(bucketName);
    }

    @Test (expected = S3ExpressHelper.S3ExpressParsingException.class)
    public void getRegionFromBucketInvalid() throws Exception {
        String bucketName = "testbucket--unknownRegion-az5--x-s3";
        S3ExpressHelper.getRegionFromBucket(bucketName);
    }

    @Test
    public void testGetRegionFromBucket() throws Exception {
        String bucketName = "testbucket--use1-az5--x-s3";
        assertEquals("us-east-1", S3ExpressHelper.getRegionFromBucket(bucketName));
    }

    @Test
    public void testGetCurrentDateHr() {
        String currentDateHr = S3ExpressHelper.getCurrentDateHr();
        assertTrue(currentDateHr.matches("\\d{2}-\\d{2}-\\d{2}-\\d{2}"));
    }
}
