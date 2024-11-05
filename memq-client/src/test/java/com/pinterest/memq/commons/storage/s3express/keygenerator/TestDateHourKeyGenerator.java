package com.pinterest.memq.commons.storage.s3express.keygenerator;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestDateHourKeyGenerator {
    @Test
    public void testGetCurrentDateHr() {
        String currentDateHr = DateHourKeyGenerator.getCurrentDateHr();
        assertTrue(currentDateHr.matches("\\d{2}\\d{2}\\d{2}-\\d{2}"));
    }
}
