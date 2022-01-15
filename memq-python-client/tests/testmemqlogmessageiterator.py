'''
Created on Feb 3, 2021

@author: ambudsharma
'''
import unittest
from io import BytesIO
from memq.inputstream import DataInputStream
import base64
from memq.memqlogmessageiterator import Deserializer, MemqLogMessage,\
    MemqLogMessageIterator

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_uncompressed_batch_iterator(self):
        # raw base64 uncompressed Batch bytes from Java client
        base64str = "AAAAHAAAAAIAAAAAAAAAIAAAAJIAAAABAAAAsgAAAJIAKABkABUErBAAJQAAAXdqDH1EAAAAAAAAAAGRLVIQAAAAAAIAAABqACAAAAF3agx9QwgAAAAAAAAAAAANAAR0ZXN0AAV2YWx1ZQAAAAAAAAALdGVzdDEyMzEyMzEAIAAAAXdqDH1DCAAAAAAAAAABAA0ABHRlc3QABXZhbHVlAAAAAAAAAAt0ZXN0MTIzMTIzMQAoAGQAFQSsEAAlAAABd2oMfUUAAAAAAAAAAf6kvmoAAAAAAgAAAGoAIAAAAXdqDH1ECAAAAAAAAAAAAA0ABHRlc3QABXZhbHVlAAAAAAAAAAt0ZXN0MTIzMTIzMQAgAAABd2oMfUUIAAAAAAAAAAEADQAEdGVzdAAFdmFsdWUAAAAAAAAAC3Rlc3QxMjMxMjMx"
        content = base64.b64decode(base64str)
        test = BytesIO(content)
        test.seek(0)
        stream = DataInputStream(test)
        
        header_deserializer = Deserializer()
        value_deserializer = Deserializer()
        curr_notification_obj = { 
            MemqLogMessage.INTERNAL_FIELD_TOPIC:"test",
            MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE:content.__sizeof__(),
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID:0,
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP:0,
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET:0  
        }
        itr = MemqLogMessageIterator("test", stream, curr_notification_obj, header_deserializer, value_deserializer)
        
        counter = 0
        while itr.has_next():
            message = itr.next()
            counter += 1
            assert "test1231231" == message.value.decode('ascii')
        
        assert counter == 4, "There should be 4 messages read from this batch"

    def test_gzip_batch_iterator(self):
        base64str = "AAAAKAAAAAMAAAAAAAAALAAAAHYAAAABAAAAogAAAHMAAAACAAABFQAAAHMAKABkABUErBAAJQAAAXdqfK1xAAAAAAAAAAFCystcAQAAAAIAAABOH4sIAAAAAAAAAGJQYGBgLM+qWVvAwQADvAwsJanFJQysZYk5palQQW6QkKGRMQgBAAAA//9igGkqhGliJKwJAAAA//8DAOE0yN1qAAAAACgAZAAVBKwQACUAAAF3anytcgAAAAAAAAABz+4rrQEAAAACAAAASx+LCAAAAAAAAABiUGBgYCzPqllbxMEAA7wMLCWpxSUMrGWJOaWpUEFukJChkTEIAQAAAP//YsDQxEhYEwAAAP//AwBlNttIagAAAAAoAGQAFQSsEAAlAAABd2p8rXMAAAAAAAAAAbP5aWQBAAAAAgAAAEsfiwgAAAAAAAAAYlBgYGAsz6pZW8zBAAO8DCwlqcUlDKxliTmlqVBBbpCQoZExCAEAAAD//2LA0MRIWBMAAAD//wMAFqtGF2oAAAA="
        content = base64.b64decode(base64str)
        test = BytesIO(content)
        stream = DataInputStream(test)
        
        header_deserializer = Deserializer()
        value_deserializer = Deserializer()
        curr_notification_obj = { 
            MemqLogMessage.INTERNAL_FIELD_TOPIC:"test",
            MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE:content.__sizeof__(),
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID:0,
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP:0,
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET:0  
        }
        itr = MemqLogMessageIterator("test", stream, curr_notification_obj, header_deserializer, value_deserializer)
        
        counter = 0
        while itr.has_next():
            message = itr.next()
            counter += 1
            assert "test1231231" == message.value.decode('ascii')
        
        assert counter == 6, "There should be 6 messages read from this batch"
        
    def test_zstd_batch_iterator(self):
        base64str = "AAAAQAAAAAUAAAAAAAAARAAAAHMAAAABAAAAtwAAAHAAAAACAAABJwAAAHAAAAADAAABlwAAAHAAAAAEAAACBwAAAHAAKABkABUErBAAJQAAAXdqfduyAAAAAAAAAAFUPvwXAgAAAAIAAABLKLUv/QBYhAEAdAIAIAAAAXdqfduwCAANAAR0ZXN0AAV2YWx1ZQALdGVzdDEyMzEyMzECAGBUAtABZAAAELIBAwDACSCHCxUEAQAAACgAZAAVBKwQACUAAAF3an3bswAAAAAAAAABCOO92wIAAAACAAAASCi1L/0AWIQBAHQCACAAAAF3an3bswgADQAEdGVzdAAFdmFsdWUAC3Rlc3QxMjMxMjMxAgBgVALQAUwAAAgBAgDAEewpIAEAAAAoAGQAFQSsEAAlAAABd2p927QAAAAAAAAAAc70T7kCAAAAAgAAAEgotS/9AFiEAQB0AgAgAAABd2p927QIAA0ABHRlc3QABXZhbHVlAAt0ZXN0MTIzMTIzMQIAYFQC0AFMAAAIAQIAwBHsKSABAAAAKABkABUErBAAJQAAAXdqfdu2AAAAAAAAAAEdbbejAgAAAAIAAABIKLUv/QBYhAEAdAIAIAAAAXdqfdu2CAANAAR0ZXN0AAV2YWx1ZQALdGVzdDEyMzEyMzECAGBUAtABTAAACAECAMAR7CkgAQAAACgAZAAVBKwQACUAAAF3an3btwAAAAAAAAABdKFLrgIAAAACAAAASCi1L/0AWIQBAHQCACAAAAF3an3btwgADQAEdGVzdAAFdmFsdWUAC3Rlc3QxMjMxMjMxAgBgVALQAUwAAAgBAgDAEewpIAEAAA=="
        content = base64.b64decode(base64str)
        test = BytesIO(content)
        stream = DataInputStream(test)
        
        header_deserializer = Deserializer()
        value_deserializer = Deserializer()
        curr_notification_obj = { 
            MemqLogMessage.INTERNAL_FIELD_TOPIC:"test",
            MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE:content.__sizeof__(),
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID:0,
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP:0,
            MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET:0  
        }
        itr = MemqLogMessageIterator("test", stream, curr_notification_obj, header_deserializer, value_deserializer)
        
        counter = 0
        while itr.has_next():
            message = itr.next()
            counter += 1
            assert "test1231231" == message.value.decode('ascii')
        
        assert counter == 10, "There should be 10 messages read from this batch"

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()