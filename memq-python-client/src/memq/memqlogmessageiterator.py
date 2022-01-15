'''
Created on Feb 2, 2021

@author: ambudsharma
'''

import collections
from memq.inputstream import DataInputStream
import gzip
from io import BytesIO
import logging
import zstandard


class MessageId:
    
    def __init__(self, array):
        self.array = array


class BatchHeader:
    '''
    classdocs
    '''

    def __init__(self, stream):
        '''
        Constructor
        '''
        stream.read_int();  # header length
        num_index_entries = stream.read_int();  # num index entries
        index = {}
        i = 0
        while i < num_index_entries:
            idx = stream.read_int()
            offset = stream.read_int()
            size = stream.read_int()
            index[idx] = IndexEntry(offset, size)
            i += 1
        self.messageIdx = collections.OrderedDict(index)
        logging.debug("Batch header index:" + str(self.messageIdx))


class IndexEntry:
    
    def __init__(self, offset, size):
        self.offset = offset
        self.size = size
        
    def __str__(self):
        return self.offset + " " + self.size


class MemqMessageHeader:
    
    def __init__(self, stream):
        self.headerLength = stream.read_short()
        self.verion = stream.read_short()
        self.addition_header_length = stream.read_short()
        if self.addition_header_length > 0:
            self.producer_address = stream.read(stream.read_byte())
            self.producer_epoch = stream.read_long()
            self.producer_request_id = stream.read_long()
        self.crc = stream.read_int()
        self.compression_type = stream.read_byte()
        self.log_message_count = stream.read_int()
        self.message_length = stream.read_int()
        
        attrs = vars(self)
        logging.info(', '.join("%s: %s" % item for item in attrs.items()))


class MemqLogMessage:
    '''
    classdocs
    '''
    INTERNAL_FIELD_TOPIC = "topic"
    INTERNAL_FIELD_WRITE_TIMESTAMP = "wts"
    INTERNAL_FIELD_NOTIFICATION_PARTITION_ID = "npi"
    INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP = "nrts"
    INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET = "npo"
    INTERNAL_FIELD_OBJECT_SIZE = "objectSize"
    INTERNAL_FIELD_MESSAGE_OFFSET = "mo"
    
    def __init__(self, message_id, headers, key, value):
        self.message_id = message_id
        self.headers = headers
        self.key = key
        self.value = value
        self.write_timestamp = -1
        self.message_offset_in_batch = -1
        self.notification_partition_id = -1
        self.notification_partition_offset = -1
        self.notification_read_timestamp = -1


class Commons:
    
    def checksum_matches(self, batch, crc):
        # run checksum match
        return True 
        
    def uncompress_stream(self, compression_type, batch_byte_stream):
        if compression_type == 0:
            # no compression
            return DataInputStream(batch_byte_stream)
        elif compression_type == 1:
            # gzip
            return DataInputStream(gzip.GzipFile(fileobj=batch_byte_stream, mode='rb'))
        elif compression_type == 2:
            # zstd
            zstd = zstandard.ZstdDecompressor();
            output_stream = BytesIO()
            zstd.copy_stream(batch_byte_stream, output_stream)
            output_stream.seek(0)
            return DataInputStream(output_stream)
        else:
            raise Exception("Unknown compression type")


class Deserializer:
    
    def deserialize(self, bytes):
        return bytes


class MemqLogMessageIterator(object):
    '''
    classdocs
    '''

    def __init__(self, cluster, stream, curr_notification_obj, header_deserializer, value_deserializer, skip_header_read=False):
        '''
        Constructor
        '''
        self.cluster = cluster
        self.stream = stream
        self.curr_notification_obj = curr_notification_obj
        try:
            self.topic = curr_notification_obj[MemqLogMessage.INTERNAL_FIELD_TOPIC]
            self.object_size = curr_notification_obj[MemqLogMessage.INTERNAL_FIELD_OBJECT_SIZE]
            self.notification_partition_id = curr_notification_obj[MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_ID]
            self.notification_partition_offset = curr_notification_obj[MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_PARTITION_OFFSET]
            self.notification_read_timestamp = curr_notification_obj[MemqLogMessage.INTERNAL_FIELD_NOTIFICATION_READ_TIMESTAMP]
        except Exception as e:
            # do nothing
            print(e)
        
        self.current_message_offset = 0
        self.header_deserializer = header_deserializer
        self.value_deserializer = value_deserializer
        
        # add metrics registry
        if skip_header_read == False:
            logging.debug("Parsing batch header")
            self.batch_header = BatchHeader(stream)
            self.num_batches = len(self.batch_header.messageIdx)
            logging.debug("Parsed batch header and found " + str(self.num_batches) + " batches")
        self.read_header_and_load_batch()
        
    def has_next(self) -> bool:
        # DONE
        try:
            if self.num_batches == 0:
                return self.messages_to_read > 0
        except Exception as e:
            raise e
        return True
    
    def next(self) -> MemqLogMessage:
        # DONE
        if self.messages_to_read > 0:
            try:
                return self.get_memq_log_message()
            except Exception as e:
                raise e
        else:
            try:
                if self.num_batches > 0:
                    if self.read_header_and_load_batch():
                        return self.next()
            except Exception as e:
                raise e
        raise Exception("No next")
        
    def read_header_and_load_batch(self) -> bool:
        '''
        Reads the next batch's header and returns whether there are bytes to read
        true if there are batch bytes to read, false otherwise
        '''
        try:
            self.message_id_hash = None
            self.header = self.read_header()
            batch_bytes = self.stream.read(self.header.message_length)
            if Commons.checksum_matches(self, batch_bytes, self.header.crc) == False:
                raise Exception("CRC checksum mismatch")
            
            self.uncompressed_batch_input_stream = Commons.uncompress_stream(self, compression_type=self.header.compression_type, batch_byte_stream=BytesIO(batch_bytes))
        except Exception as e:
            raise e
        return self.messages_to_read > 0
        
    def read_header(self) -> MemqMessageHeader:
        # DONE
        header = MemqMessageHeader(self.stream)
        logging.debug("Num batches " + str(self.num_batches))
        self.num_batches -= 1
        if header.message_length > self.object_size:
            raise Exception("Invalid batch length, length greater than object " + header.message_length)
        elif header.message_length < 0:
            raise Exception("Invalid batch length, length less than 0")
        else:
            self.messages_to_read = header.log_message_count
            return header
        
    def get_memq_log_message(self) -> MemqLogMessage:
        '''
        Reads the uncompressed input stream and returns a MemqLogMessage
        '''
        # DONE
        stream = self.uncompressed_batch_input_stream;
        log_message_internal_fields_length = stream.read_short()
        write_timestamp = 0
        message_id = None
        headers = None
        if log_message_internal_fields_length > 0:
            write_timestamp = stream.read_long()
            # message_id
            message_id_length = stream.read_byte()
            if  message_id_length > 0:
                array = stream.read(message_id_length)
                message_id = MessageId(array)
            
            # headers    
            header_length = stream.read_short()
            if header_length:
                headers = self.deserialize_headers(header_length, stream)
        
        # read key
        key_length = stream.read_int()
        key_bytes = None
        if key_length > 0:
            key_bytes = stream.read(key_length)
        
        # read value
        value_length = stream.read_int()
        value_bytes = stream.read(value_length)
        self.messages_to_read -= 1
        
        log_message = MemqLogMessage(message_id, headers, self.header_deserializer.deserialize(key_bytes), self.value_deserializer.deserialize(value_bytes))
        self.populate_internal_fields(write_timestamp, log_message)
        self.current_message_offset += 1
        return log_message
                
    def populate_internal_fields(self, write_timestamp, log_message):
        # DONE
        log_message.write_timestamp = write_timestamp
        log_message.message_offset_in_batch = self.current_message_offset
        log_message.notification_partition_id = self.notification_partition_id
        log_message.notification_partition_offset = self.notification_partition_offset
        log_message.notification_read_timestamp = self.notification_read_timestamp
    
    def deserialize_headers(self, header_length, stream) -> dict:
        # DONE
        header_map = {}
        while header_length > 0:
            key_length = stream.read_short()
            key = stream.read(key_length)
            value_length = stream.read_short()
            value = stream.read(value_length)
            header_map[key] = value
            header_length -= (4 + key_length + value_length)
        return header_map

