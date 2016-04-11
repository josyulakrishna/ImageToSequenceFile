#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
from hadoop.io.SequenceFile import CompressionType
from hadoop.io import LongWritable
from hadoop.io import SequenceFile
from hadoop.io import Text


def writeData(writer):
    key = LongWritable()
    value = LongWritable()

    for i in xrange(1000):
        key.set(1000 - i)
        value.set(i)
        print '[%d] %s %s' % (writer.getLength(), key.toString(), value.toString())
        writer.append(key, value)


def write_text_data(writer):
    key = LongWritable()
    value = Text()

    for i in xrange(1000):
        key.set(1000 - i)
        value.set('taro {}'.format(i))
        print '[%d] %s %s' % (writer.getLength(), key.toString(), value.toString())
        writer.append(key, value)


def test():
    writer = SequenceFile.createWriter('test.seq',
                                       LongWritable,
                                       LongWritable)
    writeData(writer)
    writer.close()

    writer = SequenceFile.createWriter('test-record.seq',
                                       LongWritable,
                                       LongWritable,
                                       compression_type=CompressionType.RECORD)
    writeData(writer)
    writer.close()

    writer = SequenceFile.createWriter('test-block.seq',
                                       LongWritable,
                                       LongWritable,
                                       compression_type=CompressionType.BLOCK)
    writeData(writer)
    writer.close()


def test_text():
    writer = SequenceFile.createWriter('test_text.seq',
                                       LongWritable,
                                       Text,
                                       compression_type=CompressionType.BLOCK)
    write_text_data(writer)
    writer.close()
    

def main():
    #test()
    test_text()


if __name__ == '__main__':
    main()
