#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
from hadoop.io.SequenceFile import CompressionType
from hadoop.io import LongWritable
from hadoop.io import SequenceFile
from hadoop.io import Text


import os


def write_text_data(writer):

    key = Text()
    value = Text()

    dirpath = 'data/resume/xml/api_12'

    filenames = os.listdir(dirpath)
    filenames.sort()

    for filename in filenames:

        with open('{}/{}'.format(dirpath, filename)) as f:
            s = f.read()

        # need this to avoid barfing on value.set
        s = s.decode('utf-8')

        # need these to work nicely with hadoop streaming
        s = s.replace('\t', '\\t').replace('\n', '\\n')

        key.set(filename)
        value.set(s)
        
        #print '[%d] %s %s' % (writer.getLength(), key.toString(), value.toString())
        print '{}'.format(filename)

        writer.append(key, value)


def test_text():

    from hadoop.io.compress.ZlibCodec import ZlibCodec
    from hadoop.io.compress.GzipCodec import GzipCodec
    from hadoop.io.compress.BZip2Codec import BZip2Codec
    from hadoop.io.compress.LzoCodec import LzoCodec
    from hadoop.io.compress.SnappyCodec import SnappyCodec

    writer = SequenceFile.createWriter('resume_compressed.seq', Text, Text,
                                       compression_codec=SnappyCodec(),
                                       compression_type=CompressionType.BLOCK)
    write_text_data(writer)
    writer.close()
    

def main():
    #test()
    test_text()


if __name__ == '__main__':
    main()
