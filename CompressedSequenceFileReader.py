import io
from cStringIO import StringIO
import binascii
import gzip
import numpy as np
import cv2
import sys
from hadoop.io import SequenceFile
from hadoop.io.compress.BZip2Codec import BZip2Codec


class SequenceFileReader:
	def __init__(self, pathtoseqfile):
		self.path=pathtoseqfile

	def getValue(self):
		def seqReader(pathtpsaveimage): 
			reader = SequenceFile.Reader(self.path)
			key_class = reader.getKeyClass()
			value_class = reader.getValueClass()
			key = key_class()
			value = value_class()
			position = reader.getPosition()
			compression_codec=BZip2Codec()
			while reader.next(key, value):
				position = reader.getPosition()
				name,d1,d2,ext=key.toString().split(".")
				arr=compression_codec.decompress(value.getBytes())
				nparr = np.frombuffer(arr, np.uint8)
				img = cv2.imdecode(nparr, cv2.CV_LOAD_IMAGE_COLOR)
				print name,img.shape
			reader.close()
		return seqReader
	
	def run(self): 
		caller=self.getValue()
		value=caller(self.path)


if __name__ == '__main__':
	path=sys.argv[1]
	seq=SequenceFileReader(path)
	seq.run()
