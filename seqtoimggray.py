#! /Users/josyulakrishna/Documents/PythonPackages/anaconda/bin/python
import io,sys
import numpy as np
import cv2
from hadoop.io import SequenceFile

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
			while reader.next(key, value):
				position = reader.getPosition()
				name,d1,d2,ext=key.toString().split(".")
				print len(value.getBytes())
				nparr = np.fromstring(value.getBytes(), np.uint8)
				img = cv2.imdecode(nparr, cv2.CV_LOAD_IMAGE_COLOR)
				print np.array(img).size
			reader.close()
		return seqReader
	
	def run(self): 
		caller=self.getValue()
		value=caller(self.path)


if __name__ == '__main__':
	path=sys.argv[1]
	seq=SequenceFileReader(path)
	seq.run()