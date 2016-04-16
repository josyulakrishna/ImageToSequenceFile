#! /Users/josyulakrishna/Documents/PythonPackages/anaconda/bin/python
import io,sys
import numpy as np
import cv2
from hadoop.io import SequenceFile

class SequenceFileReader:
	def getValue(self):
		def seqReader(path): 
			reader = SequenceFile.Reader(path)
			key_class = reader.getKeyClass()
			value_class = reader.getValueClass()
			key = key_class()
			value = value_class()
			position = reader.getPosition()
			while reader.next(key, value):
				position = reader.getPosition()
				name,d1,d2=key.toString().split(".")
				nparr = np.array(value.toString().split(","), np.uint8).reshape(int(d1),int(d2))
				#img = cv2.imdecode(nparr, cv2.CV_LOAD_IMAGE_COLOR)
				print nparr.shape
			reader.close()
		return seqReader
	
	def run(self,path): 
		caller=self.getValue()
		value=caller(path)


if __name__ == '__main__':
	seq=SequenceFileReader()
	seq.run(sys.argv[1])