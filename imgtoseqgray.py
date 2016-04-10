import io
import glob
import sys
import cv2
from hadoop.io import *
from hadoop.io.compress.GzipCodec import GzipCodec

class SequenceFileWriter:
	def __init__(self, path, pathtosave, exts):
		self.path = path 
		self.pathtosave=pathtosave
		self.ext = exts
		self.listofimagepaths=[]

	def createSeqFile(self,path): 
		ext=path.split(".")[-1]
		img=cv2.imread(path,0)
		name=path.split(".")[-2]+'.'+'.'.join(str(x) for x in img.shape)
		img_str = cv2.imencode('.jpg', img)[1].tostring()
		name+='.'+ext
		def writeData(writer):
			key=Text()
			value = BytesWritable()
			key.set(name)
			value.set(img_str)
			print " in writer key ", name, " ext is ",ext, " size ",len(img_str)
			writer.append(key, value)
		return writeData

	def getWriter(self):
		writer = SequenceFile.createWriter(pathtosave+'/cvucimaged.seq', Text, BytesWritable)
		for imagefilepath in self.listofimagepaths:
			imgData= self.createSeqFile(imagefilepath)
			imgData(writer)
		writer.close()
		self.compress

	def getFiles(self):
		for i in self.ext:
			self.listofimagepaths+=glob.glob(self.path+'/*.{0}'.format(i))

	
	def putInHDFS(self):
		""""TO DO AFTER RECTIFYING PYDOOP INSTALLATION ERRORS"""
		pass

	def compress(self):
		gzip = GzipCodec()
		gzip.compress(pathtosave+'/cvucimaged.seq')

	def run(self):
		self.getFiles()
		self.getWriter()




if __name__ == '__main__':
	path=sys.argv[1]
	pathtosave=sys.argv[2]
	exts=sys.argv[3]
	seq=SequenceFileWriter(path, pathtosave, exts.split(','))
	seq.run()

