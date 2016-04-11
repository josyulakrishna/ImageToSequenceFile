# ImageToSequenceFile
This code creates Hadoop Sequence Files, Requirements: You should install https://github.com/matteobertozzi/Hadoop/tree/master/python-hadoop before using the code
(In the compressed variation, this program uses BZip compression you must have that package installed) 

There are two variations: 1. Without Compression: imgtoseqgray.py , seqtoimggray.py	
2. With Compression: CompressedSequenceFileReader.py, SequenceFileWriterWithCompression.py

To create the sequence file the arguments to the program have to be 'path to the images directory' 'path to save the sequence file' 'extension type(jpg,tiff,png..)'
Store the created file in hdfs using any library of your choice 
Skeleton to read the images from Sequencefile is given in CompressedSequenceFileReader and seqtoimggray
