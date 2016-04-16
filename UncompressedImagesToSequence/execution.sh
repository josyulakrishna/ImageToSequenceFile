chmod a+x imgtoseqtext.py mapper.py seqtoimgmodified.py
path=${XDG_DESKTOP_DIR:-$HOME/Desktop}
echo "converting into sequencefile make sure the current directory has a folder named pictures with images in it"  
./imgtoseqtext.py pictures $path tiff,jpg,png
echo "testing the mapper"
./seqtoimgmodified.py path+"/images.seq"

echo "after testing use these commands to put in hdfs and run the program"
echo "hadoop fs -mkdir -p /input"
echo " hadoop fs -put -p images.seq /input"
echo "hadoop jar $HADOOP_HOME/jars/hadoop-streaming-2.7.2.jar    -D mapred.reduce.tasks=0   -input /input/imagestext.seq/   -inputformat SequenceFileAsTextInputFormat   -output test_output   -file mapper.py -mapper mapper.py"