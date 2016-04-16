#! /Users/josyulakrishna/Documents/PythonPackages/anaconda/bin/python
import cv2
import numpy as np
import sys
for line in sys.stdin:
	k,v=line.split("\t")
	name,d1,d2=k.split(".")
	nparr = np.array(v.split(","), np.uint8).reshape(int(d1),int(d2))
	#img = cv2.imdecode(nparr, cv2.CV_LOAD_IMAGE_COLOR)
	print nparr.shape

