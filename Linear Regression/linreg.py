# linreg.py

# Standalone Python-Spark program to perform linear regression.
# Performs linear regression by computing the summation form of the
# closed form expression for the ordinary least squares estimate of beta.
# 
# Takes the yx file as input, where on each line y is the first element 
# and the remaining elements constitute the x.
#
# Usage: spark-submit linreg.py <inputdatafile>
# Example usage: spark-submit linreg.py yxlin2.csv
#
# Copy <inputdatafile> and provide HDFS path for execution

import sys
import numpy as np

from pyspark import SparkContext
from array import array 

# Beta[] =  INV(Summation{(X*XT)}) * Summation{(X*Y)}
# Calculation of X * XTranspose
def XiXiTrans(X):
    X[0] = 1.0 
    XArray=np.array(X).astype('f')
    XMat=np.asmatrix(XArray).T
    XMatTrans=np.transpose(XMat)
    dotProduct = np.dot(XMat, XMatTrans)
    return dotProduct

#Calculation of X * Y
def XiYi(X):
  Y = float(X[0])
  X[0]=1.0
  XArray = np.array(X).astype('f')
  XMat=np.asmatrix(XArray).T
  dotProduct=np.dot(XMat,Y)
  return dotProduct

if __name__ == "__main__":
  if len(sys.argv) !=2:
    print >> sys.stderr, "Usage: linreg <datafile>"
    exit(-1)

  sc = SparkContext(appName="LinearRegression")

  # Input yx file has y_i as the first element of each line 
  # and the remaining elements constitute x_i
  yxinputFile = sc.textFile(sys.argv[1])
  #yxinputFile = sc.textFile('/home/prateek/Pyspark/yxlin2.csv')

  yxlines = yxinputFile.map(lambda line: line.split(','))
  yxfirstline = yxlines.first()
  yxlength = len(yxfirstline)

  #get Xi*XiT
  dataXXTRDD=yxlines.map(lambda x: ("PART1",XiXiTrans(x)));
  dataXXT=dataXXTRDD.collect();
  #print("dataXXT :",dataXXT)

  dataXXTReduceRDD=dataXXTRDD.reduceByKey(lambda x1,x2: np.add(x1,x2));
  dataXXTReduce=dataXXTReduceRDD.collect();
  print("dataXXTReduce : ",dataXXTReduce)

  dataXXTFinalRDD = dataXXTReduceRDD.map(lambda x: x[1]);
  dataXXTFinal = dataXXTFinalRDD.collect()[0]
  print("dataXXTFinal :",dataXXTFinal)

  #convert into matrix representation
  XXT=np.asmatrix(dataXXTFinal);

  #get Xi*Yi
  dataXYRDD=yxlines.map(lambda x: ("PART2",XiYi(x)))
  dataXY = dataXYRDD.collect()
  #print("dataXY: ",dataXY)

  dataXYReduceRDD=dataXYRDD.reduceByKey(lambda x1,x2: np.add(x1,x2))
  dataXYReduce=dataXYReduceRDD.collect()
  print("dataXYReduce: ",dataXYReduce)

  dataXYFinalRDD=dataXYReduceRDD.map(lambda x:x[1])
  dataXYFinal=dataXYFinalRDD.collect()[0]

  print("dataXYFinal:",dataXYFinal)

  #convert into matrix representation
  XY=np.asmatrix(dataXYFinal)
  
  # dummy floating point array for beta to illustrate desired output format
  beta = np.zeros(yxlength, dtype=float)

  #Get inverse of XXT
  XXTI=np.linalg.inv(XXT);

  #print matrix values 
  print("XXTI",XXTI)
  print("XY",XY)

  #dot product of XXTI and XY
  beta = np.dot(XXTI,XY);

  #convert into list for printing
  betaList = np.array(beta).tolist()

  print("betaList: ",betaList)

  # print the linear regression coefficients in desired output format
  i=0
  print ("beta values: ")
  print("--------------------")
  for coeff in betaList:
    print ("\tbeta["+str(i)+"]: "+str(coeff)[1:-1])
    i=i+1
  sc.stop()

