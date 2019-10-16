from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def loadJson(lines):
    pyObj=json.loads(lines)
    return pyObj

def removeAttr(arg):
    return arg['subreddit'],[1,arg['score']]
# add more functions as necessary

def add_pairs(a,b):
    return (a[0]+b[0],a[1]+b[1])

def calAvg(kv):
    if kv[1][1]/kv[1][0] > 0:
        return kv[0],kv[1][1]/kv[1][0]
    else:
        pass


def main(inputs, output):
    rdd= sc.textFile(inputs)
    pyObj=rdd.map(loadJson).cache()#parse JSON string to a python object
    keyValue=pyObj.map(removeAttr)#create key value pairs of subreddit, score
    reducedValue=keyValue.reduceByKey(add_pairs)#create sum over similar key
    avg=reducedValue.map(calAvg)#calculate average
    #avg.map(json.dumps).saveAsTextFile(output)#saving output as json format

    commentData=pyObj.map(lambda x:(x['subreddit'],x))#create subreddit,comment
    comment=commentData.join(avg)#join two rdd
    relativeAvg=comment.map(lambda x:[x[1][0]['score']/x[1][1],x[1][0]['author']])#calculate relative score
    relativeAvg.sortByKey(False).map(json.dumps).saveAsTextFile(output)#save in output file


if __name__ == '__main__':
	conf = SparkConf().setAppName('example code')
	sc = SparkContext(conf=conf)
	sc.setLogLevel('WARN')
	assert sc.version >= '2.4'  # make sure we have Spark 2.4+
	inputs = sys.argv[1]
	output = sys.argv[2]
	#inputs="reddit-2"
	#output="jsonOut.json"
	main(inputs,output)
