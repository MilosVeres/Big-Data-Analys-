#!/usr/bin/python

import pyspark

sc=pyspark.SparkContext()

def attachID(arr):
	#get game_id for each game (ex: BOS192704230)
	id = arr[0].split(',')[1]
	return map(lambda s: id + ',' + s, arr)
	

(
sc.textFile('gs://veres-declass/retrosheet/games/*games')
#[[u'id,BOS192704230', u'version,1',...
.map(lambda s: s.split('*EOL*'))
#now, we want to append BOS192704230 at the begining of every element of the array
#using 'attachID' function
#so it should look like this: [[u'BOS192704230,id,BOS192704230', u'BOS192704230,version,1',...
.map(attachID)
#now we put every elem of the array in separate line
.flatMap(lambda x:x)
#take first 50 lines
.take(50)
#save the file to google cloud storage
.saveAsTextFile('gs://veres-declass/retrosheet2/revised_game_info/output')
)
