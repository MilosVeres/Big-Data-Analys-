#!/usr/bin/python

import pyspark


sc=pyspark.SparkContext()

#tuple goes in
def to_String(tuple):
	year = tuple[0][3:7]
	month = tuple[0][3:7]+'-'+tuple[0][7:9]
	day = tuple[0][3:7]+'-'+tuple[0][7:9]+'-'+tuple[0][9:11]
	player = tuple[0][13:]
	hr = tuple[1]
    #output is a string with 5 fields: year, year_month, whole_date, id, HR the player hit that day
	return ('{},{},{},{},{}'.format(year,YearMonth, date, player, hr))


players=(
sc.textFile('gs://nemanja-declass/retrosheet/players')
#get an array: pl_id, f_name, l_name
.map(lambda s:s.split(','))
#get a tuple: (pl_id, f_name l_name)
.map(lambda a:(a[0], a[2]+' '+a[1]))
#make us a dict, so we can look up on the player by their id: players=dict(players)
.collect()
#check the output as a checkpoint
.take(100)
)

#make us a dict, so we can look up on the player by their id: player_id: f_name l_name
players=dict(players)


result=(
#read in 26 million lines
sc.textFile('gs://nemanja-declass/retrosheet/revised_game_info/output/part*')
#make every string an array
.map(lambda s:s.split(','))
#get only plays from the array
.filter(lambda a: a[1] == 'play')
#get only those who have H or HR as 7th element
.filter(lambda a: a[7][0] == 'H' and a[7][1] != 'P')
#produce a tuple that contains key (0th and 4th element together sep by " "), and just num 1 as second elem
#we get: (u'BAL196204140 robib104', 1)
#add player name from players dict: palyers[a[4]]; since a[4] is the player_id, we can match that with the key in players dict
.map(lambda a:(a[0]+' '+a[4]+' '+players[a[4]], 1))
#sum up all HRs for every player
.groupByKey()
.mapValues(sum)
.map(to_String)

#save the final result to google cloud storage
.saveAsTextFile('gs://nemanja-declass/retrosheet/result')
)

#print(result)
