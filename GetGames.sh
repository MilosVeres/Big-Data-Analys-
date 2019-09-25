#!/bin/bash

for i in {1925..2017}
	do
		#get data from the web
		curl http://www.retrosheet.org/events/${i}eve.zip > data.zip
		unzip data.zip

		#remove unnecessary data
		rm *.ROS
		rm TEAM*
		rm data.zip

		#read all the files and put them into game_info
		cat *.E* >> game_info
		rm *.E*

		#first put the whole file into the single line
		line=$(cat game_info)

		echo $line | sed 's/\(id,[[:alpha:]]\{3\}[[:digit:]]\{9\}\)/\n\1/g' > temp

		#get rid of the first blank line
		sed '/^$/d' < temp > temp2

		#\r is ^M in a file, *EOL* - where the original breaks were
		sed 's/\r /*EOL*/g' < temp2 > temp3

		#get rid of the *EOL* at the end of the file
		sed 's/\*EOL\*^$//' < temp3 > ${i}games

		#copy the final output to google cloud storage
		gsutil cp ${i}games gs://nemanja-data/retrosheet/games/${i}games

		#remove temp files
		rm temp
		rm temp2
		rm temp3
		rm ${i}games
		rm game_info
    
