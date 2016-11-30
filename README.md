# es.firehose
##Examples for parsing a regular json file and sending it to firehose for onward delivery to elastic search.
Clone the repository: git clone https://github.com/cheefoo/es.firehose.git
#1.Replace the Firehose Delivery Stream with your stream name
#2. Replace the path to your JSON movie data file.
#3. mvn exec:java -Dexec.mainClass=com.tayo.es.firehose.FirehoseToES 
