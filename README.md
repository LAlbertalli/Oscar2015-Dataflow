# !!!Warning!!! #
This code is 6 years old and no longer maintained. Also, this code used a technology that was in beta at the time, so it will probably no longer work.
Please, consider it as an early POC for a new technology, nothing more.

# Oscar 2015 Dataflow #
This repository contains the code used for analyzing the data for http://ecesena.github.io/oscars2015/#/

The code runs on Google Cloud Dataflow. You can read about Google Cloud Dataflow and apply to the beta at 
this address: https://cloud.google.com/dataflow/

### Disclaimer ###
This code has been written on the beta of Google Cloud Dataflow (February 2015). I'm not sure it will work correctly in the final release. 
The analysis made by this code is pretty simple and has been made mainly to experiment with this new tool and explain 
the potentiality it has

## Usage ##
Set up the Google Cloud Dataflow according to Google documentation. For using Google Cloud Dataflow you need the official 
Java SDK 1.7 or higher and Maven to manage the compilation process.

To compile the packages:
`mvn clean compile bundle:bundle`

To run the code on Google Cloud Dataflow:
* Data extract and summarize:
`java -cp target/dataflow-1.jar com.tests.dataflow.BatchTweet --project=<projectname> --stagingLocation=<Cloud Store Staing path> --runner=BlockingDataflowPipelineRunner --saveTo=<path to save> --readFrom=<table to read from> --start=<start time> --stop=<stop time>`
* Data analyze:
`java -cp target/dataflow-1.jar com.tests.dataflow.BatchTweetAnalysis --project=<projectname> --stagingLocation=<Cloud Store Staing path> --runner=BlockingDataflowPipelineRunner --saveTo=<path to save> --readFrom=<table to read from> --start=<start time> --stop=<stop time> --filterEntities='#oscars,#oscars2015,#redcarpet'--retweet=false`

### Params ###
#### Google Cloud Dataflow ####
* `--project` The name of the project on Google Dataflow
* `--stagingLocation` The bucket/path of the staging location on Google Cloud store
* `--runner` The runner program

#### Both extract and analyze ####
* `--saveTo` The file on Google Cloud Store to write to. In the format gs://<bucket>/<path>
* `--readFrom` The table from which read the data. In the format <project>:<database>.<tablename>
* `--windowSize` The size of the window used to aggregate the data. In seconds for extract and in minutes for analyze
* `--start` The time from which the extraction should start. In the format YYYY-MM-DDTHH:MM:SS.mmmZ
* `--stop` The time at which the extraction should stop. In the format YYYY-MM-DDTHH:MM:SS.mmmZ
* `--retweet` False to exclude the retweets. Default True
* `--contains` Comma separated list of word to search in the tweet text to include it
* `--notContains` Comma separated list of word to search in the tweet text to exclude it

#### Analyze only ####
* `--windowFreq` Every how many minutes the sliding window should be applied
* `--filterEntities` Comma separated list of entities to remove after the count

### Data and Tools ###
The directory `data/` contains the bulk dump aggregated in 5 seconds windows (`datadump.csv`) and 
an analysis run for the top tweets(`top.csv`). The directory contains two supporting python script 
to quickly manipulate the data.
* `convert_file.py` Extract the selected entities and format it for 
http://ecesena.github.io/oscars2015/#/ (source at https://github.com/ecesena/oscars2015). 
The command format is: `./convert_file.py file entity [entity] --window N` where:
  * `file` is the input file name
  * `entity` is a list of entity to extract
  * `--window N` is the number of period to aggregate to get a sliding window. Default is 1 so no
aggregation
* `convert_file2.py` reformat the output of analyze to make it easier to visualize in a worksheet. 
It accepts only one argument, the file name. The input format is 
`type,timestamp,ent1,count1,ent2,count2,...countn,entn`. The ouput format is `timestamp,count1,count2,count3...` with an header containing all the entities extracted.
