# Oscar 2015 Dataflow #
This repository contains the code used for analyzing the data for http://ecesena.github.io/oscars2015/#/

The code runs on Google Cloud Dataflow. You can read about Google Cloud Dataflow and apply to the beta at 
this address: https://cloud.google.com/dataflow/

### Disclaimer ###
This code has been written on the beta of Google Cloud Dataflow. I'm not sure it will work correctly in the final release. 
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
