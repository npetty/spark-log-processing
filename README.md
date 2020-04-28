# spark-log-processing
Using scala/spark to analyze an httpd log dataset and deploying as a dockerfile.

## Problem Statement
Write a program in Scala that downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz 
and uses Apache Spark to determine the top-n most frequent visitors and urls for each day of the trace. Package 
the application in a docker container.

## My Solution
The code is available in this github repo: https://github.com/npetty/spark-log-processing.git

### Assumptions
The following assumptions were made during the development of this solution:

1. **Final output is displayed in console.** 
    * It would be a relatively simple update to save the resulting dataframe as a csv, parquet, or something similar
      for storage and/or transmission but given the nature of the project, the end result set is printed to the console 
      for delivery.
2. **Output format is a single table with rows keyed and sorted on day and daily rank.**
    * For the day/rank key, the corresponding URL and visitor (with counts) are shown. row lists URL and visitor with 
    the same rank on one row. In case of a tie rank, the rank is shared. This causes occasional nulls to be seen in 
    the output when a rank is skipped in one column but not the other. This is by design and not an error.
2. **All HTTP Status Codes are considered.**
    * I could see an argument to remove 404 Not Found codes or even be more restrictive, but in this case I kept all 
    codes in the dataset. Various codes could be easily removed with a filter, so this did not impact design.
4. **This solution runs Spark in local mode.**
    * The requirements stated deployment in a docker container, so I opted for a stand-alone spark instance rather than 
    a cluster. I tried out a cluster configuration and have the dockerfiles and docker-compose.yml file available, 
    but this seemed to go against the single container deployment.
    
### Pre-reqs
Previously installed packages:
* Docker
* Git (if you want to build from source) 

### Running the solution


#### Option 1: Run published Docker image: Quickest Way
Pull from DockerHub and run the image

```dtd

```

#### Option 2: Build from source: Slow but thorough
1. Clone this github repository to a host with the pre-reqs from above.

```dtd
git clone https://github.com/npetty/spark-log-processing.git spark-log-processing
```

2. Build the docker image passing in versions for Scala and SBT. These versions were used
because they are compatible with the version of Spark I used 2.4.5-bin-hadoop2.7.

```dtd
cd spark-log-processing
docker build -t top-n-app/scala:latest --build-arg SCALA_VERSION=2.11.2 --build-arg SBT_VERSION=1.2.7 .
```
* Note this image builds SBT and Maven. Near the end of the project I had an issue with the sbt plugin
used to build assemblies, which caused problems building the shaded uber jar for this project. I switched
to building with Maven last minute but left both dependencies to allow for easier refactor once the sbt
issue is resolved.

3. Run the newly created docker image
    
     
```dtd
docker run --rm -it --name spark-stand-alone --hostname spark-stand-alone -p 7077:7077 -p 8080:8080 top-n-app/scala:latest /bin/sh
```
* This command will start the container and drop you in a bash shell in the spark-log-processing directory.
    
4. Build the code
    * From the shell in the container, cd into the code directory and build with maven.
    
```dtd
cd project_code
mvn install
``` 

5. Optionally update the number N for Top-N users/visitors (with your preferred editor)

```
vi /project/conf/application.conf

# Edit value in conf file for n. Default is set to 5.
```

5. Submit the spark job in local mode

```dtd
/spark/bin/spark-submit --master local[1] --driver-class-path=/project/conf/ --class my.challenge.TopN target/top-n-app-1.0.jar
```


## What Would I do Better

There are several things I would have liked to given more attention, but time did not allow.

1. *Tests are not running in Maven*
* I had an issue in the last hour getting SBT to build a full assembly with all dependencies.
  I knew how to do this in Maven, so promptly switched in order to use the shade plugin. This 
  fixed the first problem, but I was not able to get the Maven Scalatest plugin to function
  correctly. Thereby rendering my tests less-than-useful in the current submission. 
  
  With SBT tests were running smoothly, so I can at least claim that the code is there and was
  used during my development at least a couple times to catch an inadvertant bug. I feel this 
  could be fixed relatively quickly.
  
2. *Performance Tuning*
* While the given solution runs and produces output, I did not spend much time worrying about 
performance. While this is a one-time batch job, perhaps performance is low on the priority list,
but after wiring in the test with Maven, second order would be to address performance. I would
approach this both from a configuration tuning angle, and a dataset partitioning and caching angle.
I know there are at least a couple inefficient operations going on.

3. *Run on a cluster*
* While I'll made an assumption about the requirements leading toward a stand-alone Spark instance,
I would have like use docker compose to build a cluster and submit the job. I did get this working
in a lab environment, but was not able to wire it all up for efficient deployment.


## Skip To The Result

For the impatient or those who don't have access right now to a host with Docker, I've provided a few
screenshots below to follow along the process.

* Pull Docker Container

* Execute the run script

* View the output

