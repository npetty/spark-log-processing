# spark-log-processing
Using scala/spark to analyze an httpd log dataset and deploying as a dockerfile.

## Problem Statement
Write a program in Scala that downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz 
and uses Apache Spark to determine the top-n most frequent visitors and urls for each day of the trace. Package 
the application in a docker container.

## My Solution
The code is available in this github repo: https://github.com/npetty/spark-log-processing.git

### Assumptions
First lets list some assumptions made during the development of this solution:

1. Output displayed to user is final. 
    * It would not be difficult to save the resulting dataframe as a csv or parquet file for storage,
      but given the nature of the project, the result set is printed to the console for delivery.
2. Output format is a single table listing both URL and visitor with the same rank on one row. In case of a tie rank,
   the rank is shared. This causes occasional nulls to be seen in the output when a rank is skipped in one column but
   not the other. This is by design and not an error.
2. All HTTP Status Codes are considered. I could see an argument to remove 404 Not Found codes or even be more
   restrictive, but in this case I kept all codes in the dataset.
    * Various codes could be easily removed with a filter, so this did not impact design.
4. This solution runs Spark in local mode. The requirements stated deployment in a docker container, so I opted for 
   a stand-alone spark instance rather than a cluster. I tried out a cluster configuration and have the dockerfiles 
   and docker-compose.yml file available, but this seemed to go against the single container deployment.
    
### Pre-reqs
Previously installed packages:
* Git for initial pull 
* Docker

### Running the solution

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


## Skip To The Result

All of those steps can take some time to complete. The clean docker build + maven install can take several minutes. 
For the impatient I've posted some screenshots from each step.

