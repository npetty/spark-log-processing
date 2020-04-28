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
    * The requirements stated deployment in a docker container, so I opted for a stand-alone spark instance
    rather than the cluster. I did test out this configuration and have the dockerfiles and docker-compose.yml
    file as a possible extension.
     
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
/spark/bin/spark-submit --master local[1] --driver-class-path=/project/conf/ --class my.challenge.TopN target/top-n-app-1.0-SNAPSHOT.jar
```


## Skip To The Result

All of those steps can take some time to complete. The clean docker build + maven install can take several minutes. 
For the impatient I've posted some screenshots from each step.

