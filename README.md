# spark-log-processing
Using scala/spark to analyze an httpd log dataset and deploying as a dockerfile.

## Problem Statement
Write a program in Scala that downloads the dataset at ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz 
and uses Apache Spark to determine the top-n most frequent visitors and urls for each day of the trace. Package 
the application in a docker container.

## My Solution
The code is available in this github repo: https://github.com/npetty/spark-log-processing.git

The packaged docker container is on DockerHub at https://hub.docker.com/repository/docker/nmpetty/top-n-app

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
      codes in the dataset. It could be important to see a spike in requests for a page that is not found. Various codes
      could be easily removed with a filter, so this did not impact design.
4. **This solution runs Spark in local mode.**
    * The requirements stated deployment in a docker container, so I opted for a stand-alone spark instance rather than 
    a cluster. I tried out a cluster configuration and have the Dockerfiles and docker-compose.yml file available, 
    but this seemed to go against the single container deployment.
    
### Pre-reqs
Previously installed packages:
* Docker
* Git (if you want to build from source) 

### Running the solution
I have documented two approaches to running this application. One streamlined and straightforward, the other longer
and more tedious, but ensures you are executing the source code.

#### Option 1: Run published Docker image: Quickest Way
Pull from DockerHub and run the image

```dtd
docker run --rm -it --name spark-stand-alone --hostname spark-stand-alone -p 7077:7077 -p 8080:8080 nmpetty/top-n-app:1.0 /bin/sh
```

Assuming you don't already have an image with the same name locally, this will pull my image from DockerHub, start it
up, and drop you in a shell within the /project directory.

From here, you could open up the app-readme.txt file for instructions, but it will tell you to run the script in
the same dir.
```dtd
./run-top-n-app.sh
```
Then sit back and watch it run (for longer than I would like). Later I'll mention that I'm not happy with the run 
performance... :( If you'd like to change the value for N, edit the conf/application.conf file as described in the 
app-readme.txt file.

#### Option 2: Build everything from source (including Docker image): Slow but thorough
1. Clone this github repository to a host with the pre-reqs from above.

```dtd
git clone https://github.com/npetty/spark-log-processing.git spark-log-processing
```

2. Build the docker image passing in versions for Scala and SBT. These versions were used
because they are compatible with the version of Spark I used 2.4.5-bin-hadoop2.7.

```dtd
cd spark-log-processing
docker build -t top-n-app/app:localbuild --build-arg SCALA_VERSION=2.11.2 --build-arg SBT_VERSION=1.2.7 .
```
* Note this image builds SBT and Maven. Near the end of the project I had an issue with the sbt plugin
used to build assemblies, which caused problems building the shaded uber jar for this project. I switched
to building with Maven last minute but left both dependencies to allow for easier refactor once the sbt
issue is resolved.

3. Run the newly created docker image
     
```dtd
docker run --rm -it --name spark-stand-alone --hostname spark-stand-alone -p 7077:7077 -p 8080:8080 top-n-app/app:localbuild /bin/sh
```

This command will start the container and drop you in a bash shell in the /project directory.
    
4. Build the code
    * From the shell in the container, cd into the code directory and build with maven. With this option, you must
    build the code because I did not check in the jar file to github.
    
```dtd
cd project_code
mvn install
``` 

5. Optionally update the number N for Top-N users/visitors (with your preferred editor)
The value is nested in my.challenge.app.n (you'll find it!).
```
vi /project/conf/application.conf
```

5. Submit the spark job in local mode

```dtd
/spark/bin/spark-submit --master local[1] --driver-class-path=/project/conf/ --class my.challenge.TopN /project/project_code/target/top-n-app-1.0.jar
```

## Project Overview

### Code Structure
The code has four main files described below and easily accessible above in the project_code dir for more detail.

1. **my.challenge.TopN**
* This file extends the App class and is the main driver for the application. The flow of execution follows:
    * Set config from file
    * Load data set while calling the LogEntry parser to return structured rows.
    * Create a dataframes ranking each url/visitor by day.
    * Join these datasets by day and rank to produce a single dataframe for display
    * Query the final dataset to retrieve rows with rank <= N.
2.  **my.challenge.DataGrabber**
* This class is a utility for loading data. It was needed because I ran into issues using an ftp:// URL directly
with sc.textFile. It matches on the input string to determine which data loading method to use.
3. **my.challenge.LogEntry**
* This file declares a case class to capture LogEntries and a companion object to perform the parsing. I found several
example regexes for parsing access logs, but did a little tweaking to be more open on what was allowed.

* Additionally, the log entry parses the timestamp string to produce a calculated Day column. This is used to 
group entries by day for the top user calculations.
4. **my.challenge.TopNCalculator**
* This class has methods that capture the grouping/joining/query logic, therefore it was useful to have a separate
class with functions that could be tested. Specifically it defines functions called
    * rankByDay
    * joinDfsOnDayAndRank
    * buildSqlQuery
These perform the dataframe operations described in their names.

### Testing

I created test classes for
* TopNCalculator
* LogEntry

These contained nearly all of the application logic, so there is good coverage on that part. I did not want to test
the Spark framework, so did not create a test class for DataGrabber or the driver, TopN. The DataGrabber class
is a loose wrapper around the data loading methods available in Scala.

To run the tests, access the project_code directory and run 'mvn test'. Additionally, all tests will be run at build
time using 'mvn install'. If any test fails, the package will not build.

### What Would I do Better

There are several things I wanted to give more attention, but time did not allow.
  
1. *Performance Tuning*
    *   While the given solution runs and produces output, I did not spend much time worrying about 
performance. While this is a one-time batch job, perhaps performance is low on the priority list,
but after wiring in the test with Maven, second order would be to address performance. I would
approach this both from a configuration tuning angle, and a dataset partitioning and caching angle.
I know there are at least a couple inefficient operations going on.

2. *Integration Testing*
    * There is always room for more testing. Currently, unit tests are applied to the logic functionality
    within the application. However, I could have done more integration testing and even more on the unit
    testing front to handle all the edge cases I can think of. Additionally, I do not have automated tests
    around the deployment phase with Docker. Building the image and deploying to DockerHub is not yet
    automated.
  
3. *Run on a cluster*
    *   While I'll made an assumption about the requirements leading toward a stand-alone Spark instance,
    I would have like use docker compose to build a cluster and submit the job. I did get this working
    in a lab environment, but was not able to wire it all up for efficient deployment. If one had a working 
    Spark cluster available, it would not be difficult to submit to that cluster.
    

## Skip To The Result

For the impatient or those who don't have access right now to a host with Docker, I've provided a few
screenshots below to follow along the process.

* Pull Docker Container
![image](https://user-images.githubusercontent.com/14127655/80536794-889fce80-8968-11ea-8539-801fbdcd52a6.png)

* Execute the run script (N = 5)
![image](https://user-images.githubusercontent.com/14127655/80536515-1a5b0c00-8968-11ea-9f67-8ad36281f0f1.png)

* View the output
![topn-logs-2](https://user-images.githubusercontent.com/14127655/80536574-3199f980-8968-11ea-9945-aa91fd3afd6d.PNG)

and some more...

![topn-out2](https://user-images.githubusercontent.com/14127655/80536610-3e1e5200-8968-11ea-9f99-252842a44c0d.PNG)

... and some more

![topn-out3](https://user-images.githubusercontent.com/14127655/80536653-4f675e80-8968-11ea-9e19-b368ba56125e.PNG)

... and finally

![topn-out4](https://user-images.githubusercontent.com/14127655/80536680-5aba8a00-8968-11ea-8a8d-11f0c3ccc8c2.PNG)

Thank you for reading, this was fun!!

-Nick