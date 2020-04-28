#
# TopN Users/Visitors application
#

Application file structure

Base Dir: 

/project - run-top-n.sh: Script for easy running of the app.

/project/artifacts - top-n-app-1.0.jar: pre-built application jar for easy deployment,
		     NASA_access_log_Jul95.gz copy of the data set in case there are 
			connectiity issues.

/project/conf - application.conf: Read by the app to configure parameters including
	the N value.
		log4j.properties: Helps to rid of those pesky Spark messages.

/project/project_code - all of the application source code. It is tied to the
	current github project, so if you want the latest codeset you'll have
	to jump in that directory and run 'git pull'.


#
# Running the application
#

1. By default, the N value is set to 5. If you want to adjust this value, 
edit ./conf/application.conf and adjust the setting in my.challenge.app.n.

2. In order to run this application, execute the run-top-n.sh script in this directory


#
# Building from Source
#

The complete source code is available at ./project_code. to build the application,
entry the project code dir and run 'mvn install'. This will build a shaded jar with
all dependencies at ./project_code/target/top-n-app-1.0.jar. To use this jar, edit
the run-top-n.sh script to point to that jar instead of /project/artifacts/top-n-app-1.0.jar
