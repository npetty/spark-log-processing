#
# TopN Users/Visitors application
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
