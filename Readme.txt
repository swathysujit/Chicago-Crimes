/**************************************************************************************************************\

PROJECT :
	Chicago Crimes - Does weather affect crime?
	
Presentation is in /doc directory
	
/**************************************************************************************************************\

To run the pipeline : 

A. MySQL - (/src/mysql/)

	1. Create the base table and populate it by loading the csv file into the sql base table, to be used for building the dimensional model

	2. Run the DDL scripts to create the Star Schema fact and dimension tables and their connections

B. Python - (/src/py/)

	1. Open "Data_ATL.py" and change global variables specified at the top to connect to any specific IP. Run using "python Data_ATL.py".
	This opens up a connection to the broker and awaits for a signal.

	2. Open "AutoMail.py" and change global variables specified at the top to connect to any specific IP. Run using "python AutoMail.py".
	This opens up a connection to the broker and awaits for a signal.

	3. Open "Data_EL.py" and in the main function, modify the variable "FOLDER_PATH" to the path where the file "Crime_Weather_2017.csv" resides
	Run the file using "python Data_EL.py"

	4. Once the process is complete, the local database in the machine will have a copy of the database star schema which can be analysed / visualised
	
	NOTE: The connections to the broker(RabbitMQ) can be monitored under - http://35.231.210.202:15672/#/ with username "admin" and password "password"

C. Tableau - (/src/tableau/)

	1. Open "Chicago_Crimes_Dashboard.twbx" and change credentials to the local database to pull and visualise the dashboard.
	
D. R- (/src/r/)
	1. The R markdown and the html output are in the "src/r" directory to give a view of the analysis
	
/**************************************************************************************************************\
