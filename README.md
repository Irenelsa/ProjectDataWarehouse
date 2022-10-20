# Purpose
Sparkify is a music application that takes inforation from several logs to get information about Songs, Artist and when users plays these songs.

Requeriments:
- This code project is created with python3
- dwh.cfg is a properties file where you should fill all your aws credentials 
- Database is postgres, and it will be created on redshift cluster. 
- You must have an AWS profile, and you are going to upload one Redshift cluster, so charges will surely be made to your account.



What does this project include?:
- The project has all properties that you can use in this project, but it doen't have all values for security  
- etl.py contains main function that contains functions to load json files from S3 into the database.
- In aws_cluster.ipynb you can find a jupyter notebook to create iam roles, cluster and a postgres database. Once you have test this project you can delete all to avoid charges.


ETL Process:
- You need to complete dwh.cfg file with your credential
- If you don't have a cluster, you can create one from aws_cluser.ipynb file
- When you have all connections working, you can run main function from etl.py file. These are the steps:
    - Get events logs from S3 using Jsonpath file to match columns
    - get songs files copying Json S3 files info. 
    - Once we have all data loaded, you made some insert from these two staging tables to load analytics tables.

Results:
We have a music database where you have load and select metrics. Once you have some reports you can study if you can optimize some tables designating distribution styles, devaluating fact and dimension tables to destinate distribution styles and designating distribution keys.

