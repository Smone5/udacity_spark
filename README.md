# ETL Pipeline for Datalake on AWS

## Summary of Project
This project is to build a data lake for a fictional music streaming startup called Sparkify using an ETL process. The ETL process loads json data from S3, transforms the data using PySpark and outputs the data into parquet files for analytics to use efficiently.  

## How to run Python Scripts
To the run the ETL script, in the directory with the file "etl.py":
1. Run pip install -r requirements.txt if python environment is not already setup.
2. If not done already [create an access key and secrect key](https://aws.amazon.com/premiumsupport/knowledge-center/create-access-key/) on AWS with enough privileges and programmatic access.
3. In the file "dl.cfg", enter your AWS access key and secret key:

        Example:
        AWS_ACCESS_KEY_ID=<access key>
        AWS_SECRET_ACCESS_KEY=<secret key>
  
4. Ensure the folders on AWS S3 are empty:
    1. artist_table
    2. user_table
    3. time_table
    4. songs_table
    5. songsplays_table
    
5. Open terminal and run:
        python etl.py
 
## Explanation of files in Repository
+ **dl.cfg**: The configuration file to help the program access AWS with correct credentials.
+ **emr_spark.ipynb**: A Jupyter notebook to run the ETL process on Amazon EMR.
+ **etl.py**: The python script that runs the extract, transform and load (ETL) process.
+ **local_spark.ipynb**: A Jupyter notebook to test the ETL process locally on a sample of data.
+ **requirements.txt**: A list of python libraries used in the environment to run the ETL process.
+ **s3**: The folder with data used for the Jupyter notebook "local_spark".
