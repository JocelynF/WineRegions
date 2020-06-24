# Appellation Trail

## Background

I consulted with a largest online media company targeting millennial drinkers. They have 3-5 million page views per month so their web traffic data can be used to inform future content and marketing decisions.

## Pipeline

Page view and search data are collected from the Google Search and Analytics API and combined with the company WordPress database to in an AWS EC2-hosted MySQL database. This database is updated daily and annually, and can also be updated manually. 

Using the company WordPress database as well as the AWS-hosted MySQL database, I organize the data by wine type and regional tags. Page views are filtered for outliers. They are then weighted by the importance of each page to the individual wine type and regional groups. Page views are aggregated into regional groups for each wine type and then normalized to the wine type. Files are output to csv and json files to send to company front end.

## Running the Code

The code uses Python 3.8.3. ScoreScript.py takes in user input and calls VinePairFunctions.py. Third-party packages used:

* numpy
* pandas
* sqlalchemy
* skleanr

## Branches

The master branch includes the finalized code. The testing branch includes code and jupyter notebooks used for testing and plotting. 
