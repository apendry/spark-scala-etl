# spark-scala-etl

Here we have a very basic example of a Spark/Scala ETL framework.
Contained within are ETLs that reflect commonly encountered data 
engineering problems.

The _common_ module contains methods, classes, and objects that are
used in more than one ETL. Generally you'd see things like Dataset
schemas, UDFs, timestamp related methods, etc etc.

The _batch_ module thus far only contains a single example ETL that
handles basic array functions. As time progresses more examples 
will populate this module to showcase different approaches to common
ETL related issues.

## Dependencies
- spark 3.0.3
- scala 2.13
- java 11
