# tedstats

A package for calculating yearly statistics for EU awarded public procurement notices.

## Input data 
The package downloads TED CAN reports in CSV from [http://data.europa.eu/euodp/repository/ec/dg-grow/mapps/](http://data.europa.eu/euodp/repository/ec/dg-grow/mapps/). The data treament applied accounted for the information contained in [PDF](http://data.europa.eu/euodp/repository/ec/dg-grow/mapps/TED\(csv\)_data_information.pdf).
## Output data
The following numbers are calculated:

1. Total yearly averaged number of Contract Award Notices (CAN) for each country. The average is calculated only for years of nonzero value of CAN for a particular country. 
2. Average values of CAN. The average does not include CAN with zero CAN value. The currency is EUR.


## Requirements

This package requires:

 * Spark 2.3.0
 * Scala 2.11


## Usage
This program take the following parameters:

 * start year of statistics
 * end year of statistics
 * temprary data and output system path
 * keep temporary files (true/false).
 
 | Parameter   |     Type      |  Default |
|----------|:-------------:|------:|
| output path | system path |     |
| start year |  number | 2006 |
| end year |    number   |   2017 |
| keep temp files | true/false |  false   |
 
 
 Example:
 
 `./bin/spark-submit --class sparkTEDstats.AllegroSpark  /srv/spark/tedstats_2.11-0.1.jar 2006 2007 /srv/spark/ true `



## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html). To build a JAR file simply run `sbt/sbt package` from the project root.

