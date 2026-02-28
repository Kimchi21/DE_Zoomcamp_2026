## Bruin

### Question 1

The required files and directories when initializing/running bruin are the following:
1. There should be a **.bruin.yml** which will serve as project configuration file.
2. A **pipeline/directory** which should contain a **pipeline.yml** which defines the pipeline and a **assets/ folder** which contains assets defined like python, SQL, reports, staging, etc.


### Question 2

Since the Since the NYC taxi data is organized by month using *pickup_datetime*, the best strategy is to use the **time_interval** so that it processes/reprocesses data for a set defined time window, and this deletes existing data for that interval and inserts refreshed data within the same time interval.


### Question 3

To bypass the taxi_types variable to only process yellow taxis, the variable of the yellow taxis should be passed during or at runtime. So this should be using **bruin run --var 'taxi_types=["yellow"]'** to run the pipeline and will only process yellow taxis.


### Question 4

In order to run the ingestions/trips.py script to include also downstream assets the right command to use is **bruin run --select ingestion.trips+** as the "+" selector includes/indicates all downstream assets will be included when executed.


### Question 5

To ensure that pickup_datetime never contains NULL values, a **not_null** quality check should be added to the asset definition.


### Question 6

**Bruin graph** generates a visualization of your pipeline’s asset dependency graph.


### Question 7

When running a Bruin pipeline for the first time the command used/enabled should be bruin run **--full-refresh**. This will drop existing tables (if there are any), creates the table again, and rebuilds all incremental models fully.