# Module 2 Homework: Orchestration with Kestra

Assignment: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/02-workflow-orchestration/homework.md
Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw2

## Assignment:

_For the homework, we'll be working with the green taxi dataset._
_So far in the course, we processed data for the year 2019 and 2020. Your task is to extend the existing flows to include data for the year 2021._
_You can leverage the backfill functionality in the scheduled flow to backfill the data ... Alternatively, run the flow manually for each of the seven months of 2021 for both yellow and green taxi data. Challenge for you: find out how to loop over the combination of Year-Month and taxi-type using ForEach_

## Solution:

- inspiration mainly from ```09_gcp_taxi_scheduled``` and ```06_gcp_kv```
- I got rid of ```inputs``` and yellow data part since the assignment asks for green data and all available dates
- I added more ```variables``` as its something specific for the flow, not the whole domain (instead of using e.g. ```kv store```)
- --> this meant I had to replace some ```kv(...)``` to ```render(var.var_name)``` in some tasks (mainly queries)
- most headache I had with secret ... for some reason it was throwing error below:
```log
ERROR 2026-01-26T21:33:42.000211Z Use JsonReader.setStrictness(Strictness.LENIENT) to accept malformed JSON at line 1 column 3 path $.
See https://github.com/google/gson/blob/main/Troubleshooting.md#malformed-json
```
- bcs I first took the same approach as with other new vars and put there a var: ```GCP_CREDS: "{{ secret('GCP_SERVICE_ACCOUNT') }}"```
- ... it dint like it, so I tried putting directly in ``` part like ```serviceAccount: "{{secret('GCP_SERVICE_ACCOUNT')}}"```
- ... same result, end up doing the same way as during the course:
1) added dedicated kv setting task
1) added dedicated kv setting task```
```yaml
id: gcp_service_account
type: io.kestra.plugin.core.kv.Set
key: GCP_CREDS
kvType: STRING
value: "{{ secret('GCP_SERVICE_ACCOUNT') }}"
```
2) referenced as ```kv``` in ```pluginDefaults```
```yaml
pluginDefaults:
  - type: io.kestra.plugin.gcp
    values:
      serviceAccount: "{{kv('GCP_CREDS')}}"
      projectId: "{{vars.GCP_PROJECT_ID}}"
      location: "{{vars.GCP_LOCATION}}"
      bucket: "{{vars.GCP_BUCKET_NAME}}"
```
- finally I ran it from ```Triggers``` section with start date ```2019-01-01 00:00:00``` and end date ```2021-07-02 00:00:00```
- ... since the quiz questions also ask for yellow data --> creating dedicated flow for it

## Quiz questions with my solution

1) Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?

```128.3 MiB```

2) What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?

```{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv```

3) How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

6405009 + 6299355 + 3007293 + 237994 + 348372 + 549761 + 800413 + 1007285 + 1341013 + 1681132 + 1508986 + 1461898 
= 24648511 - 12 = ```24,648,499```

4) How many rows are there for the Green Taxi data for all CSV files in the year 2020?

447771 + 398633 + 223407 + 35613 + 57361 + 63110 + 72258 + 81064 + 87988 + 95121 + 88606 + 83131 
= 1734063 - 12 = ```1,734,051```

5) How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

```1,925,152```

6) How would you configure the timezone to New York in a Schedule trigger?

```Add a timezone property set to America/New_York in the Schedule trigger configuration```