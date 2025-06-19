# hmpps-dpr-multiphase-cleanup-lambda

[![Ministry of Justice Repository Compliance Badge](https://github-community.service.justice.gov.uk/repository-standards/api/hmpps-dpr-multiphase-cleanup-lambda/badge)](https://github-community.service.justice.gov.uk/repository-standards/hmpps-dpr-multiphase-cleanup-lambda)

A Lambda function to remove expired records from the Redshift table which manages the multiphase query states.
It deletes all records older than the EXPIRY_TIME environment variable value or if that is not set, 7 days by default.
This runs daily on an EventBridge schedule.

#### The required environment variables needed to be set are:
- REDSHIFT_CLUSTER_ID
- REDSHIFT_CREDENTIAL_SECRET_ARN

#### Optional environment variables:
- DB_NAME
- MULTIPHASE_SCHEMA
- MULTIPHASE_TABLE
- REDSHIFT_STATUS_POLLING_WAIT_MS
- RETRY_TIMES
- RETRY_DELAY
- EXPIRY_TIME - This must be an integer followed by the time granularity. e.g `7 days`. 
<br> Possible time granularity values:
```
    second
    minute
    hour
    day
    week
    month
    year
```

