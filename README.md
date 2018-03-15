# Bigquery Utility module

Node module to access bigquery.

## Prerequisites

You'll need to set up a google project with a service user and download the service user p12 file.  You'll also need to put the service user and p12 file into your environment as described here: https://cloud.google.com/bigquery/docs/authentication/service-account-file  
If you are installing on a shared server, try the `dotenv` node package.  

## Installing

To add this to your node project type the following at the command line:
```
npm install --save bqutil
```

## Using the package

All auth is handled for you via the environment variables.  Call the module like so:
```
bq.listDatasets(projectId);

```
Commands are not currently chainable.  Each command is it's own mini-session with BQ.  
