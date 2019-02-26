'use strict';
const BigQuery = require('@google-cloud/bigquery');
const Storage = require('@google-cloud/storage');

const assert = require('assert');

module.exports = {
  /**
   * Create a dataset from scratch.
   */
  createDataset: function(datasetId, projectId) {
    // [START bigquery_create_dataset]

    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Creates a new dataset
    bigquery
      .createDataset(datasetId)
      .then(results => {
        const dataset = results[0];
        console.log(`Dataset ${dataset.id} created.`);
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * Delete existing dataset.
   */
  deleteDataset:function (datasetId, projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Creates a reference to the existing dataset
    const dataset = bigquery.dataset(datasetId);

    // Deletes the dataset
    dataset
      .delete()
      .then(() => {
        console.log(`Dataset ${dataset.id} deleted.`);
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * List existing datasets.
   */
  listDatasets: function (projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Lists all datasets in the specified project
    bigquery
      .getDatasets()
      .then(results => {
        const datasets = results[0];
        console.log('Datasets:');
        datasets.forEach(dataset => console.log(dataset.id));
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * Create a table from scratch.
   */
  createTable: function (datasetId, tableId, schema, projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // For all options, see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
    const options = {
      schema: schema,
    };

    // Create a new table in the dataset
    bigquery
      .dataset(datasetId)
      .createTable(tableId, options)
      .then(results => {
        const table = results[0];
        console.log(`Table ${table.id} created.`);
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * Create and load a table from local file.
   */
  createAndLoadTable: function (datasetId, tableId, schema, filename, projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // For all options, see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource
    const options = {
      schema: schema,
    };

    // For passing into callbacks
    var that = this;

    // Create a new table in the dataset
    bigquery
      .dataset(datasetId)
      .createTable(tableId, options)
      .then(results => {
        that.loadLocalFile(datasetId, tableId, filename, projectId)
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * Delete an existing table.
   */
  deleteTable: function(datasetId, tableId, projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Deletes the table
    bigquery
      .dataset(datasetId)
      .table(tableId)
      .delete()
      .then(() => {
        console.log(`Table ${tableId} deleted.`);
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * List all tables in a dataset.
   */
  listTables: function (datasetId, projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Lists all tables in the dataset
    bigquery
      .dataset(datasetId)
      .getTables()
      .then(results => {
        const tables = results[0];
        console.log('Tables:');
        tables.forEach(table => console.log(table.id));
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  loadFileComplete: function(datasetId, tableId, schema, filename, projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    var that = this;

    // Lists all tables in the dataset
    bigquery
      .dataset(datasetId)
      .getTables()
      .then(results => {
        const tables = results[0];
        var exists = false;
        tables.forEach(function(table){
          if(table.id.trim() == tableId.trim()){
            exists = true
          };
        });
        if(!exists){
          that.createAndLoadTable(datasetId, tableId, schema, filename, projectId);
        }else{
          that.loadLocalFile(datasetId, tableId, filename, projectId);
        }
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * Browse rows in a table.
   */
  browseRows: function(datasetId, tableId, projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Lists rows in the table
    bigquery
      .dataset(datasetId)
      .table(tableId)
      .getRows()
      .then(results => {
        const rows = results[0];
        console.log('Rows:');
        rows.forEach(row => console.log(row));
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },
  /**
   * Copy srcDataset.srcTable to destDataset.destTable.
   */
  copyTable: function(srcDatasetId,srcTableId,destDatasetId,destTableId,projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Copies the table contents into another table
    bigquery
      .dataset(srcDatasetId)
      .table(srcTableId)
      .copy(bigquery.dataset(destDatasetId).table(destTableId))
      .then(results => {
        const job = results[0];

        // load() waits for the job to finish
        assert.equal(job.status.state, 'DONE');
        console.log(`Job ${job.id} completed.`);

        // Check the job's status for errors
        const errors = job.status.errors;
        if (errors && errors.length > 0) {
          throw errors;
        }
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * Load a local file into table.
   */
  loadLocalFile: function(datasetId, tableId, filename, projectId) {
   // Creates a client
   const bigquery = new BigQuery({
     projectId: projectId,
   });

   // Loads data from a local file into the table
   bigquery
     .dataset(datasetId)
     .table(tableId)
     .load(filename)
     .then(results => {
       const job = results[0];

       // load() waits for the job to finish
       assert.equal(job.status.state, 'DONE');
       console.log(`Job ${job.id} completed.`);

       // Check the job's status for errors
       const errors = job.status.errors;
       if (errors && errors.length > 0) {
         throw errors;
       }
     })
     .catch(err => {
       console.error('ERROR:', err);
     });
  },

  /**
   * Load a file from Google Cloud Storage
   */
  loadFileFromGCS: function(datasetId, tableId, bucketName, filename, projectId) {
    // Instantiates clients
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    const storage = new Storage({
      projectId: projectId,
    });

    // Loads data from a Google Cloud Storage file into the table
    bigquery
      .dataset(datasetId)
      .table(tableId)
      .load(storage.bucket(bucketName).file(filename))
      .then(results => {
        const job = results[0];

        // load() waits for the job to finish
        assert.equal(job.status.state, 'DONE');
        console.log(`Job ${job.id} completed.`);

        // Check the job's status for errors
        const errors = job.status.errors;
        if (errors && errors.length > 0) {
          throw errors;
        }
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * Extracts entire table to google cloud storage.
   */
  extractTableToGCS: function(datasetId,tableId,bucketName,filename,projectId){
   // Instantiates clients
   const bigquery = new BigQuery({
     projectId: projectId,
   });

   const storage = new Storage({
     projectId: projectId,
   });

   // Exports data from the table into a Google Cloud Storage file
   bigquery
     .dataset(datasetId)
     .table(tableId)
     .extract(storage.bucket(bucketName).file(filename))
     .then(results => {
       const job = results[0];

       // load() waits for the job to finish
       assert.equal(job.status.state, 'DONE');
       console.log(`Job ${job.id} completed.`);

       // Check the job's status for errors
       const errors = job.status.errors;
       if (errors && errors.length > 0) {
         throw errors;
       }
     })
     .catch(err => {
       console.error('ERROR:', err);
     });
  },

  /**
   * Insert rows into table as a stream.
   */
  insertRowsAsStream: function(datasetId, tableId, rows, projectId) {
   // Creates a client
   const bigquery = new BigQuery({
     projectId: projectId,
   });

   // Inserts data into a table
   bigquery
     .dataset(datasetId)
     .table(tableId)
     .insert(rows)
     .then(() => {
       console.log(`Inserted ${rows.length} rows`);
     })
     .catch(err => {
       if (err && err.name === 'PartialFailureError') {
         if (err.errors && err.errors.length > 0) {
           console.log('Insert errors:');
           err.errors.forEach(err => console.error(err));
         }
       } else {
         console.error('ERROR:', err);
       }
     });
  },

  /**
   * Run a query synchronously.
   */
  syncQuery: function(sqlQuery, projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Query options list: https://cloud.google.com/bigquery/docs/reference/v2/jobs/query
    const options = {
      query: sqlQuery,
      timeoutMs: 10000, // Time out after 10 seconds.
      useLegacySql: true, // Use standard SQL syntax for queries.
    };

    // Runs the query
    bigquery
      .query(options)
      .then(results => {
        const rows = results[0];
        console.log('Rows:');
        rows.forEach(row => console.log(row));
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * Just a plain ol' query.
   */
  query: function(sqlQuery, projectId,callback){
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Query options list: https://cloud.google.com/bigquery/docs/reference/v2/jobs/query
    const options = {
      query: sqlQuery,
      timeoutMs: 10000, // Time out after 10 seconds.
      useLegacySql: true, // Use standard SQL syntax for queries.
    };

    // Runs the query
    bigquery
      .query(options)
      .then(callback)
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * Run a query asynchronously.
   */
  asyncQuery: function(sqlQuery, projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Query options list: https://cloud.google.com/bigquery/docs/reference/v2/jobs/query
    const options = {
      query: sqlQuery,
      useLegacySql: true, // Use standard SQL syntax for queries.
    };

    let job;

    // Runs the query as a job
    bigquery
      .createQueryJob(options)
      .then(results => {
        job = results[0];
        console.log(`Job ${job.id} started.`);
        return job.promise();
      })
      .then(() => {
        // Get the job's status
        return job.getMetadata();
      })
      .then(metadata => {
        // Check the job's status for errors
        const errors = metadata[0].status.errors;
        if (errors && errors.length > 0) {
          throw errors;
        }
      })
      .then(() => {
        console.log(`Job ${job.id} completed.`);
        return job.getQueryResults();
      })
      .then(results => {
        const rows = results[0];
        console.log('Rows:');
        rows.forEach(row => console.log(row));
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },
  /**
   * Run a query synchronously.
   */
  syncQueryStd: function(sqlQuery, projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Query options list: https://cloud.google.com/bigquery/docs/reference/v2/jobs/query
    const options = {
      query: sqlQuery,
      timeoutMs: 10000, // Time out after 10 seconds.
      useLegacySql: false, // Use standard SQL syntax for queries.
    };

    // Runs the query
    bigquery
      .query(options)
      .then(results => {
        const rows = results[0];
        console.log('Rows:');
        rows.forEach(row => console.log(row));
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * Just a plain ol' query.
   */
  queryStd: function(sqlQuery, projectId,callback){
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Query options list: https://cloud.google.com/bigquery/docs/reference/v2/jobs/query
    const options = {
      query: sqlQuery,
      timeoutMs: 10000, // Time out after 10 seconds.
      useLegacySql: false, // Use standard SQL syntax for queries.
    };

    // Runs the query
    bigquery
      .query(options)
      .then(callback)
      .catch(err => {
        console.error('ERROR:', err);
      });
  },

  /**
   * Run a query asynchronously.
   */
  asyncQueryStd: function(sqlQuery, projectId) {
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Query options list: https://cloud.google.com/bigquery/docs/reference/v2/jobs/query
    const options = {
      query: sqlQuery,
      useLegacySql: false, // Use standard SQL syntax for queries.
    };

    let job;

    // Runs the query as a job
    bigquery
      .createQueryJob(options)
      .then(results => {
        job = results[0];
        console.log(`Job ${job.id} started.`);
        return job.promise();
      })
      .then(() => {
        // Get the job's status
        return job.getMetadata();
      })
      .then(metadata => {
        // Check the job's status for errors
        const errors = metadata[0].status.errors;
        if (errors && errors.length > 0) {
          throw errors;
        }
      })
      .then(() => {
        console.log(`Job ${job.id} completed.`);
        return job.getQueryResults();
      })
      .then(results => {
        const rows = results[0];
        console.log('Rows:');
        rows.forEach(row => console.log(row));
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },
  /**
   * Run a query asynchronously.
   */
  createTableFromQuery: function(sqlQuery, projectId, datasetId, destinationTable, legacy) {
    if(legacy === undefined){
      legacy = false;
    }
    // Creates a client
    const bigquery = new BigQuery({
      projectId: projectId,
    });

    // Query options list: https://cloud.google.com/bigquery/docs/reference/v2/jobs/query
    const options = {
      query: sqlQuery,
      useLegacySql: legacy,
      destinationTable: (bigquery.dataset(datasetId).table(destinationTable)),
    };

    let job;

    // Runs the query as a job
    bigquery
      .createQueryJob(options)
      .then(results => {
        job = results[0];
        console.log(`Job ${job.id} started.`);
        return job.promise();
      })
      .then(() => {
        // Get the job's status
        return job.getMetadata();
      })
      .then(metadata => {
        // Check the job's status for errors
        const errors = metadata[0].status.errors;
        if (errors && errors.length > 0) {
          throw errors;
        }
      })
      .then(() => {
        console.log(`Job ${job.id} completed.`);
        return job.getQueryResults();
      })
      .then(results => {
        const rows = results[0];
        console.log('Rows:');
        rows.forEach(row => console.log(row));
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  },
}
