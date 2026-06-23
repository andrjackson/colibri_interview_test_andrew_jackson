# Wind Turbine Pipeline

A small Databricks pipeline that ingests raw turbine telemetry, cleans it, and
produces daily summary stats. Following the medallion (bronze → silver → gold) pattern.

## Notes
- This pipeline is written with PySpark in anticipation of strong growth for this data. As it currrently stands Pandas or SQL would be really be a better fit for this amount of data, however Pandas doesnt really allow Spark/Databricks to shine so I made the assumption that this pipeline would see drastic growth.
- This is a daily batch pipeline. If there are security or maintenance concerns that require streamed monitoring, we could turn this into a real-time or near-real-time pipeline. However, in the interest of cost-savings this

## Notebooks

Each notebook takes catalog and schema as DAB set variables, so you point it at
whichever workspace/environment you like without editing code. Bronze uses Auto Loader, so
re-running only ingests new files

## HOW TO RUN
To run them as a single scheduled job, deploy the bundle in the CLI using these commands, or just use the databricks UI to autodeploy.
```bash
databricks bundle deploy -t dev
databricks bundle run wind_turbine_pipeline -t dev
```

## Notes & assumptions
- Cleaning drops only unusable rows
- Valid but
  unusual readings are kept and shown as anomolies
- Bronze ingests incrementally (Auto Loader, exactly-once via the checkpoint);
- silver and gold are rebuilt from the layer below on each run (overwrite, not append), so re-running is safe
