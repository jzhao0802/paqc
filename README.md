# Data Quality Control package of the Predictive Analytics team

This Python package was designed to perform and automate all the QC 
steps we take when we receive a new or updated data file. 
This package was designed to ensure consistent data quality across 
projects and minimise the effort spent on manual data QC, by automating
 most of our standard data checking procedures in the form of 
 pre-compiled list of tests. 

# Getting help
The package is fully tested and documented [here](ref). Please refer to 
 the documentation site for additional info about how to run, parametrise
  and customise your QC pipeline.
 
As a quick example, you can run paqc with the following line:
 ```python
 paqc.py -c "test_suite1.yml"
 ```

# Reporting issues

Reporting bugs or suggesting improvements can be done on Jira:

http://rwes-jira01.internal.imsglobal.com/browse/PA-684