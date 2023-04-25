# EXPLORING COVID-19 DATA

# I. Visualizations link

Check my [Tableau dashboards](https://public.tableau.com/views/covid-project_16753597683370/Tableaudebord1?:language=en-US&:display_count=n&:origin=viz_share_link)!

# II. Source OWID repository link

For more COVID-19 related datasets, check the Our World In Data [GitHub repository](https://github.com/owid/covid-19-data)!

# III. Project structure

The project's flow is as follows:

* 1. `/data/src` contains all the data downloaded from the OWID repo
* 2. `/queries/import_tasks` contains the SQL queries which import the source data into PostgreSQL
* 3. `/queries/preparation_queries.sql` contains the SQL queries to work on the imported data
* 4. `/data/pg_exports` contains the fully prepared dataset exported from PostgreSQL