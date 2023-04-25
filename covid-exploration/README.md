# EXPLORING COVID-19 DATA

# I. Visualizations link

Check my [Tableau dashboards](https://public.tableau.com/views/covid-project_16753597683370/Tableaudebord1?:language=en-US&:display_count=n&:origin=viz_share_link)!

<div class='tableauPlaceholder' id='viz1682431828262' style='position: relative'><noscript><a href='#'><img alt='Tableau de bord 1 ' src='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;co&#47;covid-project_16753597683370&#47;Tableaudebord1&#47;1_rss.png' style='border: none' /></a></noscript><object class='tableauViz'  style='display:none;'><param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' /> <param name='embed_code_version' value='3' /> <param name='site_root' value='' /><param name='name' value='covid-project_16753597683370&#47;Tableaudebord1' /><param name='tabs' value='no' /><param name='toolbar' value='yes' /><param name='static_image' value='https:&#47;&#47;public.tableau.com&#47;static&#47;images&#47;co&#47;covid-project_16753597683370&#47;Tableaudebord1&#47;1.png' /> <param name='animate_transition' value='yes' /><param name='display_static_image' value='yes' /><param name='display_spinner' value='yes' /><param name='display_overlay' value='yes' /><param name='display_count' value='yes' /><param name='language' value='en-US' /></object></div>                <script type='text/javascript'>                    var divElement = document.getElementById('viz1682431828262');                    var vizElement = divElement.getElementsByTagName('object')[0];                    if ( divElement.offsetWidth > 800 ) { vizElement.style.minWidth='420px';vizElement.style.maxWidth='100%';vizElement.style.minHeight='587px';vizElement.style.maxHeight=(divElement.offsetWidth*0.75)+'px';} else if ( divElement.offsetWidth > 500 ) { vizElement.style.minWidth='420px';vizElement.style.maxWidth='100%';vizElement.style.minHeight='587px';vizElement.style.maxHeight=(divElement.offsetWidth*0.75)+'px';} else { vizElement.style.width='100%';vizElement.style.height='1027px';}                     var scriptElement = document.createElement('script');                    scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';                    vizElement.parentNode.insertBefore(scriptElement, vizElement);                </script>

# II. Source OWID repository link

For more COVID-19 related datasets, check the Our World In Data [GitHub repository](https://github.com/owid/covid-19-data)!

# III. Project structure

The project's flow is as follows:

* 1. `/data/src` contains all the data downloaded from the OWID repo
* 2. `/queries/import_tasks` contains the SQL queries which import the source data into PostgreSQL
* 3. `/queries/preparation_queries.sql` contains the SQL queries to work on the imported data
* 4. `/data/pg_exports` contains the fully prepared dataset exported from PostgreSQL