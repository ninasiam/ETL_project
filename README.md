# ETL_project
ETL pipeline to process, enrich and load the data collected from user searches on SpaN portal. 
    
   In the directory `etl_dir/`: \
      - `ETL_fun.py` contains the functionality for the ETL pipeline. \
      - `etl_main.py` contain the main function, that initializes the sparkSession. \
      - `preprocess_data.py` initializes a sparkSession to preprocess the log files.
        
   In the project directory two shell scripts exist, that activate the conda enviroment and manage the directories.
   
   In the directory `report/`: \
      - the file `slides.pdf` is a small report on my approach.
   
   ## Built with
  - tools: pyspark, pandas.
  - python version: 3.8.5
