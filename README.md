# tippers-benchmark
A concurrent transaction simulator that mimics a real life transaction workload of an IoT based system on a database 
system.

## Setup
1. First and foremost, verify that you have the following: MySQL (version ___ was used), PostgreSQL (version ____ was used), and Anaconda.
2. Next, we want to create a Python virtual environment for testing. Run the following commands: 

     ```bash
     > cd tippers-benchmark
     > conda env create -f environment.yml
     > conda activate tippers-benchmark-env
     ```

3. Download the _reorganized_ project files. From the project source, these have been sorted and modified to work with our simulator.

    Low Concurrency - https://drive.google.com/open?id=1GrfETEZ1QZeUi-7WRedQtHmHNBsrgeth
    High Concurrency - https://drive.google.com/open?id=1tfoi5-H4fQMxM07gz2DoQgvkH5QEJjQg 

    Place the low-concurrency file in the `resources/data/low_concurrency` folder. Place the high-concurrency file in the `resources/data/high_concurrency` folder. You should now have the following project structure:
    ```
    tippers-benchmark
    |- config
    |- resources
       |- schema
       |- data
          |- low_concurrency
             |- metadata.sql
             |- sorted_low_concurrency_queries.txt
          |- high_concurrency
             |- metadata.sql
             |- sorted_high_concurrency_queries.txt
    |- ...
    ```
    
    If necessary, modify the `config/general.json` to point to the correct project files.

5. Create a fresh MySQL instance and PostgreSQL instance. Modify the parameters in `config/mysql.json` and `config/postgres.json` to include your credentials. **Ensure that the user you provide in both instances is a super-user.**

6. While your Anaconda environment is activated, run the launcher script with the `-r` option to verify MySQL and Postgres connections. You may have to change permissions on the script (`chmod +x launcher.sh`) before running it:

    ```bash
    > cd tippers-benchmark
    > ./launcher.sh -r mysql
    [mysql] experiment database has been destroyed.
    [mysql] has been initialized.
 
    > ./launcher.sh -r postgres
    [postgres] experiment database has been destroyed.
    [postgres] has been initialized.
    ```
    
7. You are now ready to run experiments! Feel free to modify the experiment parameters below in `config/general.json`:
    ```
    "observation-frequency": 0.1           # Determines the polling frequency of records, in actions / minute.
    "testing-mpl": [1, 2, 3, 4, 5, 6]      # Determines the MPL to test with.
    "testing-concurrency": ["high", "low"] # Determines the concurrency levels to test with (must be a list). 
    "testing-experiments": ["t", "q", "w"] # Determines which experiments to run (t=load, q=query, w=workload).
                                           # Note: order matters in experiments! "t" must come before "q".
    ```
    
    Run the experiments by running the launcher script with the `-x` option and the database you wish to collect data on.
    ```
    > cd tippers-benchmark
    > ./launcher.sh -x mysql 
    Running: experiment [t], high concurrency, MPL 1.
    .
    .
    .
    Experiments are finished!
    > ./launcher.sh -x postgres
    Running: experiment [t], high concurrency, MPL 1.
    .
    .
    .    
    Experiments are finished!
    ```
    
8. View the results by querying the observation SQLite database (i.e. `results/observation.db`).

    ```
    > cd tippers-benchmark
    > sqlite3 results/observation.db
    >> .tables
    MySQLStatisticsOnHost         MySQLStatisticsParent       
    MySQLStatisticsOnIndex        PostgresResultsOnTable      
    MySQLStatisticsOnLock         PostgresStatisticsOnDatabase
    MySQLStatisticsOnTable        PostgresStatisticsParent    
    >> .schema
    CREATE TABLE MySQLStatisticsOnHost (
                measurement_time DATETIME,
                statements INTEGER,
                statement_latency TEXT,
                statement_avg_latency TEXT,
                statement_lock_latency TEXT,
                statement_rows_sent INTEGER,
                statement_rows_examined INTEGER,
                statement_rows_affected INTEGER,
                table_scans INTEGER,
                file_ios INTEGER,
                file_io_latency TEXT,
                current_memory TEXT,
                total_memory_allocated TEXT,
                FOREIGN KEY(measurement_time) REFERENCES MySQLStatisticsParent(measurement_time)
            );
    >> .quit
    ```