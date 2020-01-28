# tippers-benchmark
A concurrent transaction simulator that mimics a real life transaction workload of an IoT based system on a database 
system.

## Setup
1. First and foremost, verify that you have the following: MySQL (version ___ was used), PostgreSQL (version ____ was used), and Anaconda.
2. Next, we want to create a Python virtual environment for testing. Run the following commands: 

     ```bash
     cd tippers-benchmark
     conda env create -f environment.yml
     conda activate tippers-benchmark-env
     ```

3. Download the _reorganized_ project files. From the project source, these have been sorted and modified to work with our simulator.

    **PUT IN NEW LINK HERE**

    Unzip the folder, and copy the sub-folders `data` and `queries` to the `resources` folder. You should now have the following project structure:
    ```
    tippers-benchmark
    |- resources
       |- create.sql
       |- drop.sql
       |- data
       |- queries
    |- ...
    ```
4. Create a MySQL instance with the following options (TODO: take out??). Start your instance.
5. Create a PostgreSQL instance with the following options (TODO: take out??). Start your instance.
6. Run the DDL `create.sql` in the `resources` folder for both Postgres and MySQL.
    
    ```bash
    
    ```