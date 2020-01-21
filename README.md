# tippers-benchmark
A concurrent transaction simulator that mimics a real life transaction workload of an IoT based system on a database 
system.

## Setup
1. First, install Anaconda: https://docs.anaconda.com/anaconda/install/
2. Next, we want to create a Python virtual environment as well as install PostgreSQL and MySQL-- the databases we wish 
to test. Run the following commands: 

    ```bash
    cd tippers-benchmark
    conda env create -f environment.yml
    conda activate tippers-benchmark-env
    ```

3. Download the project files (DDLs and DMLs) from the project website: 

    https://drive.google.com/open?id=1Z3_8AK3NHjecnZsWlAf91uxAdf6Y0H70 

    Unzip the folder, and put this in the same path as this repository. Do not change the folder name from `project1`.
    