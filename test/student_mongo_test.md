### Testing MongoDB Integration
This document outlines the steps to test the MongoDB integration within the Apache Airflow environment. Follow the instructions below to set up and run the tests successfully.

#### Prerequisites
- Ensure that you convert the student_blank.env file to student.env by filling in your specific MongoDB and SSH credentials.

#### Steps to Test MongoDB Integration
1. **Open New Terminal Window**: Start by opening a new terminal window on your local machine.
2. **Copy Paste Command**: Copy and paste the following command into your terminal to execute the MongoDB test script within the Airflow Docker container:
   ```bash
    docker-compose exec airflow-scheduler python /opt/airflow/test/student_tunnel_test.py
   ```
3. **Review Output**: After executing the command, review the output in the terminal to verify that the MongoDB integration is functioning as expected. It will display **"Tunnel running on port: 270017"** if the connection is successful.