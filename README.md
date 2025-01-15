# Generic Database Loading

This repository contains the source code for databricks, for loading multiple databases, currently compatible with SQL Server and MongoDB.

## Objective

The main goal of these scripts is to facilitate the efficient and flexible loading of data from different database schemas, specifically designed to integrate with **Databricks** for streamlined data handling.

## Features

- **Generic Configuration**: Allows configuration of different databases by simply passing connection parameters such as host, user, password, and database name.
- **Incremental Loading**: Supports incremental loading, enabling the specification of the table and date column to load only the most recent data.
- **Compatibility**: Currently supports MSSQL Server and MongoDB, with the possibility of expanding to other databases easily with providers
- **Ease of Use**: Scripts are designed to be easily configurable and executable, minimizing the need for manual intervention.

## How to Use

1. **Configuration**:
   - Create a configuration file or pass the parameters directly, including details like database type, host, port, user, password, and database name.

2. **Full Loading**:
   - To perform a full data load, simply configure the connection parameters and run the corresponding script.

3. **Incremental Loading**:
   - For incremental loading, specify the table and date column. The script will fetch records added or updated since the last execution.

## Why Use Databricks Over Azure Data Factory (ADF) or other manager?

Using Databricks for data extraction and loading offers several advantages over traditional ETL tools like Azure Data Factory (ADF):

### 1. **Unified Data Processing and Analytics**
   - **Databricks** provides a unified platform for data engineering, machine learning, and analytics, which allows for a seamless transition between data loading and processing tasks.
   - Unlike ADF, which focuses primarily on orchestration and pipeline management, Databricks integrates data processing capabilities directly into the loading process, reducing the need for multiple tools.

### 2. **Scalability and Performance**
   - Databricks is built on **Apache Spark**, known for its ability to process large volumes of data efficiently. This makes it ideal for handling big data workloads that might be cumbersome in ADF.
   - **Parallel processing** and **in-memory computation** in Databricks can significantly reduce data load times compared to ADF's batch-oriented approach.

### 3. **Flexibility and Customization**
   - Databricks allows for greater flexibility in data transformations and custom scripting, which can be written in languages like Python, Scala, and SQL.
   - ADF has predefined activities and limited flexibility for custom transformations, whereas Databricks allows for complex, tailored data processing pipelines.

### 4. **Cost Efficiency**
   - Databricks can often be more cost-effective for extensive data processing tasks as it eliminates the need for separate data transformation stages that ADF might require.
   - It reduces the complexity of managing multiple services, as data extraction, transformation, and analysis can all occur within Databricks.

### 5. **Real-Time Data Processing**
   - With Databricks, it is easier to implement **real-time streaming** and process data as it arrives, a feature that ADF does not natively support.
   - This real-time capability is crucial for applications that need immediate insights from continuously flowing data.

### 6. **Integration with Advanced Analytics**
   - Databricks seamlessly integrates with **machine learning** and **advanced analytics** workflows, allowing for immediate utilization of loaded data in predictive modeling and other analytic processes.
   - ADF would typically require integration with other services to achieve similar functionality, adding complexity and potential points of failure.


## Connect with Me

I would love to connect with you on LinkedIn to discuss data engineering, technology, and share ideas. Feel free to connect with me by clicking the link below:

[**Connect with Me on LinkedIn**](https://www.linkedin.com/in/marcops)