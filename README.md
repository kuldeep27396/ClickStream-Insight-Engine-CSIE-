# RealTime-ECommerce-Analytics (RTEA)

## Overview

RealTime-ECommerce-Analytics (RTEA) is a high-performance, scalable solution for processing and analyzing e-commerce clickstream data in real-time. It handles data from both mobile and web sources, processes up to 50,000 messages per second, and generates valuable metrics for business intelligence.

The project uses Apache Kafka for data ingestion, Apache Flink for stream processing, and Google BigQuery for storing the computed metrics.

## Features

- High-volume data generation simulating realistic e-commerce clickstream events
- Real-time data processing with Apache Flink
- Multi-source data handling (mobile app and website)
- Scalable architecture capable of processing 50k messages per second
- Key e-commerce metrics computation:
  - Event counts by type and source
  - Revenue by product category
  - Unique visitor counts
- Seamless integration with Google BigQuery for metric storage and further analysis

## Prerequisites

- Python 3.7+
- Apache Kafka
- Apache Flink 1.15.0
- Google Cloud account with BigQuery enabled
- Java 8 or 11 (for running Flink)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/RealTime-ECommerce-Analytics.git
   cd RealTime-ECommerce-Analytics
   ```

2. Install the required Python packages:
   ```
   pip install kafka-python pyflink google-cloud-bigquery
   ```

3. Download the following JAR files and place them in a `jars` directory in the project root:
   - flink-sql-connector-kafka-1.15.0.jar
   - flink-connector-jdbc-1.15.0.jar
   - google-cloud-bigquery-1.27.0.jar
   - slf4j-api-1.7.30.jar
   - slf4j-log4j12-1.7.30.jar

4. Set up Google Cloud credentials for BigQuery access:
   ```
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-file.json"
   ```

## Configuration

1. Update the Kafka broker address in both `ecommerce_data_generator.py` and `pyflink_ecommerce_analysis.py` if different from `localhost:9092`.

2. In `pyflink_ecommerce_analysis.py`, update the BigQuery connection details:
   - Replace `your-project-id` with your Google Cloud project ID
   - Replace `your-dataset.ecommerce_metrics` with your desired BigQuery dataset and table name

## Usage

1. Start your Kafka cluster and create the required topic:
   ```
   bin/kafka-topics.sh --create --topic ecommerce_clickstream --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
   ```

2. Run the data generator:
   ```
   python ecommerce_data_generator.py
   ```

3. In a separate terminal, run the Flink job:
   ```
   python pyflink_ecommerce_analysis.py
   ```

The Flink job will start processing the incoming data and writing metrics to BigQuery.

## Project Structure

- `ecommerce_data_generator.py`: Generates simulated e-commerce clickstream data and pushes it to Kafka
- `pyflink_ecommerce_analysis.py`: Defines and executes the Flink job for processing the data and computing metrics
- `jars/`: Directory containing necessary JAR files for Flink connectors
- `README.md`: This file

## Metrics Generated

1. Event counts by type and source (e.g., page_view_mobile, add_to_cart_website)
2. Revenue by product category
3. Unique visitors count

These metrics are computed over 1-minute tumbling windows and stored in BigQuery for further analysis.

## Scaling

The current configuration is set to handle 50,000 messages per second. To scale the system:

- Increase the number of Kafka partitions
- Adjust the Flink job parallelism in `pyflink_ecommerce_analysis.py`
- Scale your BigQuery resources as needed

## Contributing

Contributions to RealTime-ECommerce-Analytics are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments

- Apache Flink community
- Apache Kafka community
- Google Cloud BigQuery team