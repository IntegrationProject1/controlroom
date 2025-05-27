# Controlroom Monitoring Service
As part of our Integration Project, this service monitors the status of all system components by receiving heartbeat and log messages through RabbitMQ. Our consumer container receives these messages, and forwards them to the Elastic Stack (Logstash -> Elasticsearch -> Kibana). It provides visibility into service availability and logs for analysis and alerting.

## Features
- Listens for hearbeat/log messages on RabbitMQ
- Detects service downtime (default: 5s treshold)
- Forwards structured log and heartbeat data to Logstash
- Automatically tracks uptime/downtime
- Alerts administrators in case of service failure or errors
- Integrates with Kibana dashboards for real-time visualization

## Dashboard
On our monitoring dashboard on Kibana, administrators can see a structured overview of:
- Which service is currently up or down, and their latest recorded log message
- The downtime duration of each service for the chosen time period
- The moment each service was last seen, to pinpoint exactly when a service went down
- A list of all received logs, with a timestamp, the log status, and the log message to quickly filter through logs
- A graph of logs per time unit (depending of the chosen time period) to see if there were sudden spikes of errors or warnings at certain moments
- Users can filter by service name or status to display relevant info

<img width="1439" alt="dashboard-1" src="https://github.com/user-attachments/assets/a23b08aa-3682-4504-b150-821a0c8b5063" />
<img width="1439" alt="dashboard-2" src="https://github.com/user-attachments/assets/9bdc14c1-fd5e-4234-b151-a6b2a1c869e7" />

## Project Structure
controlroom/
├── consumer/  # Main RabbitMQ consumer and processor
├── elasticsearch/  # Contains Elasticsearch configuration
├── kibana/  # Contains Kibana configuration
├── kibana-objects/  # Contains saved Kibana data views, configs, and dashboards
├── logstash/  # Contains Logstash configuration
├── publisher/  # Simulates a real service, sending heartbeats and logs for testing purposes
├── tests/  # Unit tests with mock queues
├── docker-compose.yml         # Compose setup (optional)
├── .env                       # Environment variables
├── .env-example               # Example of the environment variables the system expects
└── README.md                  # This file
