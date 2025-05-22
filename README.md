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

