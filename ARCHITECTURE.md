
# GuardianVision System Architecture

This document provides a detailed breakdown of each component within the GuardianVision system, designed for scalable, real-time safety monitoring.

---

## Component 1: Scalable Camera Integration and Frame Storage

**Description**: "First, our system begins with the scalable integration of camera feeds and frame storage. Administrators onboard cameras by entering details like the RTSP connection string, camera ID, and site ID, typically through Kissflow. This onboarding process standardizes camera setup and ensures smooth integration with our system, though we also support alternative platforms for flexibility.

Once cameras are onboarded, our system captures frames from each feed at 30-second intervals using Spark Streaming within Databricks. This interval is configurable to meet specific monitoring requirements. Captured frames are stored in Azure Data Lake Storage (ADLS), providing a highly scalable and reliable data storage solution, facilitating efficient access and retrieval for subsequent processing and analysis.

The frames stored in ADLS are then incrementally read using Databricks Autoloader, which pulls new data as it becomes available. The Autoloader scales automatically to accommodate increased data flows from additional cameras. After retrieving frames, Autoloader pushes them in real time to a message queue in Azure Service Bus. This design decouples the data ingestion process from downstream multimodal LLM processing, establishing an efficient and scalable data pipeline."

---

## Component 2: Dynamic Checklist Generation and Safety Evaluation

**Description**: "After frames are queued in Azure Service Bus, the system moves to the next component, which combines Retrieval-Augmented Generation (RAG) with a multimodal LLM for safety evaluations. The RAG system dynamically generates a safety checklist based on a user-specified activity description or, if no description is provided, by analyzing an initial frame to infer the activity. Safety protocols stored in a Databricks volume are retrieved for RAG, scaling automatically to handle additional data as ...

For efficient processing, the system reads messages from Azure Service Bus in batches of 100 (configurable), then triggers the LLM asynchronously for each batch. As responses from the LLM are captured, they are collected in a list and then written in batches to a Delta table in Databricks. This approach optimizes processing efficiency by minimizing write operations and ensuring high throughput as the system scales."

---

## Component 3: Alerting and Real-Time Dashboard Reporting

**Description**: "Finally, the system’s third component supports real-time incident tracking and reporting. Safety assessment results, including safety scores, descriptions of detected violations, and violation categories, are stored in the Delta table in Databricks. This efficient storage method enables the system to handle large incident volumes and maintain historical data for detailed trend analysis.

For incidents with a safety score of 1 or 2, alerts are sent to users with details about the incident type and description. Users can access the Kissflow portal for tracking and resolving incidents or use alternative platforms if preferred. The Databricks dashboard displays all incident data in real time, allowing stakeholders to filter by safety scores, incident counts, and compliance trends. Supported by Databricks’ autoscaling environment, the dashboard remains responsive even with increased data, pproviding decision-makers with up-to-date insights.

