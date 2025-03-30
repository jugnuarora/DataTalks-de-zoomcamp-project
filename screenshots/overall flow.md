```mermaid
A[Raw Data Sources: Courses, Enrollments, Formacode] --> B(DLT: Data Ingestion & Initial Spark Transformations);
    B --> C[GCS: Transformed Data Lake];
    C --> F{Kestra: Orchestration};
    F --> G[BigQuery: Source Tables];
    G --> H(dbt Cloud: Data Modeling);
    H --> I[BigQuery: Fact & Dimension Tables];
    I --> J[Looker Studio: Visualizations];

    subgraph Data Ingestion & Transformation
        B; C;
    end

    subgraph Orchestration & Data Warehouse
        F; G; H; I;
    end

    subgraph Visualization
        J;
    end

    style A fill:#f9f,stroke:#333,stroke-width:2px,color:#000;
    style B fill:#ccf,stroke:#333,stroke-width:2px,color:#000;
    style C fill:#efe,stroke:#333,stroke-width:2px,color:#000;
    style F fill:#ccf,stroke:#333,stroke-width:2px,color:#000;
    style G fill:#efe,stroke:#333,stroke-width:2px,color:#000;
    style H fill:#ccf,stroke:#333,stroke-width:2px,color:#000;
    style I fill:#efe,stroke:#333,stroke-width:2px,color:#000;
    style J fill:#ccf,stroke:#333,stroke-width:2px,color:#000;
```
