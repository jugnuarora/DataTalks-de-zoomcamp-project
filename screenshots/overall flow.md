```mermaid
graph LR
    subgraph "Data Sources (EL)"
        A[Courses]
        B[Enrollments]
        C[Formacode]
    end

    subgraph "Data Ingestion & Initial Transformation"
        subgraph "Kestra & GCS"
            A --> D(DLT: Raw Data)
            B --> D
            C --> D
            D --> E[Spark: Initial Transformed Data]
        end
    end

    subgraph "Data Transformation"
        subgraph "BigQuery"
            E --> F[Partitioned & Clustered source_tables]
            F --> G(dbt: Facts & Dimensions)
        end
    end

    subgraph "Analytics Tools"
        G --> H[Visualization (Looker)]
    end

    style A fill:#f9f,stroke:#333,stroke-width:2px,color:#000;
    style B fill:#f9f,stroke:#333,stroke-width:2px,color:#000;
    style C fill:#f9f,stroke:#333,stroke-width:2px,color:#000;
    style D fill:#ccf,stroke:#333,stroke-width:2px,color:#000;
    style E fill:#ccf,stroke:#333,stroke-width:2px,color:#000;
    style F fill:#efe,stroke:#333,stroke-width:2px,color:#000;
    style G fill:#ccf,stroke:#333,stroke-width:2px,color:#000;
    style H fill:#ccf,stroke:#333,stroke-width:2px,color:#000;
```
