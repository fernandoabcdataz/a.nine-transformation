### **DBT Project README**

---

## **Overview**

This **dbt project** is designed to manage and transform data collected from the **Xero API**, specifically handling the ingestion and transformation of **invoices**, **payments**, **bills**, and their related entities. The project uses a **hybrid data modeling approach** that combines **normalized** tables (which mirror the raw JSON data from Xero) and **queryable** tables (which optimize common queries and calculations for use by an LLM-based application). 

This setup provides the flexibility needed to support **ad-hoc querying** via the LLM while ensuring that frequently used queries are optimized for performance.

---

## **Limitations of Dimensional Modeling**

Traditionally, **dimensional models** (e.g., **star schemas**) are widely used for reporting and analytics. While dimensional models work great for predefined reporting, they face limitations when trying to address **ad-hoc, dynamic queries** that may span various levels of granularity or dimensions. In an LLM-based application, where users can ask a wide variety of questions (from simple metrics to complex relationships between entities), **dimensional models alone** are insufficient.

### **Key Limitations:**
1. **Rigid Structure:**  
   Dimensional models are designed for a specific set of questions and require fixed relationships between facts and dimensions, making it hard to handle ad-hoc queries.
   
2. **Lack of Granularity:**  
   Granular data is often lost or aggregated in a dimensional model, which makes it difficult for LLMs to respond to detailed questions (e.g., line-level details of a specific invoice).

3. **Performance Overhead for Complex Queries:**  
   Dimensional models can struggle with performance when handling complex, non-predefined queries that require many joins and filters.

---

## **Hybrid Approach: Normalized + Queryable Models**

To overcome these limitations, this dbt project uses a **hybrid approach** that leverages:
- **Normalized Models:**  
  These models flatten the raw JSON data from the Xero API into structured tables. No aggregation or filtering is applied, ensuring **data granularity** and flexibility. These tables reflect the data as-is from the API, allowing the LLM to generate any kind of query based on user input.
  
- **Queryable Models:**  
  These models are designed to **optimize performance** for common queries by pre-aggregating and pre-joining data. The queryable models are useful for scenarios where predefined metrics or calculations are required quickly.

This combination ensures the system can efficiently handle both **complex ad-hoc queries** and **frequent, predefined metrics**.

---

## **Project Structure**

```plaintext
dbt_project/
├── dbt_project.yml         # configuration file for dbt project
├── models/
│   ├── normalized/         # raw, flattened data from Xero API
│   │   ├── xero_accounts.sql
│   │   ├── xero_bills.sql
│   │   ├── xero_bill_line_items.sql
│   │   ├── xero_contacts.sql
│   │   ├── xero_invoice_line_items.sql
│   │   ├── xero_invoices.sql
│   │   ├── xero_items.sql
│   │   └── xero_payments.sql
│   └── queryable/          # optimized, aggregated data for querying
│       ├── invoice_summary.sql
│       ├── payment_summary.sql
│       ├── account_performance.sql
│       ├── contact_performance.sql
│       └── invoice_aging.sql
├── snapshots/              # captures historical states of tables (if applicable)
├── tests/                  # contains custom tests for dbt models
├── macros/                 # reusable macros to simplify SQL logic
└── seeds/                  # static datasets that are loaded into the warehouse
```

### **Normalized Models (`models/normalized/`)**
- These models take the raw JSON data from Xero (in the `data` column) and **flatten** it into a structured format. Each normalized table corresponds to an endpoint in the Xero API, and no joins or aggregations are applied, preserving the full detail of the data.
  
  **Key Models:**
  - `xero_accounts.sql`: Raw account data from Xero.
  - `xero_invoices.sql`: Raw invoice data with line items.
  - `xero_bills.sql`: Raw bills (accounts payable).
  - `xero_invoice_line_items.sql`: Detailed line items for invoices.
  - `xero_bill_line_items.sql`: Detailed line items for bills.

### **Queryable Models (`models/queryable/`)**
- These models perform **aggregations** and **joins** to create summary tables that are frequently queried by the LLM. By pre-joining relevant entities and calculating key metrics, these models are optimized for faster querying of commonly requested data.

  **Key Models:**
  - `invoice_summary.sql`: Combines invoices, line items, and contact details into a summary for fast querying.
  - `payment_summary.sql`: Aggregates payments related to invoices and contacts.
  - `account_performance.sql`: Tracks financial metrics for accounts, linking invoices and payments.
  - `contact_performance.sql`: Provides a summary of financial interactions (invoices, payments) for contacts.
  - `invoice_aging.sql`: Calculates overdue invoices based on due dates and payment status.

---

## **Deployment Instructions**

1. **Set Up Your Environment:**
   - Install dbt using the official [dbt installation guide](https://docs.getdbt.com/docs/installation).
   
2. **Configure Profiles:**
   - Set up your `profiles.yml` file to connect to your data warehouse (e.g., BigQuery). Here’s an example for BigQuery:

   ```yaml
   transformation:
      target: dev
      outputs:
         dev:
            type: bigquery
            method: service-account
            project: "{{ var('project') }}"
            dataset: "{{ var('client') }}_ingestion"
            threads: 4
            keyfile: service-account.json
   ```

3. **Run dbt Models:**
   - Run all models in the project:
     ```bash
     dbt run
     ```

   - Or run only the **normalized** or **queryable** models:
     ```bash
     dbt run --select normalized.*
     dbt run --select queryable.*
     ```

4. **Testing and Validation:**
   - Run tests to ensure that the models are functioning as expected:
     ```bash
     dbt test
     ```

---

## **Conclusion**

This dbt project is designed with flexibility and scalability in mind, ensuring that both detailed, ad-hoc queries and common pre-aggregated metrics can be served efficiently. The hybrid approach combining **normalized** and **queryable** models addresses the limitations of traditional dimensional models and ensures that the LLM application can respond to a wide variety of queries without sacrificing performance or accuracy.

For future development, a **semantic layer** and **knowledge base** will be set up in a separate pipeline, ensuring contextualized responses from the LLM and a more comprehensive query experience.