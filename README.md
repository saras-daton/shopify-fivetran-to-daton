# Project Documentation

## Release Notes: Klaviyo

### Overview
This release introduces a package that delivers the exact same dbt models as provided by Fivetran’s Klaviyo integration, now powered by Daton as the data source. The models’ structure, logic, and outputs are fully aligned with the Fivetran Klaviyo dbt package, ensuring seamless compatibility for all downstream analytics and reporting.

### Key Highlights
- **Source Substitution Only**: The only change is the underlying source, which is now Daton instead of Fivetran. No modifications have been made to the model logic or structure.
- **Seamless Integration**: Downstream queries, dashboards, and data products built on top of these models will work without any changes.
- **Best Practices**: The package follows dbt and analytics engineering best practices, including robust testing, documentation, and incremental loading where applicable.

### Model List
- `event`
- `campaign`
- `campaign_list`
- `campaign_message`
- `campaign_message_send_time`
- `campaign_tracking_utm_param`
- `flow`
- `flow_action`
- `flow_action_tracking_utm_param`
- `flow_message`
- `metric`
- `person`
- `segment`

### Migration Notes
- **No Action Required**: Consumers of these models do not need to update queries or dashboards, as all output remains identical to the Fivetran-based package.
- **Configuration**: Ensure your `dbt_project.yml` and source configurations reference Daton as the source for Klaviyo data.
- **Validation**: All standard dbt tests (e.g., `unique`, `not null`) are in place to ensure data integrity.

---

## Prerequisites for Fivetran to Daton dbt Package

- [Daton integration for Klaviyo](https://www.sarasanalytics.com/daton/klaviyo)

### Supported Data Warehouses
- BigQuery

---

## Configuration for dbt Package

### Required Variables
This dbt package assumes that you have an existing dbt project with a BigQuery/Snowflake profile connected & tested.

Set the following in your `dbt_project.yml` file:

```yaml
vars:
  raw_database: "your_database"
  raw_schema: "your_schema"
