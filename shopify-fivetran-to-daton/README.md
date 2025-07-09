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


## DBT Tests

The tests property defines assertions about a column, table, or view. The property contains a list of generic tests, referenced by name, which can include the four built-in generic tests available in dbt. For example, you can add tests that ensure a column contains no duplicates and zero null values. Any arguments or configurations passed to those tests should be nested below the test name.

| **Tests**  | **Description** |
| ---------------| ------------------------------------------- |
| [Not Null Test](https://docs.getdbt.com/reference/resource-properties/tests#testing-an-expression)  | This test validates that there are no null values present in a column |
| [Data Recency Test](https://github.com/dbt-labs/dbt-utils/blob/main/macros/generic_tests/recency.sql)  | This is used to check for issues with data refresh within {{ x }} days, please specify the value of number of days at {{ x }} |
| [Accepted Value Test](https://docs.getdbt.com/reference/resource-properties/tests#accepted_values)  | This test validates that all of the values in a column are present in a supplied list of values. If any values other than those provided in the list are present, then the test will fail, by default it consists of default values and this needs to be changed based on the project |
| [Uniqueness Test](https://docs.getdbt.com/reference/resource-properties/tests#testing-an-expression)  | This test validates that there are no duplicate values present in a field |


### Table Name: KlaviyoSegments
| **Columns** | **Not Null Test** | **Uniqueness Test** |
|-------------|:-----------------:|:-------------------:|
| id          |         ✓         |         ✓           |
| list_name   |         ✓         |                     |
| created     |         ✓         |                     |
| updated     |         ✓         |                     |

### Table Name: KlaviyoPerson
| **Columns**                           | **Not Null Test** | **Uniqueness Test** |
|---------------------------------------|:-----------------:|:-------------------:|
| id                                   |         ✓         |         ✓           |
| created                              |         ✓         |                     |
| updated                              |         ✓         |                     |
| subscriptions_email_marketing_consent |         ✓         |                     |
| subscriptions_sms_marketing_consent   |         ✓         |                     |

### Table Name: KlaviyoMetric
| **Columns**           | **Not Null Test** | **Uniqueness Test** |
|-----------------------|:-----------------:|:-------------------:|
| id                    |         ✓         |         ✓           |
| name                  |         ✓         |                     |
| created               |         ✓         |                     |
| updated               |         ✓         |                     |
| integration_category  |         ✓         |                     |
| integration_id        |         ✓         |                     |
| integration_name      |         ✓         |                     |
| integration_object    |         ✓         |                     |

### Table Name: KlaviyoFlows
| **Columns**    | **Not Null Test** | **Uniqueness Test** |
|---------------|:-----------------:|:-------------------:|
| id            |         ✓         |         ✓           |
| name          |         ✓         |                     |
| created       |         ✓         |                     |
| updated       |         ✓         |                     |
| status        |         ✓         |                     |
| trigger_type  |         ✓         |                     |
| archived      |         ✓         |                     |

### Table Name: KlaviyoFlowMessages
| **Columns**      | **Not Null Test** | **Uniqueness Test** |
|------------------|:-----------------:|:-------------------:|
| id               |         ✓         |         ✓           |
| channel          |         ✓         |                     |
| created          |         ✓         |                     |
| name             |         ✓         |                     |
| updated          |         ✓         |                     |

### Table Name: KlaviyoFlowActionTrackingUtmParam
| **Columns**     | **Not Null Test** | **Uniqueness Test** |
|-----------------|:-----------------:|:-------------------:|
| flow_action_id  |         ✓         |         ✓           |
| name            |         ✓         |                     |
| value           |         ✓         |                     |

### Table Name: KlaviyoFlowActions
| **Columns**                        | **Not Null Test** | **Uniqueness Test** |
|------------------------------------|:-----------------:|:-------------------:|
| id                                |         ✓         |         ✓           |
| flow_id                           |         ✓         |                     |
| action_type                       |         ✓         |                     |
| created                           |         ✓         |                     |
| status                            |         ✓         |                     |
| updated                           |         ✓         |                     |
| render_options_add_info_link      |         ✓         |                     |
| render_options_add_opt_out_language|        ✓         |                     |
| render_options_add_org_prefix     |         ✓         |                     |
| render_options_shorten_links      |         ✓         |                     |
| send_option_is_transactional      |         ✓         |                     |
| send_option_use_smart_sending     |         ✓         |                     |
| tracking_options_is_add_utm       |         ✓         |                     |
| tracking_options_is_tracking_clicks|        ✓         |                     |
| tracking_options_is_tracking_opens|         ✓         |                     |

### Table Name: KlaviyoEvents
| **Columns** | **Not Null Test** | **Uniqueness Test** |
|-------------|:-----------------:|:-------------------:|
| id          |         ✓         |         ✓           |
| metric_id   |         ✓         |                     |
| datetime    |         ✓         |                     |
| timestamp   |         ✓         |                     |
| type        |         ✓         |                     |
| uuid        |         ✓         |         ✓           |

### Table Name: KlaviyoCampaignTrackingUtmParam
| **Columns**  | **Not Null Test** | **Uniqueness Test** |
|--------------|:-----------------:|:-------------------:|
| campaign_id  |         ✓         |         ✓           |

### Table Name: KlaviyoCampaigns
| **Columns**                     | **Not Null Test** | **Uniqueness Test** |
|---------------------------------|:-----------------:|:-------------------:|
| id                             |         ✓         |         ✓           |
| name                           |         ✓         |                     |
| created                        |         ✓         |                     |
| updated                        |         ✓         |                     |
| status                         |         ✓         |                     |
| archived                       |         ✓         |                     |
| send_strategy_method           |         ✓         |                     |
| send_option_use_smart_sending  |         ✓         |                     |
| tracking_options_is_add_utm    |         ✓         |                     |

### Table Name: KlaviyoCampaignMessageSendTime
| **Columns**           | **Not Null Test** | **Uniqueness Test** |
|-----------------------|:-----------------:|:-------------------:|
| campaign_message_id   |         ✓         |         ✓           |
| datetime              |         ✓         |                     |

### Table Name: KlaviyoCampaignMessages
| **Columns**             | **Not Null Test** | **Uniqueness Test** |
|-------------------------|:-----------------:|:-------------------:|
| id                      |         ✓         |         ✓           |
| campaign_id             |         ✓         |                     |
| template_id             |         ✓         |                     |
| channel                 |         ✓         |                     |
| content_cc_email        |         ✓         |                     |
| content_from_email      |         ✓         |                     |
| content_from_label      |         ✓         |                     |
| content_preview_text    |         ✓         |                     |
| content_reply_to_email  |         ✓         |                     |
| content_subject         |         ✓         |                     |
| created                 |         ✓         |                     |
| label                   |         ✓         |                     |
| updated                 |         ✓         |                     |

### Table Name: KlaviyoCampaignList
| **Columns**   | **Not Null Test** | **Uniqueness Test** |
|---------------|:-----------------:|:-------------------:|
| campaign_id   |         ✓         |         ✓           |


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
```
