# Project Documentation

### Overview
This release introduces a package that delivers the exact same dbt models as provided by Fivetran's Shopify integration, now powered by **Daton** as the data source. The models’ structure, logic, and outputs are fully aligned with the Fivetran Shopify dbt package, ensuring seamless compatibility for all downstream analytics and reporting. The current release supports **BigQuery** as the data warehouse to which Daton is loading.

### Key Highlights
- **Source Substitution Only**: The package replicates the raw tables previously loaded by Fivetran. No modifications have been made to the model logic or structure.
- **Seamless Integration**: Downstream queries, dashboards, and data products built on top of these models will work without any changes.
- **Best Practices**: The package follows dbt and analytics engineering best practices, including robust testing, documentation, and incremental loading where applicable.

### Model List

| Model Name                    | Description                                           |
|------------------------------|-------------------------------------------------------|
| Customer                     | A list of Shopify customers                          |
| Customer Address             | Customer address records                             |
| Customer Tag                 | Tags assigned to customers                           |
| Customer Tax Exemption       | Tax exemption details for customers                  |
| Customer Visit               | Customer visits and interactions                     |
| Discount Code                | Discount codes used in orders                        |
| Fulfilment Event             | Fulfillment status events                            |
| Fulfilment Order             | Fulfillment orders                                   |
| Fulfilment Order Line Item   | Line items within a fulfillment order                |
| Gift Card                    | Issued Shopify gift cards                            |
| Inventory Item               | Inventory items listed in the store                  |
| Inventory Level              | Inventory quantity per location                      |
| Metafield                    | Additional custom fields (metafields)                |
| Order                        | Shopify orders                                       |
| Discount Allocation          | Allocation of discounts per order line               |
| Discount Application         | Discounts applied to the order                       |
| Fulfilment                   | Fulfillments processed                               |
| Fulfilment Order Line        | Fulfillment order line details                       |
| Order Adjustment             | Price adjustments applied to orders                  |
| Order Discount Code          | Discount codes used per order                        |
| Order Line                   | Items purchased in an order                          |
| Order Line Refund            | Refund details per line item                         |
| Order Note Attribute         | Custom notes on orders                               |
| Order Shipping Line          | Shipping method and cost for each order              |
| Order Shipping Tax Line      | Taxes applied to shipping lines                      |
| Order Tag                    | Tags associated with orders                          |
| Refund                       | Refunds processed for orders                         |
| Tax Line                     | Line-level tax details                               |
| Price Rule                   | Rules for automatic discounts                        |
| Product                      | Shopify products                                     |
| Product Image                | Images associated with products                      |
| Product Option               | Options (e.g. size, color) defined on products       |
| Product Option Value         | Values of product options                            |
| Product Tag                  | Tags assigned to products                            |
| Product Variant              | Variants of products (e.g. color, size combinations) |
| Product Variant Option Value | Option values specific to variants                   |
| Shop                         | General shop/store details                           |
| Transaction                  | Payment transactions related to orders               |

Below is the ERD for the Shopify models in this package. This diagram shows the tables, their columns, and the relationships between them. You can visualize this diagram using Mermaid in supported markdown viewers.


```mermaid
erDiagram
    CUSTOMER {
        NUMERIC id PK
        BOOLEAN accepts_marketing
        TIMESTAMP accepts_marketing_updated_at
        TIMESTAMP created_at
        STRING currency
        STRING email
        TIMESTAMP email_marketing_consent_consent_updated_at
        TIMESTAMP email_marketing_consent_opt_in_level
        TIMESTAMP email_marketing_consent_state
        STRING first_name
        STRING last_name
        STRING note
        NUMERIC order_count
        STRING phone
        STRING state
        BOOLEAN tax_exempt
        STRING total_spent
        TIMESTAMP updated_at
        BOOLEAN verified_email
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    CUSTOMER_ADDRESS {
        NUMERIC id PK
        NUMERIC customer_id FK
        STRING address1
        STRING address2
        STRING city
        STRING company
        STRING country
        STRING country_code
        STRING first_name
        BOOLEAN is_default
        STRING last_name
        STRING name
        STRING phone
        STRING province
        STRING province_code
        STRING zip
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    CUSTOMER_TAG {
        INTEGER index PK
        NUMERIC customer_id PK
        STRING value
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    CUSTOMER_TAX_EXEMPTION {
        INTEGER index PK
        NUMERIC customer_id PK
        STRING value
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    CUSTOMER_VISIT {
        INTEGER id PK
        INTEGER order_id FK
        STRING landing_page
        STRING landing_page_html
        DATETIME occurred_at
        STRING referral_code
        STRING referral_info_html
        STRING referrer_url
        STRING source
        STRING source_description
        STRING source_type
        STRING type
        STRING utm_parameters_campaign
        STRING utm_parameters_content
        STRING utm_parameters_medium
        STRING utm_parameters_source
        STRING utm_parameters_term
    }

    ORDERS {
        NUMERIC id PK
        NUMERIC customer_id FK
        NUMERIC location_id FK
        NUMERIC user_id FK
        NUMERIC company_id FK
        NUMERIC company_location_id FK
        NUMERIC app_id
        STRING browser_ip
        BOOLEAN buyer_accepts_marketing
        STRING cancel_reason
        DATETIME cancelled_at
        DATETIME closed_at
        STRING cart_token
        NUMERIC checkout_id
        STRING checkout_token
        BOOLEAN confirmed
        DATETIME created_at
        STRING currency
        STRING customer_locale
        NUMERIC device_id
        STRING email
        STRING financial_status
        STRING fulfillment_status
        STRING landing_site_base_url
        STRING landing_site_ref
        STRING name
        STRING note
        STRING note_attribute
        NUMERIC number
        NUMERIC order_number
        STRING order_status_url
        STRING original_total_duties_set
        STRING client_details_user_agent
        STRING current_subtotal_details
        STRING current_total_details
        STRING payment_gateway_names
        STRING presentment_currency
        DATETIME processed_at
        STRING reference
        STRING referring_site
        STRING source_identifier
        STRING source_name
        STRING source_url
        NUMERIC subtotal_price
        STRING subtotal_price_set
        BOOLEAN taxes_included
        BOOLEAN test
        NUMERIC total_discounts
        STRING total_discounts_set
        NUMERIC total_line_items_price
        STRING total_line_items_price_set
        NUMERIC total_price
        STRING total_price_set
        STRING total_shipping_price_set
        NUMERIC total_tax
        STRING total_tax_set
        NUMERIC total_tip_received
        NUMERIC total_weight
        DATETIME updated_at
        STRING billing_address_details
        STRING note_attribute_details
        STRING shipping_address_details
    }

    ORDER_LINE {
        NUMERIC id PK
        NUMERIC order_id FK
        NUMERIC product_id FK
        NUMERIC variant_id FK
        NUMERIC fulfillable_quantity
        STRING fulfillment_status
        BOOLEAN gift_card
        NUMERIC grams
        STRING sku
        STRING name
        STRING price
        STRING price_set
        STRING pre_tax_price
        STRING pre_tax_price_set
        BOOLEAN product_exists
        NUMERIC quantity
        BOOLEAN requires_shipping
        STRING tax_code
        BOOLEAN taxable
        STRING title
        STRING total_discount
        STRING total_discount_set
        STRING variant_inventory_management
        STRING variant_title
        STRING vendor
        INTEGER index
        STRING property_name
        STRING property_value
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    ORDER_TAG {
        INTEGER index PK
        NUMERIC order_id PK
        STRING value
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    ORDER_NOTE_ATTRIBUTE {
        NUMERIC order_id PK
        STRING name PK
        STRING value
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    ORDER_SHIPPING_LINE {
        NUMERIC id PK
        NUMERIC order_id FK
        STRING carrier_identifier
        STRING code
        STRING discounted_price
        STRING discounted_price_set__shop_money__amount
        STRING price
        STRING price_set__shop_money__amount
        STRING source
        STRING title
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    ORDER_SHIPPING_TAX_LINE {
        STRING order_shipping_line_id PK
        INTEGER index PK
        STRING price
        NUMERIC rate
        STRING title
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    ORDER_DISCOUNT_CODE {
        INTEGER index PK
        NUMERIC order_id PK
        STRING code FK
        STRING amount
        STRING type
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    DISCOUNT_APPLICATION {
        NUMERIC order_id PK
        INTEGER index PK
        STRING allocation_method
        STRING code
        STRING description
        STRING target_selection
        STRING target_type
        STRING title
        STRING type
        STRING value
        STRING value_type
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    DISCOUNT_ALLOCATION {
        NUMERIC order_line_id PK
        INTEGER index PK
        NUMERIC discount_application_index FK
        STRING amount
        STRING amount_set_shop_money_amount
        STRING amount_set_shop_money_currency_code
        STRING amount_set_presentment_money_amount
        STRING amount_set_presentment_money_currency_code
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    FULFILLMENT {
        NUMERIC id PK
        NUMERIC order_id FK
        NUMERIC location_id FK
        STRING name
        TIMESTAMP created_at
        STRING receipt_authorization
        STRING service
        STRING shipment_status
        STRING status
        STRING tracking_company
        STRING tracking_numbers
        STRING tracking_urls
        TIMESTAMP updated_at
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    FULFILLMENT_EVENT {
        INTEGER id PK
        INTEGER fulfillment_id FK
        INTEGER order_id FK
        INTEGER shop_id FK
        STRING city
        STRING country
        TIMESTAMP created_at
        STRING estimated_delivery_at
        TIMESTAMP happened_at
        FLOAT latitude
        FLOAT longitude
        STRING message
        STRING province
        STRING status
        TIMESTAMP updated_at
        INTEGER zip
        INTEGER _daton_user_id
        INTEGER _daton_batch_runtime
        INTEGER _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    FULFILLMENT_ORDER {
        NUMERIC id PK
        NUMERIC order_id FK
        DATETIME created_at
        DATETIME fulfill_at
        DATETIME fulfill_by
        STRING international_duties_incoterm
        STRING request_status
        STRING status
        STRING supported_actions
        DATETIME updated_at
        STRING assigned_location_details
        STRING delivery_method_details
        STRING destination_details
        INTEGER _daton_user_id
        INTEGER _daton_batch_runtime
        INTEGER _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    FULFILLMENT_ORDER_LINE {
        NUMERIC order_line_id PK
        NUMERIC fulfillment_id FK
        NUMERIC order_id FK
        NUMERIC product_id FK
        NUMERIC variant_id FK
        NUMERIC fulfillable_quantity
        BOOLEAN gift_card
        NUMERIC grams
        STRING name
        STRING price_set
        STRING price
        STRING properties
        NUMERIC quantity
        BOOLEAN requires_shipping
        STRING sku
        BOOLEAN taxable
        STRING title
        STRING variant_title
        STRING vendor
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    FULFILLMENT_ORDER_LINE_ITEM {
        NUMERIC id PK
        NUMERIC fulfillment_order_id FK
        NUMERIC inventory_item_id FK
        NUMERIC remaining_quantity
        NUMERIC total_quantity
        INTEGER _daton_user_id
        INTEGER _daton_batch_runtime
        INTEGER _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    TRANSACTION {
        NUMERIC id PK
        NUMERIC order_id FK
        STRING refund_id FK
        NUMERIC location_id FK
        NUMERIC parent_id FK
        STRING amount
        STRING authorization
        TIMESTAMP created_at
        STRING currency
        NUMERIC device_id
        STRING error_code
        STRING gateway
        STRING kind
        STRING message
        STRING payment_avs_result_code
        STRING payment_credit_card_bin
        STRING payment_cvv_result_code
        STRING payment_credit_card_number
        STRING payment_credit_card_company
        DATETIME processed_at
        STRING source_name
        STRING status
        BOOLEAN test
        NUMERIC user_id
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    REFUND {
        NUMERIC id PK
        NUMERIC order_id FK
        NUMERIC user_id FK
        DATETIME created_at
        DATETIME processed_at
        STRING note
        BOOLEAN restock
        RECORD total_duties_set
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    ORDER_ADJUSTMENT {
        NUMERIC id PK
        NUMERIC order_id FK
        NUMERIC refund_id FK
        STRING amount
        STRING amount_set
        STRING tax_amount
        STRING tax_amount_set
        STRING kind
        STRING reason
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    ORDER_LINE_REFUND {
        NUMERIC id PK
        NUMERIC location_id FK
        NUMERIC order_line_id FK
        NUMERIC refund_id FK
        NUMERIC quantity
        STRING restock_type
        NUMERIC subtotal
        RECORD subtotal_set
        NUMERIC total_tax
        RECORD total_tax_set
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    TAX_LINE {
        NUMERIC order_line_id PK
        INTEGER index PK
        STRING price
        NUMERIC rate
        STRING title
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    PRODUCT {
        NUMERIC id PK
        NUMERIC featured_media_id FK
        TIMESTAMP created_at
        STRING description_html
        STRING description
        STRING gift_card_template_suffix
        STRING handle
        STRING legacy_resource_id
        INTEGER online_store_preview_url
        FLOAT max_variant_price_amount
        FLOAT min_variant_price_amount
        STRING max_variant_price_currency_code
        STRING min_variant_price_currency_code
        STRING product_type
        TIMESTAMP published_at
        BOOLEAN requires_selling_plan
        STRING status
        STRING template_suffix
        STRING title
        NUMERIC total_inventory
        BOOLEAN tracks_inventory
        TIMESTAMP updated_at
        STRING vendor
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    PRODUCT_TAG {
        INTEGER index PK
        NUMERIC product_id PK
        STRING value
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    PRODUCT_OPTION {
        NUMERIC id PK
        NUMERIC product_id FK
        STRING name
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    PRODUCT_OPTION_VALUE {
        NUMERIC id PK
        NUMERIC product_option_id FK
        STRING name
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    PRODUCT_VARIANT {
        NUMERIC id PK
        NUMERIC product_id FK
        NUMERIC inventory_item_id FK
        NUMERIC image_id FK
        STRING barcode
        STRING compare_at_price
        TIMESTAMP created_at
        STRING inventory_policy
        NUMERIC inventory_quantity
        NUMERIC position
        STRING price
        STRING sku
        STRING tax_code
        BOOLEAN taxable
        STRING title
        TIMESTAMP updated_at
        NUMERIC datonuser_id
        NUMERIC datonbatch_runtime
        NUMERIC datonbatch_id
        TIMESTAMP lastupdated
        STRING runid
    }

    PRODUCT_VARIANT_OPTION_VALUE {
        INTEGER option_value_id PK
        NUMERIC variant_id PK
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    PRODUCT_IMAGE {
        NUMERIC id PK
        NUMERIC product_id FK
        STRING alt_text
        NUMERIC height
        STRING status
        STRING url
        NUMERIC width
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    INVENTORY_ITEM {
        NUMERIC id PK
        STRING country_code_of_origin
        DATETIME created_at
        NUMERIC duplicate_sku_count
        STRING harmonized_system_code
        STRING inventory_history_url
        NUMERIC legacy_resource_id
        NUMERIC measurement_id
        DECIMAL measurement_weight_value
        STRING measurement_weight_unit
        STRING province_code_of_origin
        BOOLEAN requires_shipping
        STRING sku
        BOOLEAN tracked
        BOOLEAN tracked_editable_locked
        STRING tracked_editable_reason
        DECIMAL unit_cost_amount
        STRING unit_cost_currency_code
        DATETIME updated_at
    }

    INVENTORY_LEVEL {
        NUMERIC inventory_item_id PK
        NUMERIC location_id FK
        DATETIME updated_at
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    PRICE_RULE {
        NUMERIC id PK
        NUMERIC allocation_limit
        STRING allocation_method
        DATETIME created_at
        STRING customer_selection
        DATETIME ends_at
        BOOLEAN once_per_customer
        STRING prerequisite_quantity_range
        STRING prerequisite_shipping_price_range
        STRING prerequisite_subtotal_range
        STRING prerequisite_to_entitlement_purchase_details
        NUMERIC quantity_ratio_entitled_quantity
        NUMERIC quantity_ratio_prerequisite_quantity
        DATETIME starts_at
        STRING target_selection
        STRING target_type
        STRING title
        NUMERIC usage_limit
        DATETIME updated_at
        NUMERIC value
        STRING value_type
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    DISCOUNT_CODE {
        STRING id PK
        STRING code
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    GIFT_CARD {
        NUMERIC id PK
        NUMERIC customer_id FK
        NUMERIC line_item_id FK
        NUMERIC order_id FK
        NUMERIC user_id FK
        NUMERIC api_client_id
        STRING balance
        DATETIME created_at
        STRING currency
        DATETIME disabled_at
        DATE expires_on
        STRING initial_value
        STRING last_characters
        STRING note
        STRING template_suffix
        DATETIME updated_at
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        STRING _dbt_source_relation
        STRING _daton_sourceversion_integration_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    METAFIELD {
        INTEGER id PK
        STRING namespace
        INTEGER key
        STRING value
        STRING description
        INTEGER owner_id FK
        TIMESTAMP created_at
        TIMESTAMP updated_at
        STRING owner_resource
        STRING type
        INTEGER _daton_user_id
        INTEGER _daton_batch_runtime
        INTEGER _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    SHOP {
        NUMERIC id PK
        STRING address1
        STRING address2
        BOOLEAN checkout_api_supported
        STRING city
        STRING cookie_consent_level
        STRING country
        STRING country_code
        STRING country_name
        BOOLEAN county_taxes
        TIMESTAMP created_at
        STRING currency
        STRING customer_email
        STRING domain
        BOOLEAN eligible_for_payments
        STRING email
        STRING enabled_presentment_currencies
        BOOLEAN has_discounts
        BOOLEAN has_gift_cards
        BOOLEAN has_storefront
        STRING iana_timezone
        NUMERIC latitude
        NUMERIC longitude
        BOOLEAN multi_location_enabled
        STRING myshopify_domain
        STRING name
        BOOLEAN password_enabled
        STRING phone
        STRING plan_display_name
        STRING plan_name
        BOOLEAN pre_launch_enabled
        STRING primary_locale
        NUMERIC primary_location_id
        STRING province
        STRING province_code
        BOOLEAN requires_extra_payments_agreement
        BOOLEAN setup_required
        STRING shop_owner
        BOOLEAN taxes_included
        STRING timezone
        TIMESTAMP updated_at
        STRING visitor_tracking_consent_preference
        STRING weight_unit
        STRING zip
        STRING money_format
        STRING money_with_currency_format
        STRING money_in_emails_format
        STRING money_with_currency_in_emails_format
        NUMERIC _daton_user_id
        NUMERIC _daton_batch_runtime
        NUMERIC _daton_batch_id
        TIMESTAMP _last_updated
        STRING _run_id
    }

    %% Customer Relationships
    CUSTOMER ||--o{ CUSTOMER_ADDRESS : has
    CUSTOMER ||--o{ CUSTOMER_TAG : has
    CUSTOMER ||--o{ CUSTOMER_TAX_EXEMPTION : has
    CUSTOMER ||--o{ ORDERS : places
    CUSTOMER ||--o{ GIFT_CARD : owns

    %% Order Core Relationships
    ORDERS ||--o{ CUSTOMER_VISIT : generates
    ORDERS ||--o{ ORDER_LINE : contains
    ORDERS ||--o{ ORDER_TAG : has
    ORDERS ||--o{ ORDER_NOTE_ATTRIBUTE : has
    ORDERS ||--o{ ORDER_SHIPPING_LINE : has
    ORDERS ||--o{ ORDER_DISCOUNT_CODE : applies
    ORDERS ||--o{ DISCOUNT_APPLICATION : has
    ORDERS ||--o{ FULFILLMENT : has
    ORDERS ||--o{ FULFILLMENT_ORDER : has
    ORDERS ||--o{ TRANSACTION : has
    ORDERS ||--o{ REFUND : has
    ORDERS ||--o{ ORDER_ADJUSTMENT : has

    %% Order Line Relationships
    ORDER_LINE ||--o{ TAX_LINE : has
    ORDER_LINE ||--o{ DISCOUNT_ALLOCATION : has
    ORDER_LINE ||--o{ ORDER_LINE_REFUND : has
    ORDER_LINE ||--o{ FULFILLMENT_ORDER_LINE : fulfills
    ORDER_LINE ||--|| PRODUCT : references
    ORDER_LINE ||--|| PRODUCT_VARIANT : references
    ORDER_LINE ||--|| GIFT_CARD : creates

    %% Order Shipping Relationships
    ORDER_SHIPPING_LINE ||--o{ ORDER_SHIPPING_TAX_LINE : has

    %% Product Core Relationships
    PRODUCT ||--o{ PRODUCT_TAG : has
    PRODUCT ||--o{ PRODUCT_OPTION : has
    PRODUCT ||--o{ PRODUCT_VARIANT : has
    PRODUCT ||--o{ PRODUCT_IMAGE : has
    PRODUCT ||--|| PRODUCT_IMAGE : featured_media

    %% Product Options Relationships
    PRODUCT_OPTION ||--o{ PRODUCT_OPTION_VALUE : has
    PRODUCT_OPTION_VALUE ||--o{ PRODUCT_VARIANT_OPTION_VALUE : maps_to
    PRODUCT_VARIANT ||--o{ PRODUCT_VARIANT_OPTION_VALUE : has

    %% Product Variant Relationships
    PRODUCT_VARIANT ||--|| INVENTORY_ITEM : references
    PRODUCT_VARIANT ||--|| PRODUCT_IMAGE : has_image

    %% Inventory Relationships
    INVENTORY_ITEM ||--o{ INVENTORY_LEVEL : has

    %% Discount Relationships
    PRICE_RULE ||--o{ DISCOUNT_CODE : creates
    DISCOUNT_CODE ||--o{ ORDER_DISCOUNT_CODE : used_in
    DISCOUNT_APPLICATION ||--o{ DISCOUNT_ALLOCATION : allocates

    %% Fulfillment Relationships
    FULFILLMENT ||--o{ FULFILLMENT_EVENT : has
    FULFILLMENT ||--o{ FULFILLMENT_ORDER_LINE : contains
    FULFILLMENT_ORDER ||--o{ FULFILLMENT_ORDER_LINE_ITEM : contains
    FULFILLMENT_ORDER_LINE_ITEM ||--|| INVENTORY_ITEM : references

    %% Refund Relationships
    REFUND ||--o{ ORDER_LINE_REFUND : contains
    REFUND ||--o{ ORDER_ADJUSTMENT : has
    REFUND ||--o{ TRANSACTION : has

    %% Transaction Relationships
    TRANSACTION ||--|| TRANSACTION : parent

    %% Metafield Relationships
    METAFIELD ||--o{ CUSTOMER : attached_to
    METAFIELD ||--o{ ORDERS : attached_to
    METAFIELD ||--o{ PRODUCT : attached_to
    METAFIELD ||--o{ PRODUCT_VARIANT : attached_to
    METAFIELD ||--o{ PRODUCT_IMAGE : attached_to
```

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


---

## Prerequisites for Fivetran to Daton dbt Package

- [Daton integration for Shopify](https://www.sarasanalytics.com/daton/shopify)

| **Model Name**                 | **Raw Table Name / Pattern**                                   |
|-------------------------------|-----------------------------------------------------------------|
| customer                      | shopify_customers                                               |
| customer_address              | shopify_customers (addresses array)                             |
| customer_tag                  | shopify_customers (tags array)                                  |
| customer_tax_exemption        | shopify_customers (tax_exemptions array)                        |
| customer_visit                | shopify_orders                                                  |
| discount_code                 | shopify_discount_codes                                          |
| fulfilment_event              | shopify_fulfillments (events array)                             |
| fulfilment_order              | shopify_fulfillment_orders                                      |
| fulfilment_order_line_item    | shopify_fulfillment_orders (line_items array)                   |
| gift_card                     | shopify_gift_cards                                              |
| inventory_item                | shopify_inventory_items                                         |
| inventory_level               | shopify_inventory_levels                                        |
| metafield                     | shopify_metafields                                              |
| order                         | shopify_orders                                                  |
| discount_allocation           | shopify_orders (line_items.discount_allocations array)          |
| discount_application          | shopify_orders (discount_applications array)                    |
| fulfilment                    | shopify_fulfillments                                            |
| fulfilment_order_line         | shopify_fulfillment_orders (line_items array)                   |
| order_adjustment              | shopify_orders (order_adjustments array)                        |
| order_discount_code           | shopify_orders (discount_codes array)                           |
| order_line                    | shopify_orders (line_items array)                               |
| order_line_refund             | shopify_refunds (refund_line_items array)                       |
| order_note_attribute          | shopify_orders (note_attributes array)                          |
| order_shipping_line           | shopify_orders (shipping_lines array)                           |
| order_shipping_tax_line       | shopify_orders (shipping_lines.tax_lines array)                 |
| order_tag                     | shopify_orders (tags array)                                     |
| refund                        | shopify_refunds                                                 |
| tax_line                      | shopify_orders (line_items.tax_lines array)                     |
| price_rule                    | shopify_price_rules                                             |
| product                       | shopify_products                                                |
| product_image                 | shopify_products (images array)                                 |
| product_option                | shopify_products (options array)                                |
| product_option_value          | shopify_products (options.values array)                         |
| product_tag                   | shopify_products (tags array)                                   |
| product_variant               | shopify_products (variants array)                               |
| product_variant_option_value  | shopify_products (variants.option_values array)                 |
| shop                          | shopify_shop                                                    |
| transaction                   | shopify_transactions                                            |

### Supported Data Warehouses
- BigQuery

---

## Configuration for dbt Package

### Required Variables

This dbt package assumes that you have an existing dbt project with a BigQuery profile connected and tested.

Set the following in your `dbt_project.yml` file:

```yaml
vars:
  raw_database: "your_database"
  raw_schema: "your_schema"
```
### Other Configurable Variables

In addition to `raw_database` and `raw_schema`, you can configure the following variables in your `dbt_project.yml` file:

#### Model Enable/Disable Flags
Set these to `true` or `false` to enable or disable specific Shopify models:

- `shopify_customers`
- `shopify_orders`
- `shopify_discount_codes`
- `shopify_fulfillments`
- `shopify_fulfillment_orders`
- `shopify_gift_cards`
- `shopify_inventory_items`
- `shopify_inventory_levels`
- `shopify_metafields`
- `shopify_refunds`
- `shopify_price_rules`
- `shopify_products`
- `shopify_shop`
- `shopify_transactions`

#### Raw Table Pattern Variables
Override these to change the pattern used to find the raw data tables for each model:

- `shopify_customers_table_pattern`
- `shopify_orders_table_pattern`
- `shopify_discount_codes_table_pattern`
- `shopify_fulfillments_table_pattern`
- `shopify_fulfillment_orders_table_pattern`
- `shopify_gift_cards_table_pattern`
- `shopify_inventory_items_table_pattern`
- `shopify_inventory_levels_table_pattern`
- `shopify_metafields_table_pattern`
- `shopify_refunds_table_pattern`
- `shopify_price_rules_table_pattern`
- `shopify_products_table_pattern`
- `shopify_shop_table_pattern`
- `shopify_transactions_table_pattern`

Refer to the `dbt_project.yml` file for the default values and update as needed for your environment.