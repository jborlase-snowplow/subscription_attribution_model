**Disclaimer: This is not officially supported by Snowplow**

# Snowplow dbt Subscription Attribution Model

This repository serves to provide a starting point to be able to run the [Snowplow dbt Attribution Package]([url](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-models/dbt-attribution-data-model/)) that allows subscription revenue to be attributed to the initial conversion point. This can be useful to help understand the total value that is brought in by your marketing channels even if the value does not occur until a later point, such as with subscriptions or free trials.

For example: 
1. 2023-12-20: User lands on website from Facebook ad
2. 2024-01-01: User lands on website from Paid Google Search
3. 2024-01-01: User signs up for a monthly subscription and pays $10
4. 2024-02-01: User's subscription automatically renews for $10

Initially the path to conversion will be calculated as "Facebook -> Google PPC" and the revenue attributed will be $10. However after the second renewal event occurs the attributed revenue will be $20.

---

# Warehouse Compatability

This repository has been tested with the following warehouses. If you manage to run this in another warehouse or modify it in a way so that it can run in another warehouse, please create a pull request with the changes.

| Warehouse  | Result |
| ------------- | ------------- |
| BigQuery  | ✅ |
| Spark  | ✅  |

--- 
# Installation Instructions

TBD
