# digdag-operator-cost_explorer
[![Jitpack](https://jitpack.io/v/pro.civitaspo/digdag-operator-cost_explorer.svg)](https://jitpack.io/#pro.civitaspo/digdag-operator-cost_explorer) [![CircleCI](https://circleci.com/gh/civitaspo/digdag-operator-cost_explorer.svg?style=shield)](https://circleci.com/gh/civitaspo/digdag-operator-cost_explorer) [![Digdag](https://img.shields.io/badge/digdag-v0.9.27-brightgreen.svg)](https://github.com/treasure-data/digdag/releases/tag/v0.9.27)

digdag plugin for AWS Cost Explorer.

# Overview

- Plugin type: operator

# Usage

```yaml
_export:
  plugin:
    repositories:
      - https://jitpack.io
    dependencies:
      - pro.civitaspo:digdag-operator-cost_explorer:0.0.1
  cost_explorer:
    auth_method: profile

+step1:
  cost_explorer.get_cost>:
  filter: "enviornment = 'production' and INSTANCE_TYPE in ('p3.16xlarge', 'p2.16xlarge')"

+step2:
  echo>: ${cost_explorer.last_get_cost}

```

# Configuration

## Remarks

- type `DurationParam` is strings matched `\s*(?:(?<days>\d+)\s*d)?\s*(?:(?<hours>\d+)\s*h)?\s*(?:(?<minutes>\d+)\s*m)?\s*(?:(?<seconds>\d+)\s*s)?\s*`.
  - The strings is used as `java.time.Duration`.
- **Dimension** and **Tag** that have a key and a value are used for filtering and grouping.
  - **Dimension** key is one of `AZ`, `INSTANCE_TYPE`, `LINKED_ACCOUNT`, `OPERATION`, `PURCHASE_TYPE`, `REGION`, `SERVICE`, `USAGE_TYPE`, `USAGE_TYPE_GROUP`, `RECORD_TYPE`, `OPERATING_SYSTEM`, `TENANCY`, `SCOPE`, `PLATFORM`, `SUBSCRIPTION_ID`, `LEGAL_ENTITY_NAME`, `DEPLOYMENT_OPTION`, `DATABASE_ENGINE`, `CACHE_ENGINE`, `INSTANCE_TYPE_FAMILY`.
  - **Tag** is defined by user on AWS Services.

## Common Configuration

### System Options

Define the below options on properties (which is indicated by `-c`, `--config`).

- **cost_explorer.allow_auth_method_env**: Indicates whether users can use **auth_method** `"env"` (boolean, default: `false`)
- **cost_explorer.allow_auth_method_instance**: Indicates whether users can use **auth_method** `"instance"` (boolean, default: `false`)
- **cost_explorer.allow_auth_method_profile**: Indicates whether users can use **auth_method** `"profile"` (boolean, default: `false`)
- **cost_explorer.allow_auth_method_properties**: Indicates whether users can use **auth_method** `"properties"` (boolean, default: `false`)
- **cost_explorer.assume_role_timeout_duration**: Maximum duration which server administer allows when users assume **role_arn**. (`DurationParam`, default: `1h`)

### Secrets

- **cost_explorer.access_key_id**: The AWS Access Key ID to use Cost Explorer. (optional)
- **cost_explorer.secret_access_key**: The AWS Secret Access Key to use Cost Explorer. (optional)
- **cost_explorer.session_token**: The AWS session token to use Cost Explorer. This is used only **auth_method** is `"session"` (optional)
- **cost_explorer.role_arn**: The AWS Role to assume when using Cost Explorer. (optional)
- **cost_explorer.role_session_name**: The AWS Role Session Name when assuming the role. (default: `digdag-cost_explorer-${session_uuid}`)
- **cost_explorer.http_proxy.host**: proxy host (required if **use_http_proxy** is `true`)
- **cost_explorer.http_proxy.port** proxy port (optional)
- **cost_explorer.http_proxy.scheme** `"https"` or `"http"` (default: `"https"`)
- **cost_explorer.http_proxy.user** proxy user (optional)
- **cost_explorer.http_proxy.password**: http proxy password (optional)

### Options

- **auth_method**: name of mechanism to authenticate requests (`"basic"`, `"env"`, `"instance"`, `"profile"`, `"properties"`, `"anonymous"`, or `"session"`. default: `"basic"`)
  - `"basic"`: uses access_key_id and secret_access_key to authenticate.
  - `"env"`: uses AWS_ACCESS_KEY_ID (or AWS_ACCESS_KEY) and AWS_SECRET_KEY (or AWS_SECRET_ACCESS_KEY) environment variables.
  - `"instance"`: uses EC2 instance profile.
  - `"profile"`: uses credentials written in a file. Format of the file is as following, where `[...]` is a name of profile.
    - **profile_file**: path to a profiles file. (string, default: given by AWS_CREDENTIAL_PROFILES_FILE environment varialbe, or ~/.aws/credentials).
    - **profile_name**: name of a profile. (string, default: `"default"`)
  - `"properties"`: uses aws.accessKeyId and aws.secretKey Java system properties.
  - `"anonymous"`: uses anonymous access. This auth method can access only public files.
  - `"session"`: uses temporary-generated access_key_id, secret_access_key and session_token.
- **use_http_proxy**: Indicate whether using when accessing AWS via http proxy. (boolean, default: `false`)
- **region**: The AWS region to use for Cost Explorer service. (string, optional)
- **endpoint**: The AWS Cost Explorer endpoint address to use. (string, optional)

## Configuration for `cost_explorer.get_cost>` operator

### Options

- **filter**: Filters AWS costs by different dimensions. For example, you can specify `SERVICE` and `LINKED_ACCOUNT` and get the costs that are associated with that account's usage of that service. Specify the filter condition by using the syntax like SQL Where clause. See the below [About `filter` option](#about-filter-option) section. (string, optional)
- **granularity**: Sets the AWS cost granularity to `"MONTHLY"` or `"DAILY"`. If Granularity isn't set, the response object doesn't include the Granularity, either `"MONTHLY"` or `"DAILY"`. (string, default: `"DAILY"`)
- **group_by**: You can group AWS costs using up to two different groups, either **Dimension** keys, **Tag** keys, or both. (array of string, optional)
- **metrics**: Which metrics are returned in the request. For more information about blended and unblended rates, see [Why does the "blended" annotation appear on some line items in my bill?](https://aws.amazon.com/premiumsupport/knowledge-center/blended-rates-intro/). Valid values are `"AmortizedCost"`, `"BlendedCost"`, `"UnblendedCost"`, and `"UsageQuantity"`. (array of string, default: `["UnblendedCost"]`)
  - NOTE: If you return the `"UsageQuantity"` metric, the service aggregates all usage numbers without taking into account the units. For example, if you aggregate `"UsageQuantity"` across all of EC2, the results aren't meaningful because EC2 compute hours and data transfer are measured in different units (for example, hours vs. GB). To get more meaningful `"UsageQuantity"` metrics, filter by `UsageType` or `UsageTypeGroups`.
- **start_date**: The beginning of the time period that you want the usage and costs for. The **start_date** is inclusive. For example, if **start_date** is `"2017-01-01"`, AWS retrieves cost and usage data starting at 2017-01-01 up to the **end_date**. (string matching `\d{4}-\d{2}-\d{2}`, default: yesterday)
- **end_date**: The end of the time period that you want the usage and costs for. The **end_date** is exclusive. For example, if **end_date** is 2017-05-01, AWS retrieves cost and usage data from the **start_date** up to, but not including, 2017-05-01. (string matching `\d{4}-\d{2}-\d{2}`, default: yesterday)

#### About `filter` option

Specify the filter condition by using the syntax like SQL Where clause. Compared to SQL Where clause syntax, the syntax are limited. See below to understand the syntax.

##### Available columns
- any **Dimension**s
- any **Tag**s

##### Available operators

- `AND`
- `OR`
- `=`
- `!=`
- `LIKE`
- `NOT LIKE`
- `IN`
- `NOT IN`
- `(...expressions...)`
- `NOT (...expressions...)`

##### Examples

- ```filter: "enviornment = 'production' and INSTANCE_TYPE in ('p3.16xlarge', 'p2.16xlarge')"```
- ```filter: "environment = 'development' and `project:name` like '%first-project%'"```
- ```filter: "user = 'civitaspo' AND not (INSTANCE_TYPE != 'p3.16xlarge' OR INSTANCE_TYPE != 'p2.16xlarge')"```


### Output Parameters

- **cost_explorer.last_get_cost.results**: The results by each time period. (array of map)
  - **start_date**: The beginning of the time period. (string)
  - **end_date**: The end of the time period. (string)
  - **group_keys**: The group keys that are included in this time period. (array of string)
  - **amortized_cost**:
    - **amount**: The actual number that represents the metric. (string)
    - **unit**: The unit that the metric is given in. (string)
  - **blended_cost**:
    - **amount**: The actual number that represents the metric. (string)
    - **unit**: The unit that the metric is given in. (string)
  - **unblended_cost**:
    - **amount**: The actual number that represents the metric. (string)
    - **unit**: The unit that the metric is given in. (string)
  - **usage_quantity**:
    - **amount**: The actual number that represents the metric. (string)
    - **unit**: The unit that the metric is given in. (string)
  - **estimated**: Whether this result is estimated. (boolean)

# Development

## Run an Example

### 1) build

```sh
./gradlew publish
```

Artifacts are build on local repos: `./build/repo`.

### 2) get your aws profile

```sh
aws configure
```

### 3) run an example

```sh
./example/run.sh
```

## (TODO) Run Tests

```sh
./gradlew test
```

# ChangeLog

[CHANGELOG.md](./CHANGELOG.md)

# License

[Apache License 2.0](./LICENSE.txt)

# Author

@civitaspo
