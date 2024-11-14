{
    dag_name: "ods_get_tms_data",
    dev_dag_schedule: null,
    dag_schedule: null,
    dag_retries: 1,
    slack_channel: "",
    dependencies: [
    "process_charge_details",
    "process_order_plan"],
    tasks: [
    {
    task_type: "data",
    task_name: "process_charge_details",
    source_module: "getTmsdata",
    source_attribute: "getChargeDetails",
    source_sql: "",
    variable_module: "",
    variable_attribute: "",
    variable_connection: "target",
    target_table: "staging.tms_charge_details",
    truncate_table: true,
    source_hook: "snowflake_edw",
    source_hook_type: "jdbc",
    target_hook: "snowflake_ods_staging",
    target_hook_type: "snowflake",
    post_process_module: "getTmsdata",
    post_process_attribute: "postChargeDetails",
    post_process_sql: { },
    active: true
    },
    {
        task_type: "data",
        task_name: "process_order_plan",
        source_module: "getTmsdata",
        source_attribute: "getOrderPlan",
        source_sql: "",
        variable_module: "",
        variable_attribute: "",
        variable_connection: "target",
        target_table: "staging.tms_orderplan",
        truncate_table: true,
        source_hook: "snowflake_edw",
        source_hook_type: "jdbc",
        target_hook: "snowflake_ods_staging",
        target_hook_type: "snowflake",
        post_process_module: "getTmsdata",
        post_process_attribute: "postOrderPlan",
        post_process_sql: { },
        active: true
        }
    ]
    }