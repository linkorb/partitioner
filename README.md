Console Command:

`app:make-partitioning mysql://<user>:<pass>@<host>:<port>/<database> <table_name> <partition_mode> <column> <min_stamp>`

Example:

`app:make-partitioning mysql://user:pass@mysql:3306/partition log_visit YEAR_MONTH_DAY visit_first_action_time 2018-10-01`
