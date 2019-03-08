# Description

In several applications we have some tables that are getting very large (usually event logs, audit trails, etc).

This card is about creating a PHP library and a console command that can help us to partition a large table into multiple smaller tables, segmented by date ranges (like year+month, or year+month+day for very large tables)

### Interface:

```php
interface TablePartitionerInterface
{​
    public function partition(PDO $pdo, string $tableName, $partitionMode, string $stampColumn,  $minStamp);
}​
```

### Arguments:

* $pdo: an active PDO connection
* $tableName: name of the table to partition
* $partitionMode: to indicate to partion by `YEAR`, `YEAR_MONTH`, `YEAR_MONTH_DAY`
* $stampColumn: name of the column to partition by (either a unix timestamp or date/datetime column)
* $minStamp: minimum stamp of the event to start partitioning (for example, to indicate that only events older than 1 year need to be partitioned)

This function would:

1. find all rows in the table that are older than minStamp (based on stampColumn)
2. group them by $partitionMode (i.e. year, year+month, etc)
3. for each &quot;partition&quot;, ensure a table exists called `'_' . $tablename . '__2018-12'` etc that copies the schema + indexes from the main table
4. move over the events from the main table onto the partitioned tables (keeping all data as-is, including ids)

To support this library, we'll also need a console command that can execute this function to partition any database table. input arguments are the same as the function arguments. the PDO connection string should be provided in one string. i.e. `mysql://username:password@localhost/mydb`

### Requirements

* Ensure the function can handle large amounts of records (by chunking the partitioning rows, not loading everything in memory, etc)
* Ensure the function can be executed multiple times, continuing the process where it left last time
* Use database transactions around the copying + deleting (moving) of records, so that a failed transaction can simply be restarted.

# Installation

### Standalone Console Command

```bash
git clone https://github.com/pythagor/partitioner
composer install
```
### Library into an existing project

TBD

# Usage

### Console Command:

```bash
bin/console app:make-partitioning mysql://<user>:<pass>@<host>:<port>/<database> <table_name> <partition_mode> <column> <min_stamp>
```

* `<column>` - name of the column to partition by (either a `INT` or `DATE`/`DATETIME` database column type)
* `<min_stamp>` - `Y-m-d` value to paritioning rows older than given value.

### Example:

```bash
bin/console app:make-partitioning mysql://user:pass@localhost:3306/partition log_visit YEAR_MONTH_DAY visit_first_action_time 2018-10-01
```
