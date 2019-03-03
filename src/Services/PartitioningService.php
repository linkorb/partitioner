<?php

namespace App\Services;

use App\Interfaces\TablePartitionerInterface;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Table;
use Doctrine\ORM\Configuration;
use PDO;

class PartitioningService implements TablePartitionerInterface
{
    const BULK_SIZE = 100;

    public function partition(PDO $pdo, string $tableName, $partitionMode, string $stampColumn, $minStamp)
    {
        $schemaManager = $this->getSchemaManager($pdo);

        [$columns, $indexes] = $this->getSchemaTable($tableName, $schemaManager);

        $partitionCriteria = $this->getPartitionCriteria($partitionMode, $minStamp);

        $partitionTables = $this->getPartitionTables(
            $stampColumn,
            $partitionMode,
            $partitionCriteria,
            $tableName,
            $pdo
        );

        $fields = $this->getTableFields($columns);

        foreach ($partitionTables as $partitionTableName => $dates) {
            if (!$schemaManager->tablesExist([$partitionTableName])) {
                echo 'There\'s no table with name: ' . $partitionTableName . PHP_EOL;
                echo 'Trying to create new table...' . PHP_EOL;
                $newTable = new Table('`' . $partitionTableName . '`', $columns, $indexes);
                $schemaManager->createTable($newTable);
                $firstPartitionedEntryAfter = $dates['start']->format('Y-m-d');
                echo 'Last Paritioned Entry is for: ' . $firstPartitionedEntryAfter . PHP_EOL;
            } else {
                echo 'Partition table exists' . PHP_EOL;
                $stmtPartitioned = $pdo->query("SELECT MAX(`$stampColumn`) AS last FROM `$partitionTableName`");
                $lastRow = $stmtPartitioned->fetchAll();
                $firstPartitionedEntryAfter = $lastRow[0]['last'] ?: $dates['start']->format('Y-m-d');
                echo 'Last Paritioned Entry is for: ' . $firstPartitionedEntryAfter . PHP_EOL;
            }

            $lastPartitionedEntryBefore = $dates['end']->format('Y-m-d');

            $query = "
              SELECT *
              FROM $tableName
              WHERE
                    $stampColumn > '$firstPartitionedEntryAfter'
              AND
                    $stampColumn < '$lastPartitionedEntryBefore'
            ";

            $queryCount = "
              SELECT
                     COUNT(*) AS rows
              FROM $tableName
              WHERE
                    $stampColumn > '$firstPartitionedEntryAfter'
              AND $stampColumn < '$lastPartitionedEntryBefore'
            ";

            $stmtCount = $pdo->query($queryCount);
            $partitionRowsCount = (int)($stmtCount->fetchAll())[0]['rows'];

            $stmt = $pdo->query($query);

            $valuesPlaceholdersStrings = $values = [];
            $queryInsert = "INSERT IGNORE INTO `$partitionTableName`";

            $bulkCounter = 0;
            $totalCounter = 0;

            while ($data = $stmt->fetch()) {
                $valuesPlaceholders = [];
                $bulkCounter++;
                $totalCounter++;
                foreach ($data as $key => $value) {
                    if (is_numeric($key)) {
                        continue;
                    }
                    $valuesPlaceholders[] = '?';
                    $values[] = $value;
                }

                $valuesPlaceholdersStrings[] = implode(', ', $valuesPlaceholders);

                if (($bulkCounter === self::BULK_SIZE) || ($totalCounter === $partitionRowsCount)) {
                    $queryAppend = ' (' . implode(', ', $fields) . ') VALUES (' . implode('), (', $valuesPlaceholdersStrings) . ')';
                    try {
                        $pdo->beginTransaction();
                        $insertStmt = $pdo->prepare($queryInsert . $queryAppend);
                        $insertStmt->execute($values);

                        // @todo Old Data Deletion Here

                        $pdo->commit();
                    } catch (\Exception $e) {
                        echo $e->getMessage() . PHP_EOL;
                        $pdo->rollBack();
                    }
                    $bulkCounter = 0;
                    $valuesPlaceholdersStrings = [];
                    $values = [];
                }
            }
        }
    }

    private function getSchemaManager(PDO $pdo)
    {
        $connConfig = new Configuration();
        $connection = DriverManager::getConnection(
            [
                'driver' => 'pdo_' . $pdo->getAttribute(PDO::ATTR_DRIVER_NAME),
                'pdo'    => $pdo,
            ],
            $connConfig);

        return $connection->getSchemaManager();
    }

    /**
     * @param string                $tableName
     * @param AbstractSchemaManager $schemaManager
     * @return array
     * @throws \Exception
     */
    private function getSchemaTable(string $tableName, AbstractSchemaManager $schemaManager): array
    {
        $table = null;
        foreach ($schemaManager->listTables() as $item) {
            if ($item->getName() === $tableName) {
                $table = $item;
                break;
            }
        }

        if (null === $table) {
            throw new \Exception('There\'s no table with name ' . $tableName . '.');
        }

        $columns = $table->getColumns();
        $indexes = $table->getIndexes();

        return [$columns, $indexes];
    }

    private function getPartitionCriteria($partitionMode, $minStamp)
    {
        $partitionCriteria = '1970-01-01';
        $stampDate = new \DateTime($minStamp);

        switch ($partitionMode) {
            case self::PARTITION_YEAR:
                $partitionString = $stampDate->format('Y');
                $partitionCriteria = $partitionString . '-01-01';
                break;
            case self::PARTITION_YEAR_MONTH:
                $partitionString = $stampDate->format('Y-m');
                $partitionCriteria = $partitionString . '-01';
                break;
            case self::PARTITION_YEAR_MONTH_DAY:
                $partitionString = $stampDate->format('Y-m-d');
                $partitionCriteria = $partitionString;
                break;
        }

        return $partitionCriteria;
    }

    private function getPartitionTables($stampColumn, $partitionMode, $partitionCriteria, $tableName, $pdo)
    {
        $queryRange = "
            SELECT
              MIN(`$stampColumn`) AS minDate,
              MAX(`$stampColumn`) AS maxDate
            FROM $tableName
            WHERE
              `$stampColumn` < '$partitionCriteria'
        ";
        $stmtRange = $pdo->query($queryRange);
        $dataRangeRow = $stmtRange->fetchAll();
        $dateFirst = $dataRangeRow[0]['minDate'];
        $dateLast = $dataRangeRow[0]['maxDate'];

        if (empty($dateFirst) || empty($dateLast)) {
            throw  new \Exception('Something went wrong with dates');
        }

        $dateStart = new \DateTime($dateFirst);
        $dateEnd = new \DateTime($dateLast);
        $partitionTables = [];

        switch ($partitionMode) {
            case self::PARTITION_YEAR:
                $start = $dateStart->modify('first day of this year');
                $end = $dateEnd->modify('last day of this year');
                $interval = \DateInterval::createFromDateString('1 year');
                $period = new \DatePeriod($start, $interval, $end);
                foreach ($period as $dt) {
                    $suffix = $dt->format('Y');
                    $partitionTables['_' . $tableName . '__' . $suffix] = [
                        'start' => new \DateTime($suffix . '-01-01'),
                        'end'   => (new \DateTime($suffix . '-01-01'))->modify('first day of next year'),
                    ];
                }
                break;
            case self::PARTITION_YEAR_MONTH:
                $start = $dateStart->modify('first day of this month');
                $end = $dateEnd->modify('last day of this month');
                $interval = \DateInterval::createFromDateString('1 month');
                $period = new \DatePeriod($start, $interval, $end);
                foreach ($period as $dt) {
                    $suffix = $dt->format('Y-m');
                    $partitionTables['_' . $tableName . '__' . $suffix] = [
                        'start' => new \DateTime($suffix . '-01'),
                        'end'   => (new \DateTime($suffix . '-01'))->modify('first day of next month'),
                    ];
                }
                break;
            case self::PARTITION_YEAR_MONTH_DAY:
                $start = $dateStart;
                $end = $dateEnd;
                $interval = \DateInterval::createFromDateString('1 day');
                $period = new \DatePeriod($start, $interval, $end);
                foreach ($period as $dt) {
                    $suffix = $dt->format('Y-m-d');
                    $partitionTables['_' . $tableName . '__' . $suffix] = [
                        'start' => new \DateTime($suffix),
                        'end'   => new \DateTime($suffix . '+ 1 day'),
                    ];
                }
                break;
        }

        return $partitionTables;
    }

    /**
     * @param $columns
     * @return array
     */
    private function getTableFields($columns): array
    {
        $fields = [];
        foreach ($columns as $item) {
            $fields[] = $item->getName();
        }

        return $fields;
    }
}
