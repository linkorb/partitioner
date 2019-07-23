<?php

namespace App\Services;

use App\Interfaces\TablePartitionerInterface;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\AbstractSchemaManager;
use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\DateTimeType;
use Doctrine\DBAL\Types\DateType;
use Doctrine\DBAL\Types\IntegerType;
use Doctrine\ORM\Configuration;
use PDO;

class PartitioningService implements TablePartitionerInterface
{
    private const BULK_SIZE = 100;

    private const STAMP_COLUMNT_TYPE_INTEGER  = 'int';
    private const STAMP_COLUMNT_TYPE_DATETIME = 'datetime';

    /** @var PDO */
    private $pdo;

    /** @var string */
    private $tableName;

    private $partitionMode;

    /** @var string */
    private $stampColumn;

    /** @var \DateTime|string */
    private $minStamp;

    /** @var AbstractSchemaManager */
    private $schemaManager;

    /** @var Column[] */
    private $columns;

    /** @var Index[] */
    private $indexes;

    /** @var string[] */
    private $primaryKeys;

    /** @var string */
    private $stampColumnType;

    public static function getPartitionModes(): array
    {
        return [
            self::PARTITION_YEAR,
            self::PARTITION_YEAR_MONTH,
            self::PARTITION_YEAR_MONTH_DAY,
        ];
    }

    public function partition(PDO $pdo, string $tableName, $partitionMode, string $stampColumn, $minStamp): void
    {
        $this->pdo = $pdo;
        $this->tableName = $tableName;
        $this->partitionMode = $partitionMode;
        $this->stampColumn = $stampColumn;
        $this->minStamp = $minStamp;

        $this->schemaManager = $this->getSchemaManager();

        [$this->columns, $this->indexes] = $this->getSchemaTable();

        $this->validateArguments();

        $partitionCriteria = (self::STAMP_COLUMNT_TYPE_DATETIME === $this->stampColumnType) ?
            $this->getPartitionCriteriaDateTime() :
            $this->getPartitionCriteriaTimestamp();

        $partitionTables = $this->getPartitionTables($partitionCriteria);

        $fields = $this->getTableFields();

        foreach ($partitionTables as $partitionTable) {
            $firstPartitionedEntryAfter = $this->preparePartitionTable($partitionTable);
            $lastPartitionedEntryBefore = (self::STAMP_COLUMNT_TYPE_DATETIME === $this->stampColumnType) ?
                $partitionTable['end']->format('Y-m-d') :
                $partitionTable['end']->getTimestamp();

            $partitionRowsCount = $this->getPartitionRowsCount(
                $firstPartitionedEntryAfter,
                $lastPartitionedEntryBefore
            );

            $valuesPlaceholdersStrings = [];
            $values = [];
            $primaryKeysToDelete = [];

            $queryInsert = "INSERT IGNORE INTO `{$partitionTable['name']}`";

            $queryForPartition = $this->getQueryForPartition($firstPartitionedEntryAfter, $lastPartitionedEntryBefore);
            $stmt = $this->pdo->query($queryForPartition);

            $bulkCounter = 0;
            $totalCounter = 0;

            while ($data = $stmt->fetch()) {
                $valuesPlaceholders = [];
                $rowPrimaryKeys = [];
                $bulkCounter++;
                $totalCounter++;
                foreach ($data as $key => $value) {
                    if (is_numeric($key)) {
                        continue;
                    }
                    $valuesPlaceholders[] = '?';
                    $values[] = $value;

                    if (in_array($key, $this->primaryKeys, true)) {
                        $rowPrimaryKeys[$key] = $value;
                    }
                }

                $primaryKeysToDelete[] = $rowPrimaryKeys;

                $valuesPlaceholdersStrings[] = implode(', ', $valuesPlaceholders);

                if (($bulkCounter === self::BULK_SIZE) || ($totalCounter === $partitionRowsCount)) {
                    $bulkQuery = $queryInsert .
                        ' (' . implode(', ', $fields) . ') VALUES (' . implode('), (', $valuesPlaceholdersStrings) . ')';

                    $this->updateCurrentDataBulk($bulkQuery, $values, $primaryKeysToDelete);

                    $bulkCounter = 0;
                    $valuesPlaceholdersStrings = [];
                    $values = [];
                    $primaryKeysToDelete = [];
                }
            }
        }
    }

    private function updateCurrentDataBulk(string $query, array $values, array $primaryKeysToDelete): void
    {
        try {
            $this->pdo->beginTransaction();
            $insertStmt = $this->pdo->prepare($query);
            $insertStmt->execute($values);

            $this->deleteByPrimaryKeys($primaryKeysToDelete);

            $this->pdo->commit();
        } catch (\Exception $e) {
            echo $e->getMessage() . PHP_EOL;
            $this->pdo->rollBack();
        }
    }

    private function deleteByPrimaryKeys(array $primaryKeysToDelete): void
    {
        $primaryKeys = '`' . implode('`, `', $this->primaryKeys) . '`';
        $values = [];
        foreach ($primaryKeysToDelete as $keys) {
            $values[] = "('" . implode("', '", array_values($keys)) . "')";
        }

        $query = "DELETE FROM $this->tableName WHERE (" . $primaryKeys . ') IN (' . implode(', ', $values) . ')';
        $deleteStatement = $this->pdo->query($query);
        $deleteStatement->execute();
    }

    private function getSchemaManager(): AbstractSchemaManager
    {
        $connConfig = new Configuration();
        $connection = DriverManager::getConnection(
            [
                'driver' => 'pdo_' . $this->pdo->getAttribute(PDO::ATTR_DRIVER_NAME),
                'pdo'    => $this->pdo,
            ],
            $connConfig);

        return $connection->getSchemaManager();
    }

    /**
     * @return array
     * @throws \Exception
     */
    private function getSchemaTable(): array
    {
        $table = $this->schemaManager->listTableDetails($this->tableName);

        if (null === $table) {
            throw new \Exception('There\'s no table with name ' . $this->tableName . '.');
        }

        $this->primaryKeys = $table->getPrimaryKeyColumns();

        $column = $table->getColumn($this->stampColumn);

        if ($column->getType() instanceof IntegerType) {
            $this->stampColumnType = self::STAMP_COLUMNT_TYPE_INTEGER;
        } else if (
            $column->getType() instanceof DateTimeType ||
            $column->getType() instanceof DateType
        ) {
            $this->stampColumnType = self::STAMP_COLUMNT_TYPE_DATETIME;
        } else {
            throw new \Exception('Unknown Stamp Column Type');
        }

        return [
            $table->getColumns(),
            $table->getIndexes(),
        ];
    }

    private function getPartitionCriteriaDateTime(): string
    {
        $partitionCriteria = '1970-01-01';

        switch ($this->partitionMode) {
            case self::PARTITION_YEAR:
                $partitionCriteria = $this->minStamp->format('Y') . '-01-01';
                break;
            case self::PARTITION_YEAR_MONTH:
                $partitionCriteria = $this->minStamp->format('Y-m') . '-01';
                break;
            case self::PARTITION_YEAR_MONTH_DAY:
                $partitionCriteria = $this->minStamp->format('Y-m-d');
                break;
        }

        return $partitionCriteria;
    }

    private function getPartitionCriteriaTimestamp(): string
    {
        $partitionCriteria = $this->getPartitionCriteriaDateTime();

        $startPeriodDateTime = new \DateTime($partitionCriteria);

        return (string)$startPeriodDateTime->getTimestamp();
    }

    private function getPartitionTables(string $partitionCriteria): array
    {
        $queryRange = "
            SELECT
              MIN(`$this->stampColumn`) AS minDate,
              MAX(`$this->stampColumn`) AS maxDate
            FROM $this->tableName
            WHERE
              `$this->stampColumn` < :partitionCriteria
        ";
        $stmtRange = $this->pdo->prepare($queryRange);
        $stmtRange->execute(
            [
                ':partitionCriteria' => $partitionCriteria
            ]
        );
        $dataRangeRow = $stmtRange->fetchAll();
        $dateFirst = $dataRangeRow[0]['minDate'];
        $dateLast = $dataRangeRow[0]['maxDate'];

        if (empty($dateFirst) || empty($dateLast)) {
            throw  new \Exception('Something went wrong with dates');
        }

        $dateStart = (self::STAMP_COLUMNT_TYPE_DATETIME === $this->stampColumnType) ?
            new \DateTime($dateFirst) :
            \DateTime::createFromFormat('U', $dateFirst);
        $dateEnd = (self::STAMP_COLUMNT_TYPE_DATETIME === $this->stampColumnType) ?
            new \DateTime($dateLast) :
            \DateTime::createFromFormat('U', $dateLast);
        $partitionTables = [];

        switch ($this->partitionMode) {
            case self::PARTITION_YEAR:
                $start = $dateStart->modify('first day of this year');
                $end = $dateEnd->modify('last day of this year')->setTime(0, 0);
                $interval = \DateInterval::createFromDateString('1 year');
                $period = new \DatePeriod($start, $interval, $end);
                foreach ($period as $dt) {
                    $suffix = $dt->format('Y');
                    $partitionTables[] = [
                        'name'  => '_' . $this->tableName . '__' . $suffix,
                        'start' => new \DateTime($suffix . '-01-01'),
                        'end'   => (new \DateTime($suffix . '-01-01'))->modify('first day of next year'),
                    ];
                }
                break;
            case self::PARTITION_YEAR_MONTH:
                $start = $dateStart->modify('first day of this month');
                $end = $dateEnd->modify('last day of this month')->setTime(0, 0);
                $interval = \DateInterval::createFromDateString('1 month');
                $period = new \DatePeriod($start, $interval, $end);
                foreach ($period as $dt) {
                    $suffix = $dt->format('Y-m');
                    $partitionTables[] = [
                        'name'  => '_' . $this->tableName . '__' . $suffix,
                        'start' => new \DateTime($suffix . '-01'),
                        'end'   => (new \DateTime($suffix . '-01'))->modify('first day of next month'),
                    ];
                }
                break;
            case self::PARTITION_YEAR_MONTH_DAY:
                $start = $dateStart;
                $end = $dateEnd->modify('next day')->setTime(0, 0);
                $interval = \DateInterval::createFromDateString('1 day');
                $period = new \DatePeriod($start, $interval, $end);
                foreach ($period as $dt) {
                    $suffix = $dt->format('Y-m-d');
                    $partitionTables[] = [
                        'name'  => '_' . $this->tableName . '__' . $suffix,
                        'start' => new \DateTime($suffix),
                        'end'   => new \DateTime($suffix . '+ 1 day'),
                    ];
                }
                break;
        }

        return $partitionTables;
    }

    /**
     * @return array
     */
    private function getTableFields(): array
    {
        $fields = [];
        foreach ($this->columns as $item) {
            $fields[] = $item->getName();
        }

        return $fields;
    }

    private function getPartitionRowsCount($firstPartitionedEntryAfter, $lastPartitionedEntryBefore): int
    {
        $queryCount = "
                SELECT
                    COUNT(*) AS rows
                FROM
                    $this->tableName
                WHERE
                    $this->stampColumn >= '$firstPartitionedEntryAfter'
                AND
                    $this->stampColumn < '$lastPartitionedEntryBefore'
            ";

        $stmtCount = $this->pdo->query($queryCount);

        return (int)($stmtCount->fetchAll())[0]['rows'];
    }

    private function preparePartitionTable($partitionTable)
    {
        if (!$this->schemaManager->tablesExist([$partitionTable['name']])) {
            echo 'There\'s no table with name: ' . $partitionTable['name'] . PHP_EOL;
            echo 'Trying to create new table...' . PHP_EOL;
            $newTable = new Table('`' . $partitionTable['name'] . '`', $this->columns, $this->indexes);
            $this->schemaManager->createTable($newTable);
            $firstPartitionedEntryAfter =
                (self::STAMP_COLUMNT_TYPE_DATETIME === $this->stampColumnType) ?
                    $partitionTable['start']->format('Y-m-d') :
                    $partitionTable['start']->getTimestamp();
        } else {
            echo 'Partition table exists' . PHP_EOL;
            $stmtPartitioned = $this->pdo->query("SELECT MAX(`$this->stampColumn`) AS last FROM `{$partitionTable['name']}`");
            $lastRow = $stmtPartitioned->fetchAll();

            $firstPartitionedEntryAfter = (self::STAMP_COLUMNT_TYPE_DATETIME === $this->stampColumnType) ?
                $lastRow[0]['last'] ?: $partitionTable['start']->format('Y-m-d H:i:s') :
                $lastRow[0]['last'] ?: $partitionTable['start']->getTimestamp();
        }

        echo 'Last Paritioned Entry is for: ' . $firstPartitionedEntryAfter . PHP_EOL;

        return $firstPartitionedEntryAfter;
    }

    private function getQueryForPartition($firstPartitionedEntryAfter, $lastPartitionedEntryBefore): string
    {
        return "
                SELECT
                    *
                FROM
                    $this->tableName
                WHERE
                    $this->stampColumn >= '$firstPartitionedEntryAfter'
                AND
                    $this->stampColumn < '$lastPartitionedEntryBefore'
            ";
    }

    /**
     * @throws \Exception
     */
    private function validateArguments(): void
    {
        if ($this->tableName) {
            echo sprintf('Table Name: %s', $this->tableName) . PHP_EOL;
        }

        if ($this->partitionMode) {
            echo sprintf('Partition Mode: %s', $this->partitionMode) . PHP_EOL;
        }

        $partitionModes = self::getPartitionModes();

        if (!in_array($this->partitionMode, $partitionModes, true)) {
            throw new \Exception('Wrong Partition Mode: ' . $this->partitionMode);
        }

        if ($this->stampColumn) {
            echo sprintf('Stamp Column: %s', $this->stampColumn) . PHP_EOL;
        }

        if ($this->minStamp) {
            try {
                $this->minStamp = new \DateTime($this->minStamp);
            } catch (\Exception $e) {
                throw new \Exception('Wrong minStamp format');
            }

            echo sprintf('Min Stamp: %s', $this->minStamp->format('Y-m-d')) . PHP_EOL;
        }
    }
}
