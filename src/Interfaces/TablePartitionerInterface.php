<?php

namespace App\Interfaces;

use PDO;

interface TablePartitionerInterface
{
    public const PARTITION_YEAR           = 'YEAR';
    public const PARTITION_YEAR_MONTH     = 'YEAR_MONTH';
    public const PARTITION_YEAR_MONTH_DAY = 'YEAR_MONTH_DAY';

    public function partition(PDO $pdo, string $tableName, $partitionMode, string $stampColumn, $minStamp);
}
