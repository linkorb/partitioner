<?php

namespace App\Command;

use App\Services\PartitioningService;
use Connector\Connector;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

class MakePartitioningCommand extends Command
{
    protected static $defaultName = 'app:make-partitioning';

    private $container;

    private $service;

    public function __construct($name = null, PartitioningService $service)
    {
        $this->service = $service;
        parent::__construct($name);
    }

    protected function configure()
    {
        $this
            ->setDescription('Add a short description for your command')
            ->addArgument('databaseURL', InputArgument::REQUIRED, 'Database URL String')
            ->addArgument('tableName', InputArgument::REQUIRED, 'Table Name')
            ->addArgument('partitionMode', InputArgument::REQUIRED, 'Partition Mode')
            ->addArgument('stampColumn', InputArgument::REQUIRED, 'Stamp Column')
            ->addArgument('minStamp', InputArgument::REQUIRED, 'Min Stamp');
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $io = new SymfonyStyle($input, $output);

        $databaseUrl = $input->getArgument('databaseURL');
        $tableName = $input->getArgument('tableName');
        $partitionMode = $input->getArgument('partitionMode');
        $stampColumn = $input->getArgument('stampColumn');
        $minStamp = $input->getArgument('minStamp');

        if ($tableName) {
            $io->note(sprintf('Table Name: %s', $tableName));
        }

        if ($partitionMode) {
            $io->note(sprintf('Partition Mode: %s', $partitionMode));
        }

        $partitionModes = [
            PartitioningService::PARTITION_YEAR,
            PartitioningService::PARTITION_YEAR_MONTH,
            PartitioningService::PARTITION_YEAR_MONTH_DAY,
        ];

        if (!in_array($partitionMode, $partitionModes, true)) {
            $io->error('Wrong Partition Mode: ' . $partitionMode);

            return 1;
        }

        if ($stampColumn) {
            $io->note(sprintf('Stamp Column: %s', $stampColumn));
        }

        if ($partitionMode) {
            $io->note(sprintf('Min Stamp: %s', $minStamp));
        }

        $connector = new Connector();
        try {
            $config = $connector->getConfig($databaseUrl);
            $pdo = $connector->getPdo($config);
        } catch (\Exception $e) {
            $io->error($e->getMessage());

            return 1;
        }

        try {
            $this->service->partition(
                $pdo,
                $tableName,
                $partitionMode,
                $stampColumn,
                $minStamp
            );
        } catch (\Exception $e) {
            $io->error($e->getMessage());

            return 1;
        }

        $io->success('The Partitioning have been made successfully.');
    }
}
