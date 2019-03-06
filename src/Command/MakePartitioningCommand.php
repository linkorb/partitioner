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

    /** @var PartitioningService */
    private $service;

    /** @var SymfonyStyle */
    private $io;

    public function __construct($name = null, PartitioningService $service)
    {
        $this->service = $service;
        parent::__construct($name);
    }

    protected function configure(): void
    {
        $this
            ->setDescription('Add a short description for your command')
            ->addArgument('databaseURL', InputArgument::REQUIRED, 'Database URL String')
            ->addArgument('tableName', InputArgument::REQUIRED, 'Table Name')
            ->addArgument('partitionMode', InputArgument::REQUIRED, 'Partition Mode')
            ->addArgument('stampColumn', InputArgument::REQUIRED, 'Stamp Column')
            ->addArgument('minStamp', InputArgument::REQUIRED, 'Min Stamp');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $this->io = new SymfonyStyle($input, $output);

        $connector = new Connector();
        try {
            $config = $connector->getConfig($input->getArgument('databaseURL'));
            $pdo = $connector->getPdo($config);
        } catch (\Exception $e) {
            $this->io->error($e->getMessage());

            return 1;
        }

        try {
            $this->service->partition(
                $pdo,
                $input->getArgument('tableName'),
                $input->getArgument('partitionMode'),
                $input->getArgument('stampColumn'),
                $input->getArgument('minStamp')
            );
        } catch (\Exception $e) {
            $this->io->error($e->getMessage());

            return 1;
        }

        $this->io->success('The Partitioning have been made successfully.');

        return 0;
    }
}
