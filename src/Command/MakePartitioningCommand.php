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

    /** @var string */
    private $databaseUrl;

    /** @var string */
    private $tableName;

    /** @var string */
    private $partitionMode;

    /** @var string */
    private $stampColumn;

    /** @var string */
    private $minStamp;

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

        try {
            $this->validateArguments($input);
        } catch (\Exception $e) {
            $this->io->error('Wrong Partition Mode: ' . $this->partitionMode);

            return 1;
        }

        $connector = new Connector();
        try {
            $config = $connector->getConfig($this->databaseUrl);
            $pdo = $connector->getPdo($config);
        } catch (\Exception $e) {
            $this->io->error($e->getMessage());

            return 1;
        }

        try {
            $this->service->partition(
                $pdo,
                $this->tableName,
                $this->partitionMode,
                $this->stampColumn,
                $this->minStamp
            );
        } catch (\Exception $e) {
            $this->io->error($e->getMessage());

            return 1;
        }

        $this->io->success('The Partitioning have been made successfully.');

        return 0;
    }

    /**
     * @param InputInterface $input
     * @throws \Exception
     */
    private function validateArguments(InputInterface $input): void
    {
        $this->databaseUrl = $input->getArgument('databaseURL');
        $this->tableName = $input->getArgument('tableName');
        $this->partitionMode = $input->getArgument('partitionMode');
        $this->stampColumn = $input->getArgument('stampColumn');
        $this->minStamp = $input->getArgument('minStamp');

        if ($this->tableName) {
            $this->io->note(sprintf('Table Name: %s', $this->tableName));
        }

        if ($this->partitionMode) {
            $this->io->note(sprintf('Partition Mode: %s', $this->partitionMode));
        }

        $partitionModes = PartitioningService::getPartitionModes();

        if (!in_array($this->partitionMode, $partitionModes, true)) {
            throw new \Exception('Wrong Partition Mode: ' . $this->partitionMode);
        }

        if ($this->stampColumn) {
            $this->io->note(sprintf('Stamp Column: %s', $this->stampColumn));
        }

        if ($this->partitionMode) {
            $this->io->note(sprintf('Min Stamp: %s', $this->minStamp));
        }
    }
}
