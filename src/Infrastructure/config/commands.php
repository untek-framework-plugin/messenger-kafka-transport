<?php

use Untek\FrameworkPlugin\MessengerKafkaTransport\Presentation\Cli\Commands\KafkaSendMessageCommand;
use Untek\Framework\Console\Symfony4\Interfaces\CommandConfiguratorInterface;

return function (CommandConfiguratorInterface $commandConfigurator) {
    $commandConfigurator->registerCommandClass(KafkaSendMessageCommand::class);
};
