<?php

namespace Untek\FrameworkPlugin\MessengerKafkaTransport\Infrastructure;

use Untek\Core\Kernel\Bundle\BaseBundle;

class KafkaBundle extends BaseBundle
{

    public function getName(): string
    {
        return 'kafka';
    }

    public function boot(): void
    {
        if ($this->isCli()) {
            $this->configureFromPhpFile(__DIR__ . '/config/commands.php');
        }
    }
}
