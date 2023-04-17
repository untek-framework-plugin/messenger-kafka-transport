<?php

namespace Untek\FrameworkPlugin\MessengerKafkaTransport\Infrastructure;

use Untek\Core\Kernel\Bundle\BaseBundle;

class KafkaBundle extends BaseBundle
{

    public function getName(): string
    {
        return 'kafka';
    }
}
