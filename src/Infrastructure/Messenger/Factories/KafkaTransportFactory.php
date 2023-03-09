<?php

namespace Untek\FrameworkPlugin\MessengerKafkaTransport\Infrastructure\Messenger\Factories;

use longlang\phpkafka\Consumer\Consumer;
use longlang\phpkafka\Consumer\ConsumerConfig;
use Mservis\Operator\Module\Bus\Infrastructure\Messenger\Interfaces\MessengerTransportFactoryInterface;
use Untek\FrameworkPlugin\MessengerKafkaTransport\Infrastructure\Messenger\Symfony\Transport\KafkaReceiver;
use Untek\FrameworkPlugin\MessengerKafkaTransport\Infrastructure\Messenger\Symfony\Transport\KafkaSender;
use Psr\Container\ContainerInterface;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class KafkaTransportFactory implements MessengerTransportFactoryInterface
{

    public function __construct(
        private ContainerInterface $container
    ) {
    }

    public function createSender(string $topic): SenderInterface
    {
        $sender = $this->container->get(KafkaSender::class);
        $sender->setBroker(getenv('KAFKA_BROKER'));
        $sender->setClientId(getenv('KAFKA_CLIENT_ID'));
        $sender->setTopic($topic);
        return $sender;
    }

    public function createReceiver(string $topic): ReceiverInterface
    {
        $serializer = $this->container->get(SerializerInterface::class);
        $consumer = $this->createConsumer($topic);
        return new KafkaReceiver($serializer, $consumer);
    }

    public function createConsumer(string $topic): Consumer
    {
        $config = new ConsumerConfig();
        $config->setBroker(getenv('KAFKA_BROKER'));
        $config->setTopic($topic);
        $config->setGroupId(getenv('KAFKA_GROUP_ID'));
        $config->setClientId(getenv('KAFKA_CLIENT_ID'));
        $config->setGroupInstanceId($topic . '-instance');
        return new Consumer($config);
    }
}
