<?php

namespace Untek\FrameworkPlugin\MessengerKafkaTransport\Presentation\Cli\Commands;

use Mservis\Operator\Module\Bus\Infrastructure\Messenger\Symfony\Stamp\TopicStamp;
use longlang\phpkafka\Producer\ProduceMessage;
use longlang\phpkafka\Producer\Producer;
use longlang\phpkafka\Producer\ProducerConfig;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class KafkaSendMessageCommand extends Command
{

    public static function getDefaultName(): string
    {
        return 'kafka:send-message';
    }

    public function __construct(
        private MessageBusInterface $bus,
        private SerializerInterface $serializer
    ) {
        parent::__construct();
    }

    protected function configure()
    {
        $this->addArgument('topic', InputArgument::REQUIRED);
        $this->addArgument('value', InputArgument::OPTIONAL);
        $this->addArgument('headers', InputArgument::OPTIONAL);
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $topic = $input->getArgument('topic');
        $value = $input->getArgument('value');
        $headers = $input->getArgument('headers');
        $headers = $headers ? json_decode($headers, JSON_OBJECT_AS_ARRAY) : [];

        $id = uniqid('', true);
        $produceMessage = new ProduceMessage(
            $topic,
            $value,
            $id,
            $headers
        );
        $this->getProducerInstance(getenv('KAFKA_BROKER'), getenv('KAFKA_CLIENT_ID'))->sendBatch([$produceMessage]);

        return 0;
    }

    protected function createProduceMessage(Envelope $envelope): ProduceMessage
    {
        $encodedEnvelope = $this->serializer->encode($envelope);
        $headers = $this->headersToRecordHeaders($encodedEnvelope['headers']);
        /** @var TopicStamp $topicStamp */
        $topicStamp = $envelope->last(TopicStamp::class);
        /** @var TransportMessageIdStamp $transportMessageIdStamp */
        $transportMessageIdStamp = $envelope->last(TransportMessageIdStamp::class);
        return new ProduceMessage(
            $topicStamp->getTopic(),
            $encodedEnvelope['body'],
            $transportMessageIdStamp->getId(),
            $headers
        );
    }

    private function getProducerInstance($broker, $clientId): Producer
    {
        $config = new ProducerConfig();
        $config->setBootstrapServer($broker);
        $config->setUpdateBrokers(true);
        $config->setClientId($clientId);
        $config->setAcks(-1);
        return new Producer($config);
    }
}
