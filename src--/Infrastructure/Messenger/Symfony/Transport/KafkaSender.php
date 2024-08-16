<?php

namespace Untek\FrameworkPlugin\MessengerKafkaTransport\Infrastructure\Messenger\Symfony\Transport;

use Untek\Framework\Messenger\Infrastructure\Messenger\Symfony\Stamp\TopicStamp;
use longlang\phpkafka\Producer\ProduceMessage;
use longlang\phpkafka\Producer\Producer;
use longlang\phpkafka\Producer\ProducerConfig;
use longlang\phpkafka\Protocol\RecordBatch\RecordHeader;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class KafkaSender implements SenderInterface
{

    private string $topic;
    private string $broker;
    private string $clientId;

    public function __construct(
        private SerializerInterface $serializer,
    ) {
    }

    public function setTopic(string $topic): void
    {
        $this->topic = $topic;
    }

    public function setBroker(string $broker): void
    {
        $this->broker = $broker;
    }

    public function setClientId(string $clientId): void
    {
        $this->clientId = $clientId;
    }

    public function send(Envelope $envelope): Envelope
    {
        $envelope = $this->addStamps($envelope);
        $produceMessage = $this->createProduceMessage($envelope);
        $this->getProducerInstance()->sendBatch([$produceMessage]);
        return $envelope;
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

    protected function addStamps(Envelope $envelope): Envelope
    {
        $id = uniqid('', true);
        $envelope = $envelope->with(new TransportMessageIdStamp($id));
        /** @var TopicStamp $topicStamp */
        $topicStamp = $envelope->last(TopicStamp::class);
        if (empty($topicStamp)) {
            $envelope = $envelope->with(new TopicStamp($this->topic));
        }
        return $envelope;
    }

    protected function headersToRecordHeaders(array $encodedEnvelopeHeaders): array
    {
        $headers = [];
        foreach ($encodedEnvelopeHeaders as $headerName => $headerValue) {
            $header = new RecordHeader();
            $header->setHeaderKey($headerName);
            $header->setValue($headerValue);
            $headers[] = $header;
        }
        return $headers;
    }

    private function getProducerInstance(): Producer
    {
        $config = new ProducerConfig();
        $config->setBootstrapServer($this->broker);
        $config->setUpdateBrokers(true);
        $config->setClientId($this->clientId);
        $config->setAcks(-1);
        return new Producer($config);
    }
}
