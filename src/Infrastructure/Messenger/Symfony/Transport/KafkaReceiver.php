<?php

namespace Untek\FrameworkPlugin\MessengerKafkaTransport\Infrastructure\Messenger\Symfony\Transport;

use longlang\phpkafka\Consumer\ConsumeMessage;
use longlang\phpkafka\Consumer\Consumer;
use longlang\phpkafka\Protocol\RecordBatch\RecordHeader;
use Untek\Framework\Messenger\Infrastructure\Messenger\Symfony\Stamp\TopicStamp;
use Untek\FrameworkPlugin\MessengerKafkaTransport\Infrastructure\Messenger\Symfony\Stamp\ConsumeMessageStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class KafkaReceiver implements ReceiverInterface
{

    public function __construct(
        protected SerializerInterface $serializer,
        protected Consumer $consumer
    ) {
    }

    public function get(): iterable
    {
        $consumeMessage = $this->consumer->consume();
        if ($consumeMessage) {
            $envelope = $this->decode($consumeMessage);
            return [$envelope];
        }
        return [];
    }

    public function ack(Envelope $envelope): void
    {
        /** @var ConsumeMessageStamp $consumeMessageStamp */
        $consumeMessageStamp = $envelope->last(ConsumeMessageStamp::class);
        $this->consumer->ack($consumeMessageStamp->getConsumeMessage());
    }

    public function reject(Envelope $envelope): void
    {
        $this->ack($envelope);
    }

    protected function decode(ConsumeMessage $consumeMessage): Envelope
    {
        $encodedEnvelope = $this->forgeEncodedEnvelope($consumeMessage);
        $envelope = $this->serializer->decode($encodedEnvelope);
        $envelope = $this->addStampsFromKafkaMessage($envelope, $consumeMessage);
        return $envelope;
    }

    protected function forgeEncodedEnvelope(
        ConsumeMessage $consumeMessage
    ): array {
        $headers = $this->recordHeadersToArray($consumeMessage->getHeaders());
        return [
            'body' => $consumeMessage->getValue(),
            'headers' => $headers,
        ];
    }

    protected function addStampsFromKafkaMessage(Envelope $envelope, ConsumeMessage $consumeMessage): Envelope
    {
        $envelope = $envelope->with(new TransportMessageIdStamp($consumeMessage->getKey()));
        $envelope = $envelope->with(new TopicStamp($consumeMessage->getTopic()));
        $envelope = $envelope->with(new ConsumeMessageStamp($consumeMessage));
        return $envelope;
    }

    /**
     * @param array | RecordHeader[] $recordHeaders
     * @return array
     */
    protected function recordHeadersToArray(array $recordHeaders): array
    {
        $headers = [];
        foreach ($recordHeaders as $recordHeader) {
            $headerKey = $recordHeader->getHeaderKey();
            if (strpos($headerKey, 'X-Message') !== 0) {
                $headers[$headerKey] = $recordHeader->getValue();
            }
        }
        return $headers;
    }
}
