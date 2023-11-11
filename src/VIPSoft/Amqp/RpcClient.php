<?php
/**
 * @copyright 2023 Anthon Pang
 * @license Apache-2.0
 */

namespace VIPSoft\Amqp;

// added in php-amqp 2.0.0
if ( ! defined('AMQP_DELIVERY_MODE_PERSISTENT')) {
    define('AMQP_DELIVERY_MODE_PERSISTENT', 2);
}

/**
 * AMQP RPC Client
 *
 * @see https://www.rabbitmq.com/tutorials/tutorial-six-php.html
 *
 * @author Anthon Pang <apang@softwaredevelopment.ca>
 */
class RpcClient
{
    /**
     * @var \AMQPExchange
     */
    private $exchange;

    /**
     * Service dependencies
     *
     * @param \AMQPExchange $exchange
     */
    public function __construct(\AMQPExchange $exchange)
    {
        $this->exchange = $exchange;
    }

    /**
     * Client RPC
     *
     * @param string $serviceName
     * @param array  $args
     * @param string $routingKey
     *
     * @return mixed|null
     */
    public function call($serviceName, $args, $routingKey = '#')
    {
        $correlationId = uniqid();

        // create temporary queue for the rpc response
        $queue = new \AMQPQueue($this->exchange->getChannel());
        $queue->setFlags(AMQP_EXCLUSIVE);
        $queue->declareQueue();

        $data = [
            'from'           => gethostname(),
            'service'        => $serviceName,
            'arguments'      => $args,
        ];

        $attributes = [
            'correlation_id' => $correlationId,
            'content_type'   => 'application/json',
            'delivery_mode'  => AMQP_DELIVERY_MODE_PERSISTENT,
            'message_id'     => uniqid(),
            'reply_to'       => $queue->getName(),
            'timestamp'      => time(),
        ];

        $this->exchange->publish(json_encode($data), $routingKey, AMQP_NOPARAM, $attributes);

        $response = null;

        $queue->consume(
            function (\AMQPEnvelope $message, \AMQPQueue $queue) use (&$response, $correlationId) {
                $deliveryTag = $message->getDeliveryTag();

                if ($message->getCorrelationId() === $correlationId) {
                    $queue->ack($deliveryTag);

                    $data = json_decode($message->getBody(), true);

                    if ($exception = $data['exception']){
                        throw new \Exception($exception);
                    }

                    $response = $data['response'];

                    return false;
                }

                // race condition where the rpc server dies after sending response but before acking message,
                // and on restart, the rpc server processes the request again
                $queue->nack($deliveryTag);
            },
            AMQP_AUTOACK
        );

        return $response;
    }
}
