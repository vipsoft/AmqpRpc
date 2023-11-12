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
 * AMQP RPC Server
 *
 * @see https://www.rabbitmq.com/tutorials/tutorial-six-php.html
 *
 * @author Anthon Pang <apang@softwaredevelopment.ca>
 */
class RpcServer
{
    /**
     * @var \AMQPQueue
     */
    private $queue;

    /**
     * Service dependencies
     *
     * @param \AMQPQueue $queue
     */
    public function __construct(\AMQPQueue $queue)
    {
        $this->queue = $queue;
    }

    /**
     * Set read timeout
     *
     * @param double $readTimeout Read timeout (in seconds)
     */
    public function setReadTimeout($readTimeout)
    {
        $connection = $this->queue->getConnection();
        $connection->setReadTimeout($readTimeout);
    }

    /**
     * Server RPC
     *
     * @param callable $dispatcher
     */
    public function answer($dispatcher)
    {
        // create nameless exchange to send back the response
        $channel    = $this->queue->getChannel();
        $exchange   = new \AMQPExchange($channel);

        try {
            $this->queue->consume(
                function (\AMQPEnvelope $message, \AMQPQueue $queue) use ($exchange, $dispatcher) {
                    $deliveryTag   = $message->getDeliveryTag();
                    $correlationId = $message->getCorrelationId();
                    $replyTo       = $message->getReplyTo();
                    $data          = json_decode($message->getBody(), true);
                    $from          = $data['from'];
                    $service       = $data['service'];
                    $arguments     = $data['arguments'];

                    try {
                        $exception = null;
                        $response  = null;
                        $response  = call_user_func($dispatcher, $from, $service, $arguments);
                    } catch (\Exception $e) {
                        $exception = $e->getMessage();
                    }

                    $data = [
                        'from'           => gethostname(),
                        'response'       => $response,
                        'exception'      => $exception,
                    ];

                    $attributes = [
                        'correlation_id' => $correlationId,
                        'content_type'   => 'application/json',
                        'delivery_mode'  => AMQP_DELIVERY_MODE_PERSISTENT,
                        'message_id'     => uniqid(),
                        'timestamp'      => time(),
                    ];

                    $exchange->publish(json_encode($data), $replyTo, AMQP_NOPARAM, $attributes);

                    $queue->ack($deliveryTag);
                },
                AMQP_NOPARAM
            );
        } catch (\Exception $e) {
            // read timeout
        }
    }
}
