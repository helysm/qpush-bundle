<?php

/**
 * Copyright 2014 Underground Elephant
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @package     qpush-bundle
 * @copyright   Underground Elephant 2014
 * @license     Apache License, Version 2.0
 */

namespace Uecode\Bundle\QPushBundle\Provider;

use Aws\Sns\Exception\SnsException;
use Aws\Sns\SnsClient;
use Aws\Sqs\Exception\SqsException;
use Aws\Sqs\SqsClient;

use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Uecode\Bundle\QPushBundle\Event\Events;
use Uecode\Bundle\QPushBundle\Event\MessageEvent;
use Uecode\Bundle\QPushBundle\Event\NotificationEvent;
use Uecode\Bundle\QPushBundle\Message\Message;

/**
 * @author Keith Kirk <kkirk@undergroundelephant.com>
 */
class AwsProvider extends AbstractProvider
{
    /**
     * Aws SQS Client
     *
     * @var SqsClient
     */
    private $sqs;

    /**
     * Aws SNS Client
     *
     * @var SnsClient
     */
    private $sns;

    /**
     * SQS Queue URL
     *
     * @var string
     */
    private $queueUrl;

    /**
     * SNS Topic ARN
     *
     * @var string
     */
    private $topicArn;

    /**
     * @param string $name
     * @param array  $options
     * @param mixed  $client
     */
    public function __construct($name, array $options, $client)
    {
        $this->name     = $name;
        $this->options  = $options;

        // get() method used for sdk v2, create methods for v3
        $useGet = method_exists($client, 'get');
        $this->sqs = $useGet ? $client->get('Sqs') : $client->createSqs();
        $this->sns = $useGet ? $client->get('Sns') : $client->createSns();
    }

    /**
     * @return string
     */
    public function getProvider()
    {
        return 'AWS';
    }

    /**
     * Builds the configured queues
     *
     * If a Queue name is passed and configured, this method will build only that
     * Queue.
     *
     * All Create methods are idempotent, if the resource exists, the current ARN
     * will be returned
     *
     * @return bool
     */
    public function create()
    {
        $this->createQueue();

        if ($this->options['push_notifications']) {
            // Create the SNS Topic
            $this->createTopic();

            // Add the SQS Queue as a Subscriber to the SNS Topic
            $this->subscribeToTopic(
                $this->topicArn,
                'sqs',
                $this->sqs->getQueueArn($this->queueUrl)
            );

            // Add configured Subscribers to the SNS Topic
            foreach ($this->options['subscribers'] as $subscriber) {
                $this->subscribeToTopic(
                    $this->topicArn,
                    $subscriber['protocol'],
                    $subscriber['endpoint']
                );
            }
        }

        return true;
    }

    /**
     * @return Boolean
     */
    public function destroy()
    {
        if ($this->queueExists()) {
            // Delete the SQS Queue
            $this->sqs->deleteQueue([
                'QueueUrl' => $this->queueUrl
            ]);
        }

        if ($this->topicExists() || !empty($this->queueUrl)) {
            // Delete the SNS Topic
            $topicArn = !empty($this->topicArn)
                ? $this->topicArn
                : str_replace('sqs', 'sns', $this->queueUrl)
            ;

            $this->sns->deleteTopic([
                'TopicArn' => $topicArn
            ]);
        }

        return true;
    }

    /**
     * {@inheritDoc}
     *
     * This method will either use a SNS Topic to publish a queued message or
     * straight to SQS depending on the application configuration.
     *
     * @return string
     */
    public function publish(array $message, array $options = [])
    {
        $mergedOptions = $this->mergeOptions($options);

        if (isset($options['message_deduplication_id'])) {
            $mergedOptions['message_deduplication_id'] = $options['message_deduplication_id'];
        }

        if (isset($options['message_group_id'])) {
            $mergedOptions['message_group_id'] = $options['message_group_id'];
        }

        $options = $mergedOptions;

        $publishStart = microtime(true);

        // ensures that the SQS Queue and SNS Topic exist
        if (!$this->queueExists()) {
            $this->create();
        }

        if ($options['push_notifications']) {

            if (!$this->topicExists()) {
                $this->create();
            }

            $message    = [
                'default' => $this->getNameWithPrefix(),
                'sqs'     => json_encode($message),
                'http'    => $this->getNameWithPrefix(),
                'https'   => $this->getNameWithPrefix(),
            ];

            $result = $this->sns->publish([
                'TopicArn'         => $this->topicArn,
                'Subject'          => $this->getName(),
                'Message'          => json_encode($message),
                'MessageStructure' => 'json'
            ]);

            return $result->get('MessageId');
        }

        $arguments = [
            'QueueUrl'      => $this->queueUrl,
            'MessageBody'   => json_encode($message),
            'DelaySeconds'  => $options['message_delay']
        ];

        if ($this->isQueueFIFO()) {
            if (isset($options['message_deduplication_id'])) {
                // Always use user supplied dedup id
                $arguments['MessageDeduplicationId'] = $options['message_deduplication_id'];
            } elseif ($options['content_based_deduplication'] !== true) {
                // If none is supplied and option "content_based_deduplication" is not set, generate default
                $arguments['MessageDeduplicationId'] = hash('sha256', json_encode($message));
            }

            $arguments['MessageGroupId'] = $this->getNameWithPrefix();
            if (isset($options['message_group_id'])) {
                $arguments['MessageGroupId'] = $options['message_group_id'];
            }
        }

        $result = $this->sqs->sendMessage($arguments);

        if ($this->isQueueFIFO()) {
            if (isset($arguments['MessageDeduplicationId'])) {
                $context['message_deduplication_id'] = $arguments['MessageDeduplicationId'];
            }
            $context['message_group_id'] = $arguments['MessageGroupId'];
        }

        return $result->get('MessageId');
    }

    /**
     * {@inheritDoc}
     */
    public function receive(array $options = [])
    {
        $options = $this->mergeOptions($options);

        if (!$this->queueExists()) {
            $this->create();
        }

        $result = $this->sqs->receiveMessage([
            'QueueUrl'              => $this->queueUrl,
            'MaxNumberOfMessages'   => $options['messages_to_receive'],
            'WaitTimeSeconds'       => $options['receive_wait_time']
        ]);

        $messages = $result->get('Messages') ?: [];

        // Convert to Message Class
        foreach ($messages as &$message) {
            $id = $message['MessageId'];
            $metadata = [
                'ReceiptHandle' => $message['ReceiptHandle'],
                'MD5OfBody'     => $message['MD5OfBody']
            ];

            // When using SNS, the SQS Body is the entire SNS Message
            if(is_array($body = json_decode($message['Body'], true))
                && isset($body['Message'])
            ) {
                $body = json_decode($body['Message'], true);
            }

            $message = new Message($id, $body, $metadata);
        }

        return $messages;
    }

    /**
     * {@inheritDoc}
     *
     * @return bool
     */
    public function delete($id)
    {
        if (!$this->queueExists()) {
            return false;
        }

        $this->sqs->deleteMessage([
            'QueueUrl'      => $this->queueUrl,
            'ReceiptHandle' => $id
        ]);

        return true;
    }

    /**
     * Return the Queue Url
     *
     * This method relies on in-memory cache to reduce the need to needlessly
     * call the create method on an existing Queue.
     *
     * @return boolean
     */
    public function queueExists()
    {
        if (isset($this->queueUrl)) {
            return true;
        }

        try {
            $result = $this->sqs->getQueueUrl([
                'QueueName' => $this->getNameWithPrefix()
            ]);

            $this->queueUrl = $result->get('QueueUrl');
            if ($this->queueUrl !== null) {

                return true;
            }
        } catch (SqsException $e) {}

        return false;
    }

    /**
     * Checks to see if a Topic exists
     *
     * This method relies on in-memory cache to reduce the need to needlessly
     * call the create method on an existing Topic.
     *
     * @return boolean
     */
    public function topicExists()
    {
        if (isset($this->topicArn)) {
            return true;
        }

        if (!empty($this->queueUrl)) {
            $queueArn = $this->sqs->getQueueArn($this->queueUrl);
            $topicArn = str_replace('sqs', 'sns', $queueArn);

            try {
                $this->sns->getTopicAttributes([
                    'TopicArn' => $topicArn
                ]);
            } catch (SnsException $e) {
                return false;
            }

            $this->topicArn = $topicArn;

            return true;
        }

        return false;
    }


    /**
     * Creates an SQS Queue and returns the Queue Url
     *
     * The create method for SQS Queues is idempotent - if the queue already
     * exists, this method will return the Queue Url of the existing Queue.
     */
    public function createQueue()
    {
        $attributes = [
            'VisibilityTimeout'             => $this->options['message_timeout'],
            'MessageRetentionPeriod'        => $this->options['message_expiration'],
            'ReceiveMessageWaitTimeSeconds' => $this->options['receive_wait_time']
        ];

        if ($this->isQueueFIFO()) {
            $attributes['FifoQueue'] = 'true';
            $attributes['ContentBasedDeduplication'] = $this->options['content_based_deduplication'] === true
                ? 'true'
                : 'false';
        }

        $result = $this->sqs->createQueue(['QueueName' => $this->getNameWithPrefix(), 'Attributes' => $attributes]);

        $this->queueUrl = $result->get('QueueUrl');

        if ($this->options['push_notifications']) {

            $policy = $this->createSqsPolicy();

            $this->sqs->setQueueAttributes([
                'QueueUrl'      => $this->queueUrl,
                'Attributes'    => [
                    'Policy'    => $policy,
                ]
            ]);
        }
    }

    /**
     * Creates a Policy for SQS that's required to allow SNS SendMessage access
     *
     * @return string
     */
    public function createSqsPolicy()
    {
        $arn = $this->sqs->getQueueArn($this->queueUrl);

        return json_encode([
            'Version'   => '2008-10-17',
            'Id'        =>  sprintf('%s/SQSDefaultPolicy', $arn),
            'Statement' => [
                [
                    'Sid'       => 'SNSPermissions',
                    'Effect'    => 'Allow',
                    'Principal' => ['AWS' => '*'],
                    'Action'    => 'SQS:SendMessage',
                    'Resource'  => $arn
                ]
            ]
        ]);
    }

    /**
     * Creates a SNS Topic and returns the ARN
     *
     * The create method for the SNS Topics is idempotent - if the topic already
     * exists, this method will return the Topic ARN of the existing Topic.
     *
     *
     * @return bool
     */
    public function createTopic()
    {
        if (!$this->options['push_notifications']) {
            return false;
        }

        $name = str_replace('.', '-', $this->getNameWithPrefix());
        $result = $this->sns->createTopic([
            'Name' => $name
        ]);

        $this->topicArn = $result->get('TopicArn');

        return true;
    }

    /**
     * Get a list of Subscriptions for the specified SNS Topic
     *
     * @param string $topicArn The SNS Topic Arn
     *
     * @return array
     */
    public function getTopicSubscriptions($topicArn)
    {
        $result = $this->sns->listSubscriptionsByTopic([
            'TopicArn' => $topicArn
        ]);

        return $result->get('Subscriptions');
    }

    /**
     * Subscribes an endpoint to a SNS Topic
     *
     * @param string $topicArn The ARN of the Topic
     * @param string $protocol The protocol of the Endpoint
     * @param string $endpoint The Endpoint of the Subscriber
     *
     * @return string
     */
    public function subscribeToTopic($topicArn, $protocol, $endpoint)
    {
        // Check against the current Topic Subscriptions
        $subscriptions = $this->getTopicSubscriptions($topicArn);
        foreach ($subscriptions as $subscription) {
            if ($endpoint === $subscription['Endpoint']) {
                return $subscription['SubscriptionArn'];
            }
        }

        $result = $this->sns->subscribe([
            'TopicArn' => $topicArn,
            'Protocol' => $protocol,
            'Endpoint' => $endpoint
        ]);

        $arn = $result->get('SubscriptionArn');

        return $arn;
    }

    /**
     * Unsubscribes an endpoint from a SNS Topic
     *
     * The method will return TRUE on success, or FALSE if the Endpoint did not
     * have a Subscription on the SNS Topic
     *
     * @param string $topicArn The ARN of the Topic
     * @param string $protocol The protocol of the Endpoint
     * @param string $endpoint The Endpoint of the Subscriber
     *
     * @return Boolean
     */
    public function unsubscribeFromTopic($topicArn, $protocol, $endpoint)
    {
        // Check against the current Topic Subscriptions
        $subscriptions = $this->getTopicSubscriptions($topicArn);
        foreach ($subscriptions as $subscription) {
            if ($endpoint === $subscription['Endpoint']) {
                $this->sns->unsubscribe([
                    'SubscriptionArn' => $subscription['SubscriptionArn']
                ]);

                return true;
            }
        }

        return false;
    }

    /**
     * Handles SNS Notifications
     *
     * For Subscription notifications, this method will automatically confirm
     * the Subscription request
     *
     * For Message notifications, this method polls the queue and dispatches
     * the `{queue}.message_received` event for each message retrieved
     *
     * @param NotificationEvent $event The Notification Event
     * @param string $eventName Name of the event
     * @param EventDispatcherInterface $dispatcher
     *
     * @return void
     */
    public function onNotification(NotificationEvent $event, $eventName, EventDispatcherInterface $dispatcher)
    {
        if (NotificationEvent::TYPE_SUBSCRIPTION == $event->getType()) {
            $topicArn   = $event->getNotification()->getMetadata()->get('TopicArn');
            $token      = $event->getNotification()->getMetadata()->get('Token');

            $this->sns->confirmSubscription([
                'TopicArn'  => $topicArn,
                'Token'     => $token
            ]);

            return;
        }

        $messages = $this->receive();
        foreach ($messages as $message) {
            $messageEvent = new MessageEvent($this->name, $message);
            $dispatcher->dispatch($messageEvent, Events::Message($this->name));
        }
    }

    /**
     * Removes the message from queue after all other listeners have fired
     *
     * If an earlier listener has erred or stopped propagation, this method
     * will not fire and the Queued Message should become visible in queue again.
     *
     * Stops Event Propagation after removing the Message
     *
     * @param MessageEvent $event The SQS Message Event
     *
     * @return void
     */
    public function onMessageReceived(MessageEvent $event)
    {
        $receiptHandle = $event
            ->getMessage()
            ->getMetadata()
            ->get('ReceiptHandle');

        $this->delete($receiptHandle);

        $event->stopPropagation();
    }

    /**
     * @return bool
     */
    private function isQueueFIFO()
    {
        return $this->options['fifo'] === true;
    }
}
