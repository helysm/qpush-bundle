<?php

namespace Uecode\Bundle\QPushBundle\Tests\Provider;


use PHPUnit\Framework\Constraint\IsAnything;
use PHPUnit\Framework\Constraint\IsInstanceOf;
use PHPUnit\Framework\TestCase;
use Uecode\Bundle\QPushBundle\Event\Events;
use Uecode\Bundle\QPushBundle\Provider\SyncProvider;

class SyncProviderTest extends TestCase
{
    /**
     * @var \Uecode\Bundle\QPushBundle\Provider\SyncProvider
     */
    protected $provider;

    /**
     * @var \Symfony\Component\EventDispatcher\EventDispatcherInterface
     */
    protected $dispatcher;


    public function setUp(): void
    {
        $this->dispatcher = $this->createMock(
            'Symfony\Component\EventDispatcher\EventDispatcherInterface'
        );

        $this->provider = $this->getSyncProvider();
    }

    public function testGetProvider()
    {
        $provider = $this->provider->getProvider();

        $this->assertEquals('Sync', $provider);
    }

    public function testPublish()
    {
        $this->dispatcher
            ->expects($this->once())
            ->method('dispatch')
            ->with(
                Events::Message($this->provider->getName()),
                new IsInstanceOf('Uecode\Bundle\QPushBundle\Event\MessageEvent')
            );

        $this->provider->publish(['foo' => 'bar']);
    }

    public function testCreate()
    {
        $this->setNoOpExpectation();

        $this->provider->create();
    }

    public function testDestroy()
    {
        $this->setNoOpExpectation();

        $this->provider->destroy();
    }

    public function testDelete()
    {
        $this->setNoOpExpectation();

        $this->provider->delete('foo');
    }

    public function testReceive()
    {
        $this->setNoOpExpectation();

        $this->provider->receive();
    }


    protected function getSyncProvider()
    {
        $options = [
            'logging_enabled'       => false,
            'push_notifications'    => true,
            'notification_retries'  => 3,
            'message_delay'         => 0,
            'message_timeout'       => 30,
            'message_expiration'    => 604800,
            'messages_to_receive'   => 1,
            'receive_wait_time'     => 3,
            'subscribers'           => [
                [ 'protocol' => 'http', 'endpoint' => 'http://fake.com' ]
            ]
        ];

        return new SyncProvider('test', $options, $this->dispatcher);
    }

    protected function setNoOpExpectation()
    {
        $this->dispatcher
            ->expects($this->never())
            ->method(new IsAnything());
    }
}