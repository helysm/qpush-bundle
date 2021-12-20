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

namespace Uecode\Bundle\QPushBundle\DependencyInjection;

use Symfony\Component\HttpKernel\DependencyInjection\Extension;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\YamlFileLoader;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\DependencyInjection\Exception\InvalidArgumentException;
use Symfony\Component\DependencyInjection\Exception\ServiceNotFoundException;
use RuntimeException;
use Exception;

/**
 * @author Keith Kirk <kkirk@undergroundelephant.com>
 */
class UecodeQPushExtension extends Extension
{
    /**
     * @param array            $configs
     * @param ContainerBuilder $container
     *
     * @throws RuntimeException|InvalidArgumentException|ServiceNotFoundException
     */
    public function load(array $configs, ContainerBuilder $container)
    {
        $configuration = new Configuration();
        $config = $this->processConfiguration($configuration, $configs);

        $loader = new YamlFileLoader(
            $container,
            new FileLocator(__DIR__.'/../Resources/config')
        );

        $loader->load('parameters.yml');
        $loader->load('services.yml');

        $registry = $container->getDefinition('uecode_qpush.registry');

        foreach ($config['queues'] as $queue => $values) {

            $provider   = $values['provider'];
            $class      = null;
            $client     = null;

            switch ($config['providers'][$provider]['driver']) {
                case 'aws':
                    $class  = $container->getParameter('uecode_qpush.provider.aws');
                    $client = $this->createAwsClient(
                        $config['providers'][$provider],
                        $container,
                        $provider
                    );
                    break;
                case 'sync':
                    $class  = $container->getParameter('uecode_qpush.provider.sync');
                    $client = $this->createSyncClient();
                    break;
            }

            $definition = new Definition(
                $class, [$queue, $values['options'], $client]
            );

            $definition->setPublic(true);

            $isProviderAWS = $config['providers'][$provider]['driver'] === 'aws';
            $isQueueNameSet = isset($values['options']['queue_name']) && !empty($values['options']['queue_name']);

            if ($isQueueNameSet && $isProviderAWS) {
                $definition->addTag(
                    'uecode_qpush.event_listener',
                    [
                        'event' => "{$values['options']['queue_name']}.on_notification",
                        'method' => "onNotification",
                        'priority' => 255
                    ]
                );

                // Check queue name ends with ".fifo"
                $isQueueNameFIFOReady = preg_match("/$(?<=(\.fifo))/", $values['options']['queue_name']) === 1;
                if ($values['options']['fifo'] === true && !$isQueueNameFIFOReady) {
                    throw new InvalidArgumentException('Queue name must end with ".fifo" on AWS FIFO queues');
                }
            }

            $name = sprintf('uecode_qpush.%s', $queue);
            $container->setDefinition($name, $definition);

            $registry->addMethodCall('addProvider', [$queue, new Reference($name)]);
        }
    }

    /**
     * Creates a definition for the AWS provider
     *
     * @param array            $config    A Configuration array for the client
     * @param ContainerBuilder $container The container
     * @param string           $name      The provider key
     *
     * @throws RuntimeException
     *
     * @return Reference
     */
    private function createAwsClient($config, ContainerBuilder $container, $name)
    {
        $service = sprintf('uecode_qpush.provider.%s', $name);

        if (!$container->hasDefinition($service)) {

            $aws2 = class_exists('Aws\Common\Aws');
            $aws3 = class_exists('Aws\Sdk');
            if (!$aws2 && !$aws3) {
                throw new RuntimeException('You must require "aws/aws-sdk-php" to use the AWS provider.');
            }

            $awsConfig = [
                'region' => $config['region']
            ];

            $aws = new Definition('Aws\Common\Aws');
            $aws->setFactory(['Aws\Common\Aws', 'factory']);
            $aws->setArguments([$awsConfig]);

            if ($aws2) {
                $aws = new Definition('Aws\Common\Aws');
                $aws->setFactory(['Aws\Common\Aws', 'factory']);

                if (!empty($config['key']) && !empty($config['secret'])) {
                    $awsConfig['key']    = $config['key'];
                    $awsConfig['secret'] = $config['secret'];
                }

            } else {
                $aws = new Definition('Aws\Sdk');

                if (!empty($config['key']) && !empty($config['secret'])) {
                    $awsConfig['credentials'] = [
                        'key'    => $config['key'],
                        'secret' => $config['secret']
                    ];
                }
                $awsConfig['version']  = 'latest';
            }

            $aws->setArguments([$awsConfig]);

            $container->setDefinition($service, $aws)->setPublic(false);
        }

        return new Reference($service);
    }

    /**
     * @return Reference
     */
    private function createSyncClient()
    {
        return new Reference('event_dispatcher');
    }
    
    /**
     * Returns the Extension Alias
     *
     * @return string
     */
    public function getAlias()
    {
        return 'uecode_qpush';
    }
}
