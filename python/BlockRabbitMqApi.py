#!/usr/bin/env python
import pika

class BlockRabbitMqApi(object):
    __connection = None
    __channel = None

    def __init__(self):
        pass

    def connect(self, username, password, host, port, vhost, connectionProperty = {}):
        urlArgs = ""
        if connectionProperty.has_key("connection_attempts"):
            urlArgs += "&connection_attempts=%s" % connectionProperty.get("connection_attempts")

        if connectionProperty.has_key("heartbeat_interval"):
            urlArgs += "&heartbeat_interval=%s" % connectionProperty.get("heartbeat_interval")

        if urlArgs != "":
            urlArgs = "?" + urlArgs

        url = "amqp://%s:%s@%s:%s/%s%s" % (username, password, host, port, vhost, urlArgs)
        self.__connection = pika.BlockingConnection(pika.URLParameters(url))

    def newChannel(self):
        self.__channel = self.__connection.channel()

    def declareExchange(self,
                        exchangeName,
                        exchangeType = "direct",
                        passive = False,
                        durable = False,
                        autoDelete = False,
                        internal = False,
                        arguments = None):
        return self.__channel.exchange_declare(exchange = exchangeName,
                                               exchange_type = exchangeType,
                                               passive = passive,
                                               durable = durable,
                                               auto_delete = autoDelete,
                                               internal = internal,
                                               arguments = arguments)


    def declareQueue(self, queueName, queueProperty = {}):
        return self.__channel.queue_declare(queue = queueName,
                                            durable = queueProperty.get("durable", False),
                                            exclusive = queueProperty.get("exclusive", False),
                                            auto_delete = queueProperty.get("auto_delete", False))

    def closeChannel(self):
        self.__channel.close()

    def closeConnection(self):
        self.__connection.close()

    def consumeMsg(self, consumeCallback, queueName, otherProperty = {}):
        return self.__channel.basic_consume(consumeCallback,
                                            queue = queueName,
                                            no_ack = otherProperty.get("no_ack", False),
                                            exclusive = otherProperty.get("exclusive", False),
                                            consumer_tag = otherProperty.get("consumer_tag", None),
                                            arguments = otherProperty.get("arguments", None),)

    def startConsuming(self):
        self.__channel.start_consuming()

    def publishMsg(self, exchange, routingKey, body, otherProperty = {}):
        return self.__channel.basic_publish(exchange,
                                            routingKey,
                                            body,
                                            properties=otherProperty.get("properties", None),
                                            mandatory=otherProperty.get("mandatory", False),
                                            immediate=otherProperty.get("immediate", False))

    def exchangeBind(self, destination, source, routingKey, arguments = None):
        return self.__channel.exchange_bind(destination, source, routingKey, arguments)

    def queueBind(self, queue, exchange, routingKey, arguments = None):
        return self.__channel.queue_bind(queue, exchange, routingKey, arguments)

    def confirmDelivery(self):
        return self.__channel.confirm_delivery()
