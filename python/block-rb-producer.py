#!/usr/bin/env python
import pika
import time
from BlockRabbitMqApi import BlockRabbitMqApi

def callback(channel, method, properties, body):
    print("receive %s" % body)
    print("-------------------------------")
    print("method.consumer_tag: %s" % method.consumer_tag)
    print("method.delivery_tag: %s" % method.delivery_tag)
    print("method.redelivered: %s" % method.redelivered)
    print("method.exchange: %s" % method.exchange)
    print("method.routing_key: %s" % method.routing_key)
    print("-------------------------------")
    print("properties.content_type :%s" % properties.content_type)
    print("properties.content_encoding :%s" % properties.content_encoding)
    print("properties.headers :%s" % properties.headers)
    print("properties.delivery_mode :%s" % properties.delivery_mode)
    print("properties.priority :%s" % properties.priority)
    print("properties.correlation_id :%s" % properties.correlation_id)
    print("properties.reply_to :%s" % properties.reply_to)
    print("properties.expiration :%s" % properties.expiration)
    print("properties.message_id :%s" % properties.message_id)
    print("properties.timestamp :%s" % properties.timestamp)
    print("properties.type :%s" % properties.type)
    print("properties.user_id :%s" % properties.user_id)
    print("properties.app_id :%s" % properties.app_id)
    print("properties.cluster_id :%s" % properties.cluster_id)


    channel.basic_ack(delivery_tag = method.delivery_tag)
    # channel.stop_consuming()


def main():
    exchangeName = "exchange-wdq-1"
    queueName = "queue-wdq-1"
    routingKey = "routingkey-wdq-1"
    blockRabbitMqApi = BlockRabbitMqApi()
    blockRabbitMqApi.connect("seventh",
                          "qq123123",
                          "172.16.20.23",
                          5672,
                          "%2F",
                          {"connection_attempts" : 3,
                           "heartbeat_interval" : 3600})
    blockRabbitMqApi.newChannel()
    blockRabbitMqApi.declareExchange(exchangeName)
    blockRabbitMqApi.declareQueue(queueName,
                               {"durable" : False,
                                "exclusive" : False,
                                "auto_delete" : False})
    blockRabbitMqApi.queueBind(queueName,
                            exchangeName,
                            routingKey)
    # result = blockRabbitMqApi.declareQueue("",
    #                                     {"exclusive" : True})
    # callbackQueue = result.method.queue
    blockRabbitMqApi.confirmDelivery()
    i = 0
    while True:
        i += 1
        success = blockRabbitMqApi.publishMsg(exchangeName,
                                              queueName,
                                              "Good Morning %s" % i ,
                                              {"mandatory" : True,
                                               "immediate" : False,
                                               "properties" : pika.BasicProperties(reply_to = None)})
        if success:
            print("[SUCCESS] Good Morning %s" % i )
        else:
            print("[FAIL] Good Morning %s" % i )
        time.sleep(5)
    # blockRabbitMqApi.consumeMsg(callback,
    #                          callbackQueue,
    #                          {"exclusive" : True})
    # blockRabbitMqApi.startConsuming()


if __name__ == '__main__':
    main()
