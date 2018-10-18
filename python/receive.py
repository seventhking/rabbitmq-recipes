#!/usr/bin/env python
import pika
import time
import sys

def callback1(ch, method, properties, body):
    print(" [x] Received %r" %body)
    # time.sleep(3)

def case1():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')
    channel.basic_consume(callback1,
                          queue='hello',
                          no_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


def callback2(ch, method, properties, body):
    print(" [x] Received %r" %body)
    # time.sleep(body.count(b'.'))
    time.sleep(3)
    print(" [x] Done")
    ch.basic_ack(delivery_tag = method.delivery_tag)

def case2():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='world',
                          durable = True)
    channel.basic_qos(prefetch_count=5)
    channel.basic_consume(callback2,
                          queue='world',)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

def callback3(ch, method, properties, body):
    print(" [x] %r" % body)

def case3():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='logs',
                             exchange_type='fanout')

    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='logs',
                       queue=queue_name)

    print(' [*] Waiting for logs. To exit press CTRL+C')

    channel.basic_consume(callback3,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()

def callback4(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))

def case4():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='direct_logs',
                             exchange_type='direct')

    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    severities = sys.argv[1:]
    if not severities:
        sys.stderr.write("Usage: %s [info] [warning] [error]\n" % sys.argv[0])
        sys.exit(1)

    for severity in severities:
        channel.queue_bind(exchange='direct_logs',
                           queue=queue_name,
                           routing_key=severity)

    print(' [*] Waiting for logs. To exit press CTRL+C')

    channel.basic_consume(callback4,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()

def callback5(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))

def case5():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='topic_logs',
                             exchange_type='topic')

    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    binding_keys = sys.argv[1:]
    if not binding_keys:
        sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
        sys.exit(1)

    for binding_key in binding_keys:
        channel.queue_bind(exchange='topic_logs',
                           queue=queue_name,
                           routing_key=binding_key)

    print(' [*] Waiting for logs. To exit press CTRL+C')

    channel.basic_consume(callback5,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()

case5()
