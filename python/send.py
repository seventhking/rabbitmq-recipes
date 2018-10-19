#!/usr/bin/env python
import pika
import sys
import time

def case1():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')
    channel.basic_publish(exchange='',
                          routing_key='hello',
                          body='Hello World!')
    print(" [x] Sent 'Hello World!'")

    connection.close()

def case2(i):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    message = ' '.join(sys.argv[1:]) or "Hello World!" + "---" + str(i)
    channel.queue_declare(queue='world',
                          durable = True)
    channel.basic_publish(exchange='',
                          routing_key='world',
                          body=message,
                          properties = pika.BasicProperties(
                              delivery_mode = 2, # make message persistent
                          ))
    print(" [x] Sent %r" % message)

    connection.close()

def case3(i):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='logs',
                             exchange_type='fanout')

    message = ' '.join(sys.argv[1:]) or "info: Hello World!"
    channel.basic_publish(exchange='logs',
                          routing_key='',
                          body=message)
    print(" [x] Sent %r" % message)
    connection.close()


def case4(i):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='direct_logs',
                             exchange_type='direct')

    severity = sys.argv[1] if len(sys.argv) > 2 else 'info'
    message = ' '.join(sys.argv[2:]) or 'Hello World!'
    channel.basic_publish(exchange='direct_logs',
                          routing_key=severity,
                          body=message)
    print(" [x] Sent %r:%r" % (severity, message))
    connection.close()

def case5(i):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='topic_logs',
                             exchange_type='topic')

    routing_key = sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
    message = ' '.join(sys.argv[2:]) or 'Hello World!'
    channel.basic_publish(exchange='topic_logs',
                          routing_key=routing_key,
                          body=message)
    print(" [x] Sent %r:%r" % (routing_key, message))
    connection.close()

def main5():
    i = 0
    while True:
        i = i + 1
        case5(i)
        time.sleep(1)

def case6():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.confirm_delivery()
    channel.exchange_declare(exchange='test_immediate',
                             exchange_type='direct')
    channel.basic_publish(exchange = 'test_immediate',
                          routing_key = "test",
                          body = "hello",
                          properties = pika.BasicProperties(
                              delivery_mode = 2, # make message persistent
                          ),
                          mandatory = False,
                          immediate = True)
    pika.SelectConnection
    pika.BasicProperties
