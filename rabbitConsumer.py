import functools
import logging
import time
import json
import pika
from handleMessages import HandleMessages

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class RabbitConsumer:

    def __init__(self, _parameters, _id_consumer, _exchange, _queue, _routing_key, _exchange_type, _producer_to_dlx):
        self._parameters = _parameters

        self._id_consumer = _id_consumer
        self._exchange = _exchange
        self._queue = _queue
        self._routing_key = _routing_key
        self._exchange_type = _exchange_type
        self._producer_to_dlx = _producer_to_dlx
        self._handle_messages = HandleMessages(_id_consumer)

        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False

        self._prefetch_count = 125

    def connect(self):
        LOGGER.info('Connecting to %s', self._parameters)
        return pika.SelectConnection(
            parameters=self._parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        LOGGER.info('Connection opened')
        print(f' Consumer{self._id_consumer} is waiting for messages. To exit press CTRL+C')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        LOGGER.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self._exchange)

    def add_on_channel_close_callback(self):
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()

    def setup_exchange(self, exchange_name):
        LOGGER.info('Declaring exchange: %s', exchange_name)
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self._exchange_type,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue(self._queue)

    def setup_queue(self, queue_name):
        LOGGER.info('Declaring queue %s', queue_name)
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(queue=queue_name, callback=cb,
                                    arguments={
                                        'x-dead-letter-exchange': self._producer_to_dlx.get_dlx_exchange()
                                    })

    def on_queue_declareok(self, _unused_frame, userdata):
        queue_name = userdata
        LOGGER.info('Binding %s to %s with %s', self._exchange, queue_name,
                    self._routing_key)
        cb = functools.partial(self.on_bindok, userdata=queue_name)
        self._channel.queue_bind(
            queue_name,
            self._exchange,
            routing_key=self._routing_key,
            callback=cb)

    def on_bindok(self, _unused_frame, userdata):
        LOGGER.info('Queue bound: %s', userdata)
        self.set_qos()

    def set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        LOGGER.info('QOS set to: %d', self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self._queue, self.on_new_message)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_new_message(self, _unused_channel, method, properties, body):
        data = json.loads(body)
        handler_successfully = self._handle_messages.could_handle_msg(data)
        if handler_successfully:
            self._channel.basic_ack(method.delivery_tag)
            self._handle_messages.handler(data)
        else:
            # if the current msg was already requeue.
            if method.redelivered:
                # msg will be sent to dlx.
                print('dlx')
                self._channel.basic_nack(method.delivery_tag, requeue=False)
            else:
                # msg will be requeue.
                print('requeue')
                self._channel.basic_nack(method.delivery_tag, requeue=True)

    def stop_consuming(self):
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        self._consuming = False
        LOGGER.info(
            'RabbitMQ acknowledged the cancellation of the consumer: %s',
            userdata)
        self.close_channel()

    def close_channel(self):
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        if not self._closing:
            self._closing = True
            LOGGER.info('Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOGGER.info('Stopped')


class RabbitReconnectingConsumer:

    def __init__(self, _id_consumer, _exchange, _queue, _routing_key, _exchange_type, _producer_to_dlx):
        self._reconnect_delay = 0
        self._id_consumer = _id_consumer
        self._exchange = _exchange
        self._queue = _queue
        self._routing_key = _routing_key
        self._exchange_type = _exchange_type
        self._producer_to_dlx = _producer_to_dlx

        user = 'guest'
        password = 'guest'
        credentials = pika.PlainCredentials(user, password)
        self._parameters = pika.ConnectionParameters('192.168.202.128',
                                                     5672,
                                                     '/',
                                                     credentials,
                                                     heartbeat=60)
        self._consumer = RabbitConsumer(self._parameters, self._id_consumer, self._exchange, self._queue,
                                        self._routing_key, self._exchange_type, self._producer_to_dlx)

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = RabbitConsumer(self._parameters, self._id_consumer, self._exchange, self._queue,
                                            self._routing_key, self._exchange_type, self._producer_to_dlx)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
