import functools
import logging
import time
import pika

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class Dlx:

    def __init__(self, _parameters):
        self._parameters = _parameters

        self._exchange = 'dlx'
        self._queue = 'dl'
        self._routing_key = '#'
        self._exchange_type = 'topic'
        self._connection = None
        self._channel = None

        self.should_reconnect = False
        self.was_consuming = False

        self._closing = False
        self._consumer_tag = None
        self._consuming = False

        self._prefetch_count = 1

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
        print(f'channel- {channel}')
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
        self._channel.queue_declare(queue=queue_name, callback=cb)

    def on_queue_declareok(self, _unused_frame, userdata):
        queue_name = userdata
        LOGGER.info('Binding %s to %s with %s', self._exchange, queue_name,
                    self._routing_key)
        self._channel.queue_bind(
            queue_name,
            self._exchange,
            routing_key=self._routing_key)

    def get_dlx_exchange(self):
        return self._exchange

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


class ReconnectingDlx:

    def __init__(self):
        self._reconnect_delay = 0

        user = 'guest'
        password = 'guest'
        credentials = pika.PlainCredentials(user, password)
        self._parameters = pika.ConnectionParameters('192.168.202.128',
                                                     5672,
                                                     '/',
                                                     credentials,
                                                     heartbeat=60)
        self._dlx = Dlx(self._parameters)

    def get_dlx_exchange(self):
        return self._dlx.get_dlx_exchange()

    def run(self):
        while True:
            try:
                self._dlx.run()
            except KeyboardInterrupt:
                self._dlx.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._dlx.should_reconnect:
            self._dlx.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._dlx = Dlx(self._parameters)

    def _get_reconnect_delay(self):
        if self._dlx.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
