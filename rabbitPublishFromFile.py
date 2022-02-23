from publishFromFile import PublishFromFile


class RabbitPublishFromFile(PublishFromFile):

    def set_routing_key_for_producer(self, file_name):
        if file_name.endswith('.jpg'):
            self._producer.set_routing_key('s.jpg')
        elif file_name.endswith('.jpeg'):
            self._producer.set_routing_key('s.jpeg')

    def publish_messages(self, file_name):
        self.set_routing_key_for_producer(file_name)
        self.publish(file_name)
