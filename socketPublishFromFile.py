from publishFromFile import PublishFromFile


class SocketPublishFromFile(PublishFromFile):

    def publish_messages(self, file_name):
        self.publish(file_name)
