import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self, client_id=None):
        self.client_id = client_id or str(uuid.uuid4())[:4]
        self.msg_count = 0
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.msg_count += 1
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id, self.msg_count])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return fields
