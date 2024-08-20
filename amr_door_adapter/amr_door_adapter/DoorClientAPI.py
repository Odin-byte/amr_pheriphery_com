import time

from rmf_door_msgs.msg import DoorMode
from amr_msgs.srv import SetAdvantechOutput


class DoorClientAPI:
    def __init__(self, host, port, pin, set_client):
        self.url = host
        self.port = int(port)
        self.pin = pin

        self.set_client = set_client

        self.is_open = False
        self.is_offline = False

        count = 0
        self.connected = True
        while not self.check_connection():
            if count >= 5:
                print("Unable to connect to door client API.")
                self.connected = False
                break
            else:
                print(
                    "Unable to connect to door client API. Attempting to reconnect..."
                )
                count += 1
            time.sleep(1)

    def check_connection(self):
        """Return True if connection to the door API server is successful"""
        if self.set_client.service_is_ready():
            return True
        else:
            return False

    def open_door(self):
        """Return True if the door API server is successful receive open door command"""
        req = SetAdvantechOutput.Request()
        req.host = self.url
        req.port = self.port
        req.indices = [self.pin]
        req.values = [1]


        future = self.set_client.call_async(req)
        future.add_done_callback(self._door_open_done)

    def _door_open_done(self, future):
        """Future callback function to handle async service calls"""
        if future.result().success:
            self.is_open = True
            self.is_offline = False

            return True
        else:
            self.is_offline = True
            return False
        
    
        
    def close_door(self):
        """Return True if the door API server is successful receive open door command"""
        req = SetAdvantechOutput.Request()
        req.host = self.url
        req.port = self.port
        req.indices = [self.pin]
        req.values = [0]
        
        future = self.set_client.call_async(req)
        future.add_done_callback(self._door_close_done)
        

    def _door_close_done(self, future):
        """Future callback function to handle async service calls"""
        if future.result().success:
            self.is_open = False
            self.is_offline = False
            return True
        else:
            self.is_offline = True
            return False

    def get_mode(self):
        """Return the door status with reference rmf_door_msgs.
        Return DoorMode.MODE_CLOSED when door status is closed.
        Return DoorMode.MODE_MOVING when door status is moving.
        Return DoorMode.MODE_OPEN when door status is open.
        Return DoorMode.MODE_OFFLINE when door status is offline.
        Return DoorMode.MODE_UNKNOWN when door status is unknown"""

        if self.is_open == True:
            return DoorMode.MODE_OPEN
        if self.is_open == False:
            return DoorMode.MODE_CLOSED
        
        return DoorMode.MODE_OFFLINE