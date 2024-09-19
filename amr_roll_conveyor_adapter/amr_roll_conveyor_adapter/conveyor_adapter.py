import yaml
import threading
import sys

import rclpy
from rclpy.node import Node, MutuallyExclusiveCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from amr_msgs.srv import SetAdvantechOutput
from rmf_ingestor_msgs.msg import IngestorRequest, IngestorResult, IngestorState
from rmf_dispenser_msgs.msg import DispenserRequest, DispenserResult, DispenserState
from rmf_fleet_msgs.msg import ModeRequest, RobotMode, ModeParameter, FleetState
from rcl_interfaces.msg import ParameterDescriptor

###############################################################################

class ConveyorAdapter(Node):
    def __init__(self):
        super().__init__("roll_conveyor_adapter")

        # Declare parameter for the configuration file path
        self.declare_parameter(
            "config_file",
            "",
            ParameterDescriptor(
                name="config_file",
                description="Path of the config YAML for the roll conveyor adapter",
            ),
        )

        # Retrieve the configuration file path
        config_path = self.get_parameter("config_file").value

        # Ensure the config path is provided
        if config_path == "":
            self.get_logger().error(
                "No config file path provided. Please set the 'config_file' parameter."
            )
            raise ValueError("Config file path not provided")

        # Log the start of the roll conveyor adapter
        self.get_logger().info("Starting roll conveyor adapter...")

        # Attempt to load the YAML configuration file
        try:
            with open(config_path, "r") as f:
                config_yaml = yaml.safe_load(f)
            self.get_logger().info(f"Loaded config file from {config_path}")
        except FileNotFoundError:
            self.get_logger().error(f"Config file not found: {config_path}")
            raise
        except yaml.YAMLError as exc:
            self.get_logger().error(f"Error parsing YAML config file: {exc}")
            raise

        # Get value from config file
        self.conveyor_name = config_yaml["conveyor"]["name"]
        self.conveyor_id = config_yaml["conveyor"]["conveyor_id"]
        self.conveyor_on_time = config_yaml["conveyor"]["on_time"]

        self.host = config_yaml["conveyor"]["host"]
        self.port = config_yaml["conveyor"]["port"]
        self.pin = config_yaml["conveyor"]["io_pin"]

        # Subscription topics
        dispense_request_topic = config_yaml["conveyor_subscribers"]["dispense_request_topic"]
        ingest_request_topic = config_yaml["conveyor_subscribers"]["ingest_request_topic"]

        self.dispense_request_sub = self.create_subscription(DispenserRequest, dispense_request_topic, self.dispense_request_cb, 10)
        self.ingeste_request_sub = self.create_subscription(IngestorRequest, ingest_request_topic, self.ingest_request_cb, 10)

        fleet_state_cb_group = MutuallyExclusiveCallbackGroup()

        self.fleet_state_sub = self.create_subscription(FleetState, "fleet_states", self._fleet_state_cb, 5, callback_group=fleet_state_cb_group)

        # Publish topics
        dispense_result_topic = config_yaml["conveyor_publishers"]["dispenser_results_topic"]
        dispenser_state_topic = config_yaml["conveyor_publishers"]["dispenser_states_topic"]
        ingest_result_topic = config_yaml["conveyor_publishers"]["ingestor_results_topic"]
        ingestor_state_topic = config_yaml["conveyor_publishers"]["ingestor_states_topic"]
        state_publish_period = config_yaml["conveyor_publishers"]["state_publish_period"]

        self.dispense_result_pub = self.create_publisher(DispenserResult, dispense_result_topic, 10)
        self.dispenser_state_pub = self.create_publisher(DispenserState, dispenser_state_topic, 10)
        
        self.ingest_result_pub = self.create_publisher(IngestorResult, ingest_result_topic, 10)
        self.ingestor_state_pub = self.create_publisher(IngestorState, ingestor_state_topic, 10)

        self.mode_request_pub = self.create_publisher(ModeRequest, "robot_mode_requests", 10)

        self.update_timer = self.create_timer(state_publish_period, self.update_cb)


        # Client to communicate with the advantech service
        self.set_client = self.create_client(SetAdvantechOutput, "set_adv_output")

        if self.set_client.wait_for_service(timeout_sec=10):
            self.get_logger().info("Advantech Set Output Service ready!")
        else:
            self.get_logger().error("Advantech Set Output Service not ready!")
        
        # default conveyor state
        self.current_dispenser_state = 0 # idle
        self.current_ingestor_state = 0 # idle
        
        # internal flags
        self.previous_dispense_requests = {}
        self.previous_ingestion_requests = {}
        self.latest_dispens_req = None
        self.latest_ingestion_req = None
        self.latest_fleet_state = None
        self.is_request_open = False
        self.is_dispense_request = False

        self.current_guid_for_dispense = ""
        self.current_guid_for_ingestion = ""

        self.robot_tool_in_use = False
        self.tool_done_event = threading.Event()


    def update_cb(self):
        # Publish the states of the conveyor
        
        self.publish_conveyor_state()

        if self.current_dispenser_state == 1 or self.current_ingestor_state == 1:
            self.get_logger().info(f"Current dispenser state: {self.current_dispenser_state} and current ingestor state: {self.current_ingestor_state}")
            self.get_logger().info("Conveyor is currently busy. Skipping execution part...")
            return

        # Handle open request
        if self.is_request_open and self.is_dispense_request == True:
            self.handle_dispense()

        if self.is_request_open and self.is_dispense_request == False:
            self.handle_ingest()
            # This is where we start tomorrow
            self.send_ingestor_response(IngestorResult.ACKNOWLEDGED)

            self.current_ingestor_state = 1

            # Send request for dispending an item to conveyor
            req_ingest = SetAdvantechOutput.Request()
            req_ingest.host = self.host
            req_ingest.port = self.port
            req_ingest.indices = [0,1]
            req_ingest.values = [1,0]


            future = self.set_client.call_async(req_ingest)
            future.add_done_callback(self.conveyor_start_cb)

    def conveyor_start_cb(self, future):
        """If request was successful wait for the robot tool before sending an off request"""
        if future.result().success:

            if self.is_dispense_request:
                self.get_logger().info(f"Waiting for Robot {self.latest_dispens_req.transporter_id} to finish loading.")
                # Send tool cmd
                self._turn_on_onboard_tool(dispense_item=self.is_dispense_request)
                # self._wait_for_robot_tool(robot_id=self.latest_dispens_req.transporter_id)
                
            else:
                self.get_logger().info(f"Waiting for Robot {self.latest_ingestion_req.transporter_id} to finish unloading.")
                # Send tool cmd
                self._turn_on_onboard_tool(dispense_item=self.is_dispense_request)
                # self._wait_for_robot_tool(robot_id=self.latest_ingestion_req.transporter_id)

            self.get_logger().info("Done waiting!")
            # Send cmd to turn off conveyor
            stop_req = SetAdvantechOutput.Request()
            stop_req.host = self.host
            stop_req.port = self.port
            stop_req.indices = [0,1]
            stop_req.values = [0,0]

            future = self.set_client.call_async(stop_req)
            future.add_done_callback(self.conveyor_done_cb)

        else:
            self.get_logger().info("Failed!")
            if self.is_dispense_request == True:
                self.current_dispenser_state = 2 # offline
                self.send_dispenser_response(DispenserResult.FAILED)
            else:
                self.current_ingestor_state = 2 # offline
                self.send_ingestor_response(IngestorResult.FAILED)

    def conveyor_done_cb(self, future):
        """If request was sucessful return a response to the request feedback topic"""
        if future.result().success:

            if self.is_dispense_request == True:
                self.current_dispenser_state = 0
                self.previous_dispense_requests[self.latest_dispens_req.request_guid] = True
                self.send_dispenser_response(DispenserResult.SUCCESS)
            else:
                self.current_ingestor_state = 0
                self.previous_ingestion_requests[self.latest_ingestion_req.request_guid] = True
                self.send_ingestor_response(IngestorResult.SUCCESS)

        else:

            if self.is_dispense_request == True:
                self.current_dispenser_state = 2 # offline
                self.previous_dispense_requests[self.latest_dispens_req.request_guid] = False
                self.send_dispenser_response(DispenserResult.FAILED)
            else:
                self.current_ingestor_state = 2 # offline
                self.previous_ingestion_requests[self.latest_ingestion_req.request_guid] = False
                self.send_ingestor_response(IngestorResult.FAILED)

        # Reset the flag and remove unused msgs
        self.is_request_open = False


    def dispense_request_cb(self, msg):
        """Handle incomming request to dispense an item

        Args:
            msg (RMF DispenserRequest msg): ROS2 msg provided by RMF
        """
        self.latest_dispens_req = msg

        # Check if this request is for this conveyor
        if self.latest_dispens_req.target_guid.rstrip(",.") != self.conveyor_id.rstrip(",."):
            self.get_logger().debug(f"This is not for me, because the request was for {repr(self.latest_dispens_req.target_guid)} and I am {repr(self.conveyor_id)}")
            return
        
        # Check wether or not the request was already done
        if self.latest_dispens_req.request_guid in self.previous_dispense_requests:
            if self.previous_dispense_requests[self.latest_dispens_req.request_guid]:
                self.get_logger().warn(f"Request already succeeded: {self.latest_dispens_req.request_guid}")
            else:
                self.get_logger().warn(f"Request already failed: {self.latest_dispens_req.request_guid}")
            return
        else:
            self.is_request_open = True
            self.is_dispense_request = True

    def ingest_request_cb(self, msg):
        """Handle incomming request to ingest an item

        Args:
            msg (RMF IngestorRequest msg): ROS2 msg provided by RMF
        """
        self.latest_ingestion_req = msg

        # Check if this request is for this conveyor
        if self.latest_ingestion_req.target_guid.rstrip(",.") != self.conveyor_id.rstrip(",."):
            self.get_logger().debug(f"This is not for me, because the request was for {repr(self.latest_dispens_req.target_guid)} and I am {repr(self.conveyor_id)}")
            return

        # Check wether or not the request was already done
        if self.latest_ingestion_req.request_guid in self.previous_ingestion_requests:
            if self.previous_ingestion_requests[self.latest_ingestion_req.request_guid]:
                self.get_logger().warn(f"Request already succeeded: {self.latest_ingestion_req.request_guid}")
            else:
                self.get_logger().warn(f"Request already failed: {self.latest_ingestion_req.request_guid}")
            return
        else:
            self.is_request_open = True
            self.is_dispense_request = False

    def handle_dispense(self):
        self.send_dispenser_response(DispenserResult.ACKNOWLEDGED)

        self.current_dispenser_state = 1

        # Send request for dispending an item to conveyor
        req_dispense = SetAdvantechOutput.Request()
        req_dispense.host = self.host
        req_dispense.port = self.port
        req_dispense.indices = [0,1]
        req_dispense.values = [1,1]

        future = self.set_client.call_async(req_dispense)
        future.add_done_callback(self.conveyor_start_cb)

    def handle_ingest(self):
        self.send_ingestor_response(IngestorResult.ACKNOWLEDGED)

        self.current_ingestor_state = 1

        # Send request for ingesting an item 
        req_ingest = SetAdvantechOutput.Request()
        req_ingest.host = self.host
        req_ingest.port = self.port
        req_ingest.indices = [0,1]
        req_ingest.values = [1,0]

        future = self.set_client.call_async(req_ingest)
        future.add_done_callback(self.conveyor_start_cb)


    def send_dispenser_response(self, status):
        response_msg = DispenserResult()
        response_msg.status = status
        response_msg.time = self.get_clock().now().to_msg()
        response_msg.request_guid = self.latest_dispens_req.request_guid
        response_msg.source_guid = self.conveyor_id

        self.dispense_result_pub.publish(response_msg)

    def send_ingestor_response(self, status):
        response_msg = IngestorResult()
        response_msg.status = status
        response_msg.time = self.get_clock().now().to_msg()
        response_msg.request_guid = self.latest_ingestion_req.request_guid
        response_msg.source_guid = self.conveyor_id

        self.ingest_result_pub.publish(response_msg)

    def publish_conveyor_state(self): 
        # Publish the dispenser state of the conveyor
        dispenser_state_msg = DispenserState()

        dispenser_state_msg.time = self.get_clock().now().to_msg()
        dispenser_state_msg.guid = self.conveyor_id
        if self.set_client.service_is_ready() == False:
            dispenser_state_msg.mode = 2 # offline
        else:
            dispenser_state_msg.mode = self.current_dispenser_state
        
        if self.is_request_open and self.is_dispense_request == True:
            dispenser_state_msg.request_guid_queue = [self.current_guid_for_dispense]
        else:
            dispenser_state_msg.request_guid_queue = []

        self.dispenser_state_pub.publish(dispenser_state_msg)


        # Publish the ingestor state of the conveyor
        ingestor_state_msg = IngestorState()

        ingestor_state_msg.time = self.get_clock().now().to_msg()
        ingestor_state_msg.guid = self.conveyor_id
        if self.set_client.service_is_ready() == False:
            ingestor_state_msg.mode = 2 # offline
        else:
            ingestor_state_msg.mode = self.current_ingestor_state
        
        if self.is_request_open and self.is_dispense_request == False:
            ingestor_state_msg.request_guid_queue = [self.current_guid_for_ingestion]
        else:
            ingestor_state_msg.request_guid_queue = []

        self.ingestor_state_pub.publish(ingestor_state_msg)

# We might still need this
    def _fleet_state_cb(self, msg):
        self.latest_fleet_state = msg

        # Return early if there is no open request
        if not self.is_request_open:
            return

        # Determine transporter ID based on the active request type
        if self.is_dispense_request:
            if self.latest_dispens_req:
                transporter_id = self.latest_dispens_req.transporter_id
            else:
                self.get_logger().warning("Dispense request expected but not found.")
                return
        else:
            if self.latest_ingestion_req:
                transporter_id = self.latest_ingestion_req.transporter_id
            else:
                self.get_logger().warning("Ingestion request expected but not found.")
                return

        # Check if the robot tool mode has changed
        for robot in self.latest_fleet_state.robots:
            if robot.name == transporter_id:
                # Log the current mode for debugging purposes
                self.get_logger().debug(f"Robot {robot.name} current mode: {robot.mode.mode}")

                if robot.mode.mode == 10:  # Robot is using tool
                    if not self.robot_tool_in_use:
                        self.robot_tool_in_use = True
                        self.get_logger().info(f"Robot {robot.name} started using tool.")
                elif robot.mode.mode != 10:  # Robot is done using tool
                    if self.robot_tool_in_use:
                        self.robot_tool_in_use = False
                        self.get_logger().info(f"Robot {robot.name} finished using tool.")
                        self.tool_done_event.set()  # Notify waiting threads
                else:
                    self.get_logger().debug(f"Robot {robot.name} is in an unexpected mode: {robot.mode.mode}")

        # Always log the complete state for debugging purposes
        self.get_logger().debug(f"Fleet state updated: {self.latest_fleet_state}")

    
    def _turn_on_onboard_tool(self, dispense_item):
        # Tell the robot to dock with the correct orientation based on the item / slot in the request
        mode_request_msg = ModeRequest()

        if dispense_item:
            mode_request_msg.fleet_name = self.latest_dispens_req.transporter_type
            mode_request_msg.robot_name = self.latest_dispens_req.transporter_id
            mode_request_msg.task_id = self.latest_dispens_req.request_guid

            items = self.latest_dispens_req.items
        else:
            mode_request_msg.fleet_name = self.latest_ingestion_req.transporter_type
            mode_request_msg.robot_name = self.latest_ingestion_req.transporter_id
            mode_request_msg.task_id = self.latest_ingestion_req.request_guid

            items = self.latest_ingestion_req.items

        item_pos = items[0].compartment_name
        quantitiy = items[0].quantity

        mode_msg = RobotMode()
        mode_msg.mode=10

        tool_cmd_msg = ModeParameter()
        tool_cmd_msg.name="tool_cmd"
        

        # Determine the value of tool_cmd_msg based on item_pos
        if item_pos == "front":
            if dispense_item:
                tool_cmd_msg.value = "front_load"
            else:
                tool_cmd_msg.value = "front_unload"
        elif item_pos == "back":
            if dispense_item:
                tool_cmd_msg.value = "back_load"
            else:
                tool_cmd_msg.value = "back_unload"
        else:
            self.get_logger().error(f"Compartment must be either front or back. Found compartment: {item_pos}")
            # Send failed status for this request
            return

        mode_request_msg.mode = mode_msg
        mode_request_msg.parameters = [tool_cmd_msg]

        self.get_logger().info(f"Sending msg: {mode_request_msg}")

        # Publish the message
        self.mode_request_pub.publish(mode_request_msg)

        # Wait for robot to finish
        self._wait_for_robot_tool(mode_request_msg.robot_name)
        self.tool_done_event.clear()


        if quantitiy == 2:
            self.get_logger().info("Need to swap and redo last cmd")
            swap_cmd_msg = ModeParameter()
            swap_cmd_msg.name = "tool_cmd"

            if item_pos == "front":
                swap_cmd_msg.value = "back_switch"
            else:
                swap_cmd_msg.value = "front_switch"

            mode_request_msg.parameters = [swap_cmd_msg]
            mode_request_msg.task_id += "-swap"

            self.mode_request_pub.publish(mode_request_msg)
            self._wait_for_robot_tool(mode_request_msg.robot_name)
            self.tool_done_event.clear()


            # Resend original command
            mode_request_msg.parameters = [tool_cmd_msg]
            mode_request_msg.task_id += "-redo"

            self.mode_request_pub.publish(mode_request_msg)
            self._wait_for_robot_tool(mode_request_msg.robot_name)
            self.tool_done_event.clear()
        # Reset the state to idle after the robot tool operation is complete
        if dispense_item:
            self.current_dispenser_state = 0
        else:
            self.current_ingestor_state = 0

    def _wait_for_robot_tool(self, robot_id):
    # Check if the event is already set, which indicates the tool is already done being used
        if not self.tool_done_event.is_set():
            self.get_logger().info(f"Waiting for Robot {robot_id} to finish using its tool.")
            self.tool_done_event.wait()  # This will block until the tool is no longer in use
            self.get_logger().info(f"Robot {robot_id} has finished using its tool.")
        else:
            # If the event is already set, the robot has already completed the tool action
            self.get_logger().info(f"Robot {robot_id} has already finished using its tool.")
        
        # Optionally clear the event after it's been acknowledged
        self.tool_done_event.clear()




###############################################################################


def main(argv=sys.argv):
    rclpy.init(args=argv)

    door_adapter = ConveyorAdapter()
    executor = MultiThreadedExecutor()
    executor.add_node(door_adapter)
    executor.spin()

    door_adapter.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main(sys.argv)
