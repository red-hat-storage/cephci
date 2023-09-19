import pyipmi
import pyipmi.interfaces

from utility.retry import retry


class IPMIPowerControl:
    """
    IPMIPowerControl class for controlling the power state of a server via IPMI.

    Attributes:
        host (str): The hostname or IP address of the target IPMI device.
        username (str): The username for IPMI authentication.
        password (str): The password for IPMI authentication.

    Methods:
        __init__(self, host, username, password):
            Initializes the IPMIPowerControl instance and establishes an IPMI session.

        power_down(self):
            Powers down the server and validates the power state.

        power_up(self):
            Powers up the server and validates the power state.

        power_cycle(self):
            Power cycles (restarts) the server and validates the power state.

        hard_reset(self):
            Performs a hard reset on the server and validates the power state.

        diagnostic_interrupt(self):
            Sends a diagnostic interrupt to the server and validates the power state.

        soft_shutdown(self):
            Initiates a soft shutdown of the server and validates the power state.

        close(self):
            Closes the IPMI session.

    Example Usage:
        ipmi_controller = IPMIPowerControl(host, username, password)
        ipmi_controller.power_up()
        ipmi_controller.close()
    """

    def __init__(self, host, username, password):
        """
        Initializes an IPMIPowerControl instance and establishes an IPMI session.

        Args:
            host (str): The hostname or IP address of the target IPMI device.
            username (str): The username for IPMI authentication.
            password (str): The password for IPMI authentication.
        """
        self.interface = pyipmi.interfaces.create_interface(
            interface="rmcp",
            host_target_address=0x20,
            slave_address=0x81,
            keep_alive_interval=1,
        )
        self.ipmi = pyipmi.create_connection(self.interface)
        self.ipmi.session.set_session_type_rmcp(host=host, port=623)
        self.ipmi.session.set_auth_type_user(username=username, password=password)
        self.ipmi.target = pyipmi.Target(ipmb_address=0x20)
        self.ipmi.session.establish()

    @retry(Exception, tries=3, delay=60)
    def _validate_power_status(self, expected_power_state):
        """
        Validates the power state of the server.

        Args:
            expected_power_state (bool): The expected power state (True for powered on, False for powered off).

        Returns:
            bool: True if the power state matches the expected state, False otherwise.
        """
        for _ in range(3):
            try:
                chassis_status = self.ipmi.get_chassis_status()
                return chassis_status.power_on == expected_power_state
            except TimeoutError as te:
                print("Session got expired")
                print(te)
                self.ipmi.session.establish()
        return self._validate_power_status(True)

    def power_down(self):
        """
        Powers down the server and validates the power state.

        Returns:
            bool: True if the power operation is successful and the server is powered off, False otherwise.
        """
        for _ in range(3):
            try:
                self.ipmi.chassis_control_power_down()
            except TimeoutError as te:
                print("Session got expired")
                print(te)
                self.ipmi.session.establish()
        return self._validate_power_status(False)

    @retry(TimeoutError, tries=3, delay=30)
    def power_up(self):
        """
        Powers up the server and validates the power state.

        Returns:
            bool: True if the power operation is successful and the server is powered on, False otherwise.
        """
        for _ in range(3):
            try:
                self.ipmi.chassis_control_power_up()
            except TimeoutError as te:
                print("Session got expired")
                print(te)
                self.ipmi.session.establish()
        return self._validate_power_status(True)

    @retry(TimeoutError, tries=3, delay=30)
    def power_cycle(self):
        """
        Power cycles (restarts) the server and validates the power state.

        Returns:
            bool: True if the power operation is successful and the server is powered on, False otherwise.
        """
        for _ in range(3):
            try:
                self.ipmi.chassis_control_power_cycle()
            except TimeoutError as te:
                print("Session got expired")
                print(te)
                self.ipmi.session.establish()
        return self._validate_power_status(True)

    def hard_reset(self):
        """
        Performs a hard reset on the server and validates the power state.

        Returns:
            bool: True if the power operation is successful and the server is powered on, False otherwise.
        """
        self.ipmi.chassis_control_hard_reset()
        return self._validate_power_status(True)

    def diagnostic_interrupt(self):
        """
        Sends a diagnostic interrupt to the server and validates the power state.

        Returns:
            bool: True if the power operation is successful and the server is powered on, False otherwise.
        """
        self.ipmi.chassis_control_diagnostic_interrupt()
        return self._validate_power_status(True)

    def soft_shutdown(self):
        """
        Initiates a soft shutdown of the server and validates the power state.

        Returns:
            bool: True if the power operation is successful and the server is powered off, False otherwise.
        """
        self.ipmi.chassis_control_soft_shutdown()
        return self._validate_power_status(False)

    def close(self):
        """
        Closes the IPMI session.
        """
        self.ipmi.session.close()
