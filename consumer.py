import requests
import time
import json
import os
import sys

class ConsumerClient:
    """
    An intelligent consumer for the YAK message broker.

    This client:
    1.  Maintains a list of all known broker nodes.
    2.  Actively discovers the current LEADER by querying /metadata/leader.
    3.  Tracks its last-read offset locally in a file to ensure "at-least-once"
        delivery and prevent re-reading all messages.
    4.  Handles leader failure by catching connection errors, re-running
        leader discovery, and seamlessly failing over to the new leader.
    """
    def __init__(self, broker_list, offset_file='consumer_offset.txt'):
        """
        Initializes the consumer.

        Args:
            broker_list (list): A list of base URLs for all known brokers
                                (e.g., ['http://127.0.0.1:8000', 'http://127.0.0.1:8001'])
            offset_file (str): The local file to persist the last-read offset.
        """
        self.brokers = broker_list
        self.offset_file = offset_file
        self.current_leader = None
        self.offset = 0
        self.session = requests.Session() # Use a session for connection pooling

    def load_offset(self):
        """
        Loads the last-saved offset from the local offset file.
        If the file doesn't exist, starts from offset 0.
        """
        if os.path.exists(self.offset_file):
            try:
                with open(self.offset_file, 'r') as f:
                    self.offset = int(f.read().strip())
                    print(f"Loaded offset: {self.offset}")
            except (IOError, ValueError) as e:
                print(f"Warning: Could not read offset file: {e}. Starting from 0.")
                self.offset = 0
        else:
            print("No offset file found. Starting from offset 0.")
            self.offset = 0

    def save_offset(self):
        """Saves the current offset to the local file."""
        try:
            with open(self.offset_file, 'w') as f:
                f.write(str(self.offset))
        except IOError as e:
            print(f"FATAL: Could not write offset file: {e}")
            # In a real system, you might retry or exit
            
    def find_leader(self):
        """
        Queries the /metadata/leader endpoint on all known brokers
        to find the current leader.
        
        Returns:
            bool: True if a leader was found, False otherwise.
        """
        print("Attempting to find the leader...")
        for broker_url in self.brokers:
            try:
                # As per diagram, /metadata/leader can be queried on any node
                response = self.session.get(f"{broker_url}/metadata/leader", timeout=3)
                
                if response.status_code == 200:
                    leader_info = response.json()
                    self.current_leader = leader_info.get('leader_address')
                    
                    if self.current_leader:
                        print(f"Leader found: {self.current_leader}")
                        return True
                    else:
                        print(f"Error: {broker_url} responded but did not provide a leader address.")
                        
            except requests.exceptions.ConnectionError:
                print(f"Broker at {broker_url} is unreachable.")
            except requests.exceptions.Timeout:
                print(f"Request to {broker_url} timed out.")
            except requests.exceptions.RequestException as e:
                print(f"An error occurred while contacting {broker_url}: {e}")
        
        print("Could not find a leader after checking all brokers.")
        self.current_leader = None
        return False

    def consume_messages(self):
        """
        Attempts to consume a batch of messages from the current leader.
        Handles leader failure by setting self.current_leader to None.
        """
        if not self.current_leader:
            print("No leader known. Attempting to find one.")
            if not self.find_leader():
                # If still no leader, wait before retrying
                return

        try:
            # Step 6: GET /consume
            # We send our current offset to tell the broker where we are.
            print(f"Consuming from {self.current_leader} starting at offset {self.offset}...")
            response = self.session.get(
                f"{self.current_leader}/consume",
                params={'offset': self.offset},
                timeout=5
            )

            if response.status_code == 200:
                # Broker should return messages *at or below* the High Water Mark
                data = response.json()
                messages = data.get('messages', [])
                
                if not messages:
                    print("No new messages.")
                    return

                print(f"Received {len(messages)} message(s):")
                last_offset = -1
                for msg in messages:
                    print(f"  > Offset {msg['offset']}: {msg['data']}")
                    last_offset = msg['offset']
                
                # IMPORTANT: Update our offset to the *next* one we
                # expect to read.
                self.offset = last_offset + 1
                self.save_offset()
                
            elif 400 <= response.status_code < 500:
                # Handle application errors, e.g., "Not the Leader"
                print(f"Received error from broker: {response.status_code} {response.text}")
                print("Assuming leader has changed. Re-discovering...")
                self.current_leader = None # Force leader re-discovery
                
            else:
                # Handle server errors
                print(f"Server error at {self.current_leader}: {response.status_code}")
                print("Assuming leader failure. Re-discovering...")
                self.current_leader = None # Force leader re-discovery

        except requests.exceptions.ConnectionError:
            print(f"\n--- CONNECTION FAILED ---")
            print(f"Leader at {self.current_leader} is down.")
            print("Triggering failover: searching for new leader...")
            print("-------------------------\n")
            self.current_leader = None # Force leader re-discovery
            
        except requests.exceptions.RequestException as e:
            print(f"An unknown request error occurred: {e}")
            self.current_leader = None # Force leader re-discovery

    def run(self):
        """
        Main run loop for the consumer.
        
        Loads offset, finds leader, and enters an infinite loop
        to poll for messages.
        """
        self.load_offset()
        self.find_leader() # Find initial leader
        
        print("\nStarting consumer poll loop... (Press Ctrl+C to stop)")
        try:
            while True:
                self.consume_messages()
                # Poll every 3 seconds
                time.sleep(3)
        except KeyboardInterrupt:
            print("\nShutting down consumer.")
            sys.exit(0)

# --- Main execution ---
if __name__ == "__main__":
    
    # !!! IMPORTANT !!!
    # --- [ CHANGE 1: BROKER ADDRESSES ] ---
    # You MUST change these IP addresses and ports to match your
    # System 1 (Leader) and System 2 (Follower) network addresses.
    #
    # Example for local testing (default):
    # BROKER_NODES = [
    #     'http://127.0.0.1:8000',  # System 1 (Initial Leader)
    #     'http://127.0.0.1:8001'   # System 2 (Initial Follower)
    # ]
    #
    # Example for a real network setup:
    # BROKER_NODES = [
    #     'http://192.168.1.101:8000',  # System 1 (Initial Leader)
    #     'http://192.168.1.102:8000'   # System 2 (Initial Follower)
    # ]
    BROKER_NODES = [
        'http://127.0.0.1:8000',  # <-- CHANGE THIS
        'http://127.0.0.1:8001'   # <-- CHANGE THIS
    ]
    # --- [ END OF CHANGE 1 ] ---
    
    
    # You can change this to the IP of your System 4 (this machine) if needed
    # for logging, but it's not required for functionality.
    print("Starting YAK Consumer Client (System 4)")

    # --- [ CHANGE 2: OFFSET FILE PATH (Optional) ] ---
    # This is the local file where the consumer saves its progress (the
    # last-read offset). By default, it saves 'consumer_offset.txt'
    # in the same directory.
    # You can change this to an absolute path if you prefer.
    #
    # Example:
    # consumer = ConsumerClient(
    #     broker_list=BROKER_NODES,
    #     offset_file='/home/user/yak_progress.txt'
    # )
    consumer = ConsumerClient(
        broker_list=BROKER_NODES,
        offset_file='consumer_offset.txt' # <-- (Optional) CHANGE THIS
    )
    # --- [ END OF CHANGE 2 ] ---
    
    consumer.run()


