import subprocess
import os
import time

def main():
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Open log files
    f_m = open("logs/manager.txt", "w")
    f_p = open("logs/publisher.txt", "w")
    f_s = open("logs/subscriber.txt", "w")
    
    try:
        # Start all processes concurrently (non-blocking)
        # Use the CMake-built binaries from build/
        manager = subprocess.Popen(["./build/manager"], stdout=f_m, stderr=subprocess.STDOUT)
        time.sleep(0.5)  # Give manager time to start
        
        publisher = subprocess.Popen(["./build/publisher"], stdout=f_p, stderr=subprocess.STDOUT)
        subscriber = subprocess.Popen(["./build/subscriber"], stdout=f_s, stderr=subprocess.STDOUT)
        
        # Wait for all processes to complete
        manager.wait()
        publisher.wait()
        subscriber.wait()
    finally:
        # Ensure files are closed
        f_m.close()
        f_p.close()
        f_s.close()


if __name__ == "__main__":
    main()