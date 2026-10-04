import time
import random
import concurrent.futures
import paramiko

def parse_error_output(output):
    """
    Parses `show interface counters errors` output.
    Sums error columns (Align-Err, FCS-Err, Xmit-Err, Rcv-Err) to get total errors per interface.
    """
    interfaces = []
    lines = output.strip().splitlines()
    
    # Skip header lines
    for line in lines[1:]:
        parts = line.split()
        if len(parts) >= 5:
            port = parts[0]
            try:
                # Summing error columns: Align-Err, FCS-Err, Xmit-Err, Rcv-Err
                align_err = int(parts[1])
                fcs_err = int(parts[2])
                xmit_err = int(parts[3])
                rcv_err = int(parts[4])
                total_errors = align_err + fcs_err + xmit_err + rcv_err
                
                interfaces.append({
                    "interface": port,
                    "errors": total_errors
                })
            except ValueError:
                continue
                
    return interfaces

def poll_single_device(device, threshold, max_retries=3):
    """
    Connects to a single device via SSH, runs the command, handles retries, 
    and isolates authentication vs. transient errors.
    """
    host = device["host"]
    username = device["username"]
    password = device["password"]
    
    backoff = 1
    last_exception = None

    for attempt in range(1, max_retries + 1):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(
                hostname=host,
                username=username,
                password=password,
                timeout=10
            )
            
            stdin, stdout, stderr = client.exec_command("show interface counters errors")
            output = stdout.read().decode("utf-8")
            client.close()
            
            return host, "success", output
            
        except paramiko.AuthenticationException as e:
            client.close()
            # Do not retry auth failures per requirements
            return host, "error", "Authentication failed"
            
        except (paramiko.SSHException, TimeoutError, socket_error_types()) as e:
            client.close()
            last_exception = str(e)
            if attempt < max_retries:
                # Exponential backoff with jitter
                sleep_time = backoff + random.uniform(0, 1)
                time.sleep(sleep_time)
                backoff *= 2
            else:
                return host, "error", f"Connection failed after {max_retries} attempts: {last_exception}"
        except Exception as e:
            client.close()
            return host, "error", str(e)

def socket_error_types():
    import socket
    return (socket.timeout, socket.error)

def check_fleet_errors(device_list, threshold, max_concurrency=10):
    """
    Polls a fleet of edge switches concurrently, capping concurrency, 
    and aggregates results into the desired report shape.
    """
    clean = []
    over_threshold = {}
    errors = {}

    # Poll devices concurrently using a ThreadPoolExecutor to cap concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
        future_to_device = {
            executor.submit(poll_single_device, device, threshold): device 
            for device in device_list
        }
        
        for future in concurrent.futures.as_completed(future_to_device):
            host, status, result = future.result()
            
            if status == "error":
                errors[host] = result
            else:
                parsed_interfaces = parse_error_output(result)
                device_over_threshold = []
                
                for iface in parsed_interfaces:
                    if iface["errors"] > threshold:
                        device_over_threshold.append(iface)
                
                if device_over_threshold:
                    over_threshold[host] = device_over_threshold
                else:
                    clean.append(host)

    return {
        "clean": clean,
        "over_threshold": over_threshold,
        "errors": errors
    }

# --- Example Usage ---
if __name__ == "__main__":
    device_list = [
        {"host": "10.0.0.1", "username": "admin", "password": "secret"},
        {"host": "10.0.0.2", "username": "admin", "password": "secret"},
        {"host": "10.0.0.3", "username": "admin", "password": "secret"},
    ]
    
    # Run the fleet check with a threshold of 100 errors and max concurrency of 5
    report = check_fleet_errors(device_list, threshold=100, max_concurrency=5)
    import json
    print(json.dumps(report, indent=2))




