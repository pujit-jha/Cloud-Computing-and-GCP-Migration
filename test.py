import multiprocessing
import threading
import subprocess
import time
import os
import psutil

# system settings
CPU_THRESHOLD_OVERLOAD = 75    
CPU_THRESHOLD_NORMAL = 60     
CPU_CHECK_DELAY = 2           
CPU_STABLE_DURATION = 5      

# gcp settings
GCP_PROJECT = "vcc-assignment3-454514"
GCP_ZONE = "us-central1-a"
GCP_MACHINE_TYPE = "n1-standard-1"
GCP_IMAGE_FAMILY = "ubuntu-2204-lts"
GCP_IMAGE_PROJECT = "ubuntu-os-cloud"

# global states
vm_instance_name = ""
vm_log_thread = None
stop_log_thread_event = threading.Event()
tasks_moved_to_vm = False
cpu_low_since = None


# local tasks
def perform_heavy_computation(task_label):
    print(f"[{task_label}] Running on local CPU (PID {os.getpid()})")
    value = 0
    cycles = 0
    while True:
        for _ in range(10**7):
            value += 1
        cycles += 1
        if cycles % 3 == 0:
            print(f"[{task_label}] Still working (PID {os.getpid()})")


# task management
def start_local_processes(task_ids, task_process_map):
    for task_id in task_ids:
        process = multiprocessing.Process(target=perform_heavy_computation, args=(task_id,))
        process.start()
        task_process_map[task_id] = process
        print(f"[LOCAL] Task '{task_id}' started with PID {process.pid}")


def terminate_local_processes(task_ids, task_process_map):
    for task_id in task_ids:
        process = task_process_map.pop(task_id, None)
        if process and process.is_alive():
            process.terminate()
            process.join()
            print(f"[LOCAL] Task '{task_id}' (PID {process.pid}) terminated")


# gcp vm operations
def create_vm_on_gcp():
    global vm_instance_name, vm_log_thread, stop_log_thread_event

    vm_instance_name = f"vm-{int(time.time())}"
    print(f"[GCP] Spinning up VM: {vm_instance_name}")

    create_command = [
        "gcloud", "compute", "instances", "create", vm_instance_name,
        "--zone", GCP_ZONE,
        "--machine-type", GCP_MACHINE_TYPE,
        "--image-family", GCP_IMAGE_FAMILY,
        "--image-project", GCP_IMAGE_PROJECT,
        "--project", GCP_PROJECT
    ]

    try:
        subprocess.run(create_command, check=True)
        print(f"[GCP] VM '{vm_instance_name}' provisioned successfully")

        # Allow VM to initialize fully
        print("[GCP] Allowing 30s for VM bootup...")
        time.sleep(30)

        # Transfer workload script to VM
        transfer_script_to_vm()

        # Launch the workload script remotely
        start_vm_workload()

        # Start capturing logs
        start_vm_log_stream()

    except subprocess.CalledProcessError as err:
        print(f"[GCP][ERROR] VM setup failed: {err}")


def transfer_script_to_vm():
    print(f"[GCP] Uploading workload script to VM '{vm_instance_name}'...")

    scp_command = [
        "gcloud", "compute", "scp", "gcp_tasks.py",
        f"{vm_instance_name}:~/",
        "--zone", GCP_ZONE,
        "--project", GCP_PROJECT
    ]

    subprocess.run(scp_command, check=True)
    print(f"[GCP] Script uploaded successfully to VM '{vm_instance_name}'")


def start_vm_workload():
    print(f"[GCP] Starting workload on VM '{vm_instance_name}'...")

    ssh_command = [
        "gcloud", "compute", "ssh", vm_instance_name,
        "--zone", GCP_ZONE,
        "--project", GCP_PROJECT,
        "--command", "nohup python3 -u ~/gcp_tasks.py > ~/gcp_tasks.log 2>&1 &"
    ]

    subprocess.run(ssh_command, check=True)
    print(f"[GCP] Workload started on VM '{vm_instance_name}'")


def start_vm_log_stream():
    global vm_log_thread, stop_log_thread_event

    stop_log_thread_event.clear()

    def stream_logs():
        print(f"[GCP-LOG] Connecting to VM log stream for '{vm_instance_name}'...")
        ssh_log_command = [
            "gcloud", "compute", "ssh", vm_instance_name,
            "--zone", GCP_ZONE,
            "--project", GCP_PROJECT,
            "--command", "tail -f ~/gcp_tasks.log"
        ]

        process = subprocess.Popen(ssh_log_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        while not stop_log_thread_event.is_set():
            output_line = process.stdout.readline()
            if output_line:
                print(f"[VM-LOG] {output_line.strip()}")

        process.terminate()
        print("[GCP-LOG] Log stream closed")

    vm_log_thread = threading.Thread(target=stream_logs, daemon=True)
    vm_log_thread.start()


def terminate_vm():
    global vm_instance_name, vm_log_thread, stop_log_thread_event

    if not vm_instance_name:
        print("[GCP] No VM instance to terminate")
        return

    print(f"[GCP] Initiating termination of VM '{vm_instance_name}'")

    stop_log_thread_event.set()

    if vm_log_thread:
        vm_log_thread.join(timeout=5)

    delete_command = [
        "gcloud", "compute", "instances", "delete", vm_instance_name,
        "--zone", GCP_ZONE,
        "--project", GCP_PROJECT,
        "--quiet"
    ]

    try:
        subprocess.run(delete_command, check=True)
        print(f"[GCP] VM '{vm_instance_name}' has been terminated successfully")
    except subprocess.CalledProcessError as err:
        print(f"[GCP][ERROR] Failed to delete VM '{vm_instance_name}': {err}")

    vm_instance_name = ""


# monitoring logic
def monitor_system_and_manage_tasks():
    global tasks_moved_to_vm, cpu_low_since

    local_tasks = ["Alpha", "Beta", "Gamma", "Delta"]
    offloaded_tasks = ["Gamma", "Delta"]

    active_processes = {}

    # Start all tasks locally
    start_local_processes(local_tasks, active_processes)

    try:
        while True:
            current_cpu_usage = psutil.cpu_percent(interval=1)
            print(f"[MONITOR] CPU Load: {current_cpu_usage}%")

            # High CPU load detected; migrate some tasks to the VM
            if current_cpu_usage > CPU_THRESHOLD_OVERLOAD and not tasks_moved_to_vm:
                print("[DECISION] CPU overloaded. Offloading tasks to VM...")

                terminate_local_processes(offloaded_tasks, active_processes)
                create_vm_on_gcp()

                tasks_moved_to_vm = True
                cpu_low_since = None

            # CPU load is back to normal; consider bringing tasks back locally
            elif tasks_moved_to_vm:
                if current_cpu_usage < CPU_THRESHOLD_NORMAL:
                    if cpu_low_since is None:
                        cpu_low_since = time.time()
                        print("[DECISION] CPU load dropped. Monitoring for stability...")

                    elif time.time() - cpu_low_since >= CPU_STABLE_DURATION:
                        print("[DECISION] CPU stable for a while. Repatriating tasks...")

                        start_local_processes(offloaded_tasks, active_processes)
                        terminate_vm()

                        tasks_moved_to_vm = False
                        cpu_low_since = None
                else:
                    cpu_low_since = None

            time.sleep(CPU_CHECK_DELAY)

    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Interrupt received. Cleaning up resources...")
        if tasks_moved_to_vm:
            terminate_vm()
        terminate_local_processes(local_tasks, active_processes)
        print("[SHUTDOWN] Shutdown complete")



if __name__ == "__main__":
    monitor_system_and_manage_tasks()
