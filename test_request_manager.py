import datetime
import bisect



sched_task = {'deadline': 35, 'execution_time': 10, 'period': 35, 'repeat': 10, 'priority': 2, 'task_name': 'task_f_1', 'date_deadline': datetime.datetime(2024, 6, 30, 15, 27, 44, 386726), 'sched_deadline': datetime.datetime(2024, 6, 30, 15, 27, 29, 386726), 'service_started': 0, 'service_state': 'new'}

priority_queue = [
    
    {'deadline': 35, 'execution_time': 100, 'period': 35, 'repeat': 50, 'priority': 2, 'task_name': 'task_d_1', 'date_deadline': datetime.datetime(2024, 6, 30, 15, 27, 44, 338921), 'sched_deadline': datetime.datetime(2024, 6, 30, 15, 27, 29, 338921), 'service_started': 0, 'service_state': 'new'}, {'deadline': 35, 'execution_time': 100, 'period': 35, 'repeat': 10, 'priority': 2, 'task_name': 'task_e_1', 'date_deadline': datetime.datetime(2024, 6, 30, 15, 27, 44, 375240), 'sched_deadline': datetime.datetime(2024, 6, 30, 15, 27, 29, 375240), 'service_started': 0, 'service_state': 'new'}, {'deadline': 35, 'execution_time': 10, 'period': 35, 'repeat': 10, 'priority': 2, 'task_name': 'task_f_1', 'date_deadline': datetime.datetime(2024, 6, 30, 15, 27, 44, 386726), 'sched_deadline': datetime.datetime(2024, 6, 30, 15, 27, 29, 386726), 'service_started': 0, 'service_state': 'new'}]


running_services={'task_a_1': {'deadline': 15, 'execution_time': 5, 'period': 15, 'repeat': 10, 'priority': 1, 'task_name': 'task_a_1', 'date_deadline': datetime.datetime(2024, 6, 30, 15, 27, 24, 335243), 'sched_deadline': datetime.datetime(2024, 6, 30, 15, 27, 14, 335243), 'service_started': datetime.datetime(2024, 6, 30, 15, 27, 9, 339263), 'service_state': 'pending', 'work_node': 'rpi5-node01', 'work_node_ip': '192.168.1.112'}, 'task_c_1': {'deadline': 15, 'execution_time': 5, 'period': 15, 'repeat': 10, 'priority': 1, 'task_name': 'task_c_1', 'date_deadline': datetime.datetime(2024, 6, 30, 15, 27, 24, 341552), 'sched_deadline': datetime.datetime(2024, 6, 30, 15, 27, 14, 341552), 'service_started': datetime.datetime(2024, 6, 30, 15, 27, 9, 350007), 'service_state': 'pending', 'work_node': 'rpi5-node01', 'work_node_ip': '192.168.1.112'}, 'task_b_1': {'deadline': 15, 'execution_time': 5, 'period': 15, 'repeat': 10, 'priority': 1, 'task_name': 'task_b_1', 'date_deadline': datetime.datetime(2024, 6, 30, 15, 27, 24, 348328), 'sched_deadline': datetime.datetime(2024, 6, 30, 15, 27, 14, 348328), 'service_started': datetime.datetime(2024, 6, 30, 15, 27, 9, 359410), 'service_state': 'pending', 'work_node': 'rpi5-node01', 'work_node_ip': '192.168.1.112'}}


sched_prediction = []
for service in running_services:
    sched_prediction.append({"task_name": service, "work_node": running_services[service]["work_node"],"time_completion":running_services[service]["service_started"]+datetime.timedelta(seconds=(running_services[service]["execution_time"]+5)),"task_deadline":running_services[service]["date_deadline"]})


# Sorting the list of dictionaries by the 'time_completion' timestamp
sorted_tasks = sorted(sched_prediction, key=lambda x: x['time_completion'])

    
def get_time_completion(task):
    return task['time_completion']

# Displaying the sorted tasks
while sorted_tasks:
    task = sorted_tasks.pop(0)
    if task["time_completion"] > task['task_deadline']:
        print("Miss Deadline of:", task )
        break
        
    if priority_queue:
        new_task = priority_queue.pop(0)
        new_task = {"task_name":new_task["task_name"],"work_node": task["work_node"],"time_completion":(task["time_completion"]+datetime.timedelta(seconds=(new_task['execution_time']+5))),'task_deadline':new_task['date_deadline']}
        bisect.insort(sorted_tasks, new_task, key=get_time_completion)



