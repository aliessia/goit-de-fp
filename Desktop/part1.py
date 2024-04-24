import queue 

request_queue = queue.Queue()

def generate_request():
    new_request = input("Specify your request ")
    request_queue.put(new_request)

def process_request():
    if not request_queue.empty():
        request = request_queue.get()
        print(f"Processing request: {request}")
    else:
        print("The queue is empty")

while True:
    user_input = input("Press ENTER to continue or type 'exit' to stop: ")
    if user_input.lower() == 'exit':
        break
    generate_request()
    process_request()

