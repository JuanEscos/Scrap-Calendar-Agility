import datetime

# Get the current date and time
now = datetime.datetime.now()
current_time_str = now.strftime("%Y-%m-%d %H:%M:%S")

# Create a simple text file with the result
file_content = f"The current date and time is: {current_time_str}"
with open("resultado.txt", "w") as f:
    f.write(file_content)

print(f"File 'resultado.txt' created with content: '{file_content}'")
