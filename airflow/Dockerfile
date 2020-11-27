
# Use an official Python runtime as a parent image
FROM puckel/docker-airflow

# Set the working directory to / app
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

#ADD etl.py /app

# Run app.py when the container launches
#CMD ["python", "etl.py"]
