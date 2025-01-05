# Use the official Python image as the base
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt requirements.txt

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Streamlit app into the container
COPY kafka_streamlit_dashboard.py kafka_streamlit_dashboard.py

# Expose port 8501 for Streamlit
EXPOSE 8501

# Run the Streamlit application
CMD ["streamlit", "run", "kafka_streamlit_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]

