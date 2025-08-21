FROM python:3.10-slim

# Set workdir
WORKDIR /app

# Copy project files
COPY ./src /app/src
COPY requirements.txt /app/
COPY supervisord.conf /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose Streamlit (8501) and FastAPI (8000)
EXPOSE 8501 8000

# Start both using Supervisor
CMD ["supervisord", "-c", "supervisord.conf"]
