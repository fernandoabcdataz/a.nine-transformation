FROM python:3.9-slim

WORKDIR /usr/app/dbt

# Install dbt and other dependencies
RUN pip install dbt-bigquery flask gunicorn

# Copy dbt project files
COPY ./transformation .

# Copy the startup script
COPY start.sh .
RUN chmod +x start.sh

# Set environment variables (to be overridden at runtime)
ENV CLIENT_NAME=demo
ENV GCP_PROJECT=abcdataz
ENV PORT=8080

EXPOSE 8080

# Run the startup script
CMD ["./start.sh"]