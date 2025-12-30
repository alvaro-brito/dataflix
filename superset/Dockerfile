FROM apache/superset:3.1.0

# Install additional dependencies
RUN pip install --no-cache-dir \
    clickhouse-connect==0.6.8 \
    clickhouse-driver==0.4.6

# Copy configuration
COPY config/superset_config.py /app/superset_config.py

# Expose port
EXPOSE 8088

# Default command
CMD ["superset", "run", "-h", "0.0.0.0", "-p", "8088"]
