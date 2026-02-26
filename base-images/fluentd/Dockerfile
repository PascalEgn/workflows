FROM fluent/fluentd:latest
USER root
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*
RUN gem install fluent-plugin-opensearch --no-document
USER fluent
