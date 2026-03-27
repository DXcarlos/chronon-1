#!/bin/bash
# Set up opsagent to expose prometheus metrics for flink and spark jobs
# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

if [[ $(echo "${DATAPROC_IMAGE_VERSION} < 2.2" | bc -l) == 1  ]]; then
  echo "This Dataproc cluster node runs image version ${DATAPROC_IMAGE_VERSION} with pre-installed legacy monitoring agent. Skipping Ops Agent installation."
  exit 0
fi

curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
bash add-google-cloud-ops-agent-repo.sh --also-install

cat <<EOF >> /etc/google-cloud-ops-agent/config.yaml
metrics:
  receivers:
    prometheus_flink:
      type: prometheus
      config:
        scrape_configs:
          - job_name: 'flink'
            scrape_interval: 15s
            metrics_path: /metrics
            static_configs:
              - targets: [
                'localhost:9250',
                'localhost:9251',
                'localhost:9252',
                'localhost:9253',
                'localhost:9254',
                'localhost:9255',
                'localhost:9256',
                'localhost:9257',
                'localhost:9258',
                'localhost:9259',
                'localhost:9260'
              ]
                labels:
                  component: flink

  service:
    pipelines:
      flink:
        receivers: [prometheus_flink]
      otlp:
        receivers: [otlp]   # pipeline for OTLP metrics

combined:
  receivers:
    otlp:
      type: otlp

traces:
  service:
    pipelines:
      otlp:
        receivers: [otlp]
EOF

systemctl restart google-cloud-ops-agent