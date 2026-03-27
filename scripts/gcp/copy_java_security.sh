#!/bin/bash
set -euo pipefail

# Keep the TLS overrides inline so Dataproc boot does not depend on a
# second GCS object path staying in sync with this init action.
install -d /etc/flink/conf
cat > /etc/flink/conf/java.security <<'EOF'
jdk.tls.disabledAlgorithms=SSLv3, RC4, DES, MD5withRSA
jdk.tls.legacyAlgorithms=
security.provider.1=sun.security.provider.Sun
security.provider.2=sun.security.rsa.SunRsaSign
security.provider.3=sun.security.ssl.SunJSSE
EOF
