#!/usr/bin/env bash
# 20251016_001 - Replace Bitnami-specific env with vanilla Spark settings

# Spark base
export SPARK_HOME=${SPARK_HOME:-/opt/spark}

# Run foreground so containers don't exit after forking
export SPARK_NO_DAEMONIZE=${SPARK_NO_DAEMONIZE:-true}

# Worker defaults (可依機器調整)
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY:-4g}
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES:-2}

# MUST remove Bitnami preload to avoid noisy errors & startup issues
unset LD_PRELOAD 2>/dev/null || true
