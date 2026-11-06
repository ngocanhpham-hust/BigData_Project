FROM spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root
COPY spark/fetch_jars.py /tmp/fetch_jars.py
# Resolve at build time: driver/executor startup needs no Maven access.
RUN python3 /tmp/fetch_jars.py /opt/spark/jars
COPY --chown=185:185 spark-jobs/ /opt/spark/work-dir/jobs/
COPY --chown=185:185 data/sample/ /opt/spark/work-dir/data/sample/
COPY --chown=185:185 tests/spark/ /opt/spark/work-dir/tests/
COPY --chown=185:185 spark/submit.sh /opt/spark/work-dir/submit.sh
ENV SPARK_JARS_BAKED=1 PYTHONPATH=/opt/spark/work-dir/jobs PYSPARK_PYTHON=python3
USER 185
WORKDIR /opt/spark/work-dir
