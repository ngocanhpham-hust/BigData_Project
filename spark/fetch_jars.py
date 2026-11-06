"""Fetch fixed Maven coordinates and verify the strongest published digest."""
import concurrent.futures
import hashlib
from pathlib import Path
import sys
import urllib.error
import urllib.request

ARTIFACTS = [
    ("org.apache.iceberg", "iceberg-spark-runtime-3.5_2.12", "1.5.2"),
    ("org.apache.hadoop", "hadoop-aws", "3.3.4"),
    ("com.amazonaws", "aws-java-sdk-bundle", "1.12.262"),
    ("org.wildfly.openssl", "wildfly-openssl", "1.0.7.Final"),
    ("org.postgresql", "postgresql", "42.7.3"),
    ("org.apache.spark", "spark-sql-kafka-0-10_2.12", "3.5.1"),
    ("org.apache.spark", "spark-token-provider-kafka-0-10_2.12", "3.5.1"),
    ("org.apache.kafka", "kafka-clients", "3.4.1"),
    ("org.apache.commons", "commons-pool2", "2.11.1"),
]


def fetch(destination, coordinate):
    group, artifact, version = coordinate
    filename = f"{artifact}-{version}.jar"
    url = f"https://repo.maven.apache.org/maven2/{group.replace('.', '/')}/{artifact}/{version}/{filename}"
    for algorithm in ("sha512", "sha256", "sha1"):
        try:
            with urllib.request.urlopen(url + "." + algorithm, timeout=120) as response:
                digest = response.read().decode().strip().split()[0].lower()
            break
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
    else:
        raise ValueError(f"No published checksum: {filename}")
    target = destination / filename
    calculated = hashlib.new(algorithm)
    with urllib.request.urlopen(url, timeout=180) as response, target.open("wb") as output:
        while chunk := response.read(1024 * 1024):
            calculated.update(chunk)
            output.write(chunk)
    if calculated.hexdigest() != digest:
        target.unlink()
        raise ValueError(f"{algorithm} mismatch: {filename}")
    print(f"Verified {filename}", flush=True)


if __name__ == "__main__":
    destination = Path(sys.argv[1])
    destination.mkdir(parents=True, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda item: fetch(destination, item), ARTIFACTS))
