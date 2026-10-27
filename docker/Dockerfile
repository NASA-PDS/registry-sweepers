# Copyright © 2023, California Institute of Technology ("Caltech").
# U.S. Government sponsorship acknowledged.
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# • Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# • Redistributions must reproduce the above copyright notice, this list of
#   conditions and the following disclaimer in the documentation and/or other
#   materials provided with the distribution.
# • Neither the name of Caltech nor its operating division, the Jet Propulsion
#   Laboratory, nor the names of its contributors may be used to endorse or
#   promote products derived from this software without specific prior written
#   permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
# Dockerfile for PDS Registry Sweepers metadata maintainance suite
# ======================================
#
# Multi-stage build process is used to eliminate the presence of unneccessary dependencies (i.e. their vulns)
# in the deployed image
# ---------- Stage 1: builder (compilers, headers, git live and die here) ----------
FROM python:3.13-slim-trixie AS builder

RUN apt-get update --quiet \
 && apt-get install --quiet --yes --no-install-recommends git gcc libc6-dev \
 && rm --force --recursive /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# Build registry-sweepers and every dependency into wheels.
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

# ---------- Stage 2: runtime (no gcc, no libc6-dev, no linux-libc-dev) ----------
FROM python:3.13-slim-trixie

# Current Debian security fixes (glibc, etc.)
RUN apt-get update --quiet \
 && apt-get upgrade --quiet --yes \
 && rm --force --recursive /var/lib/apt/lists/*

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels /wheels/*.whl \
 && rm --force --recursive /wheels

COPY docker/sweepers_driver.py /usr/local/bin/sweepers_driver.py
RUN chmod 0755 /usr/local/bin/sweepers_driver.py

# Non-root user
RUN useradd --create-home --shell /bin/bash sweeper
USER sweeper

ENTRYPOINT ["/usr/local/bin/sweepers_driver.py"]
