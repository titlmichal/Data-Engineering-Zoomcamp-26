# base image to build on
FROM python:3.13.11

# # downloading dependencies
# RUN pip install pandas pyarrow

# switching to uv way (hence commenting out the pip)
# args: from which image; from where; to where
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# setting up current workdir inside the container
# (to not make mess in the root dir --> code is created by this command)
# --> remaning commands are done within this dir code
WORKDIR /code

# bcs of the switch to uv --> copy dependencies info (./ means to current dir)
# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# copying the script to container, 1st name is source file, 2nd is destination (. = pwd)
COPY pipeline.py .

# # i want it to run the pipeline.py script --> ENTRYPOINT
# ENTRYPOINT ["python", "pipeline.py"]

# switching to different entry points to account for uv
ENTRYPOINT ["uv", "run", "python", "pipeline.py"]