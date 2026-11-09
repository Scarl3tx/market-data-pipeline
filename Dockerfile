
FROM public.ecr.aws/lambda/python:3.13

WORKDIR ${LAMBDA_TASK_ROOT}

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY configs ./configs

RUN pip install --no-cache-dir uv \
    && uv sync --locked --no-dev

# Make both the source package and its virtual-environment
# dependencies visible to the Lambda Python runtime.
ENV PYTHONPATH="/var/task/src:/var/task/.venv/lib/python3.13/site-packages"
ENV PATH="/var/task/.venv/bin:${PATH}"
ENV MARKET_DATA_STORAGE_BACKEND=s3

CMD ["market_data.lambda_handler.lambda_handler"]
