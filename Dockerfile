FROM python:3.12-slim
ARG CODE_REVISION=unknown
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 CODE_REVISION=${CODE_REVISION}
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt && useradd --create-home runner && chown runner:runner /app
COPY --chown=runner:runner src ./src
COPY --chown=runner:runner etl ./etl
COPY --chown=runner:runner infra/coverage ./infra/coverage
USER runner
ENTRYPOINT ["python", "-m", "etl.cloud"]
CMD ["refresh"]
